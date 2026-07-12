//! Fast-by-default replication planning and direct bounded packet encoding.

use sectorsync_core::{
    entity::EntityRecord,
    ids::{EntityHandle, StationId, Tick},
    interest::{ViewerQuery, VisibilityFilter},
    policy::PolicyTable,
    replication::{
        ReplicationBatchScratch, ReplicationBudget, ReplicationPlan, ReplicationPlanner,
        ReplicationScratch, ReplicationSelectionMode,
    },
};
use sectorsync_transport::{OutboundPacket, TransportSink};
use sectorsync_wire::{
    BinaryEncodeError, ComponentSelection, ReplicationFrameBuildStats, ReplicationFrameBuilder,
};

#[cfg(feature = "parallel")]
use sectorsync_runtime::{
    ParallelReplicationScratch, ReplicationThreadPool, ReplicationThreadPoolBuildError,
    ReplicationThreadPoolConfig, StationReplicationBatchSource,
};

use crate::station::StationRuntime;

/// Caller-owned candidate eligibility used before replication budget consumption.
pub trait CandidateEligibility {
    /// Returns whether one visible candidate may consume replication budget.
    fn allows(
        &self,
        station_id: StationId,
        viewer: &ViewerQuery,
        handle: EntityHandle,
        entity: &EntityRecord,
    ) -> bool;
}

impl<F> CandidateEligibility for F
where
    F: Fn(StationId, &ViewerQuery, EntityHandle, &EntityRecord) -> bool,
{
    fn allows(
        &self,
        station_id: StationId,
        viewer: &ViewerQuery,
        handle: EntityHandle,
        entity: &EntityRecord,
    ) -> bool {
        self(station_id, viewer, handle, entity)
    }
}

/// Eligibility that admits every visible entity.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllEligible;

impl CandidateEligibility for AllEligible {
    fn allows(
        &self,
        _station_id: StationId,
        _viewer: &ViewerQuery,
        _handle: EntityHandle,
        _entity: &EntityRecord,
    ) -> bool {
        true
    }
}

/// Caller-owned per-viewer send history used for cadence decisions.
pub trait LastSentLookup {
    /// Returns the last tick at which `viewer` received `handle`.
    fn last_sent(
        &self,
        station_id: StationId,
        viewer: &ViewerQuery,
        handle: EntityHandle,
    ) -> Option<Tick>;
}

impl<F> LastSentLookup for F
where
    F: Fn(StationId, &ViewerQuery, EntityHandle) -> Option<Tick>,
{
    fn last_sent(
        &self,
        station_id: StationId,
        viewer: &ViewerQuery,
        handle: EntityHandle,
    ) -> Option<Tick> {
        self(station_id, viewer, handle)
    }
}

/// Empty send history that admits the first candidate send.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLastSent;

impl LastSentLookup for NoLastSent {
    fn last_sent(
        &self,
        _station_id: StationId,
        _viewer: &ViewerQuery,
        _handle: EntityHandle,
    ) -> Option<Tick> {
        None
    }
}

/// Configuration for one reusable replication executor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationExecutorConfig {
    /// Selection semantics fixed for this executor.
    pub mode: ReplicationSelectionMode,
    /// Entity and estimated-byte planning bounds.
    pub budget: ReplicationBudget,
    /// Whether a valid empty frame should still be sent.
    pub send_empty_frames: bool,
    /// Maximum repeated quantized AOI ranges cached within one batch call.
    pub max_cached_query_ranges: usize,
}

impl ReplicationExecutorConfig {
    /// Creates a deterministic first-fit, work-bounded configuration.
    pub const fn throughput(budget: ReplicationBudget) -> Self {
        Self {
            mode: ReplicationSelectionMode::Throughput,
            budget,
            send_empty_frames: false,
            max_cached_query_ranges: 64,
        }
    }

    /// Creates a deterministic global-priority configuration.
    pub const fn prioritized(budget: ReplicationBudget) -> Self {
        Self {
            mode: ReplicationSelectionMode::Prioritized,
            budget,
            send_empty_frames: false,
            max_cached_query_ranges: 64,
        }
    }

    /// Configures whether empty replication frames are transmitted.
    #[must_use]
    pub const fn with_empty_frames(mut self, send: bool) -> Self {
        self.send_empty_frames = send;
        self
    }

    /// Bounds within-call candidate reuse for repeated quantized AOI ranges.
    #[must_use]
    pub const fn with_cached_query_ranges(mut self, max_ranges: usize) -> Self {
        self.max_cached_query_ranges = max_ranges;
        self
    }
}

impl Default for ReplicationExecutorConfig {
    fn default() -> Self {
        Self::throughput(ReplicationBudget::default())
    }
}

/// Inputs for one viewer replication operation.
pub struct ReplicationRequest<'a, F, E = AllEligible, L = NoLastSent> {
    station: &'a StationRuntime,
    policies: &'a PolicyTable,
    selection: &'a ComponentSelection,
    viewer: &'a ViewerQuery,
    visibility: &'a F,
    eligibility: E,
    last_sent: L,
}

impl<'a, F> ReplicationRequest<'a, F> {
    /// Creates a request that admits every visible candidate with no send history.
    pub const fn new(
        station: &'a StationRuntime,
        policies: &'a PolicyTable,
        selection: &'a ComponentSelection,
        viewer: &'a ViewerQuery,
        visibility: &'a F,
    ) -> Self {
        Self {
            station,
            policies,
            selection,
            viewer,
            visibility,
            eligibility: AllEligible,
            last_sent: NoLastSent,
        }
    }
}

impl<'a, F, E, L> ReplicationRequest<'a, F, E, L> {
    /// Replaces candidate eligibility with caller-owned dirty or delivery state.
    pub fn with_eligibility<N>(self, eligibility: N) -> ReplicationRequest<'a, F, N, L> {
        ReplicationRequest {
            station: self.station,
            policies: self.policies,
            selection: self.selection,
            viewer: self.viewer,
            visibility: self.visibility,
            eligibility,
            last_sent: self.last_sent,
        }
    }

    /// Replaces cadence history with caller-owned per-viewer send state.
    pub fn with_last_sent<N>(self, last_sent: N) -> ReplicationRequest<'a, F, E, N> {
        ReplicationRequest {
            station: self.station,
            policies: self.policies,
            selection: self.selection,
            viewer: self.viewer,
            visibility: self.visibility,
            eligibility: self.eligibility,
            last_sent,
        }
    }
}

/// Inputs for an ordered viewer batch replication operation.
pub struct ReplicationBatchRequest<'a, F, E = AllEligible, L = NoLastSent> {
    station: &'a StationRuntime,
    policies: &'a PolicyTable,
    selection: &'a ComponentSelection,
    viewers: &'a [ViewerQuery],
    visibility: &'a F,
    eligibility: E,
    last_sent: L,
}

impl<'a, F> ReplicationBatchRequest<'a, F> {
    /// Creates a batch request with no caller eligibility or send history.
    pub const fn new(
        station: &'a StationRuntime,
        policies: &'a PolicyTable,
        selection: &'a ComponentSelection,
        viewers: &'a [ViewerQuery],
        visibility: &'a F,
    ) -> Self {
        Self {
            station,
            policies,
            selection,
            viewers,
            visibility,
            eligibility: AllEligible,
            last_sent: NoLastSent,
        }
    }
}

impl<'a, F, E, L> ReplicationBatchRequest<'a, F, E, L> {
    /// Replaces candidate eligibility for every viewer in the batch.
    pub fn with_eligibility<N>(self, eligibility: N) -> ReplicationBatchRequest<'a, F, N, L> {
        ReplicationBatchRequest {
            station: self.station,
            policies: self.policies,
            selection: self.selection,
            viewers: self.viewers,
            visibility: self.visibility,
            eligibility,
            last_sent: self.last_sent,
        }
    }

    /// Replaces per-viewer cadence history for the batch.
    pub fn with_last_sent<N>(self, last_sent: N) -> ReplicationBatchRequest<'a, F, E, N> {
        ReplicationBatchRequest {
            station: self.station,
            policies: self.policies,
            selection: self.selection,
            viewers: self.viewers,
            visibility: self.visibility,
            eligibility: self.eligibility,
            last_sent,
        }
    }
}

/// Accumulated product-path replication statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationExecutorStats {
    /// Viewers planned.
    pub viewers_planned: usize,
    /// Empty frames intentionally skipped.
    pub frames_skipped_empty: usize,
    /// Frames submitted successfully.
    pub frames_sent: usize,
    /// Encoded bytes submitted successfully.
    pub bytes_sent: usize,
    /// Entities selected by planning.
    pub entities_selected: usize,
    /// Entity deltas encoded.
    pub entities_encoded: usize,
    /// Component deltas encoded.
    pub components_encoded: usize,
    /// Entities rolled back at the concrete frame byte limit.
    pub entities_skipped_by_frame_bytes: usize,
    /// Maximum retained single-plan entity capacity.
    pub plan_entity_capacity_max: usize,
    /// Maximum retained batch plan slots.
    pub batch_plan_slots_max: usize,
    /// Maximum total entity capacity retained by batch plans.
    pub batch_entity_capacity_max: usize,
    /// Distinct quantized AOI ranges observed across batch calls.
    pub unique_query_ranges: usize,
    /// Viewer queries served from within-call candidate reuse.
    pub reused_query_ranges: usize,
    /// Grid cells probed by configured batch planning.
    pub grid_cells_probed: usize,
    /// Occupied cells scanned by configured batch planning.
    pub occupied_cells_scanned: usize,
    /// Maximum retained repeated-range cache entries.
    pub query_cache_slots_max: usize,
    /// Maximum candidate capacity retained across query cache entries.
    pub query_cache_candidate_capacity_max: usize,
}

/// Result of one viewer replication operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationReport {
    /// Candidate entities selected by planning.
    pub selected_entities: usize,
    /// Entity deltas encoded into the frame.
    pub encoded_entities: usize,
    /// Component deltas encoded into the frame.
    pub encoded_components: usize,
    /// Candidates skipped by the planner budget.
    pub skipped_by_budget: usize,
    /// Candidates not examined after a first-fit budget filled.
    pub unexamined_after_budget: usize,
    /// Candidates skipped because cadence had not elapsed.
    pub skipped_by_cadence: usize,
    /// Entities rolled back at the concrete frame byte limit.
    pub skipped_by_frame_bytes: usize,
    /// Encoded wire bytes submitted to transport.
    pub bytes_sent: usize,
    /// Whether a packet was sent.
    pub sent: bool,
}

/// Aggregate result of one ordered viewer batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationBatchReport {
    /// Viewers completed before return.
    pub viewers_completed: usize,
    /// Frames submitted successfully.
    pub frames_sent: usize,
    /// Selected entities across completed viewers.
    pub selected_entities: usize,
    /// Encoded entities across completed viewers.
    pub encoded_entities: usize,
    /// Encoded components across completed viewers.
    pub encoded_components: usize,
    /// Encoded wire bytes submitted successfully.
    pub bytes_sent: usize,
    /// Distinct quantized AOI ranges in this batch.
    pub unique_query_ranges: usize,
    /// Viewer queries served from within-call candidate reuse.
    pub reused_query_ranges: usize,
    /// Grid cells probed while producing this batch.
    pub grid_cells_probed: usize,
    /// Occupied cells scanned while producing this batch.
    pub occupied_cells_scanned: usize,
}

impl ReplicationBatchReport {
    fn record(&mut self, report: ReplicationReport) {
        self.viewers_completed = self.viewers_completed.saturating_add(1);
        self.frames_sent = self.frames_sent.saturating_add(usize::from(report.sent));
        self.selected_entities = self
            .selected_entities
            .saturating_add(report.selected_entities);
        self.encoded_entities = self
            .encoded_entities
            .saturating_add(report.encoded_entities);
        self.encoded_components = self
            .encoded_components
            .saturating_add(report.encoded_components);
        self.bytes_sent = self.bytes_sent.saturating_add(report.bytes_sent);
    }
}

fn record_batch_query_stats(
    stats: &mut ReplicationExecutorStats,
    batch: sectorsync_core::replication::ReplicationBatchStats,
) {
    stats.unique_query_ranges = stats
        .unique_query_ranges
        .saturating_add(batch.unique_query_ranges);
    stats.reused_query_ranges = stats
        .reused_query_ranges
        .saturating_add(batch.reused_query_ranges);
    stats.grid_cells_probed = stats
        .grid_cells_probed
        .saturating_add(batch.grid_cells_probed);
    stats.occupied_cells_scanned = stats
        .occupied_cells_scanned
        .saturating_add(batch.occupied_cells_scanned);
    stats.query_cache_slots_max = stats
        .query_cache_slots_max
        .max(batch.query_cache_capacity_max);
    stats.query_cache_candidate_capacity_max = stats
        .query_cache_candidate_capacity_max
        .max(batch.query_cache_candidate_capacity_max);
}

fn batch_report_from_stats(
    stats: sectorsync_core::replication::ReplicationBatchStats,
) -> ReplicationBatchReport {
    ReplicationBatchReport {
        unique_query_ranges: stats.unique_query_ranges,
        reused_query_ranges: stats.reused_query_ranges,
        grid_cells_probed: stats.grid_cells_probed,
        occupied_cells_scanned: stats.occupied_cells_scanned,
        ..ReplicationBatchReport::default()
    }
}

/// Failure kind produced by replication execution.
#[derive(Debug)]
pub enum ReplicationExecutionFailure<E> {
    /// Direct frame encoding failed before packet submission.
    Encode(BinaryEncodeError),
    /// The packet sink rejected the encoded packet.
    Transport(E),
}

/// Replication failure with explicit ordered-batch progress.
#[derive(Debug)]
pub struct ReplicationExecutionError<E> {
    /// Viewers completed before this failure.
    pub completed_viewers: usize,
    /// Encoding or transport failure.
    pub failure: ReplicationExecutionFailure<E>,
}

impl<E: core::fmt::Display> core::fmt::Display for ReplicationExecutionError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.failure {
            ReplicationExecutionFailure::Encode(error) => write!(
                f,
                "replication encoding failed after {} viewers: {error}",
                self.completed_viewers
            ),
            ReplicationExecutionFailure::Transport(error) => write!(
                f,
                "replication transport failed after {} viewers: {error}",
                self.completed_viewers
            ),
        }
    }
}

impl<E> std::error::Error for ReplicationExecutionError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.failure {
            ReplicationExecutionFailure::Encode(error) => Some(error),
            ReplicationExecutionFailure::Transport(error) => Some(error),
        }
    }
}

/// Reusable fast-by-default replication planner and packet encoder.
#[derive(Clone, Debug)]
pub struct ReplicationExecutor {
    config: ReplicationExecutorConfig,
    builder: ReplicationFrameBuilder,
    scratch: ReplicationScratch,
    plan: ReplicationPlan,
    batch: ReplicationBatchScratch,
    stats: ReplicationExecutorStats,
}

impl ReplicationExecutor {
    /// Creates an executor with explicit selection and frame limits.
    pub fn new(config: ReplicationExecutorConfig, builder: ReplicationFrameBuilder) -> Self {
        Self {
            config,
            builder,
            scratch: ReplicationScratch::default(),
            plan: ReplicationPlan::default(),
            batch: ReplicationBatchScratch::default(),
            stats: ReplicationExecutorStats::default(),
        }
    }

    /// Returns executor configuration.
    pub const fn config(&self) -> ReplicationExecutorConfig {
        self.config
    }

    /// Returns accumulated execution and retained-capacity statistics.
    pub const fn stats(&self) -> ReplicationExecutorStats {
        self.stats
    }

    /// Plans, directly encodes, and sends one viewer frame.
    pub fn replicate<T, F, E, L>(
        &mut self,
        request: ReplicationRequest<'_, F, E, L>,
        transport: &mut T,
    ) -> Result<ReplicationReport, ReplicationExecutionError<T::Error>>
    where
        T: TransportSink,
        F: VisibilityFilter,
        E: CandidateEligibility,
        L: LastSentLookup,
    {
        let ReplicationRequest {
            station,
            policies,
            selection,
            viewer,
            visibility,
            eligibility,
            last_sent,
        } = request;
        let station_id = station.station().config().station_id;
        ReplicationPlanner::plan_for_viewer_configured_into(
            station.station(),
            station.index(),
            policies,
            viewer,
            visibility,
            self.config.budget,
            self.config.mode,
            |viewer, handle, entity| eligibility.allows(station_id, viewer, handle, entity),
            |viewer, handle| last_sent.last_sent(station_id, viewer, handle),
            &mut self.scratch,
            &mut self.plan,
        );
        self.stats.plan_entity_capacity_max = self
            .stats
            .plan_entity_capacity_max
            .max(self.plan.entities.capacity());
        send_plan(
            self.config,
            self.builder,
            &mut self.stats,
            station,
            selection,
            viewer,
            &self.plan,
            transport,
        )
        .map_err(|failure| ReplicationExecutionError {
            completed_viewers: 0,
            failure,
        })
    }

    /// Plans and sends viewers in input order with reusable batch output.
    pub fn replicate_batch<T, F, E, L>(
        &mut self,
        request: ReplicationBatchRequest<'_, F, E, L>,
        transport: &mut T,
    ) -> Result<ReplicationBatchReport, ReplicationExecutionError<T::Error>>
    where
        T: TransportSink,
        F: VisibilityFilter,
        E: CandidateEligibility,
        L: LastSentLookup,
    {
        let ReplicationBatchRequest {
            station,
            policies,
            selection,
            viewers,
            visibility,
            eligibility,
            last_sent,
        } = request;
        let station_id = station.station().config().station_id;
        let view = ReplicationPlanner::plan_for_viewers_configured_into(
            station.station(),
            station.index(),
            policies,
            viewers,
            visibility,
            self.config.budget,
            self.config.mode,
            self.config.max_cached_query_ranges,
            |viewer, handle, entity| eligibility.allows(station_id, viewer, handle, entity),
            |viewer, handle| last_sent.last_sent(station_id, viewer, handle),
            &mut self.scratch,
            &mut self.batch,
        );
        self.stats.batch_plan_slots_max = self.stats.batch_plan_slots_max.max(view.plans.len());
        self.stats.batch_entity_capacity_max = self
            .stats
            .batch_entity_capacity_max
            .max(view.plans.iter().map(|plan| plan.entities.capacity()).sum());
        record_batch_query_stats(&mut self.stats, view.stats);

        let mut report = batch_report_from_stats(view.stats);
        for (viewer, plan) in viewers.iter().zip(view.plans) {
            match send_plan(
                self.config,
                self.builder,
                &mut self.stats,
                station,
                selection,
                viewer,
                plan,
                transport,
            ) {
                Ok(viewer_report) => report.record(viewer_report),
                Err(failure) => {
                    return Err(ReplicationExecutionError {
                        completed_viewers: report.viewers_completed,
                        failure,
                    });
                }
            }
        }
        Ok(report)
    }
}

impl Default for ReplicationExecutor {
    fn default() -> Self {
        Self::new(
            ReplicationExecutorConfig::default(),
            ReplicationFrameBuilder::default(),
        )
    }
}

/// One Station-local input batch for explicit parallel planning.
#[cfg(feature = "parallel")]
#[derive(Clone, Copy, Debug)]
pub struct ParallelStationBatch<'a> {
    /// Coherent Station-local product state.
    pub station: &'a StationRuntime,
    /// Ordered viewers for this Station.
    pub viewers: &'a [ViewerQuery],
    /// Component ids encoded for selected entities.
    pub selection: &'a ComponentSelection,
}

#[cfg(feature = "parallel")]
impl StationReplicationBatchSource for ParallelStationBatch<'_> {
    fn station(&self) -> &sectorsync_core::station::Station {
        self.station.station()
    }

    fn index(&self) -> &sectorsync_core::spatial_index::CellIndex {
        self.station.index()
    }

    fn viewers(&self) -> &[ViewerQuery] {
        self.viewers
    }
}

/// Inputs shared by explicit parallel Station batches.
#[cfg(feature = "parallel")]
pub struct ParallelReplicationRequest<'a, F, E = AllEligible, L = NoLastSent> {
    batches: &'a [ParallelStationBatch<'a>],
    policies: &'a PolicyTable,
    visibility: &'a F,
    eligibility: E,
    last_sent: L,
}

#[cfg(feature = "parallel")]
impl<'a, F> ParallelReplicationRequest<'a, F> {
    /// Creates a parallel request with no eligibility or cadence state.
    pub const fn new(
        batches: &'a [ParallelStationBatch<'a>],
        policies: &'a PolicyTable,
        visibility: &'a F,
    ) -> Self {
        Self {
            batches,
            policies,
            visibility,
            eligibility: AllEligible,
            last_sent: NoLastSent,
        }
    }
}

#[cfg(feature = "parallel")]
impl<'a, F, E, L> ParallelReplicationRequest<'a, F, E, L> {
    /// Replaces candidate eligibility for every Station batch.
    pub fn with_eligibility<N>(self, eligibility: N) -> ParallelReplicationRequest<'a, F, N, L> {
        ParallelReplicationRequest {
            batches: self.batches,
            policies: self.policies,
            visibility: self.visibility,
            eligibility,
            last_sent: self.last_sent,
        }
    }

    /// Replaces Station-aware per-viewer cadence history.
    pub fn with_last_sent<N>(self, last_sent: N) -> ParallelReplicationRequest<'a, F, E, N> {
        ParallelReplicationRequest {
            batches: self.batches,
            policies: self.policies,
            visibility: self.visibility,
            eligibility: self.eligibility,
            last_sent,
        }
    }
}

/// Explicit bounded-pool replication executor.
#[cfg(feature = "parallel")]
#[derive(Debug)]
pub struct ParallelReplicationExecutor {
    config: ReplicationExecutorConfig,
    builder: ReplicationFrameBuilder,
    pool: ReplicationThreadPool,
    scratch: ParallelReplicationScratch,
    stats: ReplicationExecutorStats,
}

#[cfg(feature = "parallel")]
impl ParallelReplicationExecutor {
    /// Creates the only threads owned by this executor.
    pub fn new(
        config: ReplicationExecutorConfig,
        builder: ReplicationFrameBuilder,
        pool: ReplicationThreadPoolConfig,
    ) -> Result<Self, ReplicationThreadPoolBuildError> {
        Ok(Self {
            config,
            builder,
            pool: ReplicationThreadPool::new(pool)?,
            scratch: ParallelReplicationScratch::default(),
            stats: ReplicationExecutorStats::default(),
        })
    }

    /// Number of explicitly created worker threads.
    pub const fn threads(&self) -> usize {
        self.pool.threads()
    }

    /// Returns accumulated execution and retained-capacity statistics.
    pub const fn stats(&self) -> ReplicationExecutorStats {
        self.stats
    }

    /// Plans Station batches in parallel, then sends packets in input order.
    pub fn replicate_batch<T, F, E, L>(
        &mut self,
        request: ParallelReplicationRequest<'_, F, E, L>,
        transport: &mut T,
    ) -> Result<ReplicationBatchReport, ReplicationExecutionError<T::Error>>
    where
        T: TransportSink,
        F: VisibilityFilter + Sync,
        E: CandidateEligibility + Sync,
        L: LastSentLookup + Sync,
    {
        let ParallelReplicationRequest {
            batches,
            policies,
            visibility,
            eligibility,
            last_sent,
        } = request;
        let view = self.pool.plan_station_configured_batches_into(
            batches,
            policies,
            visibility,
            self.config.budget,
            self.config.mode,
            self.config.max_cached_query_ranges,
            |station_id, viewer, handle, entity| {
                eligibility.allows(station_id, viewer, handle, entity)
            },
            |station_id, viewer, handle| last_sent.last_sent(station_id, viewer, handle),
            &mut self.scratch,
        );
        self.stats.batch_plan_slots_max = self.stats.batch_plan_slots_max.max(view.batches.len());
        self.stats.batch_entity_capacity_max = self.stats.batch_entity_capacity_max.max(
            view.batches
                .iter()
                .map(ReplicationBatchScratch::retained_entity_capacity)
                .sum(),
        );

        let mut report = ReplicationBatchReport::default();
        for (batch, output) in batches.iter().zip(view.batches) {
            let output_view = output.view();
            record_batch_query_stats(&mut self.stats, output_view.stats);
            report.unique_query_ranges = report
                .unique_query_ranges
                .saturating_add(output_view.stats.unique_query_ranges);
            report.reused_query_ranges = report
                .reused_query_ranges
                .saturating_add(output_view.stats.reused_query_ranges);
            report.grid_cells_probed = report
                .grid_cells_probed
                .saturating_add(output_view.stats.grid_cells_probed);
            report.occupied_cells_scanned = report
                .occupied_cells_scanned
                .saturating_add(output_view.stats.occupied_cells_scanned);
            for (viewer, plan) in batch.viewers.iter().zip(output_view.plans) {
                match send_plan(
                    self.config,
                    self.builder,
                    &mut self.stats,
                    batch.station,
                    batch.selection,
                    viewer,
                    plan,
                    transport,
                ) {
                    Ok(viewer_report) => report.record(viewer_report),
                    Err(failure) => {
                        return Err(ReplicationExecutionError {
                            completed_viewers: report.viewers_completed,
                            failure,
                        });
                    }
                }
            }
        }
        Ok(report)
    }
}

#[allow(clippy::too_many_arguments)]
fn send_plan<T: TransportSink>(
    config: ReplicationExecutorConfig,
    builder: ReplicationFrameBuilder,
    stats: &mut ReplicationExecutorStats,
    station: &StationRuntime,
    selection: &ComponentSelection,
    viewer: &ViewerQuery,
    plan: &ReplicationPlan,
    transport: &mut T,
) -> Result<ReplicationReport, ReplicationExecutionFailure<T::Error>> {
    stats.viewers_planned = stats.viewers_planned.saturating_add(1);
    stats.entities_selected = stats.entities_selected.saturating_add(plan.stats.selected);

    let capacity_hint = builder
        .sampled_binary_capacity_hint(station.station(), plan, station.components(), selection)
        .min(config.budget.max_bytes);
    let mut bytes = Vec::with_capacity(capacity_hint);
    let build = builder
        .encode_binary_bounded_into(
            viewer.client_id,
            station.station().tick(),
            station.station(),
            plan,
            station.components(),
            selection,
            config.budget.max_bytes,
            &mut bytes,
        )
        .map_err(ReplicationExecutionFailure::Encode)?;
    record_build_stats(stats, build);

    if build.encoded_entities == 0 && !config.send_empty_frames {
        stats.frames_skipped_empty = stats.frames_skipped_empty.saturating_add(1);
        return Ok(replication_report(plan, build, 0, false));
    }

    let bytes_sent = bytes.len();
    transport
        .send(OutboundPacket {
            client_id: viewer.client_id,
            bytes,
        })
        .map_err(ReplicationExecutionFailure::Transport)?;
    stats.frames_sent = stats.frames_sent.saturating_add(1);
    stats.bytes_sent = stats.bytes_sent.saturating_add(bytes_sent);
    Ok(replication_report(plan, build, bytes_sent, true))
}

fn record_build_stats(stats: &mut ReplicationExecutorStats, build: ReplicationFrameBuildStats) {
    stats.entities_encoded = stats
        .entities_encoded
        .saturating_add(build.encoded_entities);
    stats.components_encoded = stats
        .components_encoded
        .saturating_add(build.encoded_components);
    stats.entities_skipped_by_frame_bytes = stats
        .entities_skipped_by_frame_bytes
        .saturating_add(build.skipped_entities_by_frame_bytes);
}

fn replication_report(
    plan: &ReplicationPlan,
    build: ReplicationFrameBuildStats,
    bytes_sent: usize,
    sent: bool,
) -> ReplicationReport {
    ReplicationReport {
        selected_entities: plan.stats.selected,
        encoded_entities: build.encoded_entities,
        encoded_components: build.encoded_components,
        skipped_by_budget: plan.stats.skipped_by_budget,
        unexamined_after_budget: plan.stats.unexamined_after_budget,
        skipped_by_cadence: plan.stats.skipped_by_cadence,
        skipped_by_frame_bytes: build.skipped_entities_by_frame_bytes,
        bytes_sent,
        sent,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        convert::Infallible,
        net::{Ipv4Addr, SocketAddr},
    };

    use sectorsync_core::{
        component::{ComponentDescriptor, ComponentMigrationMode, ComponentSyncMode},
        ids::{ClientId, ComponentId, EntityId, InstanceId, NodeId, PolicyId, StationId},
        interest::RangeOnlyVisibility,
        policy::CompiledSyncPolicy,
        spatial::{Bounds, GridSpec, Position3},
        station::StationConfig,
    };
    use sectorsync_transport::{InboundPacket, TransportReceiver};

    use crate::{
        client::{ReceiveExecutor, ReceiveExecutorConfig},
        station::{SpawnEntity, StationRuntimeConfig},
    };

    use super::*;

    #[derive(Debug)]
    struct Loopback {
        source: ClientId,
        packets: VecDeque<InboundPacket>,
    }

    impl Loopback {
        fn new(source: ClientId) -> Self {
            Self {
                source,
                packets: VecDeque::new(),
            }
        }
    }

    impl TransportSink for Loopback {
        type Error = Infallible;

        fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
            self.packets.push_back(InboundPacket {
                client_id: Some(self.source),
                remote_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 30000)),
                bytes: packet.bytes,
            });
            Ok(())
        }
    }

    impl TransportReceiver for Loopback {
        type Error = Infallible;

        fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
            Ok(self.packets.pop_front())
        }
    }

    fn runtime_with_component_for_station(
        station_id: u32,
    ) -> (StationRuntime, PolicyTable, ComponentSelection) {
        let mut runtime = StationRuntime::new(StationRuntimeConfig::new(
            StationConfig {
                station_id: StationId::new(station_id),
                node_id: NodeId::new(1),
                instance_id: InstanceId::new(1),
                tick_rate_hz: 20,
            },
            GridSpec::new(16.0).expect("grid"),
        ));
        let handle = runtime
            .spawn_owned(SpawnEntity::new(
                EntityId::new(1),
                Position3::new(1.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            ))
            .expect("spawn");
        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "health",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            4,
        );
        runtime
            .set_component_blob(&descriptor, handle, 1, &[1, 2, 3, 4])
            .expect("component");
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 20, 20, 128.0));
        let selection = ComponentSelection {
            component_ids: vec![ComponentId::new(1)],
        };
        (runtime, policies, selection)
    }

    fn runtime_with_component() -> (StationRuntime, PolicyTable, ComponentSelection) {
        runtime_with_component_for_station(1)
    }

    fn viewer(client_id: ClientId) -> ViewerQuery {
        ViewerQuery {
            client_id,
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 16,
        }
    }

    #[test]
    fn product_executor_directly_encodes_and_visits_borrowed_frames() {
        let (runtime, policies, selection) = runtime_with_component();
        let client = ClientId::new(7);
        let server = ClientId::new(99);
        let viewer = viewer(client);
        let mut loopback = Loopback::new(server);
        let mut executor = ReplicationExecutor::new(
            ReplicationExecutorConfig::throughput(ReplicationBudget {
                max_entities: 16,
                max_bytes: 4096,
                estimated_entity_bytes: 32,
            }),
            ReplicationFrameBuilder::default(),
        );

        let sent = executor
            .replicate(
                ReplicationRequest::new(
                    &runtime,
                    &policies,
                    &selection,
                    &viewer,
                    &RangeOnlyVisibility,
                ),
                &mut loopback,
            )
            .expect("replication send");
        assert_eq!(sent.selected_entities, 1);
        assert_eq!(sent.encoded_entities, 1);
        assert!(sent.sent);

        let mut received_entities = 0;
        let mut receiver =
            ReceiveExecutor::new(ReceiveExecutorConfig::new(client).with_expected_source(server));
        let receive = receiver
            .pump(&mut loopback, 4, |frame| {
                received_entities += frame.encoded_entity_count();
                Ok::<_, Infallible>(())
            })
            .expect("borrowed receive");
        assert_eq!(receive.frames_received, 1);
        assert_eq!(received_entities, 1);
    }

    #[test]
    fn product_batch_reuses_outputs_and_reports_partial_transport_failure() {
        #[derive(Debug)]
        struct FailSecond {
            sends: usize,
        }

        impl TransportSink for FailSecond {
            type Error = &'static str;

            fn send(&mut self, _packet: OutboundPacket) -> Result<(), Self::Error> {
                self.sends += 1;
                if self.sends == 2 {
                    Err("second send")
                } else {
                    Ok(())
                }
            }
        }

        let (runtime, policies, selection) = runtime_with_component();
        let viewers = [viewer(ClientId::new(7)), viewer(ClientId::new(8))];
        let mut executor = ReplicationExecutor::default();
        let mut transport = FailSecond { sends: 0 };
        let error = executor
            .replicate_batch(
                ReplicationBatchRequest::new(
                    &runtime,
                    &policies,
                    &selection,
                    &viewers,
                    &RangeOnlyVisibility,
                ),
                &mut transport,
            )
            .expect_err("second send should fail");

        assert_eq!(error.completed_viewers, 1);
        assert!(matches!(
            error.failure,
            ReplicationExecutionFailure::Transport("second send")
        ));
        assert!(executor.stats().batch_plan_slots_max >= 2);
        assert!(executor.stats().batch_entity_capacity_max >= 2);
        assert_eq!(executor.stats().unique_query_ranges, 1);
        assert_eq!(executor.stats().reused_query_ranges, 1);
        assert_eq!(executor.stats().query_cache_slots_max, 1);
    }

    #[test]
    fn product_request_combines_eligibility_and_cadence() {
        let (mut runtime, policies, selection) = runtime_with_component();
        runtime.advance_tick();
        let viewer = viewer(ClientId::new(7));
        let mut executor = ReplicationExecutor::default();
        let mut transport = Loopback::new(ClientId::new(99));
        let skipped = executor
            .replicate(
                ReplicationRequest::new(
                    &runtime,
                    &policies,
                    &selection,
                    &viewer,
                    &RangeOnlyVisibility,
                )
                .with_eligibility(
                    |_: StationId, _: &ViewerQuery, _: EntityHandle, _: &EntityRecord| true,
                )
                .with_last_sent(
                    |_: StationId, _: &ViewerQuery, _: EntityHandle| Some(Tick::new(1)),
                ),
                &mut transport,
            )
            .expect("cadence result");

        assert_eq!(skipped.selected_entities, 0);
        assert_eq!(skipped.skipped_by_cadence, 1);
        assert!(!skipped.sent);
    }

    #[cfg(feature = "parallel")]
    #[test]
    fn explicit_parallel_executor_uses_station_aware_callbacks_and_ordered_send() {
        let (left, policies, left_selection) = runtime_with_component_for_station(1);
        let (right, _, right_selection) = runtime_with_component_for_station(2);
        let left_viewers = [viewer(ClientId::new(7))];
        let right_viewers = [viewer(ClientId::new(8))];
        let batches = [
            ParallelStationBatch {
                station: &left,
                viewers: &left_viewers,
                selection: &left_selection,
            },
            ParallelStationBatch {
                station: &right,
                viewers: &right_viewers,
                selection: &right_selection,
            },
        ];
        let mut executor = ParallelReplicationExecutor::new(
            ReplicationExecutorConfig::default(),
            ReplicationFrameBuilder::default(),
            ReplicationThreadPoolConfig::new(2, 2),
        )
        .expect("bounded pool");
        let mut transport = Loopback::new(ClientId::new(99));
        let report = executor
            .replicate_batch(
                ParallelReplicationRequest::new(&batches, &policies, &RangeOnlyVisibility)
                    .with_eligibility(
                        |station_id: StationId,
                         _: &ViewerQuery,
                         _: EntityHandle,
                         _: &EntityRecord| { station_id.get() <= 2 },
                    )
                    .with_last_sent(|_: StationId, _: &ViewerQuery, _: EntityHandle| None),
                &mut transport,
            )
            .expect("parallel replication");

        assert_eq!(
            executor.threads(),
            2.min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get))
        );
        assert_eq!(report.viewers_completed, 2);
        assert_eq!(report.frames_sent, 2);
        assert_eq!(report.selected_entities, 2);
        assert!(executor.stats().batch_plan_slots_max >= 2);
    }
}
