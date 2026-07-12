//! Explicit, bounded parallel replication planning.

use core::fmt;
#[cfg(test)]
use core::ops::Range;

use rayon::prelude::*;
use sectorsync_core::prelude::{
    CellIndex, PolicyTable, ReplicationBatchScratch, ReplicationBatchStats, ReplicationBudget,
    ReplicationPlanner, ReplicationScratch, Station, ViewerQuery, VisibilityFilter,
};
use sectorsync_core::{
    entity::EntityRecord,
    ids::{EntityHandle, StationId, Tick},
    replication::ReplicationSelectionMode,
};

/// Borrowed Station/viewer input accepted by parallel configured planning.
pub trait StationReplicationBatchSource: Sync {
    /// Station containing authoritative and ghost records.
    fn station(&self) -> &Station;
    /// Spatial index paired with [`Self::station`].
    fn index(&self) -> &CellIndex;
    /// Viewer queries retained in deterministic result order.
    fn viewers(&self) -> &[ViewerQuery];
}

/// Configuration for an explicitly created replication planning thread pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationThreadPoolConfig {
    /// Requested workers. Zero selects half of host logical parallelism.
    pub requested_threads: usize,
    /// Upper bound applied after host parallelism is detected.
    pub max_threads: usize,
}

impl Default for ReplicationThreadPoolConfig {
    fn default() -> Self {
        Self {
            requested_threads: 0,
            max_threads: 8,
        }
    }
}

impl ReplicationThreadPoolConfig {
    /// Creates an explicit bounded configuration.
    pub const fn new(requested_threads: usize, max_threads: usize) -> Self {
        Self {
            requested_threads,
            max_threads,
        }
    }

    fn resolve(self, available: usize) -> usize {
        let available = available.max(1);
        let requested = if self.requested_threads == 0 {
            available.div_ceil(2)
        } else {
            self.requested_threads
        };
        requested.clamp(1, self.max_threads.max(1).min(available))
    }
}

/// Error returned when the explicit worker pool cannot be created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationThreadPoolBuildError {
    message: String,
}

impl fmt::Display for ReplicationThreadPoolBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ReplicationThreadPoolBuildError {}

/// One immutable station-local viewer batch.
#[derive(Clone, Copy, Debug)]
pub struct StationReplicationBatch<'a> {
    /// Station containing the authoritative/ghost records used for planning.
    pub station: &'a Station,
    /// Spatial index paired with `station`.
    pub index: &'a CellIndex,
    /// Viewer queries retained in deterministic result order.
    pub viewers: &'a [ViewerQuery],
}

impl<'a> StationReplicationBatch<'a> {
    /// Creates a station-local batch without copying its inputs.
    pub const fn new(
        station: &'a Station,
        index: &'a CellIndex,
        viewers: &'a [ViewerQuery],
    ) -> Self {
        Self {
            station,
            index,
            viewers,
        }
    }
}

impl StationReplicationBatchSource for StationReplicationBatch<'_> {
    fn station(&self) -> &Station {
        self.station
    }

    fn index(&self) -> &CellIndex {
        self.index
    }

    fn viewers(&self) -> &[ViewerQuery] {
        self.viewers
    }
}

/// Caller-owned worker scratch lanes retained across parallel planning calls.
///
/// Lane count is bounded by the pool thread count rather than the number of
/// Station batches. Each lane processes a deterministic contiguous partition.
#[derive(Clone, Debug, Default)]
pub struct ParallelReplicationScratch {
    lanes: Vec<ReplicationScratch>,
    batches: Vec<ReplicationBatchScratch>,
    active_batches: usize,
    active_lanes: usize,
}

impl ParallelReplicationScratch {
    /// Creates empty scratch storage. Worker lanes are allocated on first use.
    pub const fn new() -> Self {
        Self {
            lanes: Vec::new(),
            batches: Vec::new(),
            active_batches: 0,
            active_lanes: 0,
        }
    }

    /// Number of worker scratch lanes currently retained.
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    /// Number of logical lane partitions used by the completed planning call.
    pub const fn active_lanes(&self) -> usize {
        self.active_lanes
    }

    /// Number of Station batch output slots retained for later calls.
    pub fn retained_batch_slots(&self) -> usize {
        self.batches.len()
    }

    /// Total selected-entity capacity retained by all Station batch outputs.
    pub fn retained_entity_capacity(&self) -> usize {
        self.batches
            .iter()
            .map(ReplicationBatchScratch::retained_entity_capacity)
            .sum()
    }

    fn prepare_lanes(&mut self, lanes: usize) {
        self.lanes.resize_with(lanes, ReplicationScratch::default);
    }

    fn prepare_output(&mut self, batches: usize) {
        if self.batches.len() < batches {
            self.batches
                .resize_with(batches, ReplicationBatchScratch::default);
        }
        self.active_batches = batches;
    }
}

#[cfg(test)]
fn partition_bounds(items: usize, partitions: usize, partition: usize) -> Range<usize> {
    let base = items / partitions;
    let remainder = items % partitions;
    let start = partition * base + partition.min(remainder);
    let len = base + usize::from(partition < remainder);
    start..start + len
}

fn chunked_logical_lanes(items: usize, retained_lanes: usize) -> usize {
    if retained_lanes == 0 {
        0
    } else {
        items.div_ceil(items.div_ceil(retained_lanes))
    }
}

/// Borrowed ordered Station results produced from reusable parallel storage.
#[derive(Clone, Copy, Debug)]
pub struct ParallelReplicationView<'a> {
    /// One reusable batch slot per input Station batch, retaining input order.
    pub batches: &'a [ReplicationBatchScratch],
    /// Aggregate statistics merged in Station input order.
    pub stats: ReplicationBatchStats,
}

/// Explicit Rayon pool used only when the embedding application constructs it.
pub struct ReplicationThreadPool {
    pool: rayon::ThreadPool,
    threads: usize,
}

impl fmt::Debug for ReplicationThreadPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplicationThreadPool")
            .field("threads", &self.threads)
            .finish_non_exhaustive()
    }
}

impl ReplicationThreadPool {
    /// Creates a bounded worker pool. No threads exist before this call.
    pub fn new(
        config: ReplicationThreadPoolConfig,
    ) -> Result<Self, ReplicationThreadPoolBuildError> {
        let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let threads = config.resolve(available);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("sectorsync-replication-{index}"))
            .build()
            .map_err(|error| ReplicationThreadPoolBuildError {
                message: error.to_string(),
            })?;
        Ok(Self { pool, threads })
    }

    /// Creates a conservative host-sized pool using [`ReplicationThreadPoolConfig::default`].
    pub fn for_host() -> Result<Self, ReplicationThreadPoolBuildError> {
        Self::new(ReplicationThreadPoolConfig::default())
    }

    /// Number of worker threads created in this pool.
    pub const fn threads(&self) -> usize {
        self.threads
    }

    /// Plans configured Station batches in deterministic worker partitions.
    ///
    /// Eligibility and cadence callbacks receive the Station id so identical
    /// Station-local handles cannot alias caller-owned state across batches.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_station_configured_batches_into<'a, B, F, E, L>(
        &self,
        batches: &[B],
        policies: &PolicyTable,
        filter: &F,
        budget: ReplicationBudget,
        mode: ReplicationSelectionMode,
        max_cached_query_ranges: usize,
        eligible: E,
        last_sent: L,
        scratch: &'a mut ParallelReplicationScratch,
    ) -> ParallelReplicationView<'a>
    where
        B: StationReplicationBatchSource,
        F: VisibilityFilter + Sync,
        E: Fn(StationId, &ViewerQuery, EntityHandle, &EntityRecord) -> bool + Sync,
        L: Fn(StationId, &ViewerQuery, EntityHandle) -> Option<Tick> + Sync,
    {
        let lanes = self.threads.min(batches.len());
        scratch.prepare_lanes(lanes);
        scratch.prepare_output(batches.len());
        scratch.active_lanes = chunked_logical_lanes(batches.len(), lanes);
        if lanes != 0 {
            let chunk_size = batches.len().div_ceil(lanes);
            self.pool.install(|| {
                batches
                    .par_chunks(chunk_size)
                    .zip(scratch.batches[..batches.len()].par_chunks_mut(chunk_size))
                    .zip(scratch.lanes[..lanes].par_iter_mut())
                    .for_each(|((input, output), lane)| {
                        for (batch, batch_output) in input.iter().zip(output) {
                            let station_id = batch.station().config().station_id;
                            ReplicationPlanner::plan_for_viewers_configured_into(
                                batch.station(),
                                batch.index(),
                                policies,
                                batch.viewers(),
                                filter,
                                budget,
                                mode,
                                max_cached_query_ranges,
                                |viewer, handle, entity| {
                                    eligible(station_id, viewer, handle, entity)
                                },
                                |viewer, handle| last_sent(station_id, viewer, handle),
                                lane,
                                batch_output,
                            );
                        }
                    });
            });
        }
        reusable_view(scratch)
    }

    /// Maps an owned batch synchronously and retains input order.
    ///
    /// Work runs only for the duration of this call. The pool does not retain
    /// inputs, outputs, jobs, or cross-client state after it returns.
    pub fn map_ordered<T, R, F>(&self, inputs: Vec<T>, operation: F) -> Vec<R>
    where
        T: Send,
        R: Send,
        F: Fn(T) -> R + Send + Sync,
    {
        self.pool
            .install(|| inputs.into_par_iter().map(operation).collect())
    }

    /// Plans generic visibility batches into caller-owned reusable Station outputs.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_station_batches_into<'a, F>(
        &self,
        batches: &[StationReplicationBatch<'_>],
        policies: &PolicyTable,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &'a mut ParallelReplicationScratch,
    ) -> ParallelReplicationView<'a>
    where
        F: VisibilityFilter + Sync,
    {
        let lanes = self.threads.min(batches.len());
        scratch.prepare_lanes(lanes);
        scratch.prepare_output(batches.len());
        scratch.active_lanes = chunked_logical_lanes(batches.len(), lanes);
        if lanes != 0 {
            let chunk_size = batches.len().div_ceil(lanes);
            self.pool.install(|| {
                batches
                    .par_chunks(chunk_size)
                    .zip(scratch.batches[..batches.len()].par_chunks_mut(chunk_size))
                    .zip(scratch.lanes[..lanes].par_iter_mut())
                    .for_each(|((input, output), lane)| {
                        for (batch, batch_output) in input.iter().zip(output) {
                            ReplicationPlanner::plan_for_viewers_into(
                                batch.station,
                                batch.index,
                                policies,
                                batch.viewers,
                                filter,
                                budget,
                                lane,
                                batch_output,
                            );
                        }
                    });
            });
        }
        reusable_view(scratch)
    }

    /// Plans range-only batches into caller-owned reusable Station outputs.
    pub fn plan_station_range_batches_into<'a>(
        &self,
        batches: &[StationReplicationBatch<'_>],
        policies: &PolicyTable,
        budget: ReplicationBudget,
        scratch: &'a mut ParallelReplicationScratch,
    ) -> ParallelReplicationView<'a> {
        let lanes = self.threads.min(batches.len());
        scratch.prepare_lanes(lanes);
        scratch.prepare_output(batches.len());
        scratch.active_lanes = chunked_logical_lanes(batches.len(), lanes);
        if lanes != 0 {
            let chunk_size = batches.len().div_ceil(lanes);
            self.pool.install(|| {
                batches
                    .par_chunks(chunk_size)
                    .zip(scratch.batches[..batches.len()].par_chunks_mut(chunk_size))
                    .zip(scratch.lanes[..lanes].par_iter_mut())
                    .for_each(|((input, output), lane)| {
                        for (batch, batch_output) in input.iter().zip(output) {
                            ReplicationPlanner::plan_for_viewers_range_into(
                                batch.station,
                                batch.index,
                                policies,
                                batch.viewers,
                                budget,
                                lane,
                                batch_output,
                            );
                        }
                    });
            });
        }
        reusable_view(scratch)
    }
}

fn reusable_view(scratch: &ParallelReplicationScratch) -> ParallelReplicationView<'_> {
    let batches = &scratch.batches[..scratch.active_batches];
    let mut stats = ReplicationBatchStats::default();
    for batch in batches {
        stats.merge(batch.view().stats);
    }
    ParallelReplicationView { batches, stats }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sectorsync_core::prelude::{
        Bounds, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId, PolicyId,
        Position3, RangeOnlyVisibility, StationConfig, StationId,
    };

    #[test]
    fn auto_thread_resolution_is_conservative_and_bounded() {
        assert_eq!(ReplicationThreadPoolConfig::default().resolve(12), 6);
        assert_eq!(ReplicationThreadPoolConfig::default().resolve(1), 1);
        assert_eq!(ReplicationThreadPoolConfig::new(64, 4).resolve(12), 4);
        assert_eq!(partition_bounds(10, 3, 0), 0..4);
        assert_eq!(partition_bounds(10, 3, 1), 4..7);
        assert_eq!(partition_bounds(10, 3, 2), 7..10);
        assert_eq!(chunked_logical_lanes(5, 4), 3);
    }

    #[test]
    fn parallel_station_batches_match_serial_order() {
        let mut stations = Vec::new();
        let mut indexes = Vec::new();
        let mut viewers = Vec::new();
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 128, 128.0));

        for station_index in 0_u32..6 {
            let mut station = Station::new(StationConfig {
                station_id: StationId::new(station_index),
                node_id: NodeId::new(1),
                instance_id: InstanceId::new(1),
                tick_rate_hz: 128,
            });
            let mut index = CellIndex::new(GridSpec::new(16.0).expect("valid grid"));
            for entity_index in 0_u16..16 {
                let position = Position3::new(f32::from(entity_index) * 4.0, 0.0, 0.0);
                let handle = station
                    .spawn_owned(
                        EntityId::new(u64::from(station_index) * 100 + u64::from(entity_index)),
                        position,
                        Bounds::Point,
                        PolicyId::new(1),
                    )
                    .expect("unique entity");
                index.upsert(handle, position, Bounds::Point);
            }
            stations.push(station);
            indexes.push(index);
            viewers.push(vec![ViewerQuery {
                client_id: ClientId::new(u64::from(station_index)),
                position: Position3::new(16.0, 0.0, 0.0),
                radius: 64.0,
                max_entities: 32,
            }]);
        }

        let batches = stations
            .iter()
            .zip(&indexes)
            .zip(&viewers)
            .map(|((station, index), viewers)| {
                StationReplicationBatch::new(station, index, viewers)
            })
            .collect::<Vec<_>>();
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");
        let mut parallel_scratch = ParallelReplicationScratch::new();
        let parallel = pool.plan_station_range_batches_into(
            &batches,
            &policies,
            ReplicationBudget::default(),
            &mut parallel_scratch,
        );

        for (batch_index, batch) in batches.iter().enumerate() {
            let mut serial_scratch = ReplicationScratch::default();
            let mut serial_output = ReplicationBatchScratch::default();
            let serial = ReplicationPlanner::plan_for_viewers_into(
                batch.station,
                batch.index,
                &policies,
                batch.viewers,
                &RangeOnlyVisibility,
                ReplicationBudget::default(),
                &mut serial_scratch,
                &mut serial_output,
            );
            assert_eq!(parallel.batches[batch_index].view().plans, serial.plans);
        }
        assert_eq!(parallel.stats.viewers, 6);
        assert_eq!(parallel_scratch.lanes(), 2);
    }

    #[test]
    fn reusable_parallel_output_matches_repeated_results_and_retains_capacity() {
        let mut stations = Vec::new();
        let mut indexes = Vec::new();
        let mut viewers = Vec::new();
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 128, 128.0));

        for station_index in 0_u32..6 {
            let mut station = Station::new(StationConfig {
                station_id: StationId::new(station_index),
                node_id: NodeId::new(1),
                instance_id: InstanceId::new(1),
                tick_rate_hz: 128,
            });
            let mut index = CellIndex::new(GridSpec::new(16.0).expect("valid grid"));
            for entity_index in 0_u16..24 {
                let position = Position3::new(f32::from(entity_index) * 3.0, 0.0, 0.0);
                let handle = station
                    .spawn_owned(
                        EntityId::new(u64::from(station_index) * 100 + u64::from(entity_index)),
                        position,
                        Bounds::Point,
                        PolicyId::new(1),
                    )
                    .expect("unique entity");
                index.upsert(handle, position, Bounds::Point);
            }
            stations.push(station);
            indexes.push(index);
            viewers.push(vec![
                ViewerQuery {
                    client_id: ClientId::new(u64::from(station_index) * 2),
                    position: Position3::new(16.0, 0.0, 0.0),
                    radius: 64.0,
                    max_entities: 32,
                },
                ViewerQuery {
                    client_id: ClientId::new(u64::from(station_index) * 2 + 1),
                    position: Position3::new(40.0, 0.0, 0.0),
                    radius: 32.0,
                    max_entities: 8,
                },
            ]);
        }

        let batches = stations
            .iter()
            .zip(&indexes)
            .zip(&viewers)
            .map(|((station, index), viewers)| {
                StationReplicationBatch::new(station, index, viewers)
            })
            .collect::<Vec<_>>();
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");
        let mut reusable_scratch = ParallelReplicationScratch::new();
        let first = pool.plan_station_range_batches_into(
            &batches,
            &policies,
            ReplicationBudget::default(),
            &mut reusable_scratch,
        );
        let expected_stats = first.stats;
        let expected = first
            .batches
            .iter()
            .map(|batch| (batch.view().plans.to_vec(), batch.view().stats))
            .collect::<Vec<_>>();

        {
            let reusable = pool.plan_station_range_batches_into(
                &batches,
                &policies,
                ReplicationBudget::default(),
                &mut reusable_scratch,
            );
            assert_eq!(reusable.stats, expected_stats);
            for (actual, expected) in reusable.batches.iter().zip(&expected) {
                let actual = actual.view();
                assert_eq!(actual.plans, expected.0);
                assert_eq!(actual.stats, expected.1);
            }
        }
        let retained_capacity = reusable_scratch.retained_entity_capacity();
        assert_eq!(reusable_scratch.retained_batch_slots(), batches.len());
        assert!(retained_capacity > 0);

        let reduced = pool.plan_station_batches_into(
            &batches[..2],
            &policies,
            &RangeOnlyVisibility,
            ReplicationBudget::default(),
            &mut reusable_scratch,
        );
        assert_eq!(reduced.batches.len(), 2);
        assert_eq!(reduced.stats.viewers, 4);
        assert_eq!(reusable_scratch.retained_batch_slots(), batches.len());
        assert_eq!(
            reusable_scratch.retained_entity_capacity(),
            retained_capacity
        );
    }

    #[test]
    fn ordered_map_retains_input_order() {
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");

        let output = pool.map_ordered(vec![3_u32, 1, 2], |value| value * value);

        assert_eq!(output, vec![9, 1, 4]);
    }

    #[test]
    fn empty_station_batch_retains_no_scratch_lanes() {
        let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
            .expect("pool builds");
        let mut scratch = ParallelReplicationScratch::new();
        let reusable = pool.plan_station_range_batches_into(
            &[],
            &PolicyTable::default(),
            ReplicationBudget::default(),
            &mut scratch,
        );
        assert!(reusable.batches.is_empty());
        assert_eq!(reusable.stats, ReplicationBatchStats::default());
        assert_eq!(scratch.lanes(), 0);
    }
}
