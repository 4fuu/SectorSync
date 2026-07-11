//! Replication planning helpers.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::ids::{ClientId, EntityHandle, Tick};
#[cfg(not(feature = "simd"))]
use crate::interest::RangeOnlyVisibility;
use crate::interest::{ViewerQuery, VisibilityFilter};
use crate::policy::{CompiledSyncPolicy, PolicyTable};
use crate::spatial_index::{CellIndex, CellQueryScratch, CellQueryStats, CellQueryStrategy};
use crate::station::Station;

/// Per-client replication budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationBudget {
    /// Maximum entities to include in a frame.
    pub max_entities: usize,
    /// Estimated byte budget for a frame.
    pub max_bytes: usize,
    /// Estimated bytes charged per selected entity by simple planners.
    pub estimated_entity_bytes: usize,
}

impl Default for ReplicationBudget {
    fn default() -> Self {
        Self {
            max_entities: 300,
            max_bytes: 16 * 1024,
            estimated_entity_bytes: 32,
        }
    }
}

/// Replication planner result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationPlan {
    /// Selected entity handles.
    pub entities: Vec<EntityHandle>,
    /// Planner statistics.
    pub stats: ReplicationStats,
}

/// Aggregated work and retained-capacity signals from one viewer batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationBatchStats {
    /// Viewer queries planned in input order.
    pub viewers: usize,
    /// Spatial candidates returned across all viewers.
    pub candidates: usize,
    /// Candidate records considered after stale-handle filtering.
    pub considered: usize,
    /// Entities selected across all plans.
    pub selected: usize,
    /// Estimated payload bytes across all plans.
    pub estimated_bytes: usize,
    /// Queries that probed the regular cell grid.
    pub grid_queries: usize,
    /// Queries that scanned occupied cells.
    pub occupied_queries: usize,
    /// Grid cells probed across the batch.
    pub grid_cells_probed: usize,
    /// Occupied cells scanned across the batch.
    pub occupied_cells_scanned: usize,
    /// Cells intersecting viewer query bounds across the batch.
    pub matched_cells: usize,
    /// Largest retained candidate-handle capacity.
    pub candidate_capacity_max: usize,
    /// Largest retained candidate-deduplication capacity.
    pub dedup_capacity_max: usize,
    /// Largest retained matched-cell capacity.
    pub matching_cell_capacity_max: usize,
    /// Largest retained priority candidate capacity.
    pub priority_capacity_max: usize,
}

impl ReplicationBatchStats {
    fn record(&mut self, plan: &ReplicationPlan, scratch: &ReplicationScratch) {
        self.viewers = self.viewers.saturating_add(1);
        self.candidates = self.candidates.saturating_add(plan.stats.candidates);
        self.considered = self.considered.saturating_add(plan.stats.considered);
        self.selected = self.selected.saturating_add(plan.stats.selected);
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_add(plan.stats.estimated_bytes);
        let query = scratch.query_stats();
        match query.strategy {
            CellQueryStrategy::Grid => self.grid_queries = self.grid_queries.saturating_add(1),
            CellQueryStrategy::OccupiedCells => {
                self.occupied_queries = self.occupied_queries.saturating_add(1);
            }
        }
        self.grid_cells_probed = self
            .grid_cells_probed
            .saturating_add(query.grid_cells_probed);
        self.occupied_cells_scanned = self
            .occupied_cells_scanned
            .saturating_add(query.occupied_cells_scanned);
        self.matched_cells = self.matched_cells.saturating_add(query.matched_cells);
        self.candidate_capacity_max = self
            .candidate_capacity_max
            .max(scratch.candidate_capacity());
        self.dedup_capacity_max = self
            .dedup_capacity_max
            .max(scratch.candidate_dedup_capacity());
        self.matching_cell_capacity_max = self
            .matching_cell_capacity_max
            .max(scratch.matching_cell_capacity());
        self.priority_capacity_max = self
            .priority_capacity_max
            .max(scratch.prioritized_capacity());
    }

    /// Merges another deterministic batch partition into this report.
    pub fn merge(&mut self, other: Self) {
        self.viewers = self.viewers.saturating_add(other.viewers);
        self.candidates = self.candidates.saturating_add(other.candidates);
        self.considered = self.considered.saturating_add(other.considered);
        self.selected = self.selected.saturating_add(other.selected);
        self.estimated_bytes = self.estimated_bytes.saturating_add(other.estimated_bytes);
        self.grid_queries = self.grid_queries.saturating_add(other.grid_queries);
        self.occupied_queries = self.occupied_queries.saturating_add(other.occupied_queries);
        self.grid_cells_probed = self
            .grid_cells_probed
            .saturating_add(other.grid_cells_probed);
        self.occupied_cells_scanned = self
            .occupied_cells_scanned
            .saturating_add(other.occupied_cells_scanned);
        self.matched_cells = self.matched_cells.saturating_add(other.matched_cells);
        self.candidate_capacity_max = self
            .candidate_capacity_max
            .max(other.candidate_capacity_max);
        self.dedup_capacity_max = self.dedup_capacity_max.max(other.dedup_capacity_max);
        self.matching_cell_capacity_max = self
            .matching_cell_capacity_max
            .max(other.matching_cell_capacity_max);
        self.priority_capacity_max = self.priority_capacity_max.max(other.priority_capacity_max);
    }
}

/// Ordered plans and aggregate statistics produced for a viewer batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationBatchResult {
    /// One plan per input viewer, retaining input order.
    pub plans: Vec<ReplicationPlan>,
    /// Aggregate work signals for the batch.
    pub stats: ReplicationBatchStats,
}

/// Borrowed ordered plans produced from reusable batch storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationBatchView<'a> {
    /// One plan per input viewer, retaining input order.
    pub plans: &'a [ReplicationPlan],
    /// Aggregate work signals for the active plans.
    pub stats: ReplicationBatchStats,
}

/// Caller-owned reusable output storage for viewer batch planning.
///
/// Plan slots and their entity buffers grow to the largest observed batch and
/// are retained for later calls. This storage contains no cross-client send or
/// acknowledgement state.
#[derive(Clone, Debug, Default)]
pub struct ReplicationBatchScratch {
    plans: Vec<ReplicationPlan>,
    active_plans: usize,
    stats: ReplicationBatchStats,
}

impl ReplicationBatchScratch {
    /// Creates empty batch output storage.
    pub const fn new() -> Self {
        Self {
            plans: Vec::new(),
            active_plans: 0,
            stats: ReplicationBatchStats {
                viewers: 0,
                candidates: 0,
                considered: 0,
                selected: 0,
                estimated_bytes: 0,
                grid_queries: 0,
                occupied_queries: 0,
                grid_cells_probed: 0,
                occupied_cells_scanned: 0,
                matched_cells: 0,
                candidate_capacity_max: 0,
                dedup_capacity_max: 0,
                matching_cell_capacity_max: 0,
                priority_capacity_max: 0,
            },
        }
    }

    /// Number of plan slots retained for reuse.
    pub fn retained_plan_slots(&self) -> usize {
        self.plans.len()
    }

    /// Total selected-entity capacity retained across all plan slots.
    pub fn retained_entity_capacity(&self) -> usize {
        self.plans.iter().map(|plan| plan.entities.capacity()).sum()
    }

    /// Returns the active result from the most recent planning call.
    pub fn view(&self) -> ReplicationBatchView<'_> {
        ReplicationBatchView {
            plans: &self.plans[..self.active_plans],
            stats: self.stats,
        }
    }

    fn prepare(&mut self, plans: usize) {
        if self.plans.len() < plans {
            self.plans.resize_with(plans, ReplicationPlan::default);
        }
        self.active_plans = plans;
        self.stats = ReplicationBatchStats::default();
    }
}

/// Bounded per-client replication tracking configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationTrackerConfig {
    /// Maximum tracked client/entity entries.
    pub max_entries: usize,
}

impl Default for ReplicationTrackerConfig {
    fn default() -> Self {
        Self {
            max_entries: 65_536,
        }
    }
}

/// Per-client/entity replication tracking key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationTrackKey {
    /// Client that received the entity update.
    pub client_id: ClientId,
    /// Station-local entity handle selected by the planner.
    pub entity: EntityHandle,
}

/// Per-client/entity replication tracking record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationTrackRecord {
    /// Client that received the entity update.
    pub client_id: ClientId,
    /// Station-local entity handle selected by the planner.
    pub entity: EntityHandle,
    /// Last tick where this entity was sent to the client.
    pub last_sent: Tick,
    /// Last tick where the caller confirmed delivery.
    pub last_acked: Option<Tick>,
}

/// Replication tracker statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationTrackerStats {
    /// Currently tracked entries.
    pub entries: usize,
    /// Total record insertions or updates.
    pub sent_records: usize,
    /// Total ACK updates applied.
    pub acked_records: usize,
    /// Records pruned by explicit cleanup.
    pub pruned_records: usize,
}

/// Replication tracking error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicationTrackerError {
    /// Recording would exceed the configured entry capacity.
    CapacityExceeded {
        /// Entries currently tracked.
        current: usize,
        /// New entries needed for this operation.
        needed: usize,
        /// Maximum tracked entries.
        max: usize,
    },
}

impl core::fmt::Display for ReplicationTrackerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CapacityExceeded {
                current,
                needed,
                max,
            } => write!(
                f,
                "replication tracker capacity exceeded: current {current}, needed {needed}, max {max}"
            ),
        }
    }
}

impl std::error::Error for ReplicationTrackerError {}

/// Bounded per-client replication send/ACK tracker.
#[derive(Clone, Debug)]
pub struct ReplicationTracker {
    config: ReplicationTrackerConfig,
    records: BTreeMap<ReplicationTrackKey, ReplicationTrackRecord>,
    stats: ReplicationTrackerStats,
}

impl Default for ReplicationTracker {
    fn default() -> Self {
        Self::new(ReplicationTrackerConfig::default())
    }
}

impl ReplicationTracker {
    /// Creates an empty tracker.
    pub fn new(config: ReplicationTrackerConfig) -> Self {
        Self {
            config,
            records: BTreeMap::new(),
            stats: ReplicationTrackerStats::default(),
        }
    }

    /// Returns tracker configuration.
    pub const fn config(&self) -> ReplicationTrackerConfig {
        self.config
    }

    /// Returns tracker statistics.
    pub const fn stats(&self) -> ReplicationTrackerStats {
        self.stats
    }

    /// Returns tracked entry count.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether no entries are tracked.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns the last sent tick for a client/entity pair.
    pub fn last_sent(&self, client_id: ClientId, entity: EntityHandle) -> Option<Tick> {
        self.records
            .get(&ReplicationTrackKey { client_id, entity })
            .map(|record| record.last_sent)
    }

    /// Returns a tracked record for a client/entity pair.
    pub fn get(&self, client_id: ClientId, entity: EntityHandle) -> Option<ReplicationTrackRecord> {
        self.records
            .get(&ReplicationTrackKey { client_id, entity })
            .copied()
    }

    /// Records that a planned set of entities was sent to a client.
    pub fn record_plan_sent(
        &mut self,
        client_id: ClientId,
        plan: &ReplicationPlan,
        sent_at: Tick,
    ) -> Result<usize, ReplicationTrackerError> {
        self.ensure_capacity_for(client_id, &plan.entities)?;
        let mut recorded = 0;
        for entity in &plan.entities {
            let key = ReplicationTrackKey {
                client_id,
                entity: *entity,
            };
            self.records.insert(
                key,
                ReplicationTrackRecord {
                    client_id,
                    entity: *entity,
                    last_sent: sent_at,
                    last_acked: None,
                },
            );
            recorded += 1;
        }
        self.refresh_entry_count();
        self.stats.sent_records = self.stats.sent_records.saturating_add(recorded);
        Ok(recorded)
    }

    /// Records delivery acknowledgement for one client/entity pair.
    pub fn acknowledge(
        &mut self,
        client_id: ClientId,
        entity: EntityHandle,
        acked_at: Tick,
    ) -> bool {
        let Some(record) = self
            .records
            .get_mut(&ReplicationTrackKey { client_id, entity })
        else {
            return false;
        };
        record.last_acked = Some(acked_at);
        self.stats.acked_records = self.stats.acked_records.saturating_add(1);
        true
    }

    /// Records delivery acknowledgement for every entity in a plan.
    pub fn acknowledge_plan(
        &mut self,
        client_id: ClientId,
        plan: &ReplicationPlan,
        acked_at: Tick,
    ) -> usize {
        plan.entities
            .iter()
            .filter(|entity| self.acknowledge(client_id, **entity, acked_at))
            .count()
    }

    /// Removes all entries for one client.
    pub fn clear_client(&mut self, client_id: ClientId) -> usize {
        let before = self.records.len();
        self.records.retain(|key, _| key.client_id != client_id);
        let pruned = before.saturating_sub(self.records.len());
        self.stats.pruned_records = self.stats.pruned_records.saturating_add(pruned);
        self.refresh_entry_count();
        pruned
    }

    /// Removes entries last sent before `older_than`.
    pub fn prune_sent_before(&mut self, older_than: Tick) -> usize {
        let before = self.records.len();
        self.records
            .retain(|_, record| record.last_sent.get() >= older_than.get());
        let pruned = before.saturating_sub(self.records.len());
        self.stats.pruned_records = self.stats.pruned_records.saturating_add(pruned);
        self.refresh_entry_count();
        pruned
    }

    fn ensure_capacity_for(
        &self,
        client_id: ClientId,
        entities: &[EntityHandle],
    ) -> Result<(), ReplicationTrackerError> {
        if self.records.len().saturating_add(entities.len()) <= self.config.max_entries {
            return Ok(());
        }
        let mut needed = 0_usize;
        for entity in entities {
            if !self.records.contains_key(&ReplicationTrackKey {
                client_id,
                entity: *entity,
            }) {
                needed = needed.saturating_add(1);
            }
        }
        if self.records.len().saturating_add(needed) > self.config.max_entries {
            return Err(ReplicationTrackerError::CapacityExceeded {
                current: self.records.len(),
                needed,
                max: self.config.max_entries,
            });
        }
        Ok(())
    }

    fn refresh_entry_count(&mut self) {
        self.stats.entries = self.records.len();
    }
}

/// Replication planner statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationStats {
    /// Candidate handles returned from the spatial index.
    pub candidates: usize,
    /// Candidate records considered after stale handle filtering.
    pub considered: usize,
    /// Selected entities.
    pub selected: usize,
    /// Entities skipped because the budget was exhausted.
    pub skipped_by_budget: usize,
    /// Entities skipped because their cadence interval has not elapsed.
    pub skipped_by_cadence: usize,
    /// Estimated frame bytes.
    pub estimated_bytes: usize,
}

/// Stateless distance-based replication cadence helper.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicationCadence;

impl ReplicationCadence {
    /// Returns the target update frequency for a policy at a squared distance.
    pub fn target_hz(policy: &CompiledSyncPolicy, distance_squared: f32) -> u16 {
        let min_hz = policy.min_hz.max(1);
        let max_hz = policy.max_hz.max(min_hz);
        let radius_squared = policy.interest_radius * policy.interest_radius;
        let closeness =
            if radius_squared.is_finite() && radius_squared > 0.0 && distance_squared.is_finite() {
                1.0 - (distance_squared / radius_squared).clamp(0.0, 1.0)
            } else {
                1.0
            };
        let span = f32::from(max_hz - min_hz);
        let target = f32::from(min_hz) + span * closeness;
        rounded_frequency_to_u16(target, min_hz, max_hz)
    }

    /// Returns the tick interval for a policy at a squared distance.
    pub fn interval_ticks(
        policy: &CompiledSyncPolicy,
        station_tick_rate_hz: u16,
        distance_squared: f32,
    ) -> u64 {
        let tick_rate = u64::from(station_tick_rate_hz.max(1));
        let target_hz = u64::from(Self::target_hz(policy, distance_squared).max(1));
        tick_rate.div_ceil(target_hz).max(1)
    }

    /// Returns whether a replication update should be sent at `now`.
    pub fn should_send(
        policy: &CompiledSyncPolicy,
        station_tick_rate_hz: u16,
        distance_squared: f32,
        now: Tick,
        last_sent: Option<Tick>,
    ) -> bool {
        let Some(last_sent) = last_sent else {
            return true;
        };
        let interval = Self::interval_ticks(policy, station_tick_rate_hz, distance_squared);
        now.get().saturating_sub(last_sent.get()) >= interval
    }
}

/// Stateless replication priority scoring helper.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicationPriority;

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn rounded_frequency_to_u16(target: f32, min_hz: u16, max_hz: u16) -> u16 {
    let bounded = target.round().clamp(f32::from(min_hz), f32::from(max_hz));
    bounded as u16
}

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn normalized_score_to_u64(closeness: f32) -> u64 {
    debug_assert!(closeness.is_finite() && (0.0..=1.0).contains(&closeness));
    (closeness * 1_000_000.0).round() as u64
}

impl ReplicationPriority {
    /// Returns a deterministic priority score for budgeted selection.
    pub fn score(policy: &CompiledSyncPolicy, distance_squared: f32) -> u64 {
        let weight = u64::from(policy.priority_weight.max(1));
        let radius_squared = policy.interest_radius * policy.interest_radius;
        let distance_score =
            if radius_squared.is_finite() && radius_squared > 0.0 && distance_squared.is_finite() {
                let closeness = 1.0 - (distance_squared / radius_squared).clamp(0.0, 1.0);
                normalized_score_to_u64(closeness)
            } else {
                1_000_000
            };
        weight
            .saturating_mul(1_000_000)
            .saturating_add(distance_score)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct PrioritizedReplicationCandidate {
    handle: EntityHandle,
    score: u64,
    distance_squared: f32,
}

/// Reusable scratch storage for allocation-aware replication planning.
#[derive(Clone, Debug, Default)]
pub struct ReplicationScratch {
    cell_query: CellQueryScratch,
    prioritized: Vec<PrioritizedReplicationCandidate>,
}

impl ReplicationScratch {
    /// Clears retained planning results while keeping allocated capacity.
    pub fn clear(&mut self) {
        self.cell_query.clear();
        self.prioritized.clear();
    }

    /// Number of spatial candidates retained from the last query.
    pub fn candidate_count(&self) -> usize {
        self.cell_query.len()
    }

    /// Capacity retained for priority candidate sorting.
    pub fn prioritized_capacity(&self) -> usize {
        self.prioritized.capacity()
    }

    /// Work counters from the last spatial candidate query.
    pub const fn query_stats(&self) -> CellQueryStats {
        self.cell_query.stats()
    }

    /// Capacity retained for spatial candidate handles.
    pub fn candidate_capacity(&self) -> usize {
        self.cell_query.handle_capacity()
    }

    /// Capacity retained by spatial candidate deduplication.
    pub fn candidate_dedup_capacity(&self) -> usize {
        self.cell_query.dedup_capacity()
    }

    /// Capacity retained for cells matched by sparse spatial queries.
    pub fn matching_cell_capacity(&self) -> usize {
        self.cell_query.matching_cell_capacity()
    }
}

/// Simple range/visibility-based replication planner.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicationPlanner;

impl ReplicationPlanner {
    /// Plans a frame for one viewer using the station-local spatial index.
    pub fn plan_for_viewer<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
    ) -> ReplicationPlan {
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        Self::plan_for_candidates_inner(
            station,
            &candidates,
            policies,
            viewer,
            filter,
            budget,
            |_, _, _| true,
        )
    }

    /// Plans a frame using caller-provided scratch storage.
    pub fn plan_for_viewer_with_scratch<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationPlan {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_viewer_with_scratch_into(
            station, index, policies, viewer, filter, budget, scratch, &mut plan,
        );
        plan
    }

    /// Plans one viewer into caller-owned output while retaining its entity capacity.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_with_scratch_into<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
        plan: &mut ReplicationPlan,
    ) {
        let candidates =
            index.query_sphere_into(viewer.position, viewer.radius, &mut scratch.cell_query);
        Self::plan_for_candidates_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            |_, _, _| true,
            plan,
        );
    }

    /// Plans viewers in input order while reusing caller-provided scratch.
    pub fn plan_for_viewers_with_scratch<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewers: &[ViewerQuery],
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationBatchResult {
        let mut batch = ReplicationBatchResult {
            plans: Vec::with_capacity(viewers.len()),
            stats: ReplicationBatchStats::default(),
        };
        for viewer in viewers {
            let plan = Self::plan_for_viewer_with_scratch(
                station, index, policies, viewer, filter, budget, scratch,
            );
            batch.stats.record(&plan, scratch);
            batch.plans.push(plan);
        }
        batch
    }

    /// Plans viewers into caller-owned output slots while retaining all capacities.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewers_into<'a, F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewers: &[ViewerQuery],
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
        batch: &'a mut ReplicationBatchScratch,
    ) -> ReplicationBatchView<'a> {
        batch.prepare(viewers.len());
        for (plan, viewer) in batch.plans[..viewers.len()].iter_mut().zip(viewers) {
            Self::plan_for_viewer_with_scratch_into(
                station, index, policies, viewer, filter, budget, scratch, plan,
            );
            batch.stats.record(plan, scratch);
        }
        batch.view()
    }

    /// Plans one range-only viewer using the optional SIMD candidate filter.
    ///
    /// With the `simd` feature this evaluates candidate distances in eight-lane
    /// groups. Without it, the same API uses the scalar range-only planner.
    pub fn plan_for_viewer_range_with_scratch(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationPlan {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_viewer_range_with_scratch_into(
            station, index, policies, viewer, budget, scratch, &mut plan,
        );
        plan
    }

    /// Plans one range-only viewer into caller-owned reusable output.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_range_with_scratch_into(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
        plan: &mut ReplicationPlan,
    ) {
        let candidates =
            index.query_sphere_into(viewer.position, viewer.radius, &mut scratch.cell_query);
        Self::plan_for_range_candidates_into(station, candidates, policies, viewer, budget, plan);
    }

    /// Plans a range-only viewer batch in input order with optional SIMD filtering.
    pub fn plan_for_viewers_range_with_scratch(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewers: &[ViewerQuery],
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationBatchResult {
        let mut batch = ReplicationBatchResult {
            plans: Vec::with_capacity(viewers.len()),
            stats: ReplicationBatchStats::default(),
        };
        for viewer in viewers {
            let plan = Self::plan_for_viewer_range_with_scratch(
                station, index, policies, viewer, budget, scratch,
            );
            batch.stats.record(&plan, scratch);
            batch.plans.push(plan);
        }
        batch
    }

    /// Plans a range-only viewer batch into caller-owned reusable output slots.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewers_range_into<'a>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewers: &[ViewerQuery],
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
        batch: &'a mut ReplicationBatchScratch,
    ) -> ReplicationBatchView<'a> {
        batch.prepare(viewers.len());
        for (plan, viewer) in batch.plans[..viewers.len()].iter_mut().zip(viewers) {
            Self::plan_for_viewer_range_with_scratch_into(
                station, index, policies, viewer, budget, scratch, plan,
            );
            batch.stats.record(plan, scratch);
        }
        batch.view()
    }

    /// Plans a frame and skips entities whose distance-based cadence has not elapsed.
    pub fn plan_for_viewer_with_cadence<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let tick_rate_hz = station.config().tick_rate_hz;
        let now = station.tick();
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        Self::plan_for_candidates_inner(
            station,
            &candidates,
            policies,
            viewer,
            filter,
            budget,
            |handle, policy, distance_squared| {
                ReplicationCadence::should_send(
                    policy,
                    tick_rate_hz,
                    distance_squared,
                    now,
                    last_sent(handle),
                )
            },
        )
    }

    /// Plans a cadence-aware frame using caller-provided scratch storage.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_with_cadence_and_scratch<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_viewer_with_cadence_and_scratch_into(
            station, index, policies, viewer, filter, budget, last_sent, scratch, &mut plan,
        );
        plan
    }

    /// Plans a cadence-aware frame into caller-owned reusable output.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_with_cadence_and_scratch_into<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
        scratch: &mut ReplicationScratch,
        plan: &mut ReplicationPlan,
    ) where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let tick_rate_hz = station.config().tick_rate_hz;
        let now = station.tick();
        let candidates =
            index.query_sphere_into(viewer.position, viewer.radius, &mut scratch.cell_query);
        Self::plan_for_candidates_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            |handle, policy, distance_squared| {
                ReplicationCadence::should_send(
                    policy,
                    tick_rate_hz,
                    distance_squared,
                    now,
                    last_sent(handle),
                )
            },
            plan,
        );
    }

    /// Plans a frame and selects the highest-priority entities when budgeted.
    pub fn plan_for_viewer_prioritized<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
    ) -> ReplicationPlan {
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        let mut prioritized = Vec::new();
        Self::plan_for_candidates_prioritized_inner(
            station,
            &candidates,
            policies,
            viewer,
            filter,
            budget,
            &mut prioritized,
            |_, _, _| true,
        )
    }

    /// Plans a budgeted priority frame using caller-provided scratch storage.
    pub fn plan_for_viewer_prioritized_with_scratch<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationPlan {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_viewer_prioritized_with_scratch_into(
            station, index, policies, viewer, filter, budget, scratch, &mut plan,
        );
        plan
    }

    /// Plans a priority frame into caller-owned reusable output.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_prioritized_with_scratch_into<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        scratch: &mut ReplicationScratch,
        plan: &mut ReplicationPlan,
    ) {
        let candidates =
            index.query_sphere_into(viewer.position, viewer.radius, &mut scratch.cell_query);
        Self::plan_for_candidates_prioritized_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            &mut scratch.prioritized,
            |_, _, _| true,
            plan,
        );
    }

    /// Plans a budgeted priority frame with distance-based cadence checks.
    pub fn plan_for_viewer_prioritized_with_cadence<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let tick_rate_hz = station.config().tick_rate_hz;
        let now = station.tick();
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        let mut prioritized = Vec::new();
        Self::plan_for_candidates_prioritized_inner(
            station,
            &candidates,
            policies,
            viewer,
            filter,
            budget,
            &mut prioritized,
            |handle, policy, distance_squared| {
                ReplicationCadence::should_send(
                    policy,
                    tick_rate_hz,
                    distance_squared,
                    now,
                    last_sent(handle),
                )
            },
        )
    }

    /// Plans a priority/cadence frame using caller-provided scratch storage.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_prioritized_with_cadence_and_scratch<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
        scratch: &mut ReplicationScratch,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_viewer_prioritized_with_cadence_and_scratch_into(
            station, index, policies, viewer, filter, budget, last_sent, scratch, &mut plan,
        );
        plan
    }

    /// Plans a priority/cadence frame into caller-owned reusable output.
    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_viewer_prioritized_with_cadence_and_scratch_into<F, L>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        last_sent: L,
        scratch: &mut ReplicationScratch,
        plan: &mut ReplicationPlan,
    ) where
        F: VisibilityFilter,
        L: Fn(EntityHandle) -> Option<Tick>,
    {
        let tick_rate_hz = station.config().tick_rate_hz;
        let now = station.tick();
        let candidates =
            index.query_sphere_into(viewer.position, viewer.radius, &mut scratch.cell_query);
        Self::plan_for_candidates_prioritized_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            &mut scratch.prioritized,
            |handle, policy, distance_squared| {
                ReplicationCadence::should_send(
                    policy,
                    tick_rate_hz,
                    distance_squared,
                    now,
                    last_sent(handle),
                )
            },
            plan,
        );
    }

    fn plan_for_candidates_inner<F, C>(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        cadence_allows: C,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        C: Fn(EntityHandle, &CompiledSyncPolicy, f32) -> bool,
    {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_candidates_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            cadence_allows,
            &mut plan,
        );
        plan
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_for_candidates_inner_into<F, C>(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        cadence_allows: C,
        plan: &mut ReplicationPlan,
    ) where
        F: VisibilityFilter,
        C: Fn(EntityHandle, &CompiledSyncPolicy, f32) -> bool,
    {
        let max_entities = viewer.max_entities.min(budget.max_entities);
        let max_by_bytes = budget.max_bytes / budget.estimated_entity_bytes.max(1);
        let hard_limit = max_entities.min(max_by_bytes);
        plan.entities.clear();
        plan.entities.reserve(hard_limit.min(candidates.len()));
        plan.stats = ReplicationStats {
            candidates: candidates.len(),
            ..ReplicationStats::default()
        };

        for handle in candidates {
            let Some(entity) = station.get(*handle) else {
                continue;
            };
            plan.stats.considered += 1;

            let Some(policy) = policies.get(entity.policy_id) else {
                continue;
            };
            let distance_squared = entity.position.distance_squared(viewer.position);
            let policy_radius_sq = policy.interest_radius * policy.interest_radius;
            if distance_squared > policy_radius_sq {
                continue;
            }
            if !filter.is_visible_with_distance(viewer, entity, distance_squared) {
                continue;
            }
            if !cadence_allows(*handle, policy, distance_squared) {
                plan.stats.skipped_by_cadence += 1;
                continue;
            }

            if plan.entities.len() >= hard_limit {
                plan.stats.skipped_by_budget += 1;
                continue;
            }

            plan.entities.push(*handle);
        }

        plan.stats.selected = plan.entities.len();
        plan.stats.estimated_bytes = plan.stats.selected * budget.estimated_entity_bytes;
    }

    #[cfg(all(not(feature = "simd"), test))]
    fn plan_for_range_candidates(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
    ) -> ReplicationPlan {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_range_candidates_into(
            station, candidates, policies, viewer, budget, &mut plan,
        );
        plan
    }

    #[cfg(not(feature = "simd"))]
    fn plan_for_range_candidates_into(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
        plan: &mut ReplicationPlan,
    ) {
        Self::plan_for_candidates_inner_into(
            station,
            candidates,
            policies,
            viewer,
            &RangeOnlyVisibility,
            budget,
            |_, _, _| true,
            plan,
        );
    }

    #[cfg(all(feature = "simd", test))]
    fn plan_for_range_candidates(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
    ) -> ReplicationPlan {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_range_candidates_into(
            station, candidates, policies, viewer, budget, &mut plan,
        );
        plan
    }

    #[cfg(feature = "simd")]
    fn plan_for_range_candidates_into(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
        plan: &mut ReplicationPlan,
    ) {
        use wide::{CmpLe, f32x8};

        const LANES: usize = 8;
        let max_entities = viewer.max_entities.min(budget.max_entities);
        let max_by_bytes = budget.max_bytes / budget.estimated_entity_bytes.max(1);
        let hard_limit = max_entities.min(max_by_bytes);
        plan.entities.clear();
        plan.entities.reserve(hard_limit.min(candidates.len()));
        plan.stats = ReplicationStats {
            candidates: candidates.len(),
            ..ReplicationStats::default()
        };
        let viewer_radius_squared = viewer.radius_squared();

        for handles in candidates.chunks(LANES) {
            let mut distance_squared = [f32::NAN; LANES];
            let mut policy_radius_squared = [f32::NAN; LANES];
            let mut valid_lanes = 0_u8;

            for (lane, handle) in handles.iter().copied().enumerate() {
                let Some(entity) = station.get(handle) else {
                    continue;
                };
                plan.stats.considered = plan.stats.considered.saturating_add(1);
                let Some(policy) = policies.get(entity.policy_id) else {
                    continue;
                };
                distance_squared[lane] = entity.position.distance_squared(viewer.position);
                policy_radius_squared[lane] = policy.interest_radius * policy.interest_radius;
                valid_lanes |= 1 << lane;
            }

            let visible_lanes = u8::try_from(
                (f32x8::new(distance_squared).cmp_le(f32x8::new(policy_radius_squared))
                    & f32x8::new(distance_squared).cmp_le(f32x8::splat(viewer_radius_squared)))
                .move_mask(),
            )
            .expect("eight-lane SIMD mask fits u8")
                & valid_lanes;

            for (lane, handle) in handles.iter().copied().enumerate() {
                if visible_lanes & (1 << lane) == 0 {
                    continue;
                }
                if plan.entities.len() >= hard_limit {
                    plan.stats.skipped_by_budget = plan.stats.skipped_by_budget.saturating_add(1);
                } else {
                    plan.entities.push(handle);
                }
            }
        }

        plan.stats.selected = plan.entities.len();
        plan.stats.estimated_bytes = plan.stats.selected * budget.estimated_entity_bytes;
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_for_candidates_prioritized_inner<F, C>(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        eligible: &mut Vec<PrioritizedReplicationCandidate>,
        cadence_allows: C,
    ) -> ReplicationPlan
    where
        F: VisibilityFilter,
        C: Fn(EntityHandle, &CompiledSyncPolicy, f32) -> bool,
    {
        let mut plan = ReplicationPlan::default();
        Self::plan_for_candidates_prioritized_inner_into(
            station,
            candidates,
            policies,
            viewer,
            filter,
            budget,
            eligible,
            cadence_allows,
            &mut plan,
        );
        plan
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_for_candidates_prioritized_inner_into<F, C>(
        station: &Station,
        candidates: &[EntityHandle],
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
        eligible: &mut Vec<PrioritizedReplicationCandidate>,
        cadence_allows: C,
        plan: &mut ReplicationPlan,
    ) where
        F: VisibilityFilter,
        C: Fn(EntityHandle, &CompiledSyncPolicy, f32) -> bool,
    {
        let max_entities = viewer.max_entities.min(budget.max_entities);
        let max_by_bytes = budget.max_bytes / budget.estimated_entity_bytes.max(1);
        let hard_limit = max_entities.min(max_by_bytes);
        plan.entities.clear();
        plan.entities.reserve(hard_limit.min(candidates.len()));
        plan.stats = ReplicationStats {
            candidates: candidates.len(),
            ..ReplicationStats::default()
        };
        eligible.clear();

        for handle in candidates {
            let Some(entity) = station.get(*handle) else {
                continue;
            };
            plan.stats.considered += 1;

            let Some(policy) = policies.get(entity.policy_id) else {
                continue;
            };
            let distance_squared = entity.position.distance_squared(viewer.position);
            let policy_radius_sq = policy.interest_radius * policy.interest_radius;
            if distance_squared > policy_radius_sq {
                continue;
            }
            if !filter.is_visible_with_distance(viewer, entity, distance_squared) {
                continue;
            }
            if !cadence_allows(*handle, policy, distance_squared) {
                plan.stats.skipped_by_cadence += 1;
                continue;
            }

            eligible.push(PrioritizedReplicationCandidate {
                handle: *handle,
                score: ReplicationPriority::score(policy, distance_squared),
                distance_squared,
            });
        }

        let selected = prioritize_candidates(eligible, hard_limit);

        plan.stats.skipped_by_budget = eligible.len().saturating_sub(selected);
        plan.entities.extend(
            eligible
                .iter()
                .take(selected)
                .map(|candidate| candidate.handle),
        );
        plan.stats.selected = plan.entities.len();
        plan.stats.estimated_bytes = plan.stats.selected * budget.estimated_entity_bytes;
    }
}

fn compare_prioritized_candidates(
    left: &PrioritizedReplicationCandidate,
    right: &PrioritizedReplicationCandidate,
) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.distance_squared.total_cmp(&right.distance_squared))
        .then_with(|| left.handle.cmp(&right.handle))
}

fn prioritize_candidates(eligible: &mut [PrioritizedReplicationCandidate], limit: usize) -> usize {
    let selected = eligible.len().min(limit);
    if selected == 0 {
        return 0;
    }
    if selected.saturating_mul(2) < eligible.len() {
        eligible.select_nth_unstable_by(selected, compare_prioritized_candidates);
        eligible[..selected].sort_by(compare_prioritized_candidates);
    } else {
        eligible.sort_by(compare_prioritized_candidates);
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::EntityTags;
    use crate::ids::{ClientId, EntityId, InstanceId, NodeId, PolicyId, StationId};
    use crate::interest::{AndVisibility, FrustumVisibility, RangeOnlyVisibility, TagVisibility};
    use crate::policy::CompiledSyncPolicy;
    use crate::spatial::{Aabb3, Bounds, Frustum3, GridSpec, Position3};
    use crate::station::{Station, StationConfig};

    #[test]
    fn planner_applies_composed_frustum_visibility_filter() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        let grid = GridSpec::new(16.0).expect("grid is valid");
        let mut index = CellIndex::new(grid);
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 20, 128.0));

        let visible = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(10.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn visible");
        let outside_frustum = station
            .spawn_owned(
                EntityId::new(2),
                Position3::new(-10.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn outside frustum");
        index.upsert(visible, Position3::new(10.0, 0.0, 0.0), Bounds::Point);
        index.upsert(
            outside_frustum,
            Position3::new(-10.0, 0.0, 0.0),
            Bounds::Point,
        );

        let viewer = ViewerQuery {
            client_id: ClientId::new(7),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 8,
        };
        let frustum = Frustum3::from_aabb(Aabb3::new(
            Position3::new(0.0, -20.0, -20.0),
            Position3::new(80.0, 20.0, 20.0),
        ));
        let filter = AndVisibility::new(RangeOnlyVisibility, FrustumVisibility::new(frustum));

        let plan = ReplicationPlanner::plan_for_viewer(
            &station,
            &index,
            &policies,
            &viewer,
            &filter,
            ReplicationBudget::default(),
        );

        assert_eq!(plan.entities, vec![visible]);
        assert_eq!(plan.stats.selected, 1);
        assert_eq!(plan.stats.considered, 2);
    }

    #[test]
    fn planner_applies_tag_visibility_filter() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        let grid = GridSpec::new(16.0).expect("grid is valid");
        let mut index = CellIndex::new(grid);
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 20, 128.0));

        let static_visible = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(10.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn static");
        let fast_mover = station
            .spawn_owned(
                EntityId::new(2),
                Position3::new(12.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn mover");
        station
            .set_tags(static_visible, EntityTags::from_bits(0b001))
            .expect("tag static");
        station
            .set_tags(fast_mover, EntityTags::from_bits(0b010))
            .expect("tag mover");
        index.upsert(
            static_visible,
            Position3::new(10.0, 0.0, 0.0),
            Bounds::Point,
        );
        index.upsert(fast_mover, Position3::new(12.0, 0.0, 0.0), Bounds::Point);

        let viewer = ViewerQuery {
            client_id: ClientId::new(7),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 8,
        };
        let filter = AndVisibility::new(
            RangeOnlyVisibility,
            TagVisibility::new(EntityTags::from_bits(0b001), EntityTags::from_bits(0b010)),
        );

        let plan = ReplicationPlanner::plan_for_viewer(
            &station,
            &index,
            &policies,
            &viewer,
            &filter,
            ReplicationBudget::default(),
        );

        assert_eq!(plan.entities, vec![static_visible]);
        assert_eq!(plan.stats.selected, 1);
        assert_eq!(plan.stats.considered, 2);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn range_batch_matches_ordered_scalar_plans() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 128,
        });
        let grid = GridSpec::new(16.0).expect("grid is valid");
        let mut index = CellIndex::new(grid);
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 128, 96.0));
        for entity_index in 0_u16..24 {
            let position = Position3::new(f32::from(entity_index) * 8.0 - 64.0, 0.0, 0.0);
            let handle = station
                .spawn_owned(
                    EntityId::new(u64::from(entity_index)),
                    position,
                    Bounds::Point,
                    PolicyId::new(1),
                )
                .expect("entity id is unique");
            index.upsert(handle, position, Bounds::Point);
        }
        let viewers = [
            ViewerQuery {
                client_id: ClientId::new(1),
                position: Position3::new(0.0, 0.0, 0.0),
                radius: 80.0,
                max_entities: 32,
            },
            ViewerQuery {
                client_id: ClientId::new(2),
                position: Position3::new(48.0, 0.0, 0.0),
                radius: 48.0,
                max_entities: 8,
            },
        ];
        let mut scalar_scratch = ReplicationScratch::default();
        let expected = viewers
            .iter()
            .map(|viewer| {
                ReplicationPlanner::plan_for_viewer_with_scratch(
                    &station,
                    &index,
                    &policies,
                    viewer,
                    &RangeOnlyVisibility,
                    ReplicationBudget::default(),
                    &mut scalar_scratch,
                )
            })
            .collect::<Vec<_>>();

        let mut batch_scratch = ReplicationScratch::default();
        let batch = ReplicationPlanner::plan_for_viewers_range_with_scratch(
            &station,
            &index,
            &policies,
            &viewers,
            ReplicationBudget::default(),
            &mut batch_scratch,
        );

        assert_eq!(batch.plans, expected);
        assert_eq!(batch.stats.viewers, viewers.len());
        assert_eq!(
            batch.stats.selected,
            expected.iter().map(|plan| plan.stats.selected).sum()
        );
        assert_eq!(
            batch.stats.grid_queries + batch.stats.occupied_queries,
            viewers.len()
        );

        let mut reusable_planning = ReplicationScratch::default();
        let mut reusable_output = ReplicationBatchScratch::new();
        {
            let reused = ReplicationPlanner::plan_for_viewers_range_into(
                &station,
                &index,
                &policies,
                &viewers,
                ReplicationBudget::default(),
                &mut reusable_planning,
                &mut reusable_output,
            );
            assert_eq!(reused.plans, expected);
            assert_eq!(reused.stats, batch.stats);
        }
        let retained_capacity = reusable_output.retained_entity_capacity();
        assert_eq!(reusable_output.retained_plan_slots(), viewers.len());

        {
            let reused = ReplicationPlanner::plan_for_viewers_into(
                &station,
                &index,
                &policies,
                &viewers,
                &RangeOnlyVisibility,
                ReplicationBudget::default(),
                &mut reusable_planning,
                &mut reusable_output,
            );
            assert_eq!(reused.plans, expected);
            assert_eq!(reused.stats, batch.stats);
        }

        let reused = ReplicationPlanner::plan_for_viewers_range_into(
            &station,
            &index,
            &policies,
            &viewers[..1],
            ReplicationBudget::default(),
            &mut reusable_planning,
            &mut reusable_output,
        );
        assert_eq!(reused.plans, &expected[..1]);
        assert_eq!(reusable_output.retained_plan_slots(), viewers.len());
        assert_eq!(
            reusable_output.retained_entity_capacity(),
            retained_capacity
        );
    }

    #[test]
    fn range_batch_preserves_scalar_nan_radius_semantics() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 128,
        });
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 128, 96.0));
        let handle = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(1.0, 2.0, 3.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn entity");
        let viewer = ViewerQuery {
            client_id: ClientId::new(1),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: f32::NAN,
            max_entities: 8,
        };
        let candidates = [handle];
        let scalar = ReplicationPlanner::plan_for_candidates_inner(
            &station,
            &candidates,
            &policies,
            &viewer,
            &RangeOnlyVisibility,
            ReplicationBudget::default(),
            |_, _, _| true,
        );
        let range = ReplicationPlanner::plan_for_range_candidates(
            &station,
            &candidates,
            &policies,
            &viewer,
            ReplicationBudget::default(),
        );

        assert!(scalar.entities.is_empty());
        assert_eq!(range, scalar);
    }

    #[test]
    fn cadence_scales_interval_by_squared_distance() {
        let policy = CompiledSyncPolicy::new(PolicyId::new(1), 2, 20, 100.0);

        assert_eq!(ReplicationCadence::target_hz(&policy, 0.0), 20);
        assert_eq!(ReplicationCadence::interval_ticks(&policy, 20, 0.0), 1);
        assert_eq!(ReplicationCadence::target_hz(&policy, 100.0_f32 * 100.0), 2);
        assert_eq!(
            ReplicationCadence::interval_ticks(&policy, 20, 100.0_f32 * 100.0),
            10
        );
    }

    #[test]
    fn priority_score_prefers_weight_then_distance() {
        let mut low = CompiledSyncPolicy::new(PolicyId::new(1), 1, 20, 100.0);
        low.priority_weight = 1;
        let mut high = CompiledSyncPolicy::new(PolicyId::new(2), 1, 20, 100.0);
        high.priority_weight = 10;

        assert!(
            ReplicationPriority::score(&high, 90.0 * 90.0) > ReplicationPriority::score(&low, 0.0)
        );
        assert!(
            ReplicationPriority::score(&low, 0.0) > ReplicationPriority::score(&low, 90.0 * 90.0)
        );
    }

    #[test]
    fn top_k_priority_selection_matches_full_sort_for_all_budget_edges() {
        let candidates = (0_u32..257)
            .map(|index| PrioritizedReplicationCandidate {
                handle: EntityHandle::new(index, index % 3),
                score: u64::from(index.wrapping_mul(37) % 23),
                distance_squared: f32::from(
                    u16::try_from(index.wrapping_mul(19) % 41).expect("distance fits u16"),
                ),
            })
            .collect::<Vec<_>>();

        for limit in [0, 1, 7, 64, 256, 257, 300] {
            let mut expected = candidates.clone();
            expected.sort_by(compare_prioritized_candidates);
            expected.truncate(limit.min(expected.len()));
            let mut actual = candidates.clone();
            let selected = prioritize_candidates(&mut actual, limit);

            assert_eq!(selected, expected.len());
            assert_eq!(&actual[..selected], expected.as_slice());
        }
    }

    #[test]
    fn planner_with_cadence_skips_recent_far_entities() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        for _ in 0..10 {
            station.advance_tick();
        }
        let grid = GridSpec::new(16.0).expect("grid is valid");
        let mut index = CellIndex::new(grid);
        let mut policies = PolicyTable::default();
        policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 2, 20, 128.0));

        let near = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn near");
        let far = station
            .spawn_owned(
                EntityId::new(2),
                Position3::new(120.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn far");
        index.upsert(near, Position3::new(0.0, 0.0, 0.0), Bounds::Point);
        index.upsert(far, Position3::new(120.0, 0.0, 0.0), Bounds::Point);

        let viewer = ViewerQuery {
            client_id: ClientId::new(7),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 8,
        };
        let plan = ReplicationPlanner::plan_for_viewer_with_cadence(
            &station,
            &index,
            &policies,
            &viewer,
            &RangeOnlyVisibility,
            ReplicationBudget::default(),
            |_| Some(Tick::new(9)),
        );

        assert_eq!(plan.entities, vec![near]);
        assert_eq!(plan.stats.selected, 1);
        assert_eq!(plan.stats.skipped_by_cadence, 1);

        let mut scratch = ReplicationScratch::default();
        let mut reusable = ReplicationPlan::default();
        ReplicationPlanner::plan_for_viewer_with_cadence_and_scratch_into(
            &station,
            &index,
            &policies,
            &viewer,
            &RangeOnlyVisibility,
            ReplicationBudget::default(),
            |_| Some(Tick::new(9)),
            &mut scratch,
            &mut reusable,
        );
        assert_eq!(reusable, plan);
    }

    #[test]
    fn prioritized_planner_uses_policy_weight_under_budget() {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        let grid = GridSpec::new(16.0).expect("grid is valid");
        let mut index = CellIndex::new(grid);
        let mut policies = PolicyTable::default();
        let mut low = CompiledSyncPolicy::new(PolicyId::new(1), 1, 20, 128.0);
        low.priority_weight = 1;
        let mut high = CompiledSyncPolicy::new(PolicyId::new(2), 1, 20, 128.0);
        high.priority_weight = 10;
        policies.set(low);
        policies.set(high);

        let near_low = station
            .spawn_owned(
                EntityId::new(1),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn near low priority");
        let far_high = station
            .spawn_owned(
                EntityId::new(2),
                Position3::new(96.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(2),
            )
            .expect("spawn far high priority");
        index.upsert(near_low, Position3::new(0.0, 0.0, 0.0), Bounds::Point);
        index.upsert(far_high, Position3::new(96.0, 0.0, 0.0), Bounds::Point);

        let viewer = ViewerQuery {
            client_id: ClientId::new(7),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 1,
        };
        let plan = ReplicationPlanner::plan_for_viewer_prioritized(
            &station,
            &index,
            &policies,
            &viewer,
            &RangeOnlyVisibility,
            ReplicationBudget {
                max_entities: 1,
                max_bytes: 32,
                estimated_entity_bytes: 32,
            },
        );

        assert_eq!(plan.entities, vec![far_high]);
        assert_eq!(plan.stats.selected, 1);
        assert_eq!(plan.stats.skipped_by_budget, 1);

        let mut scratch = ReplicationScratch::default();
        let scratch_plan = ReplicationPlanner::plan_for_viewer_prioritized_with_scratch(
            &station,
            &index,
            &policies,
            &viewer,
            &RangeOnlyVisibility,
            ReplicationBudget {
                max_entities: 1,
                max_bytes: 32,
                estimated_entity_bytes: 32,
            },
            &mut scratch,
        );
        assert_eq!(scratch_plan.entities, plan.entities);
        assert_eq!(scratch_plan.stats, plan.stats);
        assert_eq!(scratch.candidate_count(), 2);
        assert!(scratch.prioritized_capacity() >= 2);
        assert_eq!(scratch.query_stats().candidate_handles, 2);
        assert!(scratch.candidate_capacity() >= 2);
        assert!(scratch.candidate_dedup_capacity() >= 2);

        let budget = ReplicationBudget {
            max_entities: 1,
            max_bytes: 32,
            estimated_entity_bytes: 32,
        };
        assert_prioritized_output_reuse(
            &station,
            &index,
            &policies,
            &viewer,
            budget,
            &plan,
            &mut scratch,
        );
    }

    fn assert_prioritized_output_reuse(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        budget: ReplicationBudget,
        expected: &ReplicationPlan,
        scratch: &mut ReplicationScratch,
    ) {
        let mut reusable = ReplicationPlan::default();
        ReplicationPlanner::plan_for_viewer_prioritized_with_scratch_into(
            station,
            index,
            policies,
            viewer,
            &RangeOnlyVisibility,
            budget,
            scratch,
            &mut reusable,
        );
        let retained_entities = reusable.entities.as_ptr();
        assert_eq!(&reusable, expected);
        ReplicationPlanner::plan_for_viewer_prioritized_with_cadence_and_scratch_into(
            station,
            index,
            policies,
            viewer,
            &RangeOnlyVisibility,
            budget,
            |_| None,
            scratch,
            &mut reusable,
        );
        assert_eq!(reusable.entities.as_ptr(), retained_entities);
    }

    #[test]
    fn replication_tracker_records_sent_ack_and_prune() {
        let client_id = ClientId::new(7);
        let first = EntityHandle::new(1, 0);
        let second = EntityHandle::new(2, 0);
        let plan = ReplicationPlan {
            entities: vec![first, second],
            stats: ReplicationStats::default(),
        };
        let mut tracker = ReplicationTracker::new(ReplicationTrackerConfig { max_entries: 4 });

        let recorded = tracker
            .record_plan_sent(client_id, &plan, Tick::new(10))
            .expect("recording should fit");
        assert_eq!(recorded, 2);
        assert_eq!(tracker.last_sent(client_id, first), Some(Tick::new(10)));
        assert_eq!(tracker.stats().entries, 2);
        assert_eq!(tracker.stats().sent_records, 2);

        assert!(tracker.acknowledge(client_id, first, Tick::new(11)));
        assert_eq!(
            tracker
                .get(client_id, first)
                .expect("tracked record")
                .last_acked,
            Some(Tick::new(11))
        );
        assert_eq!(tracker.stats().acked_records, 1);

        assert_eq!(tracker.prune_sent_before(Tick::new(11)), 2);
        assert!(tracker.is_empty());
        assert_eq!(tracker.stats().pruned_records, 2);
    }

    #[test]
    fn replication_tracker_rejects_capacity_without_partial_insert() {
        let client_id = ClientId::new(7);
        let plan = ReplicationPlan {
            entities: vec![EntityHandle::new(1, 0), EntityHandle::new(2, 0)],
            stats: ReplicationStats::default(),
        };
        let mut tracker = ReplicationTracker::new(ReplicationTrackerConfig { max_entries: 1 });

        let error = tracker
            .record_plan_sent(client_id, &plan, Tick::new(10))
            .expect_err("recording should exceed capacity");

        assert_eq!(
            error,
            ReplicationTrackerError::CapacityExceeded {
                current: 0,
                needed: 2,
                max: 1,
            }
        );
        assert!(tracker.is_empty());
        assert_eq!(tracker.stats().sent_records, 0);
    }

    #[test]
    fn replication_tracker_uses_exact_capacity_check_near_limit() {
        let client_id = ClientId::new(7);
        let first = EntityHandle::new(1, 0);
        let second = EntityHandle::new(2, 0);
        let third = EntityHandle::new(3, 0);
        let mut tracker = ReplicationTracker::new(ReplicationTrackerConfig { max_entries: 2 });
        tracker
            .record_plan_sent(
                client_id,
                &ReplicationPlan {
                    entities: vec![first],
                    stats: ReplicationStats::default(),
                },
                Tick::new(1),
            )
            .expect("initial record should fit");
        tracker
            .record_plan_sent(
                client_id,
                &ReplicationPlan {
                    entities: vec![first, second],
                    stats: ReplicationStats::default(),
                },
                Tick::new(2),
            )
            .expect("one existing and one new record should fit exactly");

        let error = tracker
            .record_plan_sent(
                client_id,
                &ReplicationPlan {
                    entities: vec![first, second, third],
                    stats: ReplicationStats::default(),
                },
                Tick::new(3),
            )
            .expect_err("new record should exceed exact capacity");
        assert_eq!(
            error,
            ReplicationTrackerError::CapacityExceeded {
                current: 2,
                needed: 1,
                max: 2,
            }
        );
        assert_eq!(tracker.last_sent(client_id, first), Some(Tick::new(2)));
        assert_eq!(tracker.last_sent(client_id, second), Some(Tick::new(2)));
        assert_eq!(tracker.get(client_id, third), None);
    }
}
