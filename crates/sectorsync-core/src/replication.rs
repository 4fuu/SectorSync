//! Replication planning helpers.

use std::collections::BTreeMap;

use crate::ids::{ClientId, EntityHandle, Tick};
use crate::interest::{ViewerQuery, VisibilityFilter};
use crate::policy::{CompiledSyncPolicy, PolicyTable};
use crate::spatial_index::CellIndex;
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
        target.round().clamp(f32::from(min_hz), f32::from(max_hz)) as u16
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

impl ReplicationPriority {
    /// Returns a deterministic priority score for budgeted selection.
    pub fn score(policy: &CompiledSyncPolicy, distance_squared: f32) -> u64 {
        let weight = u64::from(policy.priority_weight.max(1));
        let radius_squared = policy.interest_radius * policy.interest_radius;
        let distance_score =
            if radius_squared.is_finite() && radius_squared > 0.0 && distance_squared.is_finite() {
                let closeness = 1.0 - (distance_squared / radius_squared).clamp(0.0, 1.0);
                (closeness * 1_000_000.0).round() as u64
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
        Self::plan_for_viewer_inner(
            station,
            index,
            policies,
            viewer,
            filter,
            budget,
            |_, _, _| true,
        )
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
        Self::plan_for_viewer_inner(
            station,
            index,
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

    /// Plans a frame and selects the highest-priority entities when budgeted.
    pub fn plan_for_viewer_prioritized<F: VisibilityFilter>(
        station: &Station,
        index: &CellIndex,
        policies: &PolicyTable,
        viewer: &ViewerQuery,
        filter: &F,
        budget: ReplicationBudget,
    ) -> ReplicationPlan {
        Self::plan_for_viewer_prioritized_inner(
            station,
            index,
            policies,
            viewer,
            filter,
            budget,
            |_, _, _| true,
        )
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
        Self::plan_for_viewer_prioritized_inner(
            station,
            index,
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

    fn plan_for_viewer_inner<F, C>(
        station: &Station,
        index: &CellIndex,
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
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        let max_entities = viewer.max_entities.min(budget.max_entities);
        let max_by_bytes = budget.max_bytes / budget.estimated_entity_bytes.max(1);
        let hard_limit = max_entities.min(max_by_bytes);

        let mut plan = ReplicationPlan {
            entities: Vec::with_capacity(hard_limit),
            stats: ReplicationStats {
                candidates: candidates.len(),
                ..ReplicationStats::default()
            },
        };

        for handle in candidates {
            let Some(entity) = station.get(handle) else {
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
            if !filter.is_visible(viewer, entity) {
                continue;
            }
            if !cadence_allows(handle, policy, distance_squared) {
                plan.stats.skipped_by_cadence += 1;
                continue;
            }

            if plan.entities.len() >= hard_limit {
                plan.stats.skipped_by_budget += 1;
                continue;
            }

            plan.entities.push(handle);
        }

        plan.stats.selected = plan.entities.len();
        plan.stats.estimated_bytes = plan.stats.selected * budget.estimated_entity_bytes;
        plan
    }

    fn plan_for_viewer_prioritized_inner<F, C>(
        station: &Station,
        index: &CellIndex,
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
        let candidates = index.query_sphere(viewer.position, viewer.radius);
        let max_entities = viewer.max_entities.min(budget.max_entities);
        let max_by_bytes = budget.max_bytes / budget.estimated_entity_bytes.max(1);
        let hard_limit = max_entities.min(max_by_bytes);
        let mut plan = ReplicationPlan {
            entities: Vec::with_capacity(hard_limit),
            stats: ReplicationStats {
                candidates: candidates.len(),
                ..ReplicationStats::default()
            },
        };
        let mut eligible = Vec::new();

        for handle in candidates {
            let Some(entity) = station.get(handle) else {
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
            if !filter.is_visible(viewer, entity) {
                continue;
            }
            if !cadence_allows(handle, policy, distance_squared) {
                plan.stats.skipped_by_cadence += 1;
                continue;
            }

            eligible.push(PrioritizedReplicationCandidate {
                handle,
                score: ReplicationPriority::score(policy, distance_squared),
                distance_squared,
            });
        }

        eligible.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.distance_squared.total_cmp(&right.distance_squared))
                .then_with(|| left.handle.cmp(&right.handle))
        });

        plan.stats.skipped_by_budget = eligible.len().saturating_sub(hard_limit);
        plan.entities.extend(
            eligible
                .into_iter()
                .take(hard_limit)
                .map(|candidate| candidate.handle),
        );
        plan.stats.selected = plan.entities.len();
        plan.stats.estimated_bytes = plan.stats.selected * budget.estimated_entity_bytes;
        plan
    }
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
}
