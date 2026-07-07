//! Replication planning helpers.

use crate::ids::{EntityHandle, Tick};
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
}
