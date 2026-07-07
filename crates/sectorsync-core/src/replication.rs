//! Replication planning helpers.

use crate::ids::EntityHandle;
use crate::interest::{ViewerQuery, VisibilityFilter};
use crate::policy::PolicyTable;
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
    /// Estimated frame bytes.
    pub estimated_bytes: usize,
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
            let policy_radius_sq = policy.interest_radius * policy.interest_radius;
            if entity.position.distance_squared(viewer.position) > policy_radius_sq {
                continue;
            }
            if !filter.is_visible(viewer, entity) {
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
