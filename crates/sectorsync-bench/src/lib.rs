//! Internal helpers shared by deliberate low-level benchmark examples.

use sectorsync_core::prelude::{
    CellIndex, PolicyTable, ReplicationBatchResult, ReplicationBatchScratch, ReplicationBudget,
    ReplicationPlan, ReplicationPlanner, ReplicationScratch, Station, ViewerQuery,
    VisibilityFilter,
};

/// Materializes one low-level plan for benchmark assertions.
pub fn plan_viewer_owned<F: VisibilityFilter>(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewer: &ViewerQuery,
    filter: &F,
    budget: ReplicationBudget,
) -> ReplicationPlan {
    let mut scratch = ReplicationScratch::default();
    let mut plan = ReplicationPlan::default();
    ReplicationPlanner::plan_for_viewer_into(
        station,
        index,
        policies,
        viewer,
        filter,
        budget,
        &mut scratch,
        &mut plan,
    );
    plan
}

/// Materializes an ordered low-level batch for benchmark assertions.
pub fn plan_viewers_owned<F: VisibilityFilter>(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    filter: &F,
    budget: ReplicationBudget,
) -> ReplicationBatchResult {
    let mut scratch = ReplicationScratch::default();
    let mut output = ReplicationBatchScratch::default();
    let view = ReplicationPlanner::plan_for_viewers_into(
        station,
        index,
        policies,
        viewers,
        filter,
        budget,
        &mut scratch,
        &mut output,
    );
    ReplicationBatchResult {
        plans: view.plans.to_vec(),
        stats: view.stats,
    }
}

/// Materializes an ordered batch while retaining caller-owned planning scratch.
pub fn plan_viewers_owned_with_scratch<F: VisibilityFilter>(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    filter: &F,
    budget: ReplicationBudget,
    scratch: &mut ReplicationScratch,
) -> ReplicationBatchResult {
    let mut output = ReplicationBatchScratch::default();
    let view = ReplicationPlanner::plan_for_viewers_into(
        station,
        index,
        policies,
        viewers,
        filter,
        budget,
        scratch,
        &mut output,
    );
    ReplicationBatchResult {
        plans: view.plans.to_vec(),
        stats: view.stats,
    }
}

/// Materializes a range-only low-level batch for benchmark assertions.
pub fn plan_viewers_range_owned(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    budget: ReplicationBudget,
) -> ReplicationBatchResult {
    let mut scratch = ReplicationScratch::default();
    let mut output = ReplicationBatchScratch::default();
    let view = ReplicationPlanner::plan_for_viewers_range_into(
        station,
        index,
        policies,
        viewers,
        budget,
        &mut scratch,
        &mut output,
    );
    ReplicationBatchResult {
        plans: view.plans.to_vec(),
        stats: view.stats,
    }
}

/// Materializes a range-only batch while retaining caller planning scratch.
pub fn plan_viewers_range_owned_with_scratch(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewers: &[ViewerQuery],
    budget: ReplicationBudget,
    scratch: &mut ReplicationScratch,
) -> ReplicationBatchResult {
    let mut output = ReplicationBatchScratch::default();
    let view = ReplicationPlanner::plan_for_viewers_range_into(
        station,
        index,
        policies,
        viewers,
        budget,
        scratch,
        &mut output,
    );
    ReplicationBatchResult {
        plans: view.plans.to_vec(),
        stats: view.stats,
    }
}
