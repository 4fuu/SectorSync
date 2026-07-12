//! Deterministic split scheduler calibration scenarios.

use sectorsync_core::prelude::{
    CellCoord3, CellLoadSample, HotspotSeverity, HotspotThresholds, StationId, StationLoadSample,
    Tick,
};
use sectorsync_runtime::{
    SplitSchedule, SplitScheduler, SplitSchedulerConfig, SplitSchedulerScratch, SplitSchedulerState,
};

/// Observable result of the smoke-safe split calibration scenarios.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SplitTuningReport {
    /// Stations classified as normal.
    pub normal_stations: usize,
    /// Stations classified as warm.
    pub warm_stations: usize,
    /// Stations classified as hot.
    pub hot_stations: usize,
    /// Split actions admitted by the primary scenario.
    pub actions_planned: usize,
    /// Hot sources rejected by cooldown state.
    pub cooldown_skips: usize,
    /// Hot sources rejected by target capacity.
    pub capacity_skips: usize,
    /// Hot sources rejected by minimum score improvement.
    pub improvement_skips: usize,
    /// Source station pressure before the planned move.
    pub source_pressure_before: u64,
    /// Estimated source pressure after removing moved-cell pressure.
    pub source_pressure_after: u64,
    /// Target station pressure before the planned move.
    pub target_pressure_before: u64,
    /// Estimated target pressure after receiving moved-cell pressure.
    pub target_pressure_after: u64,
    /// Cells proposed for movement.
    pub proposed_cells: usize,
    /// Authoritative entities represented by proposed cells.
    pub proposed_entities: usize,
}

#[cfg(not(test))]
fn main() {
    let report = run();
    println!(
        "split_tuning normal={} warm={} hot={} actions={} cooldown_skips={} capacity_skips={} improvement_skips={} source_pressure_before={} source_pressure_after={} target_pressure_before={} target_pressure_after={} proposed_cells={} proposed_entities={}",
        report.normal_stations,
        report.warm_stations,
        report.hot_stations,
        report.actions_planned,
        report.cooldown_skips,
        report.capacity_skips,
        report.improvement_skips,
        report.source_pressure_before,
        report.source_pressure_after,
        report.target_pressure_before,
        report.target_pressure_after,
        report.proposed_cells,
        report.proposed_entities,
    );
}

/// Runs normal/warm/hot classification and conservative split guard scenarios.
pub fn run() -> SplitTuningReport {
    let samples = samples();
    let scheduler = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: thresholds(),
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        split_cooldown_ticks: 10,
        ..SplitSchedulerConfig::default()
    });
    let mut state = SplitSchedulerState::default();
    let mut split_scratch = SplitSchedulerScratch::new();

    let first = SplitSchedule::from(scheduler.plan_into(
        &samples,
        Some(&state),
        Tick::new(100),
        &mut split_scratch,
    ));
    assert_eq!(first.actions.len(), 1);
    let normal_stations = count_severity(&first, HotspotSeverity::Normal);
    let warm_stations = count_severity(&first, HotspotSeverity::Warm);
    let hot_stations = count_severity(&first, HotspotSeverity::Hot);
    assert_eq!((normal_stations, warm_stations, hot_stations), (1, 1, 1));
    state.record_schedule(&first, Tick::new(100));

    let cooldown = scheduler.plan_into(&samples, Some(&state), Tick::new(105), &mut split_scratch);
    assert!(cooldown.actions.is_empty());
    assert_eq!(cooldown.skipped_cooldown, 1);
    let cooldown_skips = cooldown.skipped_cooldown;

    let capacity_scheduler = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: thresholds(),
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        max_target_score_after_move: 1,
        ..SplitSchedulerConfig::default()
    });
    let capacity = capacity_scheduler.plan_into(&samples, None, Tick::new(0), &mut split_scratch);
    assert!(capacity.actions.is_empty());
    assert_eq!(capacity.skipped_target_capacity, 1);
    let capacity_skips = capacity.skipped_target_capacity;

    let improvement_scheduler = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: thresholds(),
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        min_score_improvement: u64::MAX,
        ..SplitSchedulerConfig::default()
    });
    let improvement =
        improvement_scheduler.plan_into(&samples, None, Tick::new(0), &mut split_scratch);
    assert!(improvement.actions.is_empty());
    assert_eq!(improvement.skipped_insufficient_improvement, 1);
    let improvement_skips = improvement.skipped_insufficient_improvement;

    let action = &first.actions[0];
    let source = samples
        .iter()
        .find(|sample| sample.station_id == action.source_station)
        .expect("planned source sample should exist");
    let proposed_entities = source
        .cells
        .iter()
        .filter(|cell| action.proposal.cells_to_move.contains(&cell.cell))
        .map(|cell| cell.owned_entities.saturating_add(cell.ghost_entities))
        .sum();

    SplitTuningReport {
        normal_stations,
        warm_stations,
        hot_stations,
        actions_planned: first.actions.len(),
        cooldown_skips,
        capacity_skips,
        improvement_skips,
        source_pressure_before: action.source_score,
        source_pressure_after: action
            .source_score
            .saturating_sub(action.proposal.moved_pressure_score),
        target_pressure_before: action.target_score,
        target_pressure_after: action.estimated_target_score_after_move,
        proposed_cells: action.proposal.cells_to_move.len(),
        proposed_entities,
    }
}

fn count_severity(
    schedule: &sectorsync_runtime::SplitSchedule,
    severity: HotspotSeverity,
) -> usize {
    schedule
        .decisions
        .iter()
        .filter(|decision| decision.severity == severity)
        .count()
}

fn thresholds() -> HotspotThresholds {
    HotspotThresholds {
        max_station_entities: 10,
        max_station_subscribers: 10,
        max_cell_pressure: 10,
        ..HotspotThresholds::default()
    }
}

fn samples() -> Vec<StationLoadSample> {
    vec![
        StationLoadSample {
            station_id: StationId::new(1),
            owned_entities: 1,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(10, 0, 0),
                owned_entities: 1,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(2),
            owned_entities: 11,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(20, 0, 0),
                owned_entities: 1,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(3),
            owned_entities: 100,
            subscribers: 100,
            tick_cost_units: 1_000,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(0, 0, 0),
                owned_entities: 100,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
    ]
}
