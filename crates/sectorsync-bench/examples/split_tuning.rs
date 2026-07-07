//! Split scheduler tuning SDK example.

use sectorsync_core::prelude::{
    CellCoord3, CellLoadSample, HotspotThresholds, StationId, StationLoadSample, Tick,
};
use sectorsync_runtime::{SplitScheduler, SplitSchedulerConfig, SplitSchedulerState};

fn main() {
    let samples = samples();
    let scheduler = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: thresholds(),
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        split_cooldown_ticks: 10,
        ..SplitSchedulerConfig::default()
    });
    let mut state = SplitSchedulerState::default();

    let first = scheduler.plan_with_state(&samples, Some(&state), Tick::new(100));
    assert_eq!(first.actions.len(), 1);
    state.record_schedule(&first, Tick::new(100));

    let cooldown = scheduler.plan_with_state(&samples, Some(&state), Tick::new(105));
    assert!(cooldown.actions.is_empty());
    assert_eq!(cooldown.skipped_cooldown, 1);

    let capacity_guard = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: thresholds(),
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        max_target_score_after_move: 1,
        ..SplitSchedulerConfig::default()
    });
    let capacity = capacity_guard.plan(&samples);
    assert!(capacity.actions.is_empty());
    assert_eq!(capacity.skipped_target_capacity, 1);

    let action = &first.actions[0];
    println!(
        "split_tuning first_actions={} cooldown_skips={} capacity_skips={} source_score={} target_after={}",
        first.actions.len(),
        cooldown.skipped_cooldown,
        capacity.skipped_target_capacity,
        action.source_score,
        action.estimated_target_score_after_move
    );
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
            owned_entities: 100,
            subscribers: 100,
            tick_cost_units: 1000,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(0, 0, 0),
                owned_entities: 100,
                subscribers: 100,
                event_pressure: 10,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(2),
            owned_entities: 1,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(10, 0, 0),
                owned_entities: 1,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
    ]
}
