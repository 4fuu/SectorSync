//! Split scheduler calibration integration test.

#[path = "../examples/split_tuning.rs"]
mod split_tuning_example;

#[test]
fn split_tuning_covers_classification_and_conservative_guards() {
    let report = split_tuning_example::run();

    assert_eq!(report.normal_stations, 1);
    assert_eq!(report.warm_stations, 1);
    assert_eq!(report.hot_stations, 1);
    assert_eq!(report.actions_planned, 1);
    assert_eq!(report.cooldown_skips, 1);
    assert_eq!(report.capacity_skips, 1);
    assert_eq!(report.improvement_skips, 1);
    assert!(report.source_pressure_after < report.source_pressure_before);
    assert!(report.target_pressure_after > report.target_pressure_before);
    assert!(report.target_pressure_after < report.source_pressure_after);
    assert_eq!(report.proposed_cells, 1);
    assert_eq!(report.proposed_entities, 100);
}
