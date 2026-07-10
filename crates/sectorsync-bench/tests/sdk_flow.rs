//! Cohesive SDK flow integration test.

#[path = "../examples/sdk_flow.rs"]
mod sdk_flow_example;

#[test]
fn sdk_flow_validates_routes_applies_and_replicates() {
    let report = sdk_flow_example::run();

    assert_eq!(report.external_rejections, 1);
    assert_eq!(report.commands_enqueued, 1);
    assert_eq!(report.commands_applied, 1);
    assert_eq!(report.barrier_rejections, 1);
    assert_eq!(report.acks_encoded, 2);
    assert_eq!(report.replication_frames_sent, 1);
    assert_eq!(report.replication_frames_received, 1);
    assert_eq!(report.entities_received, 1);
    assert_eq!(report.components_received, 1);
    assert_eq!(report.final_health, 99);
}
