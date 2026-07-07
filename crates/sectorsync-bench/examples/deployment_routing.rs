//! Deployment routing SDK example.

use sectorsync_core::prelude::{NodeId, StationId, Tick};
use sectorsync_runtime::{DeploymentConfig, DeploymentNodeState, DeploymentRouteTable};

fn main() {
    let node_one = NodeId::new(1);
    let node_two = NodeId::new(2);
    let station_alpha = StationId::new(10);
    let station_beta = StationId::new(11);
    let mut deployment = DeploymentRouteTable::new(DeploymentConfig {
        max_nodes: 4,
        max_stations_per_node: 2,
        stale_after_ticks: 3,
    });

    deployment
        .register_node(node_one, 2, Tick::new(10))
        .expect("node one should register");
    deployment
        .register_node(node_two, 2, Tick::new(10))
        .expect("node two should register");
    deployment
        .assign_station(station_alpha, node_one, Tick::new(10))
        .expect("station alpha should assign to node one");
    deployment
        .assign_station(station_beta, node_two, Tick::new(10))
        .expect("station beta should assign to node two");

    let draining = deployment
        .mark_draining(node_one)
        .expect("node one should enter draining");
    assert_eq!(draining.state, DeploymentNodeState::Draining);
    assert_eq!(
        deployment
            .station_route(station_alpha)
            .expect("station alpha route should remain")
            .node_id,
        node_one
    );

    let moved = deployment
        .move_station(station_alpha, node_two, Tick::new(11))
        .expect("station alpha should move to node two");
    assert_eq!(moved.previous.node_id, node_one);
    assert_eq!(moved.current.node_id, node_two);
    assert_eq!(moved.current.route_epoch, 2);

    deployment
        .heartbeat(node_two, Tick::new(13))
        .expect("node two heartbeat should refresh");
    let stale = deployment.stale_nodes(Tick::new(14));
    assert_eq!(stale, vec![node_one]);
    assert_eq!(deployment.mark_stale_offline(Tick::new(14)), 1);
    assert_eq!(
        deployment
            .node_route(node_one)
            .expect("node one route should exist")
            .state,
        DeploymentNodeState::Offline
    );

    let node_two_stations = deployment
        .stations_on_node(node_two)
        .expect("node two should exist");
    assert_eq!(node_two_stations, vec![station_alpha, station_beta]);

    println!(
        "deployment_routing nodes={} stations={} moved={} node_two_stations={} stale={} offline={} route_epoch={}",
        deployment.node_len(),
        deployment.station_len(),
        deployment.stats().stations_moved,
        node_two_stations.len(),
        deployment.stats().stale_nodes_detected,
        deployment.stats().nodes_offline,
        deployment
            .station_route(station_alpha)
            .expect("station alpha route should exist")
            .route_epoch
    );
}
