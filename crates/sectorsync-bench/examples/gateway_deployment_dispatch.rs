//! Gateway-to-deployment command dispatch SDK example.

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandPriority, EntityId, GatewayConfig, GatewaySessionTable, NodeId,
    StationId, Tick,
};
use sectorsync_runtime::{
    DeploymentConfig, DeploymentRouteTable, GATEWAY_COMMAND_ACK_ACCEPTED, GatewayCommandPipeline,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, CommandFrame, FrameDecoder, FrameEncoder, RuntimeFrame,
};

fn main() {
    let client_id = ClientId::new(7);
    let station_id = StationId::new(10);
    let node_one = NodeId::new(1);
    let node_two = NodeId::new(2);

    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 8,
        reconnect_grace_ticks: 20,
        max_commands_per_tick: 4,
    });
    gateway
        .connect(client_id, station_id, Tick::new(10))
        .expect("client should connect");

    let mut deployment = DeploymentRouteTable::new(DeploymentConfig {
        max_nodes: 4,
        max_stations_per_node: 4,
        stale_after_ticks: 10,
    });
    deployment
        .register_node(node_one, 4, Tick::new(10))
        .expect("node one should register");
    deployment
        .register_node(node_two, 4, Tick::new(10))
        .expect("node two should register");
    deployment
        .assign_station(station_id, node_one, Tick::new(10))
        .expect("station should start on node one");

    let mut pipeline = GatewayCommandPipeline::default();
    let first = pipeline.dispatch(
        &mut gateway,
        &deployment,
        &encode_command(client_id, 1),
        Tick::new(11),
    );
    assert!(first.accepted);
    assert_eq!(first.reason_code, GATEWAY_COMMAND_ACK_ACCEPTED);
    assert_eq!(first.node_id, Some(node_one));
    assert_eq!(
        first.command.as_ref().expect("command should exist").id,
        CommandId::new(1)
    );
    assert!(decode_ack(&first.ack_bytes.expect("ACK should exist")).accepted);

    deployment
        .move_station(station_id, node_two, Tick::new(12))
        .expect("station should move to node two");
    let second = pipeline.dispatch(
        &mut gateway,
        &deployment,
        &encode_command(client_id, 2),
        Tick::new(12),
    );
    assert!(second.accepted);
    let second_delivery = second.delivery.expect("delivery should resolve");
    assert_eq!(second_delivery.node_id, node_two);
    assert_eq!(second_delivery.station_route_epoch, 2);

    println!(
        "gateway_deployment_dispatch routed={} first_node={} second_node={} station_route_epoch={} acked={}",
        pipeline.stats().commands_routed_deployment,
        node_one.get(),
        second_delivery.node_id.get(),
        second_delivery.station_route_epoch,
        pipeline.stats().acks_encoded
    );
}

fn encode_command(client_id: ClientId, sequence: u64) -> Vec<u8> {
    let command = CommandFrame {
        client_id,
        command_id: CommandId::new(sequence),
        entity_id: EntityId::new(100),
        sequence,
        kind: 1,
        priority: CommandPriority::High,
        payload: b"move:north".to_vec(),
    };
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_command(&command, &mut bytes)
        .expect("command should encode");
    bytes
}

fn decode_ack(bytes: &[u8]) -> sectorsync_wire::CommandAckFrame {
    let RuntimeFrame::CommandAck(ack) =
        BinaryFrameDecoder.decode(bytes).expect("ACK should decode")
    else {
        panic!("expected command ACK");
    };
    ack
}
