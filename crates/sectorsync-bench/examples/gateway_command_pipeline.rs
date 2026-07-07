//! Gateway command pipeline SDK example.

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, GatewayConfig, GatewaySessionTable, StationId, Tick,
};
use sectorsync_runtime::{
    GATEWAY_COMMAND_ACK_ACCEPTED, GATEWAY_COMMAND_ACK_RATE_LIMITED, GatewayCommandPipeline,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, CommandFrame, FrameDecoder, FrameEncoder, RuntimeFrame,
};

fn main() {
    let client_id = ClientId::new(7);
    let station_id = StationId::new(1);
    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 8,
        reconnect_grace_ticks: 20,
        max_commands_per_tick: 1,
    });
    gateway
        .connect(client_id, station_id, Tick::new(10))
        .expect("client should connect");

    let mut station_queues = BTreeMap::from([(
        station_id,
        CommandQueues::new(CommandQueueLimits {
            high: 4,
            normal: 4,
            low: 4,
        }),
    )]);
    let mut pipeline = GatewayCommandPipeline::default();

    let first = pipeline.process(
        &mut gateway,
        &mut station_queues,
        &encode_command(client_id, 1),
        Tick::new(10),
        CommandIngress::RUNNING,
    );
    assert!(first.accepted);
    assert_eq!(first.reason_code, GATEWAY_COMMAND_ACK_ACCEPTED);
    let first_ack = decode_ack(&first.ack_bytes.expect("accepted ACK should exist"));
    assert!(first_ack.accepted);

    let limited = pipeline.process(
        &mut gateway,
        &mut station_queues,
        &encode_command(client_id, 2),
        Tick::new(10),
        CommandIngress::RUNNING,
    );
    assert!(!limited.accepted);
    assert_eq!(limited.reason_code, GATEWAY_COMMAND_ACK_RATE_LIMITED);
    let limited_ack = decode_ack(&limited.ack_bytes.expect("rejection ACK should exist"));
    assert!(!limited_ack.accepted);
    assert_eq!(limited_ack.reason_code, GATEWAY_COMMAND_ACK_RATE_LIMITED);

    let applied = station_queues
        .get_mut(&station_id)
        .expect("station queue should exist")
        .pop_next()
        .expect("accepted command should queue");
    assert_eq!(applied.id, CommandId::new(1));

    println!(
        "gateway_command_pipeline accepted={} rejected_gateway={} applied_command={} acked={}",
        pipeline.stats().commands_enqueued,
        pipeline.stats().commands_rejected_gateway,
        applied.id.get(),
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
