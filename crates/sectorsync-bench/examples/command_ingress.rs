//! Command ingress over UDP SDK example.

use std::thread;
use std::time::Duration;

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, Tick,
};
use sectorsync_transport::{
    InboundPacket, OutboundPacket, TransportReceiver, TransportSink, UdpTransport,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, CommandAckFrame, CommandFrame, FrameDecoder,
    FrameEncoder, RuntimeFrame,
};

fn main() {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let mut server = UdpTransport::bind("127.0.0.1:0").expect("server should bind");
    let mut client = UdpTransport::bind("127.0.0.1:0").expect("client should bind");
    let server_addr = server.local_addr().expect("server addr should exist");
    let client_addr = client.local_addr().expect("client addr should exist");

    server.register_client(client_id, client_addr);
    client.register_client(server_id, server_addr);

    let command = CommandFrame {
        client_id,
        command_id: CommandId::new(42),
        entity_id: EntityId::new(100),
        sequence: 9,
        kind: 1,
        priority: CommandPriority::High,
        payload: b"move:north".to_vec(),
    };

    let mut encoder = BinaryFrameEncoder;
    let mut command_bytes = Vec::new();
    encoder
        .encode_command(&command, &mut command_bytes)
        .expect("command should encode");
    let command_bytes_len = command_bytes.len();

    client
        .send(OutboundPacket {
            client_id: server_id,
            bytes: command_bytes,
        })
        .expect("client should send command");

    let inbound = recv_with_retry(&mut server);
    assert_eq!(inbound.client_id, Some(client_id));
    let decoded = BinaryFrameDecoder
        .decode(&inbound.bytes)
        .expect("server should decode command");
    let RuntimeFrame::Command(command) = decoded else {
        panic!("expected command frame");
    };

    let mut queues = CommandQueues::new(CommandQueueLimits {
        high: 4,
        normal: 4,
        low: 4,
    });
    queues
        .push(command.clone().into_envelope(Tick::new(10)), CommandIngress::RUNNING)
        .expect("command should enqueue");
    let applied = queues.pop_next().expect("command should be ready");
    assert_eq!(applied.id, command.command_id);
    assert_eq!(applied.priority, CommandPriority::High);

    let ack = CommandAckFrame {
        client_id,
        command_id: command.command_id,
        server_tick: Tick::new(10),
        accepted: true,
        reason_code: 0,
    };
    let mut ack_bytes = Vec::new();
    encoder
        .encode_command_ack(&ack, &mut ack_bytes)
        .expect("ack should encode");
    let ack_bytes_len = ack_bytes.len();
    server
        .send(OutboundPacket {
            client_id,
            bytes: ack_bytes,
        })
        .expect("server should send ack");

    let inbound = recv_with_retry(&mut client);
    assert_eq!(inbound.client_id, Some(server_id));
    let decoded = BinaryFrameDecoder
        .decode(&inbound.bytes)
        .expect("client should decode ack");
    assert_eq!(decoded, RuntimeFrame::CommandAck(ack));

    println!(
        "command_ingress command_bytes={} ack_bytes={} queued=1 applied_command={}",
        command_bytes_len,
        ack_bytes_len,
        applied.id.get()
    );
}

fn recv_with_retry(transport: &mut UdpTransport) -> InboundPacket {
    for _ in 0..50 {
        if let Some(packet) = transport.try_recv().expect("udp receive should work") {
            return packet;
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("udp packet was not received");
}
