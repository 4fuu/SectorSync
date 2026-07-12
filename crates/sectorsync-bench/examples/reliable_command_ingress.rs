//! Reliable command ingress SDK example.

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, Tick,
};
use sectorsync_transport::{
    ClientTransportLimits, InMemoryTransportEndpoint, InMemoryTransportHub, OutboundPacket,
    ReliableClientConfig, ReliableClientEndpoint, TransportReceiver,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, CommandAckFrame, CommandFrame, FrameDecoder,
    FrameEncoder, RuntimeFrame,
};

#[allow(clippy::too_many_lines)]
fn main() {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 8,
        max_packet_bytes: 512,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:20007".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:20000".parse().expect("server addr"))
        .expect("server endpoint should register");

    let reliable_config = ReliableClientConfig {
        max_in_flight_per_peer: 8,
        retry_after_ticks: 2,
        max_attempts: 4,
        max_payload_bytes: 256,
        max_delivered_history: 16,
    };
    let mut client_link = ReliableClientEndpoint::new(reliable_config);
    let mut server_link = ReliableClientEndpoint::new(reliable_config);

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

    let command_sequence = client_link
        .send(
            &mut client_transport,
            OutboundPacket {
                client_id: server_id,
                bytes: command_bytes,
            },
            0,
        )
        .expect("reliable command should send");
    let command_retry = client_link
        .retry_due(&mut client_transport, 2)
        .expect("command should retry once");
    assert_eq!(command_retry.retried, 1);

    let first_command = server_transport
        .try_recv()
        .expect("server receive should work")
        .expect("first command packet should exist");
    let delivered_command = server_link
        .handle_inbound(&mut server_transport, first_command)
        .expect("first command packet should handle")
        .expect("first command should deliver");
    let RuntimeFrame::Command(command) = BinaryFrameDecoder
        .decode(&delivered_command.bytes)
        .expect("server should decode command")
    else {
        panic!("expected command frame");
    };
    let duplicate_command = server_transport
        .try_recv()
        .expect("server receive should work")
        .expect("duplicate command packet should exist");
    assert!(
        server_link
            .handle_inbound(&mut server_transport, duplicate_command)
            .expect("duplicate command packet should handle")
            .is_none()
    );

    let command_reliable_acks = drain_reliable_acks(&mut client_link, &mut client_transport);
    assert_eq!(command_reliable_acks, 2);
    assert_eq!(client_link.sender.in_flight_len(), 0);

    let mut queues = CommandQueues::new(CommandQueueLimits {
        high: 4,
        normal: 4,
        low: 4,
    });
    queues
        .push(
            command.clone().into_envelope(Tick::new(10)),
            CommandIngress::RUNNING,
        )
        .expect("command should enqueue");
    let applied = queues.pop_next().expect("command should be ready");
    assert_eq!(applied.id, command.command_id);
    assert_eq!(applied.priority, CommandPriority::High);

    let command_ack = CommandAckFrame {
        client_id,
        command_id: command.command_id,
        server_tick: Tick::new(10),
        accepted: true,
        reason_code: 0,
    };
    let mut ack_bytes = Vec::new();
    encoder
        .encode_command_ack(&command_ack, &mut ack_bytes)
        .expect("command ACK should encode");
    let ack_sequence = server_link
        .send(
            &mut server_transport,
            OutboundPacket {
                client_id,
                bytes: ack_bytes,
            },
            10,
        )
        .expect("reliable command ACK should send");
    let ack_retry = server_link
        .retry_due(&mut server_transport, 12)
        .expect("command ACK should retry once");
    assert_eq!(ack_retry.retried, 1);

    let first_ack = client_transport
        .try_recv()
        .expect("client receive should work")
        .expect("first command ACK packet should exist");
    let delivered_ack = client_link
        .handle_inbound(&mut client_transport, first_ack)
        .expect("first command ACK packet should handle")
        .expect("first command ACK should deliver");
    let decoded_ack = BinaryFrameDecoder
        .decode(&delivered_ack.bytes)
        .expect("client should decode command ACK");
    assert_eq!(decoded_ack, RuntimeFrame::CommandAck(command_ack));

    let duplicate_ack = client_transport
        .try_recv()
        .expect("client receive should work")
        .expect("duplicate command ACK packet should exist");
    assert!(
        client_link
            .handle_inbound(&mut client_transport, duplicate_ack)
            .expect("duplicate command ACK packet should handle")
            .is_none()
    );
    let ack_reliable_acks = drain_reliable_acks(&mut server_link, &mut server_transport);
    assert_eq!(ack_reliable_acks, 2);
    assert_eq!(server_link.sender.in_flight_len(), 0);

    println!(
        "reliable_command_ingress command_sequence={} command_retries={} command_duplicates={} command_reliable_acks={} ack_sequence={} ack_retries={} ack_duplicates={} ack_reliable_acks={} applied_command={}",
        command_sequence,
        command_retry.retried,
        server_link.receiver.stats().duplicates_suppressed,
        command_reliable_acks,
        ack_sequence,
        ack_retry.retried,
        client_link.receiver.stats().duplicates_suppressed,
        ack_reliable_acks,
        applied.id.get()
    );
}

fn drain_reliable_acks(
    endpoint: &mut ReliableClientEndpoint,
    transport: &mut InMemoryTransportEndpoint,
) -> usize {
    let mut count = 0;
    while let Some(packet) = transport
        .try_recv()
        .expect("reliable ACK receive should work")
    {
        assert!(
            endpoint
                .handle_inbound(transport, packet)
                .expect("reliable ACK should handle")
                .is_none()
        );
        count += 1;
    }
    count
}
