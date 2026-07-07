//! UDP loopback SDK example.

use std::thread;
use std::time::Duration;

use sectorsync_core::prelude::{ClientId, ComponentId, EntityId, OwnerEpoch, Tick};
use sectorsync_transport::{
    InboundPacket, OutboundPacket, TransportReceiver, TransportSink, UdpTransport,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, ComponentDelta, EntityDelta, FrameDecoder,
    FrameEncoder, ReplicationFrame, RuntimeFrame,
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

    let frame = ReplicationFrame {
        client_id,
        server_tick: Tick::new(12),
        entity_count: 1,
        estimated_payload_bytes: 4,
        entities: vec![EntityDelta {
            entity_id: EntityId::new(100),
            owner_epoch: OwnerEpoch::new(3),
            components: vec![ComponentDelta {
                component_id: ComponentId::new(1),
                version: 2,
                flags: 0,
                bytes: 100_u32.to_le_bytes().to_vec(),
            }],
        }],
    };

    let mut encoder = BinaryFrameEncoder;
    let mut bytes = Vec::new();
    encoder
        .encode_replication(&frame, &mut bytes)
        .expect("replication frame should encode");
    let sent_bytes = bytes.len();

    server
        .send(OutboundPacket { client_id, bytes })
        .expect("server should send replication frame");

    let inbound = recv_with_retry(&mut client);
    assert_eq!(inbound.client_id, Some(server_id));
    assert_eq!(inbound.remote_addr, server_addr);

    let decoded = BinaryFrameDecoder
        .decode(&inbound.bytes)
        .expect("client should decode replication frame");
    let RuntimeFrame::Replication(decoded) = decoded else {
        panic!("expected replication frame");
    };
    assert_eq!(decoded.client_id, client_id);
    assert_eq!(decoded.entities.len(), 1);
    assert_eq!(decoded.entities[0].components.len(), 1);

    println!(
        "udp_loopback bytes={} client={} entities={} components={}",
        sent_bytes,
        decoded.client_id.get(),
        decoded.entities.len(),
        decoded.entities[0].components.len()
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
