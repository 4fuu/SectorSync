//! Secure command ingress SDK example.

use sectorsync_core::prelude::{
    ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, Tick,
};
use sectorsync_transport::{
    ClientTransportLimits, InMemoryTransportHub, OutboundPacket, PacketAuthenticator, PacketCipher,
    PacketSecurityBox, PacketSecurityConfig, PacketSecurityError, PacketSecurityOpenScratch,
    PacketSecurityScratch, TransportReceiver, TransportSink,
};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, CommandAckFrame, CommandFrame, FrameDecoder,
    FrameEncoder, RuntimeFrame,
};

#[allow(clippy::too_many_lines)]
fn main() {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let client_to_server_key = 100;
    let server_to_client_key = 200;

    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 8,
        max_packet_bytes: 512,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:21007".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:21000".parse().expect("server addr"))
        .expect("server endpoint should register");

    let security_config = PacketSecurityConfig {
        max_payload_bytes: 256,
        max_tag_bytes: 16,
        max_replay_history: 16,
    };
    let mut client_security =
        PacketSecurityBox::new(security_config, ExampleAuthenticator, ExampleCipher);
    let mut server_security =
        PacketSecurityBox::new(security_config, ExampleAuthenticator, ExampleCipher);
    let mut seal_scratch = PacketSecurityScratch::with_capacity(
        security_config.max_payload_bytes,
        security_config.max_tag_bytes,
    );
    let mut open_scratch =
        PacketSecurityOpenScratch::with_capacity(security_config.max_payload_bytes);

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
    let mut secure_command = Vec::with_capacity(256);
    client_security
        .seal_into(
            client_to_server_key,
            &command_bytes,
            &mut secure_command,
            &mut seal_scratch,
        )
        .expect("command should seal");
    client_transport
        .send(OutboundPacket {
            client_id: server_id,
            bytes: secure_command,
        })
        .expect("client should send secure command");

    let inbound = server_transport
        .try_recv()
        .expect("server receive should work")
        .expect("secure command packet should exist");
    assert_eq!(inbound.client_id, Some(client_id));
    let sealed_command = inbound.bytes.clone();
    let opened_command = server_security
        .open_with_scratch(&inbound.bytes, &mut open_scratch)
        .expect("server should open secure command");
    let RuntimeFrame::Command(command) = BinaryFrameDecoder
        .decode(opened_command.payload)
        .expect("server should decode command")
    else {
        panic!("expected command frame");
    };
    let replay = server_security
        .open(&sealed_command)
        .expect_err("duplicate secure command should replay-reject");
    match replay {
        PacketSecurityError::Replay { key_id, nonce } => {
            assert_eq!(key_id, client_to_server_key);
            assert_eq!(nonce, 1);
        }
        other => panic!("unexpected replay error: {other}"),
    }

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

    let ack = CommandAckFrame {
        client_id,
        command_id: command.command_id,
        server_tick: Tick::new(10),
        accepted: true,
        reason_code: 0,
    };
    let ack_command_id = ack.command_id;
    let mut ack_bytes = Vec::new();
    encoder
        .encode_command_ack(&ack, &mut ack_bytes)
        .expect("ack should encode");
    let mut secure_ack = Vec::with_capacity(256);
    server_security
        .seal_into(
            server_to_client_key,
            &ack_bytes,
            &mut secure_ack,
            &mut seal_scratch,
        )
        .expect("ack should seal");
    server_transport
        .send(OutboundPacket {
            client_id,
            bytes: secure_ack,
        })
        .expect("server should send secure ack");

    let inbound_ack = client_transport
        .try_recv()
        .expect("client receive should work")
        .expect("secure ACK should exist");
    assert_eq!(inbound_ack.client_id, Some(server_id));
    let opened_ack = client_security
        .open_with_scratch(&inbound_ack.bytes, &mut open_scratch)
        .expect("client should open secure ACK");
    let decoded_ack = BinaryFrameDecoder
        .decode(opened_ack.payload)
        .expect("client should decode ACK");
    assert_eq!(decoded_ack, RuntimeFrame::CommandAck(ack));

    println!(
        "secure_command_ingress external_authenticator=true external_cipher=true sealed={} opened={} replay_rejected={} applied_command={} ack_command={}",
        client_security.stats().sealed + server_security.stats().sealed,
        client_security.stats().opened + server_security.stats().opened,
        server_security.stats().replay_rejected,
        applied.id.get(),
        ack_command_id.get()
    );
}

#[derive(Clone, Debug, Default)]
struct ExampleAuthenticator;

impl PacketAuthenticator for ExampleAuthenticator {
    type Error = core::convert::Infallible;

    fn sign(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.extend_from_slice(&example_tag(key_id, nonce, payload));
        Ok(())
    }

    fn verify(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        tag: &[u8],
    ) -> Result<bool, Self::Error> {
        Ok(tag == example_tag(key_id, nonce, payload))
    }
}

/// Test-only reversible hook proving that encryption stays externally supplied.
/// This is not cryptography and must never be used outside this example.
#[derive(Clone, Debug, Default)]
struct ExampleCipher;

impl PacketCipher for ExampleCipher {
    type Error = core::convert::Infallible;

    fn seal(&mut self, key_id: u32, nonce: u64, payload: &mut Vec<u8>) -> Result<(), Self::Error> {
        apply_example_cipher(key_id, nonce, payload);
        Ok(())
    }

    fn open(&mut self, key_id: u32, nonce: u64, payload: &mut Vec<u8>) -> Result<(), Self::Error> {
        apply_example_cipher(key_id, nonce, payload);
        Ok(())
    }
}

fn apply_example_cipher(key_id: u32, nonce: u64, payload: &mut [u8]) {
    let mut stream = u64::from(key_id)
        .wrapping_mul(0xA24B_AED4_963E_E407)
        .wrapping_add(nonce.rotate_left(23));
    for byte in payload {
        stream ^= stream << 13;
        stream ^= stream >> 7;
        stream ^= stream << 17;
        *byte ^= stream.to_le_bytes()[0];
    }
}

fn example_tag(key_id: u32, nonce: u64, payload: &[u8]) -> [u8; 8] {
    let mut acc = u64::from(key_id)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(nonce.rotate_left(17));
    for (index, byte) in payload.iter().copied().enumerate() {
        acc = acc.rotate_left(5) ^ (u64::from(byte) << ((index % 8) * 8));
        acc = acc.wrapping_mul(0x1000_0000_01B3);
    }
    acc.to_le_bytes()
}
