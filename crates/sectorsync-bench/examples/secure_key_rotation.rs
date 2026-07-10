//! Packet security key rotation SDK example.

use sectorsync_core::prelude::Tick;
use sectorsync_transport::{
    PacketAuthenticator, PacketKeyRing, PacketKeyRingConfig, PacketKeyRingError, PacketSecurityBox,
    PacketSecurityConfig, PacketSecurityEnvelope, PacketSecurityError, PlaintextPacketCipher,
};

fn main() {
    let config = PacketSecurityConfig {
        max_payload_bytes: 256,
        max_tag_bytes: 16,
        max_replay_history: 16,
    };
    let mut sender_ring = PacketKeyRing::new(PacketKeyRingConfig { max_keys: 4 });
    let mut receiver_ring = PacketKeyRing::new(PacketKeyRingConfig { max_keys: 4 });
    sender_ring
        .insert_active(100, Tick::new(10), 1)
        .expect("initial sender key should insert");
    receiver_ring
        .insert_active(100, Tick::new(10), 1)
        .expect("initial receiver key should insert");

    let mut sender = PacketSecurityBox::new(config, ExampleAuthenticator, PlaintextPacketCipher);
    let mut receiver = PacketSecurityBox::new(config, ExampleAuthenticator, PlaintextPacketCipher);

    let first_packet = sender
        .seal_with_key_ring(&sender_ring, b"client-command", Tick::new(10))
        .expect("first packet should seal");
    let first_key = PacketSecurityEnvelope::decode(config, &first_packet)
        .expect("first packet should decode")
        .key_id;
    let opened_first = receiver
        .open_with_key_ring(&receiver_ring, &first_packet, Tick::new(10))
        .expect("first packet should open");
    assert_eq!(opened_first, b"client-command");

    sender_ring
        .insert_active(200, Tick::new(20), 10)
        .expect("rotated sender key should insert");
    receiver_ring
        .insert_active(200, Tick::new(20), 10)
        .expect("rotated receiver key should insert");
    sender_ring
        .retire(100, Tick::new(20))
        .expect("old sender key should retire");
    receiver_ring
        .retire(100, Tick::new(20))
        .expect("old receiver key should retire");

    let rotated_packet = sender
        .seal_with_key_ring(&sender_ring, b"server-ack", Tick::new(20))
        .expect("rotated packet should seal");
    let rotated_key = PacketSecurityEnvelope::decode(config, &rotated_packet)
        .expect("rotated packet should decode")
        .key_id;
    let opened_rotated = receiver
        .open_with_key_ring(&receiver_ring, &rotated_packet, Tick::new(20))
        .expect("rotated packet should open");
    assert_eq!(opened_rotated, b"server-ack");

    let old_but_retiring = sender
        .seal_with_nonce(100, 99, b"late-old-key-packet")
        .expect("explicit old-key packet should seal");
    let opened_old = receiver
        .open_with_key_ring(&receiver_ring, &old_but_retiring, Tick::new(21))
        .expect("retiring key should still receive");
    assert_eq!(opened_old, b"late-old-key-packet");

    receiver_ring
        .revoke(100)
        .expect("old receiver key should revoke");
    let stale_packet = sender
        .seal_with_nonce(100, 100, b"stale-old-key-packet")
        .expect("stale old-key packet should seal");
    let rejected_key = match receiver
        .open_with_key_ring(&receiver_ring, &stale_packet, Tick::new(22))
        .expect_err("revoked key should be rejected")
    {
        PacketSecurityError::Key(PacketKeyRingError::KeyNotAccepted { key_id, .. }) => key_id,
        other => panic!("unexpected key rotation error: {other}"),
    };

    println!(
        "secure_key_rotation first_key={} rotated_key={} opened={} key_rejected={} rejected_key={}",
        first_key,
        rotated_key,
        receiver.stats().opened,
        receiver.stats().key_rejected,
        rejected_key
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
