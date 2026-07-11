//! Transport traits and fake transport support for `SectorSync`.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{Arc, Mutex};

use sectorsync_core::prelude::{ClientId, StationId, Tick};

const HASHED_BOUNDED_SET_MIN_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
enum BoundedLookupSet<K> {
    Ordered(BTreeSet<K>),
    Hashed(HashSet<K>),
}

impl<K: Copy + Eq + Hash + Ord> BoundedLookupSet<K> {
    fn new(max_entries: usize) -> Self {
        if max_entries >= HASHED_BOUNDED_SET_MIN_CAPACITY {
            Self::Hashed(HashSet::new())
        } else {
            Self::Ordered(BTreeSet::new())
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Ordered(entries) => entries.len(),
            Self::Hashed(entries) => entries.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Ordered(entries) => entries.is_empty(),
            Self::Hashed(entries) => entries.is_empty(),
        }
    }

    fn contains(&self, key: &K) -> bool {
        match self {
            Self::Ordered(entries) => entries.contains(key),
            Self::Hashed(entries) => entries.contains(key),
        }
    }

    fn insert(&mut self, key: K) {
        match self {
            Self::Ordered(entries) => {
                entries.insert(key);
            }
            Self::Hashed(entries) => {
                entries.insert(key);
            }
        }
    }

    fn remove(&mut self, key: &K) {
        match self {
            Self::Ordered(entries) => {
                entries.remove(key);
            }
            Self::Hashed(entries) => {
                entries.remove(key);
            }
        }
    }

    #[cfg(test)]
    fn is_hashed(&self) -> bool {
        matches!(self, Self::Hashed(_))
    }
}

/// Default maximum UDP datagram bytes read by `UdpTransport`.
pub const DEFAULT_UDP_RECV_BUFFER_SIZE: usize = 16 * 1024;

/// Outbound packet after wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboundPacket {
    /// Target client.
    pub client_id: ClientId,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Inbound packet before wire decoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundPacket {
    /// Known client id for `remote_addr`, if the transport has one registered.
    pub client_id: Option<ClientId>,
    /// Address the datagram came from.
    pub remote_addr: SocketAddr,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Borrowed inbound packet backed by a transport-owned receive buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InboundPacketRef<'a> {
    /// Known client id for `remote_addr`, if the transport has one registered.
    pub client_id: Option<ClientId>,
    /// Address the datagram came from.
    pub remote_addr: SocketAddr,
    /// Encoded bytes valid until the transport is mutably reused.
    pub bytes: &'a [u8],
}

impl InboundPacketRef<'_> {
    /// Materializes the compatible owned packet shape.
    pub fn to_owned(self) -> InboundPacket {
        InboundPacket {
            client_id: self.client_id,
            remote_addr: self.remote_addr,
            bytes: self.bytes.to_vec(),
        }
    }
}

/// Outbound station-to-station packet after wire encoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StationOutboundPacket {
    /// Source station.
    pub source_station: StationId,
    /// Target station.
    pub target_station: StationId,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Inbound station-to-station packet before wire decoding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StationInboundPacket {
    /// Source station.
    pub source_station: StationId,
    /// Target station.
    pub target_station: StationId,
    /// Encoded bytes.
    pub bytes: Vec<u8>,
}

/// Borrowed station packet backed by a transport-owned receive buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationInboundPacketRef<'a> {
    /// Source station resolved from the remote address.
    pub source_station: StationId,
    /// Target station owned by the receiving adapter.
    pub target_station: StationId,
    /// Encoded bytes valid until the transport is mutably reused.
    pub bytes: &'a [u8],
}

impl StationInboundPacketRef<'_> {
    /// Materializes the compatible owned packet shape.
    pub fn to_owned(self) -> StationInboundPacket {
        StationInboundPacket {
            source_station: self.source_station,
            target_station: self.target_station,
            bytes: self.bytes.to_vec(),
        }
    }
}

/// Batch of outbound station-to-station packets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StationPacketBatch {
    /// Packets to send together.
    pub packets: Vec<StationOutboundPacket>,
}

impl StationPacketBatch {
    /// Creates an empty station packet batch.
    pub const fn new() -> Self {
        Self {
            packets: Vec::new(),
        }
    }

    /// Adds one packet to the batch.
    pub fn push(&mut self, packet: StationOutboundPacket) {
        self.packets.push(packet);
    }

    /// Returns packet count.
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Returns total byte count.
    pub fn bytes_len(&self) -> usize {
        self.packets.iter().map(|packet| packet.bytes.len()).sum()
    }
}

/// Batch of outbound packets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PacketBatch {
    /// Packets to send together.
    pub packets: Vec<OutboundPacket>,
}

impl PacketBatch {
    /// Creates an empty batch.
    pub const fn new() -> Self {
        Self {
            packets: Vec::new(),
        }
    }

    /// Adds one packet to the batch.
    pub fn push(&mut self, packet: OutboundPacket) {
        self.packets.push(packet);
    }

    /// Returns packet count.
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Returns total byte count.
    pub fn bytes_len(&self) -> usize {
        self.packets.iter().map(|packet| packet.bytes.len()).sum()
    }
}

/// Packet sink abstraction. Real network transports should implement this at
/// batch boundaries rather than per-entity boundaries.
pub trait TransportSink {
    /// Transport error type.
    type Error;

    /// Sends one encoded packet.
    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error>;

    /// Sends a packet batch.
    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        for packet in batch.packets {
            self.send(packet)?;
        }
        Ok(())
    }
}

/// Non-blocking packet receive abstraction.
pub trait TransportReceiver {
    /// Transport error type.
    type Error;

    /// Attempts to receive one encoded packet.
    ///
    /// Implementations should return `Ok(None)` when no packet is currently
    /// available instead of blocking the caller's station tick.
    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error>;
}

const PACKET_SECURITY_MAGIC: [u8; 4] = *b"SSEC";
/// Packet security envelope header bytes before payload and tag.
pub const PACKET_SECURITY_HEADER_BYTES: usize = 22;
/// Default packet security payload budget aligned to the default packet budget
/// after envelope overhead.
pub const DEFAULT_PACKET_SECURITY_MAX_PAYLOAD_BYTES: usize =
    (16 * 1024) - PACKET_SECURITY_HEADER_BYTES;
/// Default maximum authentication tag bytes.
pub const DEFAULT_PACKET_SECURITY_MAX_TAG_BYTES: usize = 128;
/// Default replay history retained per security box.
pub const DEFAULT_PACKET_SECURITY_REPLAY_HISTORY: usize = 4096;
/// Default maximum packet keys tracked by one key ring.
pub const DEFAULT_PACKET_KEY_RING_MAX_KEYS: usize = 32;

/// Packet security framing configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketSecurityConfig {
    /// Maximum encrypted/authenticated payload bytes.
    pub max_payload_bytes: usize,
    /// Maximum authentication tag bytes.
    pub max_tag_bytes: usize,
    /// Maximum accepted `(key_id, nonce)` pairs retained for replay checks.
    pub max_replay_history: usize,
}

impl Default for PacketSecurityConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_PACKET_SECURITY_MAX_PAYLOAD_BYTES,
            max_tag_bytes: DEFAULT_PACKET_SECURITY_MAX_TAG_BYTES,
            max_replay_history: DEFAULT_PACKET_SECURITY_REPLAY_HISTORY,
        }
    }
}

/// Packet key lifecycle state tracked by `SectorSync` metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketKeyState {
    /// Key can be used for sending and receiving packets.
    Active,
    /// Key is accepted for receiving old packets but is not selected for send.
    Retiring,
    /// Key is explicitly rejected.
    Revoked,
}

impl PacketKeyState {
    fn can_send(self) -> bool {
        self == Self::Active
    }

    fn can_accept(self) -> bool {
        matches!(self, Self::Active | Self::Retiring)
    }
}

/// Metadata for one externally managed packet security key.
///
/// `SectorSync` never stores secret material. The descriptor only lets hot-path
/// packet helpers choose send keys and reject stale receive keys deterministically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketKeyDescriptor {
    /// External key identifier carried in packet security envelopes.
    pub key_id: u32,
    /// Current lifecycle state.
    pub state: PacketKeyState,
    /// Tick at which the embedding system created/imported the key metadata.
    pub created_at: Tick,
    /// First tick at which this key may be used or accepted.
    pub activated_at: Tick,
    /// Tick at which the key began retiring, if known.
    pub retires_at: Option<Tick>,
    /// First tick at which this key must be rejected and can be removed.
    pub expires_at: Option<Tick>,
    /// Higher values win when multiple active keys are eligible for send.
    pub send_priority: u32,
}

impl PacketKeyDescriptor {
    /// Creates active key metadata.
    pub const fn active(key_id: u32, now: Tick, send_priority: u32) -> Self {
        Self {
            key_id,
            state: PacketKeyState::Active,
            created_at: now,
            activated_at: now,
            retires_at: None,
            expires_at: None,
            send_priority,
        }
    }

    /// Returns a copy with an expiration tick.
    #[must_use]
    pub const fn with_expiry(mut self, expires_at: Tick) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Returns whether the key is active for sending at `now`.
    pub fn is_send_eligible(self, now: Tick) -> bool {
        self.state.can_send() && self.is_activated_at(now) && !self.is_expired_at(now)
    }

    /// Returns whether the key is accepted for receiving at `now`.
    pub fn is_accept_eligible(self, now: Tick) -> bool {
        self.state.can_accept() && self.is_activated_at(now) && !self.is_expired_at(now)
    }

    fn is_activated_at(self, now: Tick) -> bool {
        now >= self.activated_at
    }

    fn is_expired_at(self, now: Tick) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

/// Packet key ring configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketKeyRingConfig {
    /// Maximum key metadata records retained in the ring.
    pub max_keys: usize,
}

impl Default for PacketKeyRingConfig {
    fn default() -> Self {
        Self {
            max_keys: DEFAULT_PACKET_KEY_RING_MAX_KEYS,
        }
    }
}

/// Packet key ring maintenance statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PacketKeyRingStats {
    /// Key descriptors inserted.
    pub keys_inserted: usize,
    /// Key descriptors activated.
    pub keys_activated: usize,
    /// Key descriptors moved to receive-only retirement.
    pub keys_retired: usize,
    /// Key descriptors revoked.
    pub keys_revoked: usize,
    /// Expired key descriptors removed.
    pub keys_expired_removed: usize,
}

/// Packet key ring policy error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketKeyRingError {
    /// The configured key metadata capacity is full.
    CapacityFull {
        /// Configured capacity.
        capacity: usize,
    },
    /// Key id already exists.
    DuplicateKey(u32),
    /// Key id is not known.
    MissingKey(u32),
    /// No active, non-expired key can be selected for send.
    NoSendKey,
    /// Key cannot be used for sending.
    KeyNotSendable {
        /// Key id.
        key_id: u32,
        /// Current lifecycle state.
        state: PacketKeyState,
    },
    /// Key cannot be accepted for receiving.
    KeyNotAccepted {
        /// Key id.
        key_id: u32,
        /// Current lifecycle state.
        state: PacketKeyState,
    },
}

impl core::fmt::Display for PacketKeyRingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CapacityFull { capacity } => {
                write!(f, "packet key ring capacity full: capacity {capacity}")
            }
            Self::DuplicateKey(key_id) => write!(f, "packet key {key_id} already exists"),
            Self::MissingKey(key_id) => write!(f, "packet key {key_id} is missing"),
            Self::NoSendKey => f.write_str("no packet key is eligible for send"),
            Self::KeyNotSendable { key_id, state } => {
                write!(f, "packet key {key_id} is not sendable in state {state:?}")
            }
            Self::KeyNotAccepted { key_id, state } => {
                write!(f, "packet key {key_id} is not accepted in state {state:?}")
            }
        }
    }
}

impl std::error::Error for PacketKeyRingError {}

/// Bounded packet key lifecycle metadata.
#[derive(Clone, Debug)]
pub struct PacketKeyRing {
    config: PacketKeyRingConfig,
    keys: BTreeMap<u32, PacketKeyDescriptor>,
    stats: PacketKeyRingStats,
}

impl PacketKeyRing {
    /// Creates an empty key ring.
    pub fn new(config: PacketKeyRingConfig) -> Self {
        Self {
            config,
            keys: BTreeMap::new(),
            stats: PacketKeyRingStats::default(),
        }
    }

    /// Creates an empty key ring with default limits.
    pub fn with_defaults() -> Self {
        Self::new(PacketKeyRingConfig::default())
    }

    /// Returns configuration.
    pub const fn config(&self) -> PacketKeyRingConfig {
        self.config
    }

    /// Returns key metadata count.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Returns whether the ring has no key metadata.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns statistics.
    pub const fn stats(&self) -> PacketKeyRingStats {
        self.stats
    }

    /// Returns one key descriptor.
    pub fn get(&self, key_id: u32) -> Option<PacketKeyDescriptor> {
        self.keys.get(&key_id).copied()
    }

    /// Iterates key descriptors in key-id order.
    pub fn iter(&self) -> impl Iterator<Item = &PacketKeyDescriptor> {
        self.keys.values()
    }

    /// Inserts a descriptor if capacity and uniqueness allow it.
    pub fn insert(&mut self, descriptor: PacketKeyDescriptor) -> Result<(), PacketKeyRingError> {
        if self.keys.contains_key(&descriptor.key_id) {
            return Err(PacketKeyRingError::DuplicateKey(descriptor.key_id));
        }
        if self.keys.len() >= self.config.max_keys {
            return Err(PacketKeyRingError::CapacityFull {
                capacity: self.config.max_keys,
            });
        }
        self.keys.insert(descriptor.key_id, descriptor);
        self.stats.keys_inserted = self.stats.keys_inserted.saturating_add(1);
        Ok(())
    }

    /// Inserts active key metadata.
    pub fn insert_active(
        &mut self,
        key_id: u32,
        now: Tick,
        send_priority: u32,
    ) -> Result<(), PacketKeyRingError> {
        self.insert(PacketKeyDescriptor::active(key_id, now, send_priority))
    }

    /// Marks a key active and send-eligible from `activated_at`.
    pub fn activate(&mut self, key_id: u32, activated_at: Tick) -> Result<(), PacketKeyRingError> {
        let descriptor = self
            .keys
            .get_mut(&key_id)
            .ok_or(PacketKeyRingError::MissingKey(key_id))?;
        descriptor.state = PacketKeyState::Active;
        descriptor.activated_at = activated_at;
        descriptor.retires_at = None;
        self.stats.keys_activated = self.stats.keys_activated.saturating_add(1);
        Ok(())
    }

    /// Marks a key receive-only for retirement.
    pub fn retire(&mut self, key_id: u32, retires_at: Tick) -> Result<(), PacketKeyRingError> {
        let descriptor = self
            .keys
            .get_mut(&key_id)
            .ok_or(PacketKeyRingError::MissingKey(key_id))?;
        descriptor.state = PacketKeyState::Retiring;
        descriptor.retires_at = Some(retires_at);
        self.stats.keys_retired = self.stats.keys_retired.saturating_add(1);
        Ok(())
    }

    /// Marks a key rejected.
    pub fn revoke(&mut self, key_id: u32) -> Result<(), PacketKeyRingError> {
        let descriptor = self
            .keys
            .get_mut(&key_id)
            .ok_or(PacketKeyRingError::MissingKey(key_id))?;
        descriptor.state = PacketKeyState::Revoked;
        self.stats.keys_revoked = self.stats.keys_revoked.saturating_add(1);
        Ok(())
    }

    /// Updates the expiration tick for one key.
    pub fn set_expiry(
        &mut self,
        key_id: u32,
        expires_at: Option<Tick>,
    ) -> Result<(), PacketKeyRingError> {
        let descriptor = self
            .keys
            .get_mut(&key_id)
            .ok_or(PacketKeyRingError::MissingKey(key_id))?;
        descriptor.expires_at = expires_at;
        Ok(())
    }

    /// Removes expired descriptors and returns the removed count.
    pub fn remove_expired(&mut self, now: Tick) -> usize {
        let before = self.keys.len();
        self.keys
            .retain(|_, descriptor| !descriptor.is_expired_at(now));
        let removed = before.saturating_sub(self.keys.len());
        self.stats.keys_expired_removed = self.stats.keys_expired_removed.saturating_add(removed);
        removed
    }

    /// Selects the best active key for sending at `now`.
    pub fn select_send_key(&self, now: Tick) -> Result<PacketKeyDescriptor, PacketKeyRingError> {
        self.keys
            .values()
            .copied()
            .filter(|descriptor| descriptor.is_send_eligible(now))
            .max_by_key(|descriptor| {
                (
                    descriptor.send_priority,
                    descriptor.activated_at,
                    descriptor.key_id,
                )
            })
            .ok_or(PacketKeyRingError::NoSendKey)
    }

    /// Checks that `key_id` is accepted for receiving at `now`.
    pub fn accept_key(
        &self,
        key_id: u32,
        now: Tick,
    ) -> Result<PacketKeyDescriptor, PacketKeyRingError> {
        let descriptor = self
            .keys
            .get(&key_id)
            .copied()
            .ok_or(PacketKeyRingError::MissingKey(key_id))?;
        if descriptor.is_accept_eligible(now) {
            Ok(descriptor)
        } else {
            Err(PacketKeyRingError::KeyNotAccepted {
                key_id,
                state: descriptor.state,
            })
        }
    }
}

impl Default for PacketKeyRing {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Borrowed packet security envelope decoded directly from wire bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketSecurityEnvelopeRef<'a> {
    /// External key identifier chosen by the embedder.
    pub key_id: u32,
    /// Nonce scoped to `key_id`.
    pub nonce: u64,
    /// Borrowed ciphertext or plaintext payload.
    pub payload: &'a [u8],
    /// Borrowed authentication tag.
    pub tag: &'a [u8],
}

impl<'a> PacketSecurityEnvelopeRef<'a> {
    /// Decodes and validates an envelope without copying payload or tag bytes.
    pub fn decode(
        config: PacketSecurityConfig,
        input: &'a [u8],
    ) -> Result<Self, PacketSecurityDecodeError> {
        let mut cursor = SecurityCursor::new(input);
        let magic = cursor.read_array::<4>()?;
        if magic != PACKET_SECURITY_MAGIC {
            return Err(PacketSecurityDecodeError::BadMagic);
        }
        let key_id = cursor.read_u32()?;
        let nonce = cursor.read_u64()?;
        let payload_len = cursor.read_u32()? as usize;
        let tag_len = cursor.read_u16()? as usize;
        if payload_len > config.max_payload_bytes {
            return Err(PacketSecurityDecodeError::PayloadTooLarge {
                budget: config.max_payload_bytes,
                actual: payload_len,
            });
        }
        if tag_len > config.max_tag_bytes {
            return Err(PacketSecurityDecodeError::TagTooLarge {
                budget: config.max_tag_bytes,
                actual: tag_len,
            });
        }
        let payload = cursor.read_slice(payload_len)?;
        let tag = cursor.read_slice(tag_len)?;
        cursor.finish()?;
        Ok(Self {
            key_id,
            nonce,
            payload,
            tag,
        })
    }
}

/// Owned packet security envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PacketSecurityEnvelope {
    /// External key identifier chosen by the embedder.
    pub key_id: u32,
    /// Nonce scoped to `key_id`.
    pub nonce: u64,
    /// Ciphertext or plaintext payload, depending on the configured cipher.
    pub payload: Vec<u8>,
    /// Authentication tag produced by the configured authenticator.
    pub tag: Vec<u8>,
}

impl PacketSecurityEnvelope {
    /// Appends an envelope from borrowed payload and tag slices.
    pub fn encode_parts(
        config: PacketSecurityConfig,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        tag: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), PacketSecurityEncodeError> {
        if payload.len() > config.max_payload_bytes {
            return Err(PacketSecurityEncodeError::PayloadTooLarge {
                budget: config.max_payload_bytes,
                actual: payload.len(),
            });
        }
        if tag.len() > config.max_tag_bytes {
            return Err(PacketSecurityEncodeError::TagTooLarge {
                budget: config.max_tag_bytes,
                actual: tag.len(),
            });
        }

        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            PacketSecurityEncodeError::PayloadTooLarge {
                budget: config.max_payload_bytes,
                actual: payload.len(),
            }
        })?;
        let tag_len =
            u16::try_from(tag.len()).map_err(|_| PacketSecurityEncodeError::TagTooLarge {
                budget: config.max_tag_bytes,
                actual: tag.len(),
            })?;
        out.extend_from_slice(&PACKET_SECURITY_MAGIC);
        out.extend_from_slice(&key_id.to_le_bytes());
        out.extend_from_slice(&nonce.to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&tag_len.to_le_bytes());
        out.extend_from_slice(payload);
        out.extend_from_slice(tag);
        Ok(())
    }

    /// Encodes a packet security envelope.
    pub fn encode(
        &self,
        config: PacketSecurityConfig,
        out: &mut Vec<u8>,
    ) -> Result<(), PacketSecurityEncodeError> {
        Self::encode_parts(
            config,
            self.key_id,
            self.nonce,
            &self.payload,
            &self.tag,
            out,
        )
    }

    /// Decodes a packet security envelope.
    pub fn decode(
        config: PacketSecurityConfig,
        input: &[u8],
    ) -> Result<Self, PacketSecurityDecodeError> {
        let envelope = PacketSecurityEnvelopeRef::decode(config, input)?;
        Ok(Self {
            key_id: envelope.key_id,
            nonce: envelope.nonce,
            payload: envelope.payload.to_vec(),
            tag: envelope.tag.to_vec(),
        })
    }
}

/// Packet security encode error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketSecurityEncodeError {
    /// Payload exceeded configured budget.
    PayloadTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Authentication tag exceeded configured budget.
    TagTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for PacketSecurityEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PayloadTooLarge { budget, actual } => write!(
                f,
                "packet security payload exceeded byte budget: budget {budget}, actual {actual}"
            ),
            Self::TagTooLarge { budget, actual } => write!(
                f,
                "packet security tag exceeded byte budget: budget {budget}, actual {actual}"
            ),
        }
    }
}

impl std::error::Error for PacketSecurityEncodeError {}

/// Packet security decode error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PacketSecurityDecodeError {
    /// Frame magic did not match.
    BadMagic,
    /// Frame ended before all fields were available.
    Truncated {
        /// Required bytes.
        needed: usize,
        /// Available bytes.
        available: usize,
    },
    /// Payload exceeded configured budget.
    PayloadTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Authentication tag exceeded configured budget.
    TagTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Frame had trailing bytes after a complete envelope.
    TrailingBytes(usize),
}

impl core::fmt::Display for PacketSecurityDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic => f.write_str("bad packet security envelope magic"),
            Self::Truncated { needed, available } => {
                write!(
                    f,
                    "truncated packet security envelope: needed {needed}, available {available}"
                )
            }
            Self::PayloadTooLarge { budget, actual } => write!(
                f,
                "packet security payload exceeded byte budget: budget {budget}, actual {actual}"
            ),
            Self::TagTooLarge { budget, actual } => write!(
                f,
                "packet security tag exceeded byte budget: budget {budget}, actual {actual}"
            ),
            Self::TrailingBytes(bytes) => {
                write!(f, "packet security envelope has {bytes} trailing bytes")
            }
        }
    }
}

impl std::error::Error for PacketSecurityDecodeError {}

/// Packet authenticator hook. Embedders should provide a real MAC/signature
/// implementation and key management outside `SectorSync`.
pub trait PacketAuthenticator {
    /// Authenticator error type.
    type Error;

    /// Produces an authentication tag over `key_id`, `nonce`, and `payload`.
    fn sign(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Verifies an authentication tag over `key_id`, `nonce`, and `payload`.
    fn verify(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        tag: &[u8],
    ) -> Result<bool, Self::Error>;
}

/// Packet cipher hook. Embedders should provide real encryption outside
/// `SectorSync` when confidentiality is needed.
pub trait PacketCipher {
    /// Cipher error type.
    type Error;

    /// Seals a payload in place before authentication.
    fn seal(&mut self, key_id: u32, nonce: u64, payload: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Opens a payload in place after authentication.
    fn open(&mut self, key_id: u32, nonce: u64, payload: &mut Vec<u8>) -> Result<(), Self::Error>;
}

/// Explicit plaintext cipher for tests and integrations that only need
/// authentication framing.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlaintextPacketCipher;

impl PacketCipher for PlaintextPacketCipher {
    type Error = core::convert::Infallible;

    fn seal(
        &mut self,
        _key_id: u32,
        _nonce: u64,
        _payload: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn open(
        &mut self,
        _key_id: u32,
        _nonce: u64,
        _payload: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Bounded replay window for security envelopes.
#[derive(Clone, Debug)]
pub struct PacketReplayWindow {
    max_seen: usize,
    seen: BoundedLookupSet<(u32, u64)>,
    order: VecDeque<(u32, u64)>,
}

impl PacketReplayWindow {
    /// Creates an empty replay window.
    pub fn new(max_seen: usize) -> Self {
        Self {
            max_seen,
            seen: BoundedLookupSet::new(max_seen),
            order: VecDeque::new(),
        }
    }

    /// Returns retained nonce count.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Returns whether no nonces are retained.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Returns whether a `(key_id, nonce)` pair was already accepted.
    pub fn contains(&self, key_id: u32, nonce: u64) -> bool {
        self.seen.contains(&(key_id, nonce))
    }

    /// Records a `(key_id, nonce)` pair if it has not been seen.
    pub fn accept(&mut self, key_id: u32, nonce: u64) -> bool {
        if self.max_seen == 0 {
            return true;
        }

        let key = (key_id, nonce);
        if self.seen.contains(&key) {
            return false;
        }
        self.seen.insert(key);
        self.order.push_back(key);
        while self.order.len() > self.max_seen {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

impl Default for PacketReplayWindow {
    fn default() -> Self {
        Self::new(DEFAULT_PACKET_SECURITY_REPLAY_HISTORY)
    }
}

/// Packet security box statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PacketSecurityStats {
    /// Packets sealed.
    pub sealed: usize,
    /// Packets opened.
    pub opened: usize,
    /// Packets rejected by key lifecycle policy.
    pub key_rejected: usize,
    /// Packets rejected because authentication failed.
    pub auth_failed: usize,
    /// Packets rejected because the nonce was replayed.
    pub replay_rejected: usize,
}

/// Caller-owned reusable payload and tag storage for packet sealing.
#[derive(Clone, Debug, Default)]
pub struct PacketSecurityScratch {
    sealed_payload: Vec<u8>,
    tag: Vec<u8>,
}

impl PacketSecurityScratch {
    /// Creates empty sealing storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates sealing storage with explicit payload and tag capacities.
    pub fn with_capacity(payload_bytes: usize, tag_bytes: usize) -> Self {
        Self {
            sealed_payload: Vec::with_capacity(payload_bytes),
            tag: Vec::with_capacity(tag_bytes),
        }
    }

    /// Encrypted-payload bytes retained without growing the scratch buffer.
    pub fn retained_payload_capacity(&self) -> usize {
        self.sealed_payload.capacity()
    }

    /// Authentication-tag bytes retained without growing the scratch buffer.
    pub fn retained_tag_capacity(&self) -> usize {
        self.tag.capacity()
    }
}

/// Caller-owned reusable payload storage for packet opening.
#[derive(Clone, Debug, Default)]
pub struct PacketSecurityOpenScratch {
    payload: Vec<u8>,
}

impl PacketSecurityOpenScratch {
    /// Creates empty opening storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates opening storage with explicit payload capacity.
    pub fn with_capacity(payload_bytes: usize) -> Self {
        Self {
            payload: Vec::with_capacity(payload_bytes),
        }
    }

    /// Opened-payload bytes retained without growing the scratch buffer.
    pub fn retained_payload_capacity(&self) -> usize {
        self.payload.capacity()
    }
}

/// Borrowed result of opening one packet into caller-owned scratch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketSecurityOpenView<'a> {
    /// External key identifier carried by the envelope.
    pub key_id: u32,
    /// Accepted nonce carried by the envelope.
    pub nonce: u64,
    /// Opened payload, valid until the scratch is mutably reused.
    pub payload: &'a [u8],
}

/// Error produced by packet security helpers.
#[derive(Debug)]
pub enum PacketSecurityError<A, C> {
    /// Envelope encode failed.
    Encode(PacketSecurityEncodeError),
    /// Envelope decode failed.
    Decode(PacketSecurityDecodeError),
    /// Authenticator failed.
    Authenticator(A),
    /// Cipher failed.
    Cipher(C),
    /// Key lifecycle policy rejected the packet or send attempt.
    Key(PacketKeyRingError),
    /// Authenticator returned false.
    AuthenticationFailed {
        /// Key id.
        key_id: u32,
        /// Nonce.
        nonce: u64,
    },
    /// Replay window rejected the nonce.
    Replay {
        /// Key id.
        key_id: u32,
        /// Nonce.
        nonce: u64,
    },
}

impl<A: core::fmt::Display, C: core::fmt::Display> core::fmt::Display
    for PacketSecurityError<A, C>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::Authenticator(error) => write!(f, "{error}"),
            Self::Cipher(error) => write!(f, "{error}"),
            Self::Key(error) => write!(f, "{error}"),
            Self::AuthenticationFailed { key_id, nonce } => write!(
                f,
                "packet authentication failed for key {key_id} nonce {nonce}"
            ),
            Self::Replay { key_id, nonce } => {
                write!(f, "packet replay rejected for key {key_id} nonce {nonce}")
            }
        }
    }
}

impl<A, C> std::error::Error for PacketSecurityError<A, C>
where
    A: std::error::Error + 'static,
    C: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Authenticator(error) => Some(error),
            Self::Cipher(error) => Some(error),
            Self::Key(error) => Some(error),
            Self::AuthenticationFailed { .. } | Self::Replay { .. } => None,
        }
    }
}

/// Bounded packet security helper combining nonce allocation, authentication,
/// optional encryption, and replay checks.
#[derive(Clone, Debug)]
pub struct PacketSecurityBox<A, C> {
    config: PacketSecurityConfig,
    authenticator: A,
    cipher: C,
    replay: PacketReplayWindow,
    next_nonce: BTreeMap<u32, u64>,
    stats: PacketSecurityStats,
}

impl<A, C> PacketSecurityBox<A, C> {
    /// Creates a packet security box.
    pub fn new(config: PacketSecurityConfig, authenticator: A, cipher: C) -> Self {
        Self {
            config,
            authenticator,
            cipher,
            replay: PacketReplayWindow::new(config.max_replay_history),
            next_nonce: BTreeMap::new(),
            stats: PacketSecurityStats::default(),
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> PacketSecurityConfig {
        self.config
    }

    /// Returns statistics.
    pub const fn stats(&self) -> PacketSecurityStats {
        self.stats
    }

    /// Borrows the replay window.
    pub const fn replay(&self) -> &PacketReplayWindow {
        &self.replay
    }

    /// Consumes the box and returns its components.
    pub fn into_inner(self) -> (A, C, PacketReplayWindow) {
        (self.authenticator, self.cipher, self.replay)
    }
}

impl<A, C> PacketSecurityBox<A, C>
where
    A: PacketAuthenticator,
    C: PacketCipher,
{
    /// Seals and encodes one payload.
    pub fn seal(
        &mut self,
        key_id: u32,
        payload: &[u8],
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        let nonce = self.allocate_nonce(key_id);
        self.seal_with_nonce(key_id, nonce, payload)
    }

    /// Selects a send key from `key_ring`, then seals and encodes one payload.
    pub fn seal_with_key_ring(
        &mut self,
        key_ring: &PacketKeyRing,
        payload: &[u8],
        now: Tick,
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        let descriptor = key_ring
            .select_send_key(now)
            .map_err(PacketSecurityError::Key)?;
        self.seal(descriptor.key_id, payload)
    }

    /// Seals and encodes one payload with an explicit nonce.
    pub fn seal_with_nonce(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        let mut scratch = PacketSecurityScratch::with_capacity(
            payload.len().min(self.config.max_payload_bytes),
            0,
        );
        self.prepare_seal(key_id, nonce, payload, &mut scratch)?;
        let mut out = Vec::with_capacity(
            PACKET_SECURITY_HEADER_BYTES
                .saturating_add(scratch.sealed_payload.len())
                .saturating_add(scratch.tag.len()),
        );
        PacketSecurityEnvelope::encode_parts(
            self.config,
            key_id,
            nonce,
            &scratch.sealed_payload,
            &scratch.tag,
            &mut out,
        )
        .map_err(PacketSecurityError::Encode)?;
        self.stats.sealed = self.stats.sealed.saturating_add(1);
        Ok(out)
    }

    /// Seals and appends one payload using an allocated nonce and reusable storage.
    pub fn seal_into(
        &mut self,
        key_id: u32,
        payload: &[u8],
        out: &mut Vec<u8>,
        scratch: &mut PacketSecurityScratch,
    ) -> Result<u64, PacketSecurityError<A::Error, C::Error>> {
        let nonce = self.allocate_nonce(key_id);
        self.seal_with_nonce_into(key_id, nonce, payload, out, scratch)?;
        Ok(nonce)
    }

    /// Selects a key and appends one sealed payload using reusable storage.
    pub fn seal_with_key_ring_into(
        &mut self,
        key_ring: &PacketKeyRing,
        payload: &[u8],
        now: Tick,
        out: &mut Vec<u8>,
        scratch: &mut PacketSecurityScratch,
    ) -> Result<(u32, u64), PacketSecurityError<A::Error, C::Error>> {
        let descriptor = key_ring
            .select_send_key(now)
            .map_err(PacketSecurityError::Key)?;
        let nonce = self.seal_into(descriptor.key_id, payload, out, scratch)?;
        Ok((descriptor.key_id, nonce))
    }

    /// Seals and appends one payload with an explicit nonce and reusable storage.
    pub fn seal_with_nonce_into(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
        scratch: &mut PacketSecurityScratch,
    ) -> Result<(), PacketSecurityError<A::Error, C::Error>> {
        self.prepare_seal(key_id, nonce, payload, scratch)?;
        PacketSecurityEnvelope::encode_parts(
            self.config,
            key_id,
            nonce,
            &scratch.sealed_payload,
            &scratch.tag,
            out,
        )
        .map_err(PacketSecurityError::Encode)?;
        self.stats.sealed = self.stats.sealed.saturating_add(1);
        Ok(())
    }

    fn prepare_seal(
        &mut self,
        key_id: u32,
        nonce: u64,
        payload: &[u8],
        scratch: &mut PacketSecurityScratch,
    ) -> Result<(), PacketSecurityError<A::Error, C::Error>> {
        if payload.len() > self.config.max_payload_bytes {
            return Err(PacketSecurityError::Encode(
                PacketSecurityEncodeError::PayloadTooLarge {
                    budget: self.config.max_payload_bytes,
                    actual: payload.len(),
                },
            ));
        }
        scratch.sealed_payload.clear();
        scratch.sealed_payload.extend_from_slice(payload);
        self.cipher
            .seal(key_id, nonce, &mut scratch.sealed_payload)
            .map_err(PacketSecurityError::Cipher)?;
        scratch.tag.clear();
        self.authenticator
            .sign(key_id, nonce, &scratch.sealed_payload, &mut scratch.tag)
            .map_err(PacketSecurityError::Authenticator)?;
        Ok(())
    }

    /// Decodes, authenticates, replay-checks, and opens one payload.
    pub fn open(
        &mut self,
        input: &[u8],
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        let envelope = PacketSecurityEnvelopeRef::decode(self.config, input)
            .map_err(PacketSecurityError::Decode)?;
        self.open_borrowed_owned(envelope)
    }

    /// Decodes an envelope, validates its key against `key_ring`, then opens it.
    pub fn open_with_key_ring(
        &mut self,
        key_ring: &PacketKeyRing,
        input: &[u8],
        now: Tick,
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        let envelope = PacketSecurityEnvelopeRef::decode(self.config, input)
            .map_err(PacketSecurityError::Decode)?;
        if let Err(error) = key_ring.accept_key(envelope.key_id, now) {
            self.stats.key_rejected = self.stats.key_rejected.saturating_add(1);
            return Err(PacketSecurityError::Key(error));
        }
        self.open_borrowed_owned(envelope)
    }

    /// Opens one envelope into caller-owned reusable payload storage.
    pub fn open_with_scratch<'a>(
        &mut self,
        input: &[u8],
        scratch: &'a mut PacketSecurityOpenScratch,
    ) -> Result<PacketSecurityOpenView<'a>, PacketSecurityError<A::Error, C::Error>> {
        let envelope = PacketSecurityEnvelopeRef::decode(self.config, input)
            .map_err(PacketSecurityError::Decode)?;
        self.open_borrowed_with_scratch(envelope, scratch)
    }

    /// Validates a key-ring entry and opens into caller-owned reusable storage.
    pub fn open_with_key_ring_and_scratch<'a>(
        &mut self,
        key_ring: &PacketKeyRing,
        input: &[u8],
        now: Tick,
        scratch: &'a mut PacketSecurityOpenScratch,
    ) -> Result<PacketSecurityOpenView<'a>, PacketSecurityError<A::Error, C::Error>> {
        let envelope = PacketSecurityEnvelopeRef::decode(self.config, input)
            .map_err(PacketSecurityError::Decode)?;
        if let Err(error) = key_ring.accept_key(envelope.key_id, now) {
            self.stats.key_rejected = self.stats.key_rejected.saturating_add(1);
            return Err(PacketSecurityError::Key(error));
        }
        self.open_borrowed_with_scratch(envelope, scratch)
    }

    fn open_borrowed_owned(
        &mut self,
        envelope: PacketSecurityEnvelopeRef<'_>,
    ) -> Result<Vec<u8>, PacketSecurityError<A::Error, C::Error>> {
        self.verify_and_accept(envelope)?;
        let mut payload = envelope.payload.to_vec();
        self.cipher
            .open(envelope.key_id, envelope.nonce, &mut payload)
            .map_err(PacketSecurityError::Cipher)?;
        self.stats.opened = self.stats.opened.saturating_add(1);
        Ok(payload)
    }

    fn open_borrowed_with_scratch<'a>(
        &mut self,
        envelope: PacketSecurityEnvelopeRef<'_>,
        scratch: &'a mut PacketSecurityOpenScratch,
    ) -> Result<PacketSecurityOpenView<'a>, PacketSecurityError<A::Error, C::Error>> {
        self.verify_and_accept(envelope)?;
        scratch.payload.clear();
        scratch.payload.extend_from_slice(envelope.payload);
        self.cipher
            .open(envelope.key_id, envelope.nonce, &mut scratch.payload)
            .map_err(PacketSecurityError::Cipher)?;
        self.stats.opened = self.stats.opened.saturating_add(1);
        Ok(PacketSecurityOpenView {
            key_id: envelope.key_id,
            nonce: envelope.nonce,
            payload: &scratch.payload,
        })
    }

    fn verify_and_accept(
        &mut self,
        envelope: PacketSecurityEnvelopeRef<'_>,
    ) -> Result<(), PacketSecurityError<A::Error, C::Error>> {
        let verified = self
            .authenticator
            .verify(
                envelope.key_id,
                envelope.nonce,
                envelope.payload,
                envelope.tag,
            )
            .map_err(PacketSecurityError::Authenticator)?;
        if !verified {
            self.stats.auth_failed = self.stats.auth_failed.saturating_add(1);
            return Err(PacketSecurityError::AuthenticationFailed {
                key_id: envelope.key_id,
                nonce: envelope.nonce,
            });
        }
        if !self.replay.accept(envelope.key_id, envelope.nonce) {
            self.stats.replay_rejected = self.stats.replay_rejected.saturating_add(1);
            return Err(PacketSecurityError::Replay {
                key_id: envelope.key_id,
                nonce: envelope.nonce,
            });
        }
        Ok(())
    }

    fn allocate_nonce(&mut self, key_id: u32) -> u64 {
        let next = self.next_nonce.entry(key_id).or_insert(1);
        let nonce = *next;
        *next = next.saturating_add(1);
        nonce
    }
}

struct SecurityCursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> SecurityCursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_u16(&mut self) -> Result<u16, PacketSecurityDecodeError> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, PacketSecurityDecodeError> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, PacketSecurityDecodeError> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], PacketSecurityDecodeError> {
        self.require(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(&self.input[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], PacketSecurityDecodeError> {
        self.require(len)?;
        let bytes = &self.input[self.offset..self.offset + len];
        self.offset += len;
        Ok(bytes)
    }

    fn require(&self, count: usize) -> Result<(), PacketSecurityDecodeError> {
        let needed = self.offset.saturating_add(count);
        if needed > self.input.len() {
            Err(PacketSecurityDecodeError::Truncated {
                needed,
                available: self.input.len(),
            })
        } else {
            Ok(())
        }
    }

    fn finish(&self) -> Result<(), PacketSecurityDecodeError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(PacketSecurityDecodeError::TrailingBytes(
                self.input.len().saturating_sub(self.offset),
            ))
        }
    }
}

/// Bounded in-memory client transport limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientTransportLimits {
    /// Maximum queued packets per target client.
    pub max_queued_packets_per_client: usize,
    /// Maximum bytes accepted per packet.
    pub max_packet_bytes: usize,
}

impl Default for ClientTransportLimits {
    fn default() -> Self {
        Self {
            max_queued_packets_per_client: 4096,
            max_packet_bytes: 16 * 1024,
        }
    }
}

/// Statistics for the bounded in-memory client transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InMemoryTransportStats {
    /// Packets accepted for delivery.
    pub packets_sent: usize,
    /// Packets received by local endpoints.
    pub packets_received: usize,
    /// Bytes accepted for delivery.
    pub bytes_sent: usize,
    /// Bytes received by local endpoints.
    pub bytes_received: usize,
    /// Packets rejected because the target queue was full.
    pub packets_rejected_full: usize,
    /// Packets rejected because they exceeded the packet byte budget.
    pub packets_rejected_bytes: usize,
}

/// Error produced by bounded in-memory client transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InMemoryTransportError {
    /// Local endpoint has not been registered.
    MissingLocal(ClientId),
    /// Target client was not registered.
    MissingTarget(ClientId),
    /// Target client queue is full.
    QueueFull {
        /// Target client.
        client_id: ClientId,
        /// Configured queue capacity.
        capacity: usize,
    },
    /// Packet exceeded the byte budget.
    PacketTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Shared in-memory transport state was poisoned.
    Poisoned,
}

impl core::fmt::Display for InMemoryTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingLocal(client_id) => {
                write!(
                    f,
                    "in-memory transport local client {} is missing",
                    client_id.get()
                )
            }
            Self::MissingTarget(client_id) => {
                write!(
                    f,
                    "in-memory transport target client {} is missing",
                    client_id.get()
                )
            }
            Self::QueueFull {
                client_id,
                capacity,
            } => write!(
                f,
                "in-memory transport target client {} queue is full at capacity {capacity}",
                client_id.get()
            ),
            Self::PacketTooLarge { budget, actual } => {
                write!(
                    f,
                    "in-memory transport packet exceeded byte budget: budget {budget}, actual {actual}"
                )
            }
            Self::Poisoned => f.write_str("in-memory transport state is poisoned"),
        }
    }
}

impl std::error::Error for InMemoryTransportError {}

#[derive(Clone, Debug)]
struct InMemoryTransportClient {
    remote_addr: SocketAddr,
    queue: VecDeque<InboundPacket>,
}

#[derive(Debug)]
struct InMemoryTransportInner {
    limits: ClientTransportLimits,
    clients: BTreeMap<ClientId, InMemoryTransportClient>,
    addr_to_client: HashMap<SocketAddr, ClientId>,
    stats: InMemoryTransportStats,
}

/// Shared bounded in-memory client packet hub.
#[derive(Clone, Debug)]
pub struct InMemoryTransportHub {
    inner: Arc<Mutex<InMemoryTransportInner>>,
}

impl InMemoryTransportHub {
    /// Creates an empty in-memory transport hub.
    pub fn new(limits: ClientTransportLimits) -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryTransportInner {
                limits,
                clients: BTreeMap::new(),
                addr_to_client: HashMap::new(),
                stats: InMemoryTransportStats::default(),
            })),
        }
    }

    /// Registers a client endpoint address.
    pub fn register_client(
        &self,
        client_id: ClientId,
        remote_addr: SocketAddr,
    ) -> Result<Option<SocketAddr>, InMemoryTransportError> {
        let mut inner = self.lock_inner()?;
        let queue_capacity = inner.limits.max_queued_packets_per_client;
        let old_addr = inner.clients.insert(
            client_id,
            InMemoryTransportClient {
                remote_addr,
                queue: VecDeque::with_capacity(queue_capacity),
            },
        );
        let previous_addr = old_addr.map(|client| client.remote_addr);
        if let Some(previous_addr) = previous_addr {
            inner.addr_to_client.remove(&previous_addr);
        }
        if let Some(old_client) = inner.addr_to_client.insert(remote_addr, client_id)
            && old_client != client_id
        {
            inner.clients.remove(&old_client);
        }
        Ok(previous_addr)
    }

    /// Registers and returns a local endpoint for a client.
    pub fn endpoint(
        &self,
        client_id: ClientId,
        remote_addr: SocketAddr,
    ) -> Result<InMemoryTransportEndpoint, InMemoryTransportError> {
        self.register_client(client_id, remote_addr)?;
        Ok(self.endpoint_for_registered(client_id))
    }

    /// Returns an endpoint handle for a client that should already be
    /// registered.
    pub fn endpoint_for_registered(&self, client_id: ClientId) -> InMemoryTransportEndpoint {
        InMemoryTransportEndpoint {
            local_client_id: client_id,
            hub: self.clone(),
        }
    }

    /// Returns queued packet count for a client.
    pub fn queued_len(&self, client_id: ClientId) -> Result<Option<usize>, InMemoryTransportError> {
        let inner = self.lock_inner()?;
        Ok(inner
            .clients
            .get(&client_id)
            .map(|client| client.queue.len()))
    }

    /// Returns configured limits.
    pub fn limits(&self) -> Result<ClientTransportLimits, InMemoryTransportError> {
        let inner = self.lock_inner()?;
        Ok(inner.limits)
    }

    /// Returns transport statistics.
    pub fn stats(&self) -> Result<InMemoryTransportStats, InMemoryTransportError> {
        let inner = self.lock_inner()?;
        Ok(inner.stats)
    }

    fn lock_inner(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, InMemoryTransportInner>, InMemoryTransportError> {
        self.inner
            .lock()
            .map_err(|_| InMemoryTransportError::Poisoned)
    }
}

impl Default for InMemoryTransportHub {
    fn default() -> Self {
        Self::new(ClientTransportLimits::default())
    }
}

/// Local endpoint handle for a bounded in-memory client packet hub.
#[derive(Clone, Debug)]
pub struct InMemoryTransportEndpoint {
    local_client_id: ClientId,
    hub: InMemoryTransportHub,
}

impl InMemoryTransportEndpoint {
    /// Returns the local client id.
    pub const fn local_client_id(&self) -> ClientId {
        self.local_client_id
    }

    /// Returns the local endpoint address.
    pub fn local_addr(&self) -> Result<Option<SocketAddr>, InMemoryTransportError> {
        let inner = self.hub.lock_inner()?;
        Ok(inner
            .clients
            .get(&self.local_client_id)
            .map(|client| client.remote_addr))
    }
}

impl TransportSink for InMemoryTransportEndpoint {
    type Error = InMemoryTransportError;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        let actual = packet.bytes.len();
        let mut inner = self.hub.lock_inner()?;
        let limits = inner.limits;
        if actual > limits.max_packet_bytes {
            inner.stats.packets_rejected_bytes =
                inner.stats.packets_rejected_bytes.saturating_add(1);
            return Err(InMemoryTransportError::PacketTooLarge {
                budget: limits.max_packet_bytes,
                actual,
            });
        }

        let source_addr = inner
            .clients
            .get(&self.local_client_id)
            .ok_or(InMemoryTransportError::MissingLocal(self.local_client_id))?
            .remote_addr;
        let queue_len = inner
            .clients
            .get(&packet.client_id)
            .ok_or(InMemoryTransportError::MissingTarget(packet.client_id))?
            .queue
            .len();
        if queue_len >= limits.max_queued_packets_per_client {
            inner.stats.packets_rejected_full = inner.stats.packets_rejected_full.saturating_add(1);
            return Err(InMemoryTransportError::QueueFull {
                client_id: packet.client_id,
                capacity: limits.max_queued_packets_per_client,
            });
        }

        inner.stats.packets_sent = inner.stats.packets_sent.saturating_add(1);
        inner.stats.bytes_sent = inner.stats.bytes_sent.saturating_add(actual);
        let target = inner
            .clients
            .get_mut(&packet.client_id)
            .ok_or(InMemoryTransportError::MissingTarget(packet.client_id))?;
        target.queue.push_back(InboundPacket {
            client_id: Some(self.local_client_id),
            remote_addr: source_addr,
            bytes: packet.bytes,
        });
        Ok(())
    }
}

impl TransportReceiver for InMemoryTransportEndpoint {
    type Error = InMemoryTransportError;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        let mut inner = self.hub.lock_inner()?;
        let local = inner
            .clients
            .get_mut(&self.local_client_id)
            .ok_or(InMemoryTransportError::MissingLocal(self.local_client_id))?;
        let Some(packet) = local.queue.pop_front() else {
            return Ok(None);
        };
        inner.stats.packets_received = inner.stats.packets_received.saturating_add(1);
        inner.stats.bytes_received = inner
            .stats
            .bytes_received
            .saturating_add(packet.bytes.len());
        Ok(Some(packet))
    }
}

const RELIABLE_CLIENT_MAGIC: [u8; 4] = *b"SSCR";
const RELIABLE_CLIENT_KIND_DATA: u8 = 0;
const RELIABLE_CLIENT_KIND_ACK: u8 = 1;
/// Reliable client data frame header bytes before payload.
pub const RELIABLE_CLIENT_DATA_HEADER_BYTES: usize = 17;
/// Reliable client ACK frame bytes.
pub const RELIABLE_CLIENT_ACK_BYTES: usize = 13;
/// Default reliable client payload budget aligned to the default packet budget
/// after reliable header overhead.
pub const DEFAULT_RELIABLE_CLIENT_MAX_PAYLOAD_BYTES: usize =
    (16 * 1024) - RELIABLE_CLIENT_DATA_HEADER_BYTES;
/// Default duplicate-suppression history retained per reliable client endpoint.
pub const DEFAULT_RELIABLE_CLIENT_DELIVERED_HISTORY: usize = 4096;

/// Bounded reliable client link configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReliableClientConfig {
    /// Maximum in-flight reliable packets per peer client.
    pub max_in_flight_per_peer: usize,
    /// Number of ticks to wait before retrying an unacknowledged packet.
    pub retry_after_ticks: u64,
    /// Maximum send attempts before dropping an in-flight packet.
    pub max_attempts: u8,
    /// Maximum payload bytes before reliable envelope overhead.
    pub max_payload_bytes: usize,
    /// Maximum recently delivered packet ids retained for duplicate
    /// suppression. Set to zero to disable duplicate suppression history.
    pub max_delivered_history: usize,
}

impl Default for ReliableClientConfig {
    fn default() -> Self {
        Self {
            max_in_flight_per_peer: 1024,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: DEFAULT_RELIABLE_CLIENT_MAX_PAYLOAD_BYTES,
            max_delivered_history: DEFAULT_RELIABLE_CLIENT_DELIVERED_HISTORY,
        }
    }
}

/// Reliable client endpoint statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReliableClientStats {
    /// New reliable data packets sent.
    pub data_sent: usize,
    /// Retry data packets sent.
    pub retries_sent: usize,
    /// ACK packets sent.
    pub acks_sent: usize,
    /// ACK packets received.
    pub acks_received: usize,
    /// Unique data packets delivered to the caller.
    pub data_delivered: usize,
    /// Duplicate data packets suppressed.
    pub duplicates_suppressed: usize,
    /// In-flight packets dropped after exhausting attempts.
    pub timed_out: usize,
}

/// Encoded reliable client frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReliableClientFrame {
    /// Reliable data packet.
    Data {
        /// Sender-local sequence number scoped to the peer client.
        sequence: u64,
        /// Original packet payload.
        payload: Vec<u8>,
    },
    /// Acknowledgement for a reliable data packet.
    Ack {
        /// Acknowledged sequence number.
        sequence: u64,
    },
}

/// Borrowed reliable client frame decoded from caller-owned bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableClientFrameRef<'a> {
    /// Reliable data packet with borrowed payload bytes.
    Data {
        /// Sender-local sequence number scoped to the peer client.
        sequence: u64,
        /// Payload borrowed from the encoded frame.
        payload: &'a [u8],
    },
    /// Acknowledgement for a reliable data packet.
    Ack {
        /// Acknowledged sequence number.
        sequence: u64,
    },
}

impl ReliableClientFrameRef<'_> {
    /// Materializes the compatible owned reliable client frame.
    pub fn to_owned(self) -> ReliableClientFrame {
        match self {
            Self::Data { sequence, payload } => ReliableClientFrame::Data {
                sequence,
                payload: payload.to_vec(),
            },
            Self::Ack { sequence } => ReliableClientFrame::Ack { sequence },
        }
    }
}

impl ReliableClientFrame {
    /// Appends a data frame from a borrowed payload without materializing an owned frame.
    pub fn encode_data(
        sequence: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), ReliableClientEncodeError> {
        out.extend_from_slice(&RELIABLE_CLIENT_MAGIC);
        out.push(RELIABLE_CLIENT_KIND_DATA);
        out.extend_from_slice(&sequence.to_le_bytes());
        let len = u32::try_from(payload.len()).map_err(|_| {
            ReliableClientEncodeError::PayloadTooLarge {
                actual: payload.len(),
            }
        })?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(payload);
        Ok(())
    }

    /// Encodes a reliable client frame.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ReliableClientEncodeError> {
        match self {
            Self::Data { sequence, payload } => Self::encode_data(*sequence, payload, out),
            Self::Ack { sequence } => {
                out.extend_from_slice(&RELIABLE_CLIENT_MAGIC);
                out.push(RELIABLE_CLIENT_KIND_ACK);
                out.extend_from_slice(&sequence.to_le_bytes());
                Ok(())
            }
        }
    }

    /// Decodes a reliable client frame.
    pub fn decode(input: &[u8]) -> Result<Self, ReliableClientDecodeError> {
        Self::decode_ref(input).map(ReliableClientFrameRef::to_owned)
    }

    /// Decodes a reliable client frame while borrowing data payload bytes.
    pub fn decode_ref(
        input: &[u8],
    ) -> Result<ReliableClientFrameRef<'_>, ReliableClientDecodeError> {
        let mut cursor = ReliableCursor::new(input);
        let magic = cursor
            .read_array::<4>()
            .map_err(ReliableClientDecodeError::from_station_decode)?;
        if magic != RELIABLE_CLIENT_MAGIC {
            return Err(ReliableClientDecodeError::BadMagic);
        }
        let kind = cursor
            .read_u8()
            .map_err(ReliableClientDecodeError::from_station_decode)?;
        let sequence = cursor
            .read_u64()
            .map_err(ReliableClientDecodeError::from_station_decode)?;
        let frame = match kind {
            RELIABLE_CLIENT_KIND_DATA => {
                let len = cursor
                    .read_u32()
                    .map_err(ReliableClientDecodeError::from_station_decode)?
                    as usize;
                let payload = cursor
                    .read_slice(len)
                    .map_err(ReliableClientDecodeError::from_station_decode)?;
                ReliableClientFrameRef::Data { sequence, payload }
            }
            RELIABLE_CLIENT_KIND_ACK => ReliableClientFrameRef::Ack { sequence },
            other => return Err(ReliableClientDecodeError::UnknownKind(other)),
        };
        cursor
            .finish()
            .map_err(ReliableClientDecodeError::from_station_decode)?;
        Ok(frame)
    }
}

/// Reliable client frame encode error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableClientEncodeError {
    /// Payload length exceeded `u32::MAX`.
    PayloadTooLarge {
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for ReliableClientEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PayloadTooLarge { actual } => {
                write!(f, "reliable client payload too large: {actual} bytes")
            }
        }
    }
}

impl std::error::Error for ReliableClientEncodeError {}

/// Reliable client frame decode error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableClientDecodeError {
    /// Frame magic did not match.
    BadMagic,
    /// Frame kind byte is unknown.
    UnknownKind(u8),
    /// Frame ended before all fields were available.
    Truncated {
        /// Required bytes.
        needed: usize,
        /// Available bytes.
        available: usize,
    },
    /// Frame had trailing bytes after a complete payload.
    TrailingBytes(usize),
}

impl ReliableClientDecodeError {
    fn from_station_decode(error: ReliableStationDecodeError) -> Self {
        match error {
            ReliableStationDecodeError::BadMagic => Self::BadMagic,
            ReliableStationDecodeError::UnknownKind(kind) => Self::UnknownKind(kind),
            ReliableStationDecodeError::Truncated { needed, available } => {
                Self::Truncated { needed, available }
            }
            ReliableStationDecodeError::TrailingBytes(bytes) => Self::TrailingBytes(bytes),
        }
    }
}

impl core::fmt::Display for ReliableClientDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic => f.write_str("bad reliable client frame magic"),
            Self::UnknownKind(kind) => write!(f, "unknown reliable client frame kind {kind}"),
            Self::Truncated { needed, available } => {
                write!(
                    f,
                    "truncated reliable client frame: needed {needed}, available {available}"
                )
            }
            Self::TrailingBytes(bytes) => {
                write!(f, "reliable client frame has {bytes} trailing bytes")
            }
        }
    }
}

impl std::error::Error for ReliableClientDecodeError {}

/// Error produced by reliable client endpoints.
#[derive(Debug)]
pub enum ReliableClientError<E> {
    /// Underlying transport failed.
    Transport(E),
    /// Inbound packet did not contain a source client id.
    MissingSourceClient,
    /// Payload exceeded configured byte budget.
    PayloadTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Peer client in-flight window is full.
    WindowFull {
        /// Peer client.
        peer_client: ClientId,
        /// Configured in-flight capacity.
        capacity: usize,
    },
    /// Reliable frame encode failed.
    Encode(ReliableClientEncodeError),
    /// Reliable frame decode failed.
    Decode(ReliableClientDecodeError),
}

impl<E: core::fmt::Display> core::fmt::Display for ReliableClientError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::MissingSourceClient => f.write_str("reliable client packet source is unknown"),
            Self::PayloadTooLarge { budget, actual } => {
                write!(
                    f,
                    "reliable client payload exceeded byte budget: budget {budget}, actual {actual}"
                )
            }
            Self::WindowFull {
                peer_client,
                capacity,
            } => write!(
                f,
                "reliable client peer {} window is full at capacity {capacity}",
                peer_client.get()
            ),
            Self::Encode(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for ReliableClientError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::MissingSourceClient | Self::PayloadTooLarge { .. } | Self::WindowFull { .. } => {
                None
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InFlightReliableClientPacket {
    peer_client: ClientId,
    sequence: u64,
    payload: Vec<u8>,
    first_sent_tick: u64,
    last_sent_tick: u64,
    attempts: u8,
}

/// Caller-owned reusable storage for reliable client retry scans.
#[derive(Clone, Debug, Default)]
pub struct ReliableClientRetryScratch {
    due_keys: Vec<(ClientId, u64)>,
}

impl ReliableClientRetryScratch {
    /// Creates empty retry storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Due-key capacity retained across retry passes.
    pub fn retained_key_capacity(&self) -> usize {
        self.due_keys.capacity()
    }
}

/// Bounded reliable client sender state.
#[derive(Clone, Debug)]
pub struct ReliableClientSender {
    config: ReliableClientConfig,
    next_sequence: BTreeMap<ClientId, u64>,
    in_flight: BTreeMap<(ClientId, u64), InFlightReliableClientPacket>,
    in_flight_by_peer: BTreeMap<ClientId, usize>,
    stats: ReliableClientStats,
}

impl ReliableClientSender {
    /// Creates a reliable client sender.
    pub fn new(config: ReliableClientConfig) -> Self {
        Self {
            config,
            next_sequence: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            in_flight_by_peer: BTreeMap::new(),
            stats: ReliableClientStats::default(),
        }
    }

    /// Returns sender configuration.
    pub const fn config(&self) -> ReliableClientConfig {
        self.config
    }

    /// Returns sender statistics.
    pub const fn stats(&self) -> ReliableClientStats {
        self.stats
    }

    /// Returns total in-flight reliable packets.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Returns in-flight reliable packets for one peer client.
    pub fn in_flight_for(&self, peer_client: ClientId) -> usize {
        self.in_flight_by_peer
            .get(&peer_client)
            .copied()
            .unwrap_or(0)
    }

    /// Sends a new reliable packet and stores it until acknowledged or timed
    /// out.
    pub fn send<T: TransportSink>(
        &mut self,
        transport: &mut T,
        packet: OutboundPacket,
        now_tick: u64,
    ) -> Result<u64, ReliableClientError<T::Error>> {
        self.validate_payload(packet.bytes.len())?;
        if self.in_flight_for(packet.client_id) >= self.config.max_in_flight_per_peer {
            return Err(ReliableClientError::WindowFull {
                peer_client: packet.client_id,
                capacity: self.config.max_in_flight_per_peer,
            });
        }

        let sequence = self.allocate_sequence(packet.client_id);
        Self::send_data_frame(transport, packet.client_id, sequence, &packet.bytes)?;
        self.insert_in_flight(
            (packet.client_id, sequence),
            InFlightReliableClientPacket {
                peer_client: packet.client_id,
                sequence,
                payload: packet.bytes,
                first_sent_tick: now_tick,
                last_sent_tick: now_tick,
                attempts: 1,
            },
        );
        self.stats.data_sent = self.stats.data_sent.saturating_add(1);
        Ok(sequence)
    }

    /// Processes an ACK from `ack_source_client`.
    pub fn acknowledge(&mut self, ack_source_client: ClientId, sequence: u64) -> bool {
        let removed = self
            .remove_in_flight(&(ack_source_client, sequence))
            .is_some();
        if removed {
            self.stats.acks_received = self.stats.acks_received.saturating_add(1);
        }
        removed
    }

    /// Retries due in-flight packets.
    pub fn retry_due<T: TransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
    ) -> Result<ReliableRetryReport, ReliableClientError<T::Error>> {
        self.retry_due_with_scratch(
            transport,
            now_tick,
            &mut ReliableClientRetryScratch::default(),
        )
    }

    /// Retries due packets using caller-owned scan storage.
    pub fn retry_due_with_scratch<T: TransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
        scratch: &mut ReliableClientRetryScratch,
    ) -> Result<ReliableRetryReport, ReliableClientError<T::Error>> {
        scratch.due_keys.clear();
        scratch
            .due_keys
            .extend(self.in_flight.iter().filter_map(|(key, packet)| {
                let due =
                    now_tick.saturating_sub(packet.last_sent_tick) >= self.config.retry_after_ticks;
                due.then_some(*key)
            }));
        let mut report = ReliableRetryReport::default();

        for key in &scratch.due_keys {
            let Some(packet) = self.in_flight.get(key) else {
                continue;
            };
            if packet.attempts >= self.config.max_attempts {
                self.remove_in_flight(key);
                self.stats.timed_out = self.stats.timed_out.saturating_add(1);
                report.timed_out = report.timed_out.saturating_add(1);
                continue;
            }

            Self::send_data_frame(
                transport,
                packet.peer_client,
                packet.sequence,
                &packet.payload,
            )?;
            if let Some(stored) = self.in_flight.get_mut(key) {
                stored.last_sent_tick = now_tick;
                stored.attempts = stored.attempts.saturating_add(1);
            }
            self.stats.retries_sent = self.stats.retries_sent.saturating_add(1);
            report.retried = report.retried.saturating_add(1);
        }

        Ok(report)
    }

    fn validate_payload<E>(&self, bytes: usize) -> Result<(), ReliableClientError<E>> {
        if bytes > self.config.max_payload_bytes {
            Err(ReliableClientError::PayloadTooLarge {
                budget: self.config.max_payload_bytes,
                actual: bytes,
            })
        } else {
            Ok(())
        }
    }

    fn allocate_sequence(&mut self, peer_client: ClientId) -> u64 {
        let next = self.next_sequence.entry(peer_client).or_insert(1);
        let sequence = *next;
        *next = next.saturating_add(1);
        sequence
    }

    fn insert_in_flight(&mut self, key: (ClientId, u64), packet: InFlightReliableClientPacket) {
        if self.in_flight.insert(key, packet).is_none() {
            let count = self.in_flight_by_peer.entry(key.0).or_insert(0);
            *count = count.saturating_add(1);
        }
    }

    fn remove_in_flight(&mut self, key: &(ClientId, u64)) -> Option<InFlightReliableClientPacket> {
        let removed = self.in_flight.remove(key)?;
        if let Some(count) = self.in_flight_by_peer.get_mut(&removed.peer_client) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight_by_peer.remove(&removed.peer_client);
            }
        }
        Some(removed)
    }

    fn send_data_frame<T: TransportSink>(
        transport: &mut T,
        peer_client: ClientId,
        sequence: u64,
        payload: &[u8],
    ) -> Result<(), ReliableClientError<T::Error>> {
        let mut bytes = Vec::with_capacity(
            payload
                .len()
                .saturating_add(RELIABLE_CLIENT_DATA_HEADER_BYTES),
        );
        ReliableClientFrame::encode_data(sequence, payload, &mut bytes)
            .map_err(ReliableClientError::Encode)?;
        transport
            .send(OutboundPacket {
                client_id: peer_client,
                bytes,
            })
            .map_err(ReliableClientError::Transport)
    }
}

impl Default for ReliableClientSender {
    fn default() -> Self {
        Self::new(ReliableClientConfig::default())
    }
}

/// Bounded reliable client receiver state.
#[derive(Clone, Debug)]
pub struct ReliableClientReceiver {
    config: ReliableClientConfig,
    delivered: BoundedLookupSet<(ClientId, u64)>,
    delivered_order: VecDeque<(ClientId, u64)>,
    stats: ReliableClientStats,
}

impl ReliableClientReceiver {
    /// Creates a bounded reliable client receiver.
    pub fn new(config: ReliableClientConfig) -> Self {
        Self {
            config,
            delivered: BoundedLookupSet::new(config.max_delivered_history),
            delivered_order: VecDeque::new(),
            stats: ReliableClientStats::default(),
        }
    }

    /// Returns receiver configuration.
    pub const fn config(&self) -> ReliableClientConfig {
        self.config
    }

    /// Returns receiver statistics.
    pub const fn stats(&self) -> ReliableClientStats {
        self.stats
    }

    /// Handles a reliable data packet, sends an ACK, and returns a payload only
    /// for first delivery.
    pub fn handle_data<T: TransportSink>(
        &mut self,
        transport: &mut T,
        packet: InboundPacket,
        source_client: ClientId,
        sequence: u64,
        payload: Vec<u8>,
    ) -> Result<Option<InboundPacket>, ReliableClientError<T::Error>> {
        let InboundPacket {
            remote_addr,
            bytes: wire_bytes,
            ..
        } = packet;
        drop(wire_bytes);
        self.send_ack(transport, source_client, sequence)?;
        if !self.record_unique(source_client, sequence) {
            self.stats.duplicates_suppressed = self.stats.duplicates_suppressed.saturating_add(1);
            return Ok(None);
        }

        self.stats.data_delivered = self.stats.data_delivered.saturating_add(1);
        Ok(Some(InboundPacket {
            client_id: Some(source_client),
            remote_addr,
            bytes: payload,
        }))
    }

    fn send_ack<T: TransportSink>(
        &mut self,
        transport: &mut T,
        target_client: ClientId,
        sequence: u64,
    ) -> Result<(), ReliableClientError<T::Error>> {
        let mut bytes = Vec::with_capacity(RELIABLE_CLIENT_ACK_BYTES);
        ReliableClientFrame::Ack { sequence }
            .encode(&mut bytes)
            .map_err(ReliableClientError::Encode)?;
        transport
            .send(OutboundPacket {
                client_id: target_client,
                bytes,
            })
            .map_err(ReliableClientError::Transport)?;
        self.stats.acks_sent = self.stats.acks_sent.saturating_add(1);
        Ok(())
    }

    fn record_unique(&mut self, source_client: ClientId, sequence: u64) -> bool {
        if self.config.max_delivered_history == 0 {
            return true;
        }

        let key = (source_client, sequence);
        if self.delivered.contains(&key) {
            return false;
        }

        self.delivered.insert(key);
        self.delivered_order.push_back(key);
        while self.delivered_order.len() > self.config.max_delivered_history {
            if let Some(old) = self.delivered_order.pop_front() {
                self.delivered.remove(&old);
            }
        }
        true
    }
}

impl Default for ReliableClientReceiver {
    fn default() -> Self {
        Self::new(ReliableClientConfig::default())
    }
}

/// Reliable client endpoint combining sender and receiver state.
#[derive(Clone, Debug)]
pub struct ReliableClientEndpoint {
    /// Sender state.
    pub sender: ReliableClientSender,
    /// Receiver state.
    pub receiver: ReliableClientReceiver,
}

impl ReliableClientEndpoint {
    /// Creates a reliable client endpoint.
    pub fn new(config: ReliableClientConfig) -> Self {
        Self {
            sender: ReliableClientSender::new(config),
            receiver: ReliableClientReceiver::new(config),
        }
    }

    /// Sends a new reliable packet to a peer client.
    pub fn send<T: TransportSink>(
        &mut self,
        transport: &mut T,
        packet: OutboundPacket,
        now_tick: u64,
    ) -> Result<u64, ReliableClientError<T::Error>> {
        self.sender.send(transport, packet, now_tick)
    }

    /// Retries due reliable client packets.
    pub fn retry_due<T: TransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
    ) -> Result<ReliableRetryReport, ReliableClientError<T::Error>> {
        self.sender.retry_due(transport, now_tick)
    }

    /// Retries due reliable client packets using caller-owned scan storage.
    pub fn retry_due_with_scratch<T: TransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
        scratch: &mut ReliableClientRetryScratch,
    ) -> Result<ReliableRetryReport, ReliableClientError<T::Error>> {
        self.sender
            .retry_due_with_scratch(transport, now_tick, scratch)
    }

    /// Handles one inbound reliable client packet.
    pub fn handle_inbound<T: TransportSink>(
        &mut self,
        transport: &mut T,
        packet: InboundPacket,
    ) -> Result<Option<InboundPacket>, ReliableClientError<T::Error>> {
        let source_client = packet
            .client_id
            .ok_or(ReliableClientError::MissingSourceClient)?;
        match ReliableClientFrame::decode_ref(&packet.bytes).map_err(ReliableClientError::Decode)? {
            ReliableClientFrameRef::Data { sequence, payload } => {
                let payload_len = payload.len();
                let payload_offset = packet.bytes.len().saturating_sub(payload_len);
                let InboundPacket {
                    client_id,
                    remote_addr,
                    bytes,
                } = packet;
                let payload = reuse_reliable_payload(bytes, payload_offset, payload_len);
                self.receiver.handle_data(
                    transport,
                    InboundPacket {
                        client_id,
                        remote_addr,
                        bytes: Vec::new(),
                    },
                    source_client,
                    sequence,
                    payload,
                )
            }
            ReliableClientFrameRef::Ack { sequence } => {
                self.sender.acknowledge(source_client, sequence);
                Ok(None)
            }
        }
    }
}

impl Default for ReliableClientEndpoint {
    fn default() -> Self {
        Self::new(ReliableClientConfig::default())
    }
}

/// Station-to-station packet sink abstraction.
pub trait StationTransportSink {
    /// Transport error type.
    type Error;

    /// Sends one encoded station packet.
    fn send_station(&mut self, packet: StationOutboundPacket) -> Result<(), Self::Error>;

    /// Sends a station packet batch.
    fn send_station_batch(&mut self, batch: StationPacketBatch) -> Result<(), Self::Error> {
        for packet in batch.packets {
            self.send_station(packet)?;
        }
        Ok(())
    }
}

/// Non-blocking station-to-station receive abstraction.
pub trait StationTransportReceiver {
    /// Transport error type.
    type Error;

    /// Attempts to receive one encoded packet for `target_station`.
    fn try_recv_station(
        &mut self,
        target_station: StationId,
    ) -> Result<Option<StationInboundPacket>, Self::Error>;
}

const RELIABLE_STATION_MAGIC: [u8; 4] = *b"SSRP";
const RELIABLE_KIND_DATA: u8 = 0;
const RELIABLE_KIND_ACK: u8 = 1;
/// Reliable data frame header bytes before payload.
pub const RELIABLE_STATION_DATA_HEADER_BYTES: usize = 17;
/// Reliable ACK frame bytes.
pub const RELIABLE_STATION_ACK_BYTES: usize = 13;
/// Default reliable station payload budget aligned to the default station
/// packet budget after reliable header overhead.
pub const DEFAULT_RELIABLE_STATION_MAX_PAYLOAD_BYTES: usize =
    (16 * 1024) - RELIABLE_STATION_DATA_HEADER_BYTES;
/// Default duplicate-suppression history retained per reliable endpoint.
pub const DEFAULT_RELIABLE_STATION_DELIVERED_HISTORY: usize = 4096;

/// Bounded reliable station link configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReliableStationConfig {
    /// Maximum in-flight reliable packets per target station.
    pub max_in_flight_per_target: usize,
    /// Number of ticks to wait before retrying an unacknowledged packet.
    pub retry_after_ticks: u64,
    /// Maximum send attempts before dropping an in-flight packet.
    pub max_attempts: u8,
    /// Maximum payload bytes before reliable envelope overhead.
    pub max_payload_bytes: usize,
    /// Maximum recently delivered packet ids retained for duplicate
    /// suppression. Set to zero to disable duplicate suppression history.
    pub max_delivered_history: usize,
}

impl Default for ReliableStationConfig {
    fn default() -> Self {
        Self {
            max_in_flight_per_target: 1024,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: DEFAULT_RELIABLE_STATION_MAX_PAYLOAD_BYTES,
            max_delivered_history: DEFAULT_RELIABLE_STATION_DELIVERED_HISTORY,
        }
    }
}

/// Reliable station endpoint statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReliableStationStats {
    /// New reliable data packets sent.
    pub data_sent: usize,
    /// Retry data packets sent.
    pub retries_sent: usize,
    /// ACK packets sent.
    pub acks_sent: usize,
    /// ACK packets received.
    pub acks_received: usize,
    /// Unique data packets delivered to the caller.
    pub data_delivered: usize,
    /// Duplicate data packets suppressed.
    pub duplicates_suppressed: usize,
    /// In-flight packets dropped after exhausting attempts.
    pub timed_out: usize,
}

/// Encoded reliable station frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReliableStationFrame {
    /// Reliable data packet.
    Data {
        /// Sender-local sequence number scoped to the target station.
        sequence: u64,
        /// Original station packet payload.
        payload: Vec<u8>,
    },
    /// Acknowledgement for a reliable data packet.
    Ack {
        /// Acknowledged sequence number.
        sequence: u64,
    },
}

/// Borrowed reliable Station frame decoded from caller-owned bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableStationFrameRef<'a> {
    /// Reliable data packet with borrowed payload bytes.
    Data {
        /// Sender-local sequence number scoped to the target Station.
        sequence: u64,
        /// Payload borrowed from the encoded frame.
        payload: &'a [u8],
    },
    /// Acknowledgement for a reliable data packet.
    Ack {
        /// Acknowledged sequence number.
        sequence: u64,
    },
}

impl ReliableStationFrameRef<'_> {
    /// Materializes the compatible owned reliable Station frame.
    pub fn to_owned(self) -> ReliableStationFrame {
        match self {
            Self::Data { sequence, payload } => ReliableStationFrame::Data {
                sequence,
                payload: payload.to_vec(),
            },
            Self::Ack { sequence } => ReliableStationFrame::Ack { sequence },
        }
    }
}

impl ReliableStationFrame {
    /// Appends a data frame from a borrowed payload without materializing an owned frame.
    pub fn encode_data(
        sequence: u64,
        payload: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), ReliableStationEncodeError> {
        out.extend_from_slice(&RELIABLE_STATION_MAGIC);
        out.push(RELIABLE_KIND_DATA);
        out.extend_from_slice(&sequence.to_le_bytes());
        let len = u32::try_from(payload.len()).map_err(|_| {
            ReliableStationEncodeError::PayloadTooLarge {
                actual: payload.len(),
            }
        })?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(payload);
        Ok(())
    }

    /// Encodes a reliable station frame.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ReliableStationEncodeError> {
        match self {
            Self::Data { sequence, payload } => Self::encode_data(*sequence, payload, out),
            Self::Ack { sequence } => {
                out.extend_from_slice(&RELIABLE_STATION_MAGIC);
                out.push(RELIABLE_KIND_ACK);
                out.extend_from_slice(&sequence.to_le_bytes());
                Ok(())
            }
        }
    }

    /// Decodes a reliable station frame.
    pub fn decode(input: &[u8]) -> Result<Self, ReliableStationDecodeError> {
        Self::decode_ref(input).map(ReliableStationFrameRef::to_owned)
    }

    /// Decodes a reliable Station frame while borrowing data payload bytes.
    pub fn decode_ref(
        input: &[u8],
    ) -> Result<ReliableStationFrameRef<'_>, ReliableStationDecodeError> {
        let mut cursor = ReliableCursor::new(input);
        let magic = cursor.read_array::<4>()?;
        if magic != RELIABLE_STATION_MAGIC {
            return Err(ReliableStationDecodeError::BadMagic);
        }
        let kind = cursor.read_u8()?;
        let sequence = cursor.read_u64()?;
        let frame = match kind {
            RELIABLE_KIND_DATA => {
                let len = cursor.read_u32()? as usize;
                let payload = cursor.read_slice(len)?;
                ReliableStationFrameRef::Data { sequence, payload }
            }
            RELIABLE_KIND_ACK => ReliableStationFrameRef::Ack { sequence },
            other => return Err(ReliableStationDecodeError::UnknownKind(other)),
        };
        cursor.finish()?;
        Ok(frame)
    }
}

/// Reliable station frame encode error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableStationEncodeError {
    /// Payload length exceeded `u32::MAX`.
    PayloadTooLarge {
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for ReliableStationEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PayloadTooLarge { actual } => {
                write!(f, "reliable station payload too large: {actual} bytes")
            }
        }
    }
}

impl std::error::Error for ReliableStationEncodeError {}

/// Reliable station frame decode error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReliableStationDecodeError {
    /// Frame magic did not match.
    BadMagic,
    /// Frame kind byte is unknown.
    UnknownKind(u8),
    /// Frame ended before all fields were available.
    Truncated {
        /// Required bytes.
        needed: usize,
        /// Available bytes.
        available: usize,
    },
    /// Frame had trailing bytes after a complete payload.
    TrailingBytes(usize),
}

impl core::fmt::Display for ReliableStationDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BadMagic => f.write_str("bad reliable station frame magic"),
            Self::UnknownKind(kind) => write!(f, "unknown reliable station frame kind {kind}"),
            Self::Truncated { needed, available } => {
                write!(
                    f,
                    "truncated reliable station frame: needed {needed}, available {available}"
                )
            }
            Self::TrailingBytes(bytes) => {
                write!(f, "reliable station frame has {bytes} trailing bytes")
            }
        }
    }
}

impl std::error::Error for ReliableStationDecodeError {}

/// Error produced by reliable station endpoints.
#[derive(Debug)]
pub enum ReliableStationError<E> {
    /// Underlying station transport failed.
    Transport(E),
    /// Payload exceeded configured byte budget.
    PayloadTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Target station in-flight window is full.
    WindowFull {
        /// Target station.
        target_station: StationId,
        /// Configured in-flight capacity.
        capacity: usize,
    },
    /// Reliable frame encode failed.
    Encode(ReliableStationEncodeError),
    /// Reliable frame decode failed.
    Decode(ReliableStationDecodeError),
}

impl<E: core::fmt::Display> core::fmt::Display for ReliableStationError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::PayloadTooLarge { budget, actual } => {
                write!(
                    f,
                    "reliable station payload exceeded byte budget: budget {budget}, actual {actual}"
                )
            }
            Self::WindowFull {
                target_station,
                capacity,
            } => write!(
                f,
                "reliable station target {} window is full at capacity {capacity}",
                target_station.get()
            ),
            Self::Encode(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for ReliableStationError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::PayloadTooLarge { .. } | Self::WindowFull { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InFlightReliableStationPacket {
    source_station: StationId,
    target_station: StationId,
    sequence: u64,
    payload: Vec<u8>,
    first_sent_tick: u64,
    last_sent_tick: u64,
    attempts: u8,
}

/// Caller-owned reusable storage for reliable station retry scans.
#[derive(Clone, Debug, Default)]
pub struct ReliableStationRetryScratch {
    due_keys: Vec<(StationId, u64)>,
}

impl ReliableStationRetryScratch {
    /// Creates empty retry storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Due-key capacity retained across retry passes.
    pub fn retained_key_capacity(&self) -> usize {
        self.due_keys.capacity()
    }
}

/// Retry pass report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReliableRetryReport {
    /// Packets resent.
    pub retried: usize,
    /// Packets dropped after exhausting attempts.
    pub timed_out: usize,
}

/// Bounded reliable station sender state.
#[derive(Clone, Debug)]
pub struct ReliableStationSender {
    config: ReliableStationConfig,
    next_sequence: BTreeMap<StationId, u64>,
    in_flight: BTreeMap<(StationId, u64), InFlightReliableStationPacket>,
    in_flight_by_target: BTreeMap<StationId, usize>,
    stats: ReliableStationStats,
}

impl ReliableStationSender {
    /// Creates a reliable station sender.
    pub fn new(config: ReliableStationConfig) -> Self {
        Self {
            config,
            next_sequence: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            in_flight_by_target: BTreeMap::new(),
            stats: ReliableStationStats::default(),
        }
    }

    /// Returns sender configuration.
    pub const fn config(&self) -> ReliableStationConfig {
        self.config
    }

    /// Returns sender statistics.
    pub const fn stats(&self) -> ReliableStationStats {
        self.stats
    }

    /// Returns total in-flight reliable packets.
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }

    /// Returns in-flight reliable packets for one target station.
    pub fn in_flight_for(&self, target_station: StationId) -> usize {
        self.in_flight_by_target
            .get(&target_station)
            .copied()
            .unwrap_or(0)
    }

    /// Sends a new reliable packet and stores it until acknowledged or timed out.
    pub fn send<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        packet: StationOutboundPacket,
        now_tick: u64,
    ) -> Result<u64, ReliableStationError<T::Error>> {
        self.validate_payload(packet.bytes.len())?;
        if self.in_flight_for(packet.target_station) >= self.config.max_in_flight_per_target {
            return Err(ReliableStationError::WindowFull {
                target_station: packet.target_station,
                capacity: self.config.max_in_flight_per_target,
            });
        }

        let sequence = self.allocate_sequence(packet.target_station);
        Self::send_data_frame(
            transport,
            packet.source_station,
            packet.target_station,
            sequence,
            &packet.bytes,
        )?;
        self.insert_in_flight(
            (packet.target_station, sequence),
            InFlightReliableStationPacket {
                source_station: packet.source_station,
                target_station: packet.target_station,
                sequence,
                payload: packet.bytes,
                first_sent_tick: now_tick,
                last_sent_tick: now_tick,
                attempts: 1,
            },
        );
        self.stats.data_sent = self.stats.data_sent.saturating_add(1);
        Ok(sequence)
    }

    /// Processes an ACK from `ack_source_station`.
    pub fn acknowledge(&mut self, ack_source_station: StationId, sequence: u64) -> bool {
        let removed = self
            .remove_in_flight(&(ack_source_station, sequence))
            .is_some();
        if removed {
            self.stats.acks_received = self.stats.acks_received.saturating_add(1);
        }
        removed
    }

    /// Retries due in-flight packets.
    pub fn retry_due<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
    ) -> Result<ReliableRetryReport, ReliableStationError<T::Error>> {
        self.retry_due_with_scratch(
            transport,
            now_tick,
            &mut ReliableStationRetryScratch::default(),
        )
    }

    /// Retries due packets using caller-owned scan storage.
    pub fn retry_due_with_scratch<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
        scratch: &mut ReliableStationRetryScratch,
    ) -> Result<ReliableRetryReport, ReliableStationError<T::Error>> {
        scratch.due_keys.clear();
        scratch
            .due_keys
            .extend(self.in_flight.iter().filter_map(|(key, packet)| {
                let due =
                    now_tick.saturating_sub(packet.last_sent_tick) >= self.config.retry_after_ticks;
                due.then_some(*key)
            }));
        let mut report = ReliableRetryReport::default();

        for key in &scratch.due_keys {
            let Some(packet) = self.in_flight.get(key) else {
                continue;
            };
            if packet.attempts >= self.config.max_attempts {
                self.remove_in_flight(key);
                self.stats.timed_out = self.stats.timed_out.saturating_add(1);
                report.timed_out = report.timed_out.saturating_add(1);
                continue;
            }

            Self::send_data_frame(
                transport,
                packet.source_station,
                packet.target_station,
                packet.sequence,
                &packet.payload,
            )?;
            if let Some(stored) = self.in_flight.get_mut(key) {
                stored.last_sent_tick = now_tick;
                stored.attempts = stored.attempts.saturating_add(1);
            }
            self.stats.retries_sent = self.stats.retries_sent.saturating_add(1);
            report.retried = report.retried.saturating_add(1);
        }

        Ok(report)
    }

    fn validate_payload<E>(&self, bytes: usize) -> Result<(), ReliableStationError<E>> {
        if bytes > self.config.max_payload_bytes {
            Err(ReliableStationError::PayloadTooLarge {
                budget: self.config.max_payload_bytes,
                actual: bytes,
            })
        } else {
            Ok(())
        }
    }

    fn allocate_sequence(&mut self, target_station: StationId) -> u64 {
        let next = self.next_sequence.entry(target_station).or_insert(1);
        let sequence = *next;
        *next = next.saturating_add(1);
        sequence
    }

    fn insert_in_flight(&mut self, key: (StationId, u64), packet: InFlightReliableStationPacket) {
        if self.in_flight.insert(key, packet).is_none() {
            let count = self.in_flight_by_target.entry(key.0).or_insert(0);
            *count = count.saturating_add(1);
        }
    }

    fn remove_in_flight(
        &mut self,
        key: &(StationId, u64),
    ) -> Option<InFlightReliableStationPacket> {
        let removed = self.in_flight.remove(key)?;
        if let Some(count) = self.in_flight_by_target.get_mut(&removed.target_station) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.in_flight_by_target.remove(&removed.target_station);
            }
        }
        Some(removed)
    }

    fn send_data_frame<T: StationTransportSink>(
        transport: &mut T,
        source_station: StationId,
        target_station: StationId,
        sequence: u64,
        payload: &[u8],
    ) -> Result<(), ReliableStationError<T::Error>> {
        let mut bytes = Vec::with_capacity(
            payload
                .len()
                .saturating_add(RELIABLE_STATION_DATA_HEADER_BYTES),
        );
        ReliableStationFrame::encode_data(sequence, payload, &mut bytes)
            .map_err(ReliableStationError::Encode)?;
        transport
            .send_station(StationOutboundPacket {
                source_station,
                target_station,
                bytes,
            })
            .map_err(ReliableStationError::Transport)
    }
}

impl Default for ReliableStationSender {
    fn default() -> Self {
        Self::new(ReliableStationConfig::default())
    }
}

/// Bounded reliable station receiver state.
#[derive(Clone, Debug)]
pub struct ReliableStationReceiver {
    config: ReliableStationConfig,
    delivered: BoundedLookupSet<(StationId, u64)>,
    delivered_order: VecDeque<(StationId, u64)>,
    stats: ReliableStationStats,
}

impl ReliableStationReceiver {
    /// Creates a bounded reliable station receiver.
    pub fn new(config: ReliableStationConfig) -> Self {
        Self {
            config,
            delivered: BoundedLookupSet::new(config.max_delivered_history),
            delivered_order: VecDeque::new(),
            stats: ReliableStationStats::default(),
        }
    }

    /// Returns receiver configuration.
    pub const fn config(&self) -> ReliableStationConfig {
        self.config
    }

    /// Returns receiver statistics.
    pub const fn stats(&self) -> ReliableStationStats {
        self.stats
    }

    /// Handles a reliable data packet, sends an ACK, and returns a payload only
    /// for first delivery.
    pub fn handle_data<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        packet: StationInboundPacket,
        sequence: u64,
        payload: Vec<u8>,
    ) -> Result<Option<StationInboundPacket>, ReliableStationError<T::Error>> {
        let StationInboundPacket {
            source_station,
            target_station,
            bytes: wire_bytes,
        } = packet;
        drop(wire_bytes);
        self.send_ack(transport, target_station, source_station, sequence)?;
        if !self.record_unique(source_station, sequence) {
            self.stats.duplicates_suppressed = self.stats.duplicates_suppressed.saturating_add(1);
            return Ok(None);
        }

        self.stats.data_delivered = self.stats.data_delivered.saturating_add(1);
        Ok(Some(StationInboundPacket {
            source_station,
            target_station,
            bytes: payload,
        }))
    }

    fn send_ack<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        source_station: StationId,
        target_station: StationId,
        sequence: u64,
    ) -> Result<(), ReliableStationError<T::Error>> {
        let mut bytes = Vec::with_capacity(RELIABLE_STATION_ACK_BYTES);
        ReliableStationFrame::Ack { sequence }
            .encode(&mut bytes)
            .map_err(ReliableStationError::Encode)?;
        transport
            .send_station(StationOutboundPacket {
                source_station,
                target_station,
                bytes,
            })
            .map_err(ReliableStationError::Transport)?;
        self.stats.acks_sent = self.stats.acks_sent.saturating_add(1);
        Ok(())
    }

    fn record_unique(&mut self, source_station: StationId, sequence: u64) -> bool {
        if self.config.max_delivered_history == 0 {
            return true;
        }

        let key = (source_station, sequence);
        if self.delivered.contains(&key) {
            return false;
        }

        self.delivered.insert(key);
        self.delivered_order.push_back(key);
        while self.delivered_order.len() > self.config.max_delivered_history {
            if let Some(old) = self.delivered_order.pop_front() {
                self.delivered.remove(&old);
            }
        }
        true
    }
}

impl Default for ReliableStationReceiver {
    fn default() -> Self {
        Self::new(ReliableStationConfig::default())
    }
}

/// Reliable station endpoint combining sender and receiver state.
#[derive(Clone, Debug)]
pub struct ReliableStationEndpoint {
    /// Sender state.
    pub sender: ReliableStationSender,
    /// Receiver state.
    pub receiver: ReliableStationReceiver,
}

impl ReliableStationEndpoint {
    /// Creates a reliable station endpoint.
    pub fn new(config: ReliableStationConfig) -> Self {
        Self {
            sender: ReliableStationSender::new(config),
            receiver: ReliableStationReceiver::new(config),
        }
    }

    /// Sends a new reliable station packet.
    pub fn send<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        packet: StationOutboundPacket,
        now_tick: u64,
    ) -> Result<u64, ReliableStationError<T::Error>> {
        self.sender.send(transport, packet, now_tick)
    }

    /// Retries due reliable station packets.
    pub fn retry_due<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
    ) -> Result<ReliableRetryReport, ReliableStationError<T::Error>> {
        self.sender.retry_due(transport, now_tick)
    }

    /// Retries due reliable station packets using caller-owned scan storage.
    pub fn retry_due_with_scratch<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        now_tick: u64,
        scratch: &mut ReliableStationRetryScratch,
    ) -> Result<ReliableRetryReport, ReliableStationError<T::Error>> {
        self.sender
            .retry_due_with_scratch(transport, now_tick, scratch)
    }

    /// Handles one inbound reliable station packet.
    pub fn handle_inbound<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        packet: StationInboundPacket,
    ) -> Result<Option<StationInboundPacket>, ReliableStationError<T::Error>> {
        match ReliableStationFrame::decode_ref(&packet.bytes)
            .map_err(ReliableStationError::Decode)?
        {
            ReliableStationFrameRef::Data { sequence, payload } => {
                let payload_len = payload.len();
                let payload_offset = packet.bytes.len().saturating_sub(payload_len);
                let StationInboundPacket {
                    source_station,
                    target_station,
                    bytes,
                } = packet;
                let payload = reuse_reliable_payload(bytes, payload_offset, payload_len);
                self.receiver.handle_data(
                    transport,
                    StationInboundPacket {
                        source_station,
                        target_station,
                        bytes: Vec::new(),
                    },
                    sequence,
                    payload,
                )
            }
            ReliableStationFrameRef::Ack { sequence } => {
                self.sender.acknowledge(packet.source_station, sequence);
                Ok(None)
            }
        }
    }
}

impl Default for ReliableStationEndpoint {
    fn default() -> Self {
        Self::new(ReliableStationConfig::default())
    }
}

struct ReliableCursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ReliableCursor<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, ReliableStationDecodeError> {
        self.require(1)?;
        let value = self.input[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, ReliableStationDecodeError> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, ReliableStationDecodeError> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ReliableStationDecodeError> {
        self.require(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(&self.input[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], ReliableStationDecodeError> {
        self.require(len)?;
        let bytes = &self.input[self.offset..self.offset + len];
        self.offset += len;
        Ok(bytes)
    }

    fn require(&self, count: usize) -> Result<(), ReliableStationDecodeError> {
        let needed = self.offset.saturating_add(count);
        if needed > self.input.len() {
            Err(ReliableStationDecodeError::Truncated {
                needed,
                available: self.input.len(),
            })
        } else {
            Ok(())
        }
    }

    fn finish(&self) -> Result<(), ReliableStationDecodeError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(ReliableStationDecodeError::TrailingBytes(
                self.input.len().saturating_sub(self.offset),
            ))
        }
    }
}

fn reuse_reliable_payload(mut wire_bytes: Vec<u8>, offset: usize, len: usize) -> Vec<u8> {
    wire_bytes.copy_within(offset..offset.saturating_add(len), 0);
    wire_bytes.truncate(len);
    wire_bytes
}

/// Bounded in-memory station transport limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationTransportLimits {
    /// Maximum queued packets per target station.
    pub max_queued_packets_per_station: usize,
    /// Maximum bytes accepted per packet.
    pub max_packet_bytes: usize,
}

impl Default for StationTransportLimits {
    fn default() -> Self {
        Self {
            max_queued_packets_per_station: 4096,
            max_packet_bytes: 16 * 1024,
        }
    }
}

/// Statistics for the bounded in-memory station transport.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InMemoryStationTransportStats {
    /// Packets accepted for delivery.
    pub packets_sent: usize,
    /// Packets received by target stations.
    pub packets_received: usize,
    /// Bytes accepted for delivery.
    pub bytes_sent: usize,
    /// Bytes received by target stations.
    pub bytes_received: usize,
    /// Packets rejected because the target station queue was full.
    pub packets_rejected_full: usize,
    /// Packets rejected because they exceeded the packet byte budget.
    pub packets_rejected_bytes: usize,
}

/// Statistics for the UDP station transport adapter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UdpStationTransportStats {
    /// Packets sent by this local station transport.
    pub packets_sent: usize,
    /// Packets received by this local station transport.
    pub packets_received: usize,
    /// Bytes sent by this local station transport.
    pub bytes_sent: usize,
    /// Bytes received by this local station transport.
    pub bytes_received: usize,
}

/// Error produced by bounded in-memory station transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StationTransportError {
    /// Target station was not registered.
    MissingTarget(StationId),
    /// Target station queue is full.
    QueueFull {
        /// Target station.
        station_id: StationId,
        /// Configured queue capacity.
        capacity: usize,
    },
    /// Packet exceeded the byte budget.
    PacketTooLarge {
        /// Configured byte budget.
        budget: usize,
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for StationTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingTarget(station_id) => {
                write!(
                    f,
                    "station transport target {} is missing",
                    station_id.get()
                )
            }
            Self::QueueFull {
                station_id,
                capacity,
            } => write!(
                f,
                "station transport target {} queue is full at capacity {capacity}",
                station_id.get()
            ),
            Self::PacketTooLarge { budget, actual } => {
                write!(
                    f,
                    "station transport packet exceeded byte budget: budget {budget}, actual {actual}"
                )
            }
        }
    }
}

impl std::error::Error for StationTransportError {}

/// Bounded in-memory station-to-station packet transport.
#[derive(Clone, Debug)]
pub struct InMemoryStationTransport {
    limits: StationTransportLimits,
    queues: BTreeMap<StationId, VecDeque<StationInboundPacket>>,
    stats: InMemoryStationTransportStats,
}

impl InMemoryStationTransport {
    /// Creates an empty bounded station transport.
    pub fn new(limits: StationTransportLimits) -> Self {
        Self {
            limits,
            queues: BTreeMap::new(),
            stats: InMemoryStationTransportStats::default(),
        }
    }

    /// Registers a target station queue.
    pub fn register_station(&mut self, station_id: StationId) {
        self.queues
            .entry(station_id)
            .or_insert_with(|| VecDeque::with_capacity(self.limits.max_queued_packets_per_station));
    }

    /// Returns queued packet count for a station.
    pub fn queued_len(&self, station_id: StationId) -> Option<usize> {
        self.queues.get(&station_id).map(VecDeque::len)
    }

    /// Returns configured limits.
    pub const fn limits(&self) -> StationTransportLimits {
        self.limits
    }

    /// Returns transport statistics.
    pub const fn stats(&self) -> InMemoryStationTransportStats {
        self.stats
    }
}

impl Default for InMemoryStationTransport {
    fn default() -> Self {
        Self::new(StationTransportLimits::default())
    }
}

impl StationTransportSink for InMemoryStationTransport {
    type Error = StationTransportError;

    fn send_station(&mut self, packet: StationOutboundPacket) -> Result<(), Self::Error> {
        let actual = packet.bytes.len();
        if actual > self.limits.max_packet_bytes {
            self.stats.packets_rejected_bytes = self.stats.packets_rejected_bytes.saturating_add(1);
            return Err(StationTransportError::PacketTooLarge {
                budget: self.limits.max_packet_bytes,
                actual,
            });
        }

        let queue = self
            .queues
            .get_mut(&packet.target_station)
            .ok_or(StationTransportError::MissingTarget(packet.target_station))?;
        if queue.len() >= self.limits.max_queued_packets_per_station {
            self.stats.packets_rejected_full = self.stats.packets_rejected_full.saturating_add(1);
            return Err(StationTransportError::QueueFull {
                station_id: packet.target_station,
                capacity: self.limits.max_queued_packets_per_station,
            });
        }

        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(actual);
        queue.push_back(StationInboundPacket {
            source_station: packet.source_station,
            target_station: packet.target_station,
            bytes: packet.bytes,
        });
        Ok(())
    }
}

impl StationTransportReceiver for InMemoryStationTransport {
    type Error = StationTransportError;

    fn try_recv_station(
        &mut self,
        target_station: StationId,
    ) -> Result<Option<StationInboundPacket>, Self::Error> {
        let queue = self
            .queues
            .get_mut(&target_station)
            .ok_or(StationTransportError::MissingTarget(target_station))?;
        let Some(packet) = queue.pop_front() else {
            return Ok(None);
        };
        self.stats.packets_received = self.stats.packets_received.saturating_add(1);
        self.stats.bytes_received = self.stats.bytes_received.saturating_add(packet.bytes.len());
        Ok(Some(packet))
    }
}

/// Error produced by the UDP station transport adapter.
#[derive(Debug)]
pub enum UdpStationTransportError {
    /// No address has been registered for the target station.
    UnknownStation(StationId),
    /// A packet arrived from an unregistered remote address.
    UnknownRemote(SocketAddr),
    /// Outbound packet source did not match the local station.
    LocalStationMismatch {
        /// Local station owned by this transport.
        local_station: StationId,
        /// Packet source station.
        packet_source: StationId,
    },
    /// Receive was requested for a station not owned by this transport.
    TargetStationMismatch {
        /// Local station owned by this transport.
        local_station: StationId,
        /// Requested target station.
        requested_target: StationId,
    },
    /// Underlying socket error.
    Io(io::Error),
}

impl core::fmt::Display for UdpStationTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownStation(station_id) => {
                write!(
                    f,
                    "udp station target {} is not registered",
                    station_id.get()
                )
            }
            Self::UnknownRemote(addr) => {
                write!(f, "udp station remote address {addr} is not registered")
            }
            Self::LocalStationMismatch {
                local_station,
                packet_source,
            } => write!(
                f,
                "udp station local source mismatch: local {}, packet source {}",
                local_station.get(),
                packet_source.get()
            ),
            Self::TargetStationMismatch {
                local_station,
                requested_target,
            } => write!(
                f,
                "udp station receive target mismatch: local {}, requested {}",
                local_station.get(),
                requested_target.get()
            ),
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for UdpStationTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::UnknownStation(_)
            | Self::UnknownRemote(_)
            | Self::LocalStationMismatch { .. }
            | Self::TargetStationMismatch { .. } => None,
        }
    }
}

impl From<io::Error> for UdpStationTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Lightweight UDP station-to-station packet adapter.
///
/// Each instance represents one local station socket. Reliability,
/// authentication, encryption, and deployment routing remain outer-layer
/// concerns.
#[derive(Debug)]
pub struct UdpStationTransport {
    local_station: StationId,
    socket: UdpSocket,
    stations: HashMap<StationId, SocketAddr>,
    addr_to_station: HashMap<SocketAddr, StationId>,
    recv_buffer: Vec<u8>,
    stats: UdpStationTransportStats,
}

impl UdpStationTransport {
    /// Binds a non-blocking UDP station transport.
    pub fn bind<A: ToSocketAddrs>(local_station: StationId, addr: A) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        Self::from_socket(local_station, socket)
    }

    /// Wraps an existing UDP socket and configures it as non-blocking.
    pub fn from_socket(local_station: StationId, socket: UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self {
            local_station,
            socket,
            stations: HashMap::new(),
            addr_to_station: HashMap::new(),
            recv_buffer: vec![0; DEFAULT_UDP_RECV_BUFFER_SIZE],
            stats: UdpStationTransportStats::default(),
        })
    }

    /// Returns the local station id.
    pub const fn local_station(&self) -> StationId {
        self.local_station
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Borrows the underlying socket.
    pub const fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Mutably borrows the underlying socket.
    pub fn socket_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }

    /// Registers or replaces the network address for a station.
    pub fn register_station(
        &mut self,
        station_id: StationId,
        addr: SocketAddr,
    ) -> Option<SocketAddr> {
        let old_addr = self.stations.insert(station_id, addr);
        if let Some(old_addr) = old_addr {
            self.addr_to_station.remove(&old_addr);
        }
        if let Some(old_station) = self.addr_to_station.insert(addr, station_id)
            && old_station != station_id
        {
            self.stations.remove(&old_station);
        }
        old_addr
    }

    /// Removes a registered station address.
    pub fn unregister_station(&mut self, station_id: StationId) -> Option<SocketAddr> {
        let addr = self.stations.remove(&station_id)?;
        self.addr_to_station.remove(&addr);
        Some(addr)
    }

    /// Returns a registered address for a station.
    pub fn station_addr(&self, station_id: StationId) -> Option<SocketAddr> {
        self.stations.get(&station_id).copied()
    }

    /// Returns the registered station id for a remote address.
    pub fn station_for_addr(&self, addr: SocketAddr) -> Option<StationId> {
        self.addr_to_station.get(&addr).copied()
    }

    /// Sets the reusable receive buffer size.
    pub fn set_recv_buffer_size(&mut self, bytes: usize) {
        self.recv_buffer.resize(bytes.max(1), 0);
    }

    /// Returns the reusable receive buffer size.
    pub fn recv_buffer_size(&self) -> usize {
        self.recv_buffer.len()
    }

    /// Returns transport statistics.
    pub const fn stats(&self) -> UdpStationTransportStats {
        self.stats
    }

    /// Receives one station packet while borrowing the reusable UDP buffer.
    ///
    /// Consume or copy the returned bytes before the next mutable operation on
    /// this adapter. The call remains non-blocking and returns `Ok(None)` when
    /// no datagram is ready.
    pub fn try_recv_station_ref(
        &mut self,
        target_station: StationId,
    ) -> Result<Option<StationInboundPacketRef<'_>>, UdpStationTransportError> {
        if target_station != self.local_station {
            return Err(UdpStationTransportError::TargetStationMismatch {
                local_station: self.local_station,
                requested_target: target_station,
            });
        }
        match self.socket.recv_from(&mut self.recv_buffer) {
            Ok((len, remote_addr)) => {
                let source_station = self
                    .addr_to_station
                    .get(&remote_addr)
                    .copied()
                    .ok_or(UdpStationTransportError::UnknownRemote(remote_addr))?;
                self.stats.packets_received = self.stats.packets_received.saturating_add(1);
                self.stats.bytes_received = self.stats.bytes_received.saturating_add(len);
                Ok(Some(StationInboundPacketRef {
                    source_station,
                    target_station: self.local_station,
                    bytes: &self.recv_buffer[..len],
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

impl StationTransportSink for UdpStationTransport {
    type Error = UdpStationTransportError;

    fn send_station(&mut self, packet: StationOutboundPacket) -> Result<(), Self::Error> {
        if packet.source_station != self.local_station {
            return Err(UdpStationTransportError::LocalStationMismatch {
                local_station: self.local_station,
                packet_source: packet.source_station,
            });
        }
        let addr = self.stations.get(&packet.target_station).copied().ok_or(
            UdpStationTransportError::UnknownStation(packet.target_station),
        )?;
        let sent = self.socket.send_to(&packet.bytes, addr)?;
        if sent != packet.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "udp station socket reported a partial datagram send",
            )
            .into());
        }
        self.stats.packets_sent = self.stats.packets_sent.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(packet.bytes.len());
        Ok(())
    }
}

impl StationTransportReceiver for UdpStationTransport {
    type Error = UdpStationTransportError;

    fn try_recv_station(
        &mut self,
        target_station: StationId,
    ) -> Result<Option<StationInboundPacket>, Self::Error> {
        Ok(self
            .try_recv_station_ref(target_station)?
            .map(StationInboundPacketRef::to_owned))
    }
}

/// Error produced by the standard UDP transport adapter.
#[derive(Debug)]
pub enum UdpTransportError {
    /// No address has been registered for the target client.
    UnknownClient(ClientId),
    /// Underlying socket error.
    Io(io::Error),
}

impl core::fmt::Display for UdpTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::UnknownClient(client_id) => {
                write!(f, "udp target client {} is not registered", client_id.get())
            }
            Self::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for UdpTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnknownClient(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}

impl From<io::Error> for UdpTransportError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Lightweight `std::net::UdpSocket` transport adapter.
///
/// This adapter intentionally operates at packet boundaries. Reliability,
/// encryption, authentication, reconnects, and gateway/session semantics are
/// expected to live outside the core `SectorSync` hot path.
#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
    clients: HashMap<ClientId, SocketAddr>,
    addr_to_client: HashMap<SocketAddr, ClientId>,
    recv_buffer: Vec<u8>,
}

impl UdpTransport {
    /// Binds a non-blocking UDP socket.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        Self::from_socket(socket)
    }

    /// Wraps an existing UDP socket and configures it as non-blocking.
    pub fn from_socket(socket: UdpSocket) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            clients: HashMap::new(),
            addr_to_client: HashMap::new(),
            recv_buffer: vec![0; DEFAULT_UDP_RECV_BUFFER_SIZE],
        })
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Borrows the underlying socket.
    pub const fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    /// Mutably borrows the underlying socket.
    pub fn socket_mut(&mut self) -> &mut UdpSocket {
        &mut self.socket
    }

    /// Registers or replaces the address for a client.
    pub fn register_client(&mut self, client_id: ClientId, addr: SocketAddr) -> Option<SocketAddr> {
        let old_addr = self.clients.insert(client_id, addr);
        if let Some(old_addr) = old_addr {
            self.addr_to_client.remove(&old_addr);
        }
        if let Some(old_client) = self.addr_to_client.insert(addr, client_id)
            && old_client != client_id
        {
            self.clients.remove(&old_client);
        }
        old_addr
    }

    /// Removes a registered client address.
    pub fn unregister_client(&mut self, client_id: ClientId) -> Option<SocketAddr> {
        let addr = self.clients.remove(&client_id)?;
        self.addr_to_client.remove(&addr);
        Some(addr)
    }

    /// Returns a registered address for a client.
    pub fn client_addr(&self, client_id: ClientId) -> Option<SocketAddr> {
        self.clients.get(&client_id).copied()
    }

    /// Returns the registered client id for a remote address.
    pub fn client_for_addr(&self, addr: SocketAddr) -> Option<ClientId> {
        self.addr_to_client.get(&addr).copied()
    }

    /// Sets the reusable receive buffer size.
    ///
    /// Datagram payloads larger than this buffer may be truncated by the OS.
    /// Keep replication frames under the transport MTU/budget in normal use.
    pub fn set_recv_buffer_size(&mut self, bytes: usize) {
        self.recv_buffer.resize(bytes.max(1), 0);
    }

    /// Returns the reusable receive buffer size.
    pub fn recv_buffer_size(&self) -> usize {
        self.recv_buffer.len()
    }

    /// Receives one packet while borrowing the reusable UDP buffer.
    ///
    /// Consume or copy the returned bytes before the next mutable operation on
    /// this adapter. The call remains non-blocking and returns `Ok(None)` when
    /// no datagram is ready.
    pub fn try_recv_ref(&mut self) -> Result<Option<InboundPacketRef<'_>>, UdpTransportError> {
        match self.socket.recv_from(&mut self.recv_buffer) {
            Ok((len, remote_addr)) => Ok(Some(InboundPacketRef {
                client_id: self.addr_to_client.get(&remote_addr).copied(),
                remote_addr,
                bytes: &self.recv_buffer[..len],
            })),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

impl TransportSink for UdpTransport {
    type Error = UdpTransportError;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        let addr = self
            .clients
            .get(&packet.client_id)
            .copied()
            .ok_or(UdpTransportError::UnknownClient(packet.client_id))?;
        let sent = self.socket.send_to(&packet.bytes, addr)?;
        if sent == packet.bytes.len() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "udp socket reported a partial datagram send",
            )
            .into())
        }
    }
}

impl TransportReceiver for UdpTransport {
    type Error = UdpTransportError;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        Ok(self.try_recv_ref()?.map(InboundPacketRef::to_owned))
    }
}

/// Transport sink error produced by wrappers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportError<E> {
    /// Inner sink failed.
    Inner(E),
    /// A packet or batch exceeded byte budget.
    ByteBudgetExceeded {
        /// Configured budget.
        budget: usize,
        /// Actual bytes.
        actual: usize,
    },
}

impl<E: core::fmt::Display> core::fmt::Display for TransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Inner(error) => write!(f, "{error}"),
            Self::ByteBudgetExceeded { budget, actual } => {
                write!(
                    f,
                    "transport byte budget exceeded: budget {budget}, actual {actual}"
                )
            }
        }
    }
}

impl<E> std::error::Error for TransportError<E> where E: std::error::Error {}

/// Byte-budget guard for any transport sink.
#[derive(Clone, Debug)]
pub struct BudgetedTransport<T> {
    inner: T,
    max_packet_bytes: usize,
    max_batch_bytes: usize,
}

impl<T> BudgetedTransport<T> {
    /// Creates a budgeted transport wrapper.
    pub const fn new(inner: T, max_packet_bytes: usize, max_batch_bytes: usize) -> Self {
        Self {
            inner,
            max_packet_bytes,
            max_batch_bytes,
        }
    }

    /// Returns the inner transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Borrows the inner transport.
    pub const fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: TransportSink> TransportSink for BudgetedTransport<T> {
    type Error = TransportError<T::Error>;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        let bytes = packet.bytes.len();
        if bytes > self.max_packet_bytes {
            return Err(TransportError::ByteBudgetExceeded {
                budget: self.max_packet_bytes,
                actual: bytes,
            });
        }
        self.inner.send(packet).map_err(TransportError::Inner)
    }

    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        let bytes = batch.bytes_len();
        if bytes > self.max_batch_bytes {
            return Err(TransportError::ByteBudgetExceeded {
                budget: self.max_batch_bytes,
                actual: bytes,
            });
        }
        for packet in &batch.packets {
            if packet.bytes.len() > self.max_packet_bytes {
                return Err(TransportError::ByteBudgetExceeded {
                    budget: self.max_packet_bytes,
                    actual: packet.bytes.len(),
                });
            }
        }
        self.inner.send_batch(batch).map_err(TransportError::Inner)
    }
}

/// Fake transport for benchmarks and tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FakeTransport {
    packets: usize,
    bytes: usize,
    batches: usize,
}

impl FakeTransport {
    /// Returns sent packet count.
    pub const fn packets_sent(&self) -> usize {
        self.packets
    }

    /// Returns sent byte count.
    pub const fn bytes_sent(&self) -> usize {
        self.bytes
    }

    /// Returns sent batch count.
    pub const fn batches_sent(&self) -> usize {
        self.batches
    }
}

impl TransportSink for FakeTransport {
    type Error = core::convert::Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.packets += 1;
        self.bytes += packet.bytes.len();
        Ok(())
    }

    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        self.batches += 1;
        self.packets += batch.packets.len();
        self.bytes += batch.bytes_len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn packet(bytes: usize) -> OutboundPacket {
        OutboundPacket {
            client_id: ClientId::new(1),
            bytes: vec![0; bytes],
        }
    }

    fn station_packet(bytes: usize) -> StationOutboundPacket {
        StationOutboundPacket {
            source_station: StationId::new(1),
            target_station: StationId::new(2),
            bytes: vec![1; bytes],
        }
    }

    fn memory_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
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

    fn recv_ref_with_retry(
        transport: &mut UdpTransport,
    ) -> (Option<ClientId>, SocketAddr, usize, u8, u8, usize) {
        for _ in 0..50 {
            if let Some(packet) = transport
                .try_recv_ref()
                .expect("borrowed udp receive should work")
            {
                return (
                    packet.client_id,
                    packet.remote_addr,
                    packet.bytes.len(),
                    packet.bytes.first().copied().unwrap_or(0),
                    packet.bytes.last().copied().unwrap_or(0),
                    packet.bytes.as_ptr() as usize,
                );
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("borrowed udp packet was not received");
    }

    fn recv_station_with_retry(
        transport: &mut UdpStationTransport,
        station_id: StationId,
    ) -> StationInboundPacket {
        for _ in 0..50 {
            if let Some(packet) = transport
                .try_recv_station(station_id)
                .expect("udp station receive should work")
            {
                return packet;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("udp station packet was not received");
    }

    fn recv_station_ref_with_retry(
        transport: &mut UdpStationTransport,
        station_id: StationId,
    ) -> (StationId, StationId, usize, u8, u8, usize) {
        for _ in 0..50 {
            if let Some(packet) = transport
                .try_recv_station_ref(station_id)
                .expect("borrowed udp station receive should work")
            {
                return (
                    packet.source_station,
                    packet.target_station,
                    packet.bytes.len(),
                    packet.bytes.first().copied().unwrap_or(0),
                    packet.bytes.last().copied().unwrap_or(0),
                    packet.bytes.as_ptr() as usize,
                );
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("borrowed udp station packet was not received");
    }

    #[derive(Clone, Debug, Default)]
    struct TestAuthenticator;

    impl PacketAuthenticator for TestAuthenticator {
        type Error = core::convert::Infallible;

        fn sign(
            &mut self,
            key_id: u32,
            nonce: u64,
            payload: &[u8],
            out: &mut Vec<u8>,
        ) -> Result<(), Self::Error> {
            out.extend_from_slice(&test_tag(key_id, nonce, payload));
            Ok(())
        }

        fn verify(
            &mut self,
            key_id: u32,
            nonce: u64,
            payload: &[u8],
            tag: &[u8],
        ) -> Result<bool, Self::Error> {
            Ok(tag == test_tag(key_id, nonce, payload))
        }
    }

    fn test_tag(key_id: u32, nonce: u64, payload: &[u8]) -> [u8; 8] {
        let mut acc = u64::from(key_id)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(nonce.rotate_left(17));
        for (index, byte) in payload.iter().copied().enumerate() {
            acc = acc.rotate_left(5) ^ (u64::from(byte) << ((index % 8) * 8));
            acc = acc.wrapping_mul(0x1000_0000_01B3);
        }
        acc.to_le_bytes()
    }

    #[test]
    fn fake_transport_counts_batches_without_storing_packets() {
        let mut batch = PacketBatch::new();
        batch.push(packet(3));
        batch.push(packet(5));

        let mut transport = FakeTransport::default();
        transport
            .send_batch(batch)
            .expect("fake transport is infallible");

        assert_eq!(transport.batches_sent(), 1);
        assert_eq!(transport.packets_sent(), 2);
        assert_eq!(transport.bytes_sent(), 8);
    }

    #[test]
    fn budgeted_transport_rejects_large_batch() {
        let mut batch = PacketBatch::new();
        batch.push(packet(8));
        batch.push(packet(8));

        let mut transport = BudgetedTransport::new(FakeTransport::default(), 16, 12);
        let error = transport
            .send_batch(batch)
            .expect_err("batch should exceed budget");
        assert_eq!(
            error,
            TransportError::ByteBudgetExceeded {
                budget: 12,
                actual: 16
            }
        );
        assert_eq!(transport.inner().packets_sent(), 0);
    }

    #[test]
    fn packet_security_envelope_roundtrips_and_enforces_limits() {
        let config = PacketSecurityConfig {
            max_payload_bytes: 4,
            max_tag_bytes: 8,
            max_replay_history: 4,
        };
        let envelope = PacketSecurityEnvelope {
            key_id: 9,
            nonce: 42,
            payload: b"move".to_vec(),
            tag: vec![1, 2, 3, 4],
        };
        let mut bytes = Vec::new();
        envelope
            .encode(config, &mut bytes)
            .expect("envelope should encode");
        let mut borrowed_bytes = Vec::new();
        PacketSecurityEnvelope::encode_parts(
            config,
            envelope.key_id,
            envelope.nonce,
            &envelope.payload,
            &envelope.tag,
            &mut borrowed_bytes,
        )
        .expect("borrowed envelope should encode");
        assert_eq!(borrowed_bytes, bytes);
        let borrowed = PacketSecurityEnvelopeRef::decode(config, &bytes)
            .expect("borrowed envelope should decode");
        assert_eq!(borrowed.key_id, envelope.key_id);
        assert_eq!(borrowed.nonce, envelope.nonce);
        assert_eq!(borrowed.payload, envelope.payload);
        assert_eq!(borrowed.tag, envelope.tag);
        assert!(borrowed.payload.as_ptr() >= bytes.as_ptr());
        assert!(borrowed.tag.as_ptr() >= borrowed.payload.as_ptr());
        assert_eq!(
            PacketSecurityEnvelope::decode(config, &bytes).expect("envelope should decode"),
            envelope
        );

        let too_large = PacketSecurityEnvelope {
            key_id: 9,
            nonce: 43,
            payload: b"large".to_vec(),
            tag: Vec::new(),
        }
        .encode(config, &mut Vec::new())
        .expect_err("payload should exceed configured budget");
        assert_eq!(
            too_large,
            PacketSecurityEncodeError::PayloadTooLarge {
                budget: 4,
                actual: 5
            }
        );

        let mut bad = bytes;
        bad[16..20].copy_from_slice(&5_u32.to_le_bytes());
        assert_eq!(
            PacketSecurityEnvelope::decode(config, &bad)
                .expect_err("decoded payload length should exceed budget"),
            PacketSecurityDecodeError::PayloadTooLarge {
                budget: 4,
                actual: 5
            }
        );
    }

    #[test]
    fn packet_key_ring_selects_active_key_and_accepts_retiring_key() {
        let now = Tick::new(10);
        let mut ring = PacketKeyRing::new(PacketKeyRingConfig { max_keys: 4 });
        ring.insert_active(1, now, 1)
            .expect("first key should insert");
        ring.insert_active(2, Tick::new(11), 10)
            .expect("second key should insert");

        assert_eq!(
            ring.select_send_key(now).expect("key 1 should send").key_id,
            1
        );
        assert_eq!(
            ring.select_send_key(Tick::new(11))
                .expect("key 2 should send")
                .key_id,
            2
        );

        ring.retire(1, Tick::new(12)).expect("key 1 should retire");
        assert_eq!(
            ring.accept_key(1, Tick::new(12))
                .expect("retiring key should still receive")
                .state,
            PacketKeyState::Retiring
        );
        assert_eq!(
            ring.select_send_key(Tick::new(12))
                .expect("active key should win")
                .key_id,
            2
        );
        assert_eq!(ring.stats().keys_inserted, 2);
        assert_eq!(ring.stats().keys_retired, 1);
    }

    #[test]
    fn packet_key_ring_rejects_revoked_expired_and_over_capacity_keys() {
        let mut ring = PacketKeyRing::new(PacketKeyRingConfig { max_keys: 2 });
        ring.insert(PacketKeyDescriptor::active(1, Tick::new(1), 1).with_expiry(Tick::new(5)))
            .expect("expiring key should insert");
        ring.insert_active(2, Tick::new(1), 2)
            .expect("second key should insert");
        assert_eq!(
            ring.insert_active(3, Tick::new(1), 3)
                .expect_err("ring should be full"),
            PacketKeyRingError::CapacityFull { capacity: 2 }
        );
        assert_eq!(
            ring.insert_active(2, Tick::new(1), 2)
                .expect_err("duplicate should reject"),
            PacketKeyRingError::DuplicateKey(2)
        );

        assert!(ring.accept_key(1, Tick::new(4)).is_ok());
        assert_eq!(
            ring.accept_key(1, Tick::new(5))
                .expect_err("expired key should reject"),
            PacketKeyRingError::KeyNotAccepted {
                key_id: 1,
                state: PacketKeyState::Active
            }
        );
        ring.revoke(2).expect("key should revoke");
        assert_eq!(
            ring.accept_key(2, Tick::new(4))
                .expect_err("revoked key should reject"),
            PacketKeyRingError::KeyNotAccepted {
                key_id: 2,
                state: PacketKeyState::Revoked
            }
        );
        assert_eq!(ring.remove_expired(Tick::new(5)), 1);
        assert_eq!(ring.len(), 1);
        assert_eq!(ring.stats().keys_revoked, 1);
        assert_eq!(ring.stats().keys_expired_removed, 1);
    }

    #[test]
    fn packet_security_box_seals_opens_and_rejects_replay() {
        let mut sender = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );
        let mut receiver = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );
        let sealed = sender.seal(7, b"command").expect("packet should seal");
        assert_eq!(sender.stats().sealed, 1);
        let opened = receiver.open(&sealed).expect("packet should open");
        assert_eq!(opened, b"command");
        assert_eq!(receiver.stats().opened, 1);

        let replay = receiver.open(&sealed).expect_err("replay should reject");
        match replay {
            PacketSecurityError::Replay { key_id, nonce } => {
                assert_eq!(key_id, 7);
                assert_eq!(nonce, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(receiver.stats().replay_rejected, 1);
    }

    #[test]
    fn packet_security_seal_into_matches_owned_and_reuses_scratch_atomically() {
        let config = PacketSecurityConfig::default();
        let mut owned = PacketSecurityBox::new(config, TestAuthenticator, PlaintextPacketCipher);
        let mut reused = PacketSecurityBox::new(config, TestAuthenticator, PlaintextPacketCipher);
        let mut scratch = PacketSecurityScratch::with_capacity(32, 8);
        let mut out = Vec::with_capacity(64);

        let expected = owned
            .seal_with_nonce(7, 10, b"command")
            .expect("owned packet should seal");
        reused
            .seal_with_nonce_into(7, 10, b"command", &mut out, &mut scratch)
            .expect("reused packet should seal");
        assert_eq!(out, expected);
        let payload_ptr = scratch.sealed_payload.as_ptr();
        let tag_ptr = scratch.tag.as_ptr();
        let payload_capacity = scratch.retained_payload_capacity();
        let tag_capacity = scratch.retained_tag_capacity();

        out.clear();
        let nonce = reused
            .seal_into(7, b"ack", &mut out, &mut scratch)
            .expect("allocated nonce packet should seal");
        assert_eq!(nonce, 1);
        let envelope =
            PacketSecurityEnvelope::decode(config, &out).expect("reused packet should decode");
        assert_eq!(envelope.nonce, nonce);
        assert_eq!(scratch.sealed_payload.as_ptr(), payload_ptr);
        assert_eq!(scratch.tag.as_ptr(), tag_ptr);
        assert_eq!(scratch.retained_payload_capacity(), payload_capacity);
        assert_eq!(scratch.retained_tag_capacity(), tag_capacity);
        assert_eq!(reused.stats().sealed, 2);

        let before_error = out.clone();
        let too_large = vec![0_u8; config.max_payload_bytes + 1];
        assert!(matches!(
            reused.seal_with_nonce_into(7, 11, &too_large, &mut out, &mut scratch),
            Err(PacketSecurityError::Encode(
                PacketSecurityEncodeError::PayloadTooLarge { .. }
            ))
        ));
        assert_eq!(out, before_error);

        let small_tag_config = PacketSecurityConfig {
            max_tag_bytes: 4,
            ..config
        };
        let mut small_tag =
            PacketSecurityBox::new(small_tag_config, TestAuthenticator, PlaintextPacketCipher);
        assert!(matches!(
            small_tag.seal_with_nonce_into(7, 12, b"tag", &mut out, &mut scratch),
            Err(PacketSecurityError::Encode(
                PacketSecurityEncodeError::TagTooLarge { .. }
            ))
        ));
        assert_eq!(out, before_error);
    }

    #[test]
    fn packet_security_open_scratch_matches_owned_reuses_and_preserves_failed_input() {
        let config = PacketSecurityConfig::default();
        let mut sender = PacketSecurityBox::new(config, TestAuthenticator, PlaintextPacketCipher);
        let first = sender
            .seal_with_nonce(7, 10, b"first-command")
            .expect("first packet should seal");
        let second = sender
            .seal_with_nonce(7, 11, b"second-command")
            .expect("second packet should seal");
        let mut owned_receiver =
            PacketSecurityBox::new(config, TestAuthenticator, PlaintextPacketCipher);
        let expected = owned_receiver
            .open(&first)
            .expect("owned packet should open");

        let mut receiver = PacketSecurityBox::new(config, TestAuthenticator, PlaintextPacketCipher);
        let mut scratch = PacketSecurityOpenScratch::with_capacity(32);
        let first_ptr = {
            let opened = receiver
                .open_with_scratch(&first, &mut scratch)
                .expect("scratch packet should open");
            assert_eq!(opened.key_id, 7);
            assert_eq!(opened.nonce, 10);
            assert_eq!(opened.payload, expected);
            opened.payload.as_ptr()
        };
        let retained = scratch.retained_payload_capacity();
        let second_payload = {
            let opened = receiver
                .open_with_scratch(&second, &mut scratch)
                .expect("second scratch packet should open");
            assert_eq!(opened.payload.as_ptr(), first_ptr);
            opened.payload.to_vec()
        };
        assert_eq!(second_payload, b"second-command");
        assert_eq!(scratch.retained_payload_capacity(), retained);
        assert_eq!(receiver.stats().opened, 2);

        let before_failure = scratch.payload.clone();
        let mut tampered = sender
            .seal_with_nonce(7, 12, b"tampered")
            .expect("tampered source should seal");
        tampered[PACKET_SECURITY_HEADER_BYTES] ^= 0x55;
        assert!(matches!(
            receiver.open_with_scratch(&tampered, &mut scratch),
            Err(PacketSecurityError::AuthenticationFailed { .. })
        ));
        assert_eq!(scratch.payload, before_failure);
    }

    #[test]
    fn packet_security_box_uses_key_ring_for_rotation_policy() {
        let mut sender_ring = PacketKeyRing::default();
        let mut receiver_ring = PacketKeyRing::default();
        sender_ring
            .insert_active(7, Tick::new(10), 1)
            .expect("sender key should insert");
        receiver_ring
            .insert_active(7, Tick::new(10), 1)
            .expect("receiver key should insert");

        let mut sender = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );
        let mut receiver = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );
        let mut open_scratch = PacketSecurityOpenScratch::with_capacity(16);

        let sealed = sender
            .seal_with_key_ring(&sender_ring, b"command", Tick::new(10))
            .expect("packet should seal through selected key");
        let envelope = PacketSecurityEnvelope::decode(PacketSecurityConfig::default(), &sealed)
            .expect("packet should decode");
        assert_eq!(envelope.key_id, 7);
        assert_eq!(
            receiver
                .open_with_key_ring_and_scratch(
                    &receiver_ring,
                    &sealed,
                    Tick::new(10),
                    &mut open_scratch,
                )
                .expect("packet should open through accepted key")
                .payload,
            b"command"
        );

        sender_ring
            .insert_active(8, Tick::new(11), 10)
            .expect("rotated sender key should insert");
        receiver_ring
            .insert_active(8, Tick::new(11), 10)
            .expect("rotated receiver key should insert");
        receiver_ring
            .retire(7, Tick::new(11))
            .expect("old key should retire");
        let rotated = sender
            .seal_with_key_ring(&sender_ring, b"ack", Tick::new(11))
            .expect("rotated packet should seal");
        let rotated_envelope =
            PacketSecurityEnvelope::decode(PacketSecurityConfig::default(), &rotated)
                .expect("rotated packet should decode");
        assert_eq!(rotated_envelope.key_id, 8);
        assert_eq!(
            receiver
                .open_with_key_ring_and_scratch(
                    &receiver_ring,
                    &rotated,
                    Tick::new(11),
                    &mut open_scratch,
                )
                .expect("rotated packet should open")
                .payload,
            b"ack"
        );

        receiver_ring.revoke(7).expect("old key should revoke");
        let stale = sender
            .seal_with_nonce(7, 99, b"stale")
            .expect("explicit stale-key packet should seal");
        let error = receiver
            .open_with_key_ring(&receiver_ring, &stale, Tick::new(12))
            .expect_err("revoked key should reject before auth");
        match error {
            PacketSecurityError::Key(PacketKeyRingError::KeyNotAccepted { key_id, state }) => {
                assert_eq!(key_id, 7);
                assert_eq!(state, PacketKeyState::Revoked);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(receiver.stats().key_rejected, 1);
    }

    #[test]
    fn packet_security_box_rejects_tampered_payload() {
        let mut sender = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );
        let mut receiver = PacketSecurityBox::new(
            PacketSecurityConfig::default(),
            TestAuthenticator,
            PlaintextPacketCipher,
        );

        let mut sealed = sender
            .seal_with_nonce(7, 10, b"command")
            .expect("packet should seal");
        let payload_offset = PACKET_SECURITY_HEADER_BYTES;
        sealed[payload_offset] ^= 0x55;
        let error = receiver
            .open(&sealed)
            .expect_err("tampered payload should reject");
        match error {
            PacketSecurityError::AuthenticationFailed { key_id, nonce } => {
                assert_eq!(key_id, 7);
                assert_eq!(nonce, 10);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(receiver.stats().auth_failed, 1);
        assert_eq!(receiver.replay().len(), 0);
    }

    #[test]
    fn packet_replay_window_bounds_history() {
        let mut replay = PacketReplayWindow::new(2);
        assert!(replay.accept(1, 1));
        assert!(replay.accept(1, 2));
        assert!(!replay.accept(1, 2));
        assert!(replay.accept(1, 3));
        assert_eq!(replay.len(), 2);
        assert!(!replay.contains(1, 1));
        assert!(replay.accept(1, 1));
    }

    #[test]
    fn bounded_duplicate_indexes_adapt_to_configured_capacity() {
        let ordered = PacketReplayWindow::new(HASHED_BOUNDED_SET_MIN_CAPACITY - 1);
        let hashed = PacketReplayWindow::new(HASHED_BOUNDED_SET_MIN_CAPACITY);
        assert!(!ordered.seen.is_hashed());
        assert!(hashed.seen.is_hashed());

        let client = ReliableClientReceiver::new(ReliableClientConfig {
            max_delivered_history: HASHED_BOUNDED_SET_MIN_CAPACITY,
            ..ReliableClientConfig::default()
        });
        let station = ReliableStationReceiver::new(ReliableStationConfig {
            max_delivered_history: HASHED_BOUNDED_SET_MIN_CAPACITY,
            ..ReliableStationConfig::default()
        });
        assert!(client.delivered.is_hashed());
        assert!(station.delivered.is_hashed());
    }

    #[test]
    fn in_memory_transport_delivers_bounded_packets() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 2,
            max_packet_bytes: 8,
        });
        let mut client = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        let mut server = hub
            .endpoint(server_id, memory_addr(20000))
            .expect("server should register");

        client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: b"command".to_vec(),
            })
            .expect("client packet should send");
        assert_eq!(
            hub.queued_len(server_id).expect("queue should exist"),
            Some(1)
        );

        let inbound = server
            .try_recv()
            .expect("server receive should work")
            .expect("packet should exist");
        assert_eq!(inbound.client_id, Some(client_id));
        assert_eq!(inbound.remote_addr, memory_addr(20007));
        assert_eq!(inbound.bytes, b"command");

        let stats = hub.stats().expect("stats should read");
        assert_eq!(stats.packets_sent, 1);
        assert_eq!(stats.packets_received, 1);
        assert_eq!(stats.bytes_sent, 7);
        assert_eq!(stats.bytes_received, 7);
    }

    #[test]
    fn in_memory_transport_rejects_full_queue_and_large_packet() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 1,
            max_packet_bytes: 4,
        });
        let mut client = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        hub.endpoint(server_id, memory_addr(20000))
            .expect("server should register");

        client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: vec![0; 4],
            })
            .expect("first packet should send");

        let full = client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: vec![0; 4],
            })
            .expect_err("queue should be full");
        assert_eq!(
            full,
            InMemoryTransportError::QueueFull {
                client_id: server_id,
                capacity: 1
            }
        );

        let large = client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: vec![0; 5],
            })
            .expect_err("packet should exceed budget");
        assert_eq!(
            large,
            InMemoryTransportError::PacketTooLarge {
                budget: 4,
                actual: 5
            }
        );

        let stats = hub.stats().expect("stats should read");
        assert_eq!(stats.packets_rejected_full, 1);
        assert_eq!(stats.packets_rejected_bytes, 1);
    }

    #[test]
    fn reliable_client_frame_roundtrips_data_and_ack() {
        let data = ReliableClientFrame::Data {
            sequence: 42,
            payload: b"command".to_vec(),
        };
        let mut bytes = Vec::new();
        data.encode(&mut bytes).expect("data frame should encode");
        let mut direct = Vec::new();
        ReliableClientFrame::encode_data(42, b"command", &mut direct)
            .expect("borrowed data frame should encode");
        assert_eq!(direct, bytes);
        assert_eq!(
            ReliableClientFrame::decode_ref(&bytes).expect("data frame view should decode"),
            ReliableClientFrameRef::Data {
                sequence: 42,
                payload: b"command"
            }
        );
        assert_eq!(
            ReliableClientFrame::decode(&bytes).expect("data frame should decode"),
            data
        );
        let truncated = &bytes[..bytes.len() - 1];
        assert_eq!(
            ReliableClientFrame::decode_ref(truncated).expect_err("view should reject truncation"),
            ReliableClientFrame::decode(truncated).expect_err("owned should reject truncation")
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            ReliableClientFrame::decode_ref(&trailing).expect_err("view should reject trailing"),
            ReliableClientFrame::decode(&trailing).expect_err("owned should reject trailing")
        );

        let ack = ReliableClientFrame::Ack { sequence: 42 };
        bytes.clear();
        ack.encode(&mut bytes).expect("ack frame should encode");
        assert_eq!(
            ReliableClientFrame::decode(&bytes).expect("ack frame should decode"),
            ack
        );
    }

    #[test]
    fn reliable_client_endpoint_delivers_payload_and_acknowledges() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::default();
        let mut client_transport = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        let mut server_transport = hub
            .endpoint(server_id, memory_addr(20000))
            .expect("server should register");
        let mut client = ReliableClientEndpoint::default();
        let mut server = ReliableClientEndpoint::default();

        let sequence = client
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: b"command".to_vec(),
                },
                0,
            )
            .expect("reliable command should send");
        assert_eq!(sequence, 1);
        assert_eq!(client.sender.in_flight_len(), 1);

        let raw = server_transport
            .try_recv()
            .expect("server receive should work")
            .expect("data packet should exist");
        let wire_pointer = raw.bytes.as_ptr();
        let delivered = server
            .handle_inbound(&mut server_transport, raw)
            .expect("data packet should handle")
            .expect("first data packet should deliver");
        assert_eq!(delivered.client_id, Some(client_id));
        assert_eq!(delivered.remote_addr, memory_addr(20007));
        assert_eq!(delivered.bytes, b"command");
        assert_eq!(delivered.bytes.as_ptr(), wire_pointer);
        assert_eq!(server.receiver.stats().data_delivered, 1);
        assert_eq!(server.receiver.stats().acks_sent, 1);

        let ack = client_transport
            .try_recv()
            .expect("client ACK receive should work")
            .expect("ACK packet should exist");
        assert_eq!(
            client
                .handle_inbound(&mut client_transport, ack)
                .expect("ACK should handle"),
            None
        );
        assert_eq!(client.sender.in_flight_len(), 0);
        assert_eq!(client.sender.stats().acks_received, 1);
    }

    #[test]
    fn reliable_client_endpoint_retries_and_suppresses_duplicate_delivery() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::default();
        let mut client_transport = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        let mut server_transport = hub
            .endpoint(server_id, memory_addr(20000))
            .expect("server should register");
        let mut client = ReliableClientEndpoint::default();
        let mut server = ReliableClientEndpoint::default();

        client
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: b"idempotent-command".to_vec(),
                },
                0,
            )
            .expect("reliable command should send");
        let mut retry_scratch = ReliableClientRetryScratch::new();
        let retry = client
            .retry_due_with_scratch(&mut client_transport, 2, &mut retry_scratch)
            .expect("retry should send");
        assert_eq!(retry.retried, 1);
        assert_eq!(retry.timed_out, 0);
        assert_eq!(client.sender.stats().retries_sent, 1);
        let retained_keys = retry_scratch.retained_key_capacity();
        assert!(retained_keys >= 1);
        assert_eq!(
            client
                .retry_due_with_scratch(&mut client_transport, 2, &mut retry_scratch)
                .expect("non-due scan should succeed"),
            ReliableRetryReport::default()
        );
        assert_eq!(retry_scratch.retained_key_capacity(), retained_keys);
        assert_eq!(
            hub.queued_len(server_id).expect("queue should exist"),
            Some(2)
        );

        let first_raw = server_transport
            .try_recv()
            .expect("server receive should work")
            .expect("first data packet should exist");
        let delivered = server
            .handle_inbound(&mut server_transport, first_raw)
            .expect("first data packet should handle")
            .expect("first data packet should deliver");
        assert_eq!(delivered.bytes, b"idempotent-command");

        let duplicate_raw = server_transport
            .try_recv()
            .expect("server receive should work")
            .expect("duplicate data packet should exist");
        assert_eq!(
            server
                .handle_inbound(&mut server_transport, duplicate_raw)
                .expect("duplicate data packet should handle"),
            None
        );
        assert_eq!(server.receiver.stats().data_delivered, 1);
        assert_eq!(server.receiver.stats().duplicates_suppressed, 1);
        assert_eq!(server.receiver.stats().acks_sent, 2);
    }

    #[test]
    fn reliable_client_failed_retry_preserves_attempt_before_timeout() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 1,
            max_packet_bytes: 128,
        });
        let mut client_transport = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        let mut server_transport = hub
            .endpoint(server_id, memory_addr(20000))
            .expect("server should register");
        let mut sender = ReliableClientSender::new(ReliableClientConfig {
            max_in_flight_per_peer: 1,
            retry_after_ticks: 1,
            max_attempts: 2,
            max_payload_bytes: 64,
            max_delivered_history: 0,
        });
        let mut scratch = ReliableClientRetryScratch::new();
        sender
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: b"retry".to_vec(),
                },
                0,
            )
            .expect("initial packet should fill queue");

        assert!(matches!(
            sender.retry_due_with_scratch(&mut client_transport, 1, &mut scratch),
            Err(ReliableClientError::Transport(
                InMemoryTransportError::QueueFull { .. }
            ))
        ));
        server_transport
            .try_recv()
            .expect("queue should read")
            .expect("initial packet should remain");
        assert_eq!(
            sender
                .retry_due_with_scratch(&mut client_transport, 1, &mut scratch)
                .expect("failed attempt must remain retryable")
                .retried,
            1
        );
        let timeout = sender
            .retry_due_with_scratch(&mut client_transport, 2, &mut scratch)
            .expect("exhausted packet should time out");
        assert_eq!(timeout.retried, 0);
        assert_eq!(timeout.timed_out, 1);
        assert_eq!(sender.in_flight_len(), 0);
        assert_eq!(sender.in_flight_for(server_id), 0);
        assert!(!sender.in_flight_by_peer.contains_key(&server_id));
    }

    #[test]
    fn reliable_client_receiver_bounds_duplicate_history() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::default();
        hub.endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        let mut server_transport = hub
            .endpoint(server_id, memory_addr(20000))
            .expect("server should register");
        let config = ReliableClientConfig {
            max_in_flight_per_peer: 8,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: DEFAULT_RELIABLE_CLIENT_MAX_PAYLOAD_BYTES,
            max_delivered_history: 1,
        };
        let mut server = ReliableClientEndpoint::new(config);

        let packet = |sequence: u64, payload: &[u8]| {
            let mut bytes = Vec::new();
            ReliableClientFrame::Data {
                sequence,
                payload: payload.to_vec(),
            }
            .encode(&mut bytes)
            .expect("data frame should encode");
            InboundPacket {
                client_id: Some(client_id),
                remote_addr: memory_addr(20007),
                bytes,
            }
        };

        assert!(
            server
                .handle_inbound(&mut server_transport, packet(1, b"first"))
                .expect("first data packet should handle")
                .is_some()
        );
        assert_eq!(
            server
                .handle_inbound(&mut server_transport, packet(1, b"first-duplicate"))
                .expect("duplicate data packet should handle"),
            None
        );
        assert!(
            server
                .handle_inbound(&mut server_transport, packet(2, b"second"))
                .expect("second data packet should handle")
                .is_some()
        );
        assert!(
            server
                .handle_inbound(&mut server_transport, packet(1, b"first-after-eviction"))
                .expect("evicted data packet should handle")
                .is_some()
        );
        assert_eq!(server.receiver.stats().data_delivered, 3);
        assert_eq!(server.receiver.stats().duplicates_suppressed, 1);
    }

    #[test]
    fn reliable_client_sender_enforces_payload_and_window_limits() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::default();
        let mut client_transport = hub
            .endpoint(client_id, memory_addr(20007))
            .expect("client should register");
        hub.endpoint(server_id, memory_addr(20000))
            .expect("server should register");
        let config = ReliableClientConfig {
            max_in_flight_per_peer: 1,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: 4,
            max_delivered_history: DEFAULT_RELIABLE_CLIENT_DELIVERED_HISTORY,
        };
        let mut client = ReliableClientEndpoint::new(config);

        let too_large = client
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: vec![0; 5],
                },
                0,
            )
            .expect_err("payload should exceed configured budget");
        match too_large {
            ReliableClientError::PayloadTooLarge { budget, actual } => {
                assert_eq!(budget, 4);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other}"),
        }

        client
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: vec![0; 4],
                },
                0,
            )
            .expect("first packet should fit");
        let full = client
            .send(
                &mut client_transport,
                OutboundPacket {
                    client_id: server_id,
                    bytes: vec![1; 4],
                },
                0,
            )
            .expect_err("in-flight window should be full");
        match full {
            ReliableClientError::WindowFull {
                peer_client,
                capacity,
            } => {
                assert_eq!(peer_client, server_id);
                assert_eq!(capacity, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn reliable_client_window_counts_track_peers_and_saturated_sequence_replacement() {
        let first_peer = ClientId::new(1);
        let second_peer = ClientId::new(2);
        let mut transport = FakeTransport::default();
        let mut sender = ReliableClientSender::new(ReliableClientConfig {
            max_in_flight_per_peer: 3,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: 16,
            max_delivered_history: 0,
        });
        let mut send = |sender: &mut ReliableClientSender, peer_client| {
            sender
                .send(
                    &mut transport,
                    OutboundPacket {
                        client_id: peer_client,
                        bytes: b"count".to_vec(),
                    },
                    0,
                )
                .expect("bounded packet should send")
        };

        assert_eq!(send(&mut sender, first_peer), 1);
        assert_eq!(send(&mut sender, first_peer), 2);
        assert_eq!(send(&mut sender, second_peer), 1);
        assert_eq!(sender.in_flight_for(first_peer), 2);
        assert_eq!(sender.in_flight_for(second_peer), 1);
        assert!(sender.acknowledge(first_peer, 1));
        assert_eq!(sender.in_flight_for(first_peer), 1);

        sender.next_sequence.insert(first_peer, u64::MAX);
        assert_eq!(send(&mut sender, first_peer), u64::MAX);
        assert_eq!(sender.in_flight_for(first_peer), 2);
        assert_eq!(send(&mut sender, first_peer), u64::MAX);
        assert_eq!(sender.in_flight_for(first_peer), 2);

        assert!(sender.acknowledge(first_peer, u64::MAX));
        assert!(sender.acknowledge(first_peer, 2));
        assert_eq!(sender.in_flight_for(first_peer), 0);
        assert!(!sender.in_flight_by_peer.contains_key(&first_peer));
        assert_eq!(sender.in_flight_for(second_peer), 1);
    }

    #[test]
    fn reliable_client_endpoint_rejects_unknown_packet_source() {
        let mut endpoint = ReliableClientEndpoint::default();
        let mut transport = FakeTransport::default();
        let mut bytes = Vec::new();
        ReliableClientFrame::Ack { sequence: 1 }
            .encode(&mut bytes)
            .expect("ACK should encode");
        let error = endpoint
            .handle_inbound(
                &mut transport,
                InboundPacket {
                    client_id: None,
                    remote_addr: memory_addr(20007),
                    bytes,
                },
            )
            .expect_err("unknown source should be rejected");
        match error {
            ReliableClientError::MissingSourceClient => {}
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn in_memory_station_transport_delivers_bounded_packets() {
        let mut transport = InMemoryStationTransport::new(StationTransportLimits {
            max_queued_packets_per_station: 2,
            max_packet_bytes: 8,
        });
        transport.register_station(StationId::new(2));

        transport
            .send_station(station_packet(4))
            .expect("station packet should send");
        assert_eq!(transport.queued_len(StationId::new(2)), Some(1));

        let packet = transport
            .try_recv_station(StationId::new(2))
            .expect("receive should work")
            .expect("packet should exist");
        assert_eq!(packet.source_station, StationId::new(1));
        assert_eq!(packet.target_station, StationId::new(2));
        assert_eq!(packet.bytes, vec![1; 4]);
        assert_eq!(transport.stats().packets_sent, 1);
        assert_eq!(transport.stats().packets_received, 1);
    }

    #[test]
    fn in_memory_station_transport_rejects_full_queue_and_large_packet() {
        let mut transport = InMemoryStationTransport::new(StationTransportLimits {
            max_queued_packets_per_station: 1,
            max_packet_bytes: 4,
        });
        transport.register_station(StationId::new(2));
        transport
            .send_station(station_packet(4))
            .expect("first packet should send");

        let full = transport
            .send_station(station_packet(4))
            .expect_err("queue should be full");
        assert_eq!(
            full,
            StationTransportError::QueueFull {
                station_id: StationId::new(2),
                capacity: 1
            }
        );

        let large = transport
            .send_station(station_packet(5))
            .expect_err("packet should exceed budget");
        assert_eq!(
            large,
            StationTransportError::PacketTooLarge {
                budget: 4,
                actual: 5
            }
        );
        assert_eq!(transport.stats().packets_rejected_full, 1);
        assert_eq!(transport.stats().packets_rejected_bytes, 1);
    }

    #[test]
    fn reliable_station_frame_roundtrips_data_and_ack() {
        let data = ReliableStationFrame::Data {
            sequence: 42,
            payload: b"station-event".to_vec(),
        };
        let mut bytes = Vec::new();
        data.encode(&mut bytes).expect("data frame should encode");
        let mut direct = Vec::new();
        ReliableStationFrame::encode_data(42, b"station-event", &mut direct)
            .expect("borrowed data frame should encode");
        assert_eq!(direct, bytes);
        assert_eq!(
            ReliableStationFrame::decode_ref(&bytes).expect("data frame view should decode"),
            ReliableStationFrameRef::Data {
                sequence: 42,
                payload: b"station-event"
            }
        );
        assert_eq!(
            ReliableStationFrame::decode(&bytes).expect("data frame should decode"),
            data
        );
        let truncated = &bytes[..bytes.len() - 1];
        assert_eq!(
            ReliableStationFrame::decode_ref(truncated).expect_err("view should reject truncation"),
            ReliableStationFrame::decode(truncated).expect_err("owned should reject truncation")
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            ReliableStationFrame::decode_ref(&trailing).expect_err("view should reject trailing"),
            ReliableStationFrame::decode(&trailing).expect_err("owned should reject trailing")
        );

        let ack = ReliableStationFrame::Ack { sequence: 42 };
        bytes.clear();
        ack.encode(&mut bytes).expect("ack frame should encode");
        assert_eq!(
            ReliableStationFrame::decode(&bytes).expect("ack frame should decode"),
            ack
        );
    }

    #[test]
    fn reliable_station_endpoint_delivers_payload_and_acknowledges() {
        let station_one = StationId::new(1);
        let station_two = StationId::new(2);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(station_one);
        transport.register_station(station_two);
        let mut first = ReliableStationEndpoint::default();
        let mut second = ReliableStationEndpoint::default();

        let sequence = first
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: station_one,
                    target_station: station_two,
                    bytes: b"event".to_vec(),
                },
                0,
            )
            .expect("reliable packet should send");
        assert_eq!(sequence, 1);
        assert_eq!(first.sender.in_flight_len(), 1);

        let raw = transport
            .try_recv_station(station_two)
            .expect("receive should work")
            .expect("data packet should exist");
        let wire_pointer = raw.bytes.as_ptr();
        let delivered = second
            .handle_inbound(&mut transport, raw)
            .expect("data packet should handle")
            .expect("first data packet should deliver");
        assert_eq!(delivered.source_station, station_one);
        assert_eq!(delivered.target_station, station_two);
        assert_eq!(delivered.bytes, b"event");
        assert_eq!(delivered.bytes.as_ptr(), wire_pointer);
        assert_eq!(second.receiver.stats().data_delivered, 1);
        assert_eq!(second.receiver.stats().acks_sent, 1);

        let ack = transport
            .try_recv_station(station_one)
            .expect("ack receive should work")
            .expect("ack packet should exist");
        assert_eq!(
            first
                .handle_inbound(&mut transport, ack)
                .expect("ack should handle"),
            None
        );
        assert_eq!(first.sender.in_flight_len(), 0);
        assert_eq!(first.sender.stats().acks_received, 1);
    }

    #[test]
    fn reliable_station_endpoint_retries_and_suppresses_duplicate_delivery() {
        let station_one = StationId::new(1);
        let station_two = StationId::new(2);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(station_one);
        transport.register_station(station_two);
        let mut first = ReliableStationEndpoint::default();
        let mut second = ReliableStationEndpoint::default();

        first
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: station_one,
                    target_station: station_two,
                    bytes: b"idempotent-event".to_vec(),
                },
                0,
            )
            .expect("reliable packet should send");
        let mut retry_scratch = ReliableStationRetryScratch::new();
        let retry = first
            .retry_due_with_scratch(&mut transport, 2, &mut retry_scratch)
            .expect("retry should send");
        assert_eq!(retry.retried, 1);
        assert_eq!(retry.timed_out, 0);
        assert_eq!(first.sender.stats().retries_sent, 1);
        let retained_keys = retry_scratch.retained_key_capacity();
        assert!(retained_keys >= 1);
        assert_eq!(
            first
                .retry_due_with_scratch(&mut transport, 2, &mut retry_scratch)
                .expect("non-due scan should succeed"),
            ReliableRetryReport::default()
        );
        assert_eq!(retry_scratch.retained_key_capacity(), retained_keys);
        assert_eq!(transport.queued_len(station_two), Some(2));

        let first_raw = transport
            .try_recv_station(station_two)
            .expect("receive should work")
            .expect("first data packet should exist");
        let delivered = second
            .handle_inbound(&mut transport, first_raw)
            .expect("first data packet should handle")
            .expect("first data packet should deliver");
        assert_eq!(delivered.bytes, b"idempotent-event");

        let duplicate_raw = transport
            .try_recv_station(station_two)
            .expect("receive should work")
            .expect("duplicate data packet should exist");
        assert_eq!(
            second
                .handle_inbound(&mut transport, duplicate_raw)
                .expect("duplicate data packet should handle"),
            None
        );
        assert_eq!(second.receiver.stats().data_delivered, 1);
        assert_eq!(second.receiver.stats().duplicates_suppressed, 1);
        assert_eq!(second.receiver.stats().acks_sent, 2);
    }

    #[test]
    fn reliable_station_failed_retry_preserves_attempt_before_timeout() {
        let source = StationId::new(1);
        let target = StationId::new(2);
        let mut transport = InMemoryStationTransport::new(StationTransportLimits {
            max_queued_packets_per_station: 1,
            max_packet_bytes: 128,
        });
        transport.register_station(target);
        let mut sender = ReliableStationSender::new(ReliableStationConfig {
            max_in_flight_per_target: 1,
            retry_after_ticks: 1,
            max_attempts: 2,
            max_payload_bytes: 64,
            max_delivered_history: 0,
        });
        let mut scratch = ReliableStationRetryScratch::new();
        sender
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: source,
                    target_station: target,
                    bytes: b"retry".to_vec(),
                },
                0,
            )
            .expect("initial packet should fill queue");

        assert!(matches!(
            sender.retry_due_with_scratch(&mut transport, 1, &mut scratch),
            Err(ReliableStationError::Transport(
                StationTransportError::QueueFull { .. }
            ))
        ));
        transport
            .try_recv_station(target)
            .expect("queue should read")
            .expect("initial packet should remain");
        assert_eq!(
            sender
                .retry_due_with_scratch(&mut transport, 1, &mut scratch)
                .expect("failed attempt must remain retryable")
                .retried,
            1
        );
        let timeout = sender
            .retry_due_with_scratch(&mut transport, 2, &mut scratch)
            .expect("exhausted packet should time out");
        assert_eq!(timeout.retried, 0);
        assert_eq!(timeout.timed_out, 1);
        assert_eq!(sender.in_flight_len(), 0);
        assert_eq!(sender.in_flight_for(target), 0);
        assert!(!sender.in_flight_by_target.contains_key(&target));
    }

    #[test]
    fn reliable_station_receiver_bounds_duplicate_history() {
        let station_one = StationId::new(1);
        let station_two = StationId::new(2);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(station_one);
        transport.register_station(station_two);
        let config = ReliableStationConfig {
            max_in_flight_per_target: 8,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: DEFAULT_RELIABLE_STATION_MAX_PAYLOAD_BYTES,
            max_delivered_history: 1,
        };
        let mut endpoint = ReliableStationEndpoint::new(config);

        let packet = |sequence: u64, payload: &[u8]| {
            let mut bytes = Vec::new();
            ReliableStationFrame::Data {
                sequence,
                payload: payload.to_vec(),
            }
            .encode(&mut bytes)
            .expect("data frame should encode");
            StationInboundPacket {
                source_station: station_one,
                target_station: station_two,
                bytes,
            }
        };

        assert!(
            endpoint
                .handle_inbound(&mut transport, packet(1, b"first"))
                .expect("first data packet should handle")
                .is_some()
        );
        assert_eq!(
            endpoint
                .handle_inbound(&mut transport, packet(1, b"first-duplicate"))
                .expect("duplicate data packet should handle"),
            None
        );
        assert!(
            endpoint
                .handle_inbound(&mut transport, packet(2, b"second"))
                .expect("second data packet should handle")
                .is_some()
        );
        assert!(
            endpoint
                .handle_inbound(&mut transport, packet(1, b"first-after-eviction"))
                .expect("evicted data packet should handle")
                .is_some()
        );
        assert_eq!(endpoint.receiver.stats().data_delivered, 3);
        assert_eq!(endpoint.receiver.stats().duplicates_suppressed, 1);
    }

    #[test]
    fn reliable_station_sender_enforces_payload_and_window_limits() {
        let station_one = StationId::new(1);
        let station_two = StationId::new(2);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(station_one);
        transport.register_station(station_two);
        let config = ReliableStationConfig {
            max_in_flight_per_target: 1,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: 4,
            max_delivered_history: DEFAULT_RELIABLE_STATION_DELIVERED_HISTORY,
        };
        let mut endpoint = ReliableStationEndpoint::new(config);

        let too_large = endpoint
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: station_one,
                    target_station: station_two,
                    bytes: vec![0; 5],
                },
                0,
            )
            .expect_err("payload should exceed configured budget");
        match too_large {
            ReliableStationError::PayloadTooLarge { budget, actual } => {
                assert_eq!(budget, 4);
                assert_eq!(actual, 5);
            }
            other => panic!("unexpected error: {other}"),
        }

        endpoint
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: station_one,
                    target_station: station_two,
                    bytes: vec![0; 4],
                },
                0,
            )
            .expect("first packet should fit");
        let full = endpoint
            .send(
                &mut transport,
                StationOutboundPacket {
                    source_station: station_one,
                    target_station: station_two,
                    bytes: vec![1; 4],
                },
                0,
            )
            .expect_err("in-flight window should be full");
        match full {
            ReliableStationError::WindowFull {
                target_station,
                capacity,
            } => {
                assert_eq!(target_station, station_two);
                assert_eq!(capacity, 1);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn reliable_station_window_counts_track_targets_and_saturated_sequence_replacement() {
        let source = StationId::new(1);
        let first_target = StationId::new(2);
        let second_target = StationId::new(3);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(first_target);
        transport.register_station(second_target);
        let mut sender = ReliableStationSender::new(ReliableStationConfig {
            max_in_flight_per_target: 3,
            retry_after_ticks: 2,
            max_attempts: 4,
            max_payload_bytes: 16,
            max_delivered_history: 0,
        });
        let mut send = |sender: &mut ReliableStationSender, target_station| {
            sender
                .send(
                    &mut transport,
                    StationOutboundPacket {
                        source_station: source,
                        target_station,
                        bytes: b"count".to_vec(),
                    },
                    0,
                )
                .expect("bounded packet should send")
        };

        assert_eq!(send(&mut sender, first_target), 1);
        assert_eq!(send(&mut sender, first_target), 2);
        assert_eq!(send(&mut sender, second_target), 1);
        assert_eq!(sender.in_flight_for(first_target), 2);
        assert_eq!(sender.in_flight_for(second_target), 1);
        assert!(sender.acknowledge(first_target, 1));
        assert_eq!(sender.in_flight_for(first_target), 1);

        sender.next_sequence.insert(first_target, u64::MAX);
        assert_eq!(send(&mut sender, first_target), u64::MAX);
        assert_eq!(sender.in_flight_for(first_target), 2);
        assert_eq!(send(&mut sender, first_target), u64::MAX);
        assert_eq!(sender.in_flight_for(first_target), 2);

        assert!(sender.acknowledge(first_target, u64::MAX));
        assert!(sender.acknowledge(first_target, 2));
        assert_eq!(sender.in_flight_for(first_target), 0);
        assert!(!sender.in_flight_by_target.contains_key(&first_target));
        assert_eq!(sender.in_flight_for(second_target), 1);
    }

    #[test]
    fn udp_transport_sends_and_receives_registered_client() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let mut server = UdpTransport::bind("127.0.0.1:0").expect("server should bind");
        let mut client = UdpTransport::bind("127.0.0.1:0").expect("client should bind");
        let server_addr = server.local_addr().expect("server addr should exist");
        let client_addr = client.local_addr().expect("client addr should exist");

        server.register_client(client_id, client_addr);
        client.register_client(server_id, server_addr);

        client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: b"command".to_vec(),
            })
            .expect("client should send");
        let first = recv_ref_with_retry(&mut server);
        assert_eq!(first.0, Some(client_id));
        assert_eq!(first.1, client_addr);
        assert_eq!((first.2, first.3, first.4), (7, b'c', b'd'));

        client
            .send(OutboundPacket {
                client_id: server_id,
                bytes: b"next".to_vec(),
            })
            .expect("client should send another packet");
        let second = recv_ref_with_retry(&mut server);
        assert_eq!((second.2, second.3, second.4), (4, b'n', b't'));
        assert_eq!(second.5, first.5);

        server
            .send(OutboundPacket {
                client_id,
                bytes: b"replication".to_vec(),
            })
            .expect("server should send");
        let inbound = recv_with_retry(&mut client);
        assert_eq!(inbound.client_id, Some(server_id));
        assert_eq!(inbound.remote_addr, server_addr);
        assert_eq!(inbound.bytes, b"replication");
    }

    #[test]
    fn udp_transport_rejects_unknown_client() {
        let mut transport = UdpTransport::bind("127.0.0.1:0").expect("transport should bind");
        let error = transport
            .send(OutboundPacket {
                client_id: ClientId::new(99),
                bytes: Vec::new(),
            })
            .expect_err("unknown client should fail");

        match error {
            UdpTransportError::UnknownClient(client_id) => {
                assert_eq!(client_id, ClientId::new(99));
            }
            UdpTransportError::Io(error) => panic!("unexpected io error: {error}"),
        }
    }

    #[test]
    fn udp_station_transport_sends_and_receives_registered_stations() {
        let station_one = StationId::new(1);
        let station_two = StationId::new(2);
        let mut first =
            UdpStationTransport::bind(station_one, "127.0.0.1:0").expect("first should bind");
        let mut second =
            UdpStationTransport::bind(station_two, "127.0.0.1:0").expect("second should bind");
        let first_addr = first.local_addr().expect("first addr should exist");
        let second_addr = second.local_addr().expect("second addr should exist");

        first.register_station(station_two, second_addr);
        second.register_station(station_one, first_addr);

        first
            .send_station(StationOutboundPacket {
                source_station: station_one,
                target_station: station_two,
                bytes: b"handoff-prepare".to_vec(),
            })
            .expect("first station should send");
        let prepare = recv_station_ref_with_retry(&mut second, station_two);
        assert_eq!(prepare.0, station_one);
        assert_eq!(prepare.1, station_two);
        assert_eq!((prepare.2, prepare.3, prepare.4), (15, b'h', b'e'));

        first
            .send_station(StationOutboundPacket {
                source_station: station_one,
                target_station: station_two,
                bytes: b"next".to_vec(),
            })
            .expect("first station should send another packet");
        let next = recv_station_ref_with_retry(&mut second, station_two);
        assert_eq!((next.2, next.3, next.4), (4, b'n', b't'));
        assert_eq!(next.5, prepare.5);

        second
            .send_station(StationOutboundPacket {
                source_station: station_two,
                target_station: station_one,
                bytes: b"handoff-commit".to_vec(),
            })
            .expect("second station should send");
        let inbound = recv_station_with_retry(&mut first, station_one);
        assert_eq!(inbound.source_station, station_two);
        assert_eq!(inbound.target_station, station_one);
        assert_eq!(inbound.bytes, b"handoff-commit");
        assert_eq!(first.stats().packets_sent, 2);
        assert_eq!(first.stats().packets_received, 1);
        assert_eq!(second.stats().packets_sent, 1);
        assert_eq!(second.stats().packets_received, 2);
    }

    #[test]
    fn udp_station_transport_rejects_invalid_station_endpoints() {
        let local = StationId::new(1);
        let mut transport =
            UdpStationTransport::bind(local, "127.0.0.1:0").expect("transport should bind");

        let source_mismatch = transport
            .send_station(StationOutboundPacket {
                source_station: StationId::new(9),
                target_station: StationId::new(2),
                bytes: Vec::new(),
            })
            .expect_err("source should match local station");
        match source_mismatch {
            UdpStationTransportError::LocalStationMismatch {
                local_station,
                packet_source,
            } => {
                assert_eq!(local_station, local);
                assert_eq!(packet_source, StationId::new(9));
            }
            other => panic!("unexpected error: {other}"),
        }

        let unknown = transport
            .send_station(StationOutboundPacket {
                source_station: local,
                target_station: StationId::new(2),
                bytes: Vec::new(),
            })
            .expect_err("target station should be registered");
        match unknown {
            UdpStationTransportError::UnknownStation(station_id) => {
                assert_eq!(station_id, StationId::new(2));
            }
            other => panic!("unexpected error: {other}"),
        }

        let target_mismatch = transport
            .try_recv_station(StationId::new(99))
            .expect_err("receive target should match local station");
        match target_mismatch {
            UdpStationTransportError::TargetStationMismatch {
                local_station,
                requested_target,
            } => {
                assert_eq!(local_station, local);
                assert_eq!(requested_target, StationId::new(99));
            }
            other => panic!("unexpected error: {other}"),
        }
    }
}
