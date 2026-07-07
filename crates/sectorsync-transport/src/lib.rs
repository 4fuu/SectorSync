//! Transport traits and fake transport support for SectorSync.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};

use sectorsync_core::prelude::{ClientId, StationId};

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

impl ReliableStationFrame {
    /// Encodes a reliable station frame.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), ReliableStationEncodeError> {
        out.extend_from_slice(&RELIABLE_STATION_MAGIC);
        match self {
            Self::Data { sequence, payload } => {
                out.push(RELIABLE_KIND_DATA);
                out.extend_from_slice(&sequence.to_le_bytes());
                let len = u32::try_from(payload.len()).map_err(|_| {
                    ReliableStationEncodeError::PayloadTooLarge {
                        actual: payload.len(),
                    }
                })?;
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(payload);
            }
            Self::Ack { sequence } => {
                out.push(RELIABLE_KIND_ACK);
                out.extend_from_slice(&sequence.to_le_bytes());
            }
        }
        Ok(())
    }

    /// Decodes a reliable station frame.
    pub fn decode(input: &[u8]) -> Result<Self, ReliableStationDecodeError> {
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
                let payload = cursor.read_bytes(len)?;
                Self::Data { sequence, payload }
            }
            RELIABLE_KIND_ACK => Self::Ack { sequence },
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
    stats: ReliableStationStats,
}

impl ReliableStationSender {
    /// Creates a reliable station sender.
    pub fn new(config: ReliableStationConfig) -> Self {
        Self {
            config,
            next_sequence: BTreeMap::new(),
            in_flight: BTreeMap::new(),
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
        self.in_flight
            .keys()
            .filter(|(station_id, _)| *station_id == target_station)
            .count()
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
        self.send_data_frame(
            transport,
            packet.source_station,
            packet.target_station,
            sequence,
            &packet.bytes,
        )?;
        self.in_flight.insert(
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
            .in_flight
            .remove(&(ack_source_station, sequence))
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
        let keys = self
            .in_flight
            .iter()
            .filter_map(|(key, packet)| {
                let due =
                    now_tick.saturating_sub(packet.last_sent_tick) >= self.config.retry_after_ticks;
                due.then_some(*key)
            })
            .collect::<Vec<_>>();
        let mut report = ReliableRetryReport::default();

        for key in keys {
            let Some(packet) = self.in_flight.get(&key).cloned() else {
                continue;
            };
            if packet.attempts >= self.config.max_attempts {
                self.in_flight.remove(&key);
                self.stats.timed_out = self.stats.timed_out.saturating_add(1);
                report.timed_out = report.timed_out.saturating_add(1);
                continue;
            }

            self.send_data_frame(
                transport,
                packet.source_station,
                packet.target_station,
                packet.sequence,
                &packet.payload,
            )?;
            if let Some(stored) = self.in_flight.get_mut(&key) {
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

    fn send_data_frame<T: StationTransportSink>(
        &self,
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
        ReliableStationFrame::Data {
            sequence,
            payload: payload.to_vec(),
        }
        .encode(&mut bytes)
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
    delivered: BTreeSet<(StationId, u64)>,
    delivered_order: VecDeque<(StationId, u64)>,
    stats: ReliableStationStats,
}

impl ReliableStationReceiver {
    /// Creates a bounded reliable station receiver.
    pub fn new(config: ReliableStationConfig) -> Self {
        Self {
            config,
            delivered: BTreeSet::new(),
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
        self.send_ack(
            transport,
            packet.target_station,
            packet.source_station,
            sequence,
        )?;
        if !self.record_unique(packet.source_station, sequence) {
            self.stats.duplicates_suppressed = self.stats.duplicates_suppressed.saturating_add(1);
            return Ok(None);
        }

        self.stats.data_delivered = self.stats.data_delivered.saturating_add(1);
        Ok(Some(StationInboundPacket {
            source_station: packet.source_station,
            target_station: packet.target_station,
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

    /// Handles one inbound reliable station packet.
    pub fn handle_inbound<T: StationTransportSink>(
        &mut self,
        transport: &mut T,
        packet: StationInboundPacket,
    ) -> Result<Option<StationInboundPacket>, ReliableStationError<T::Error>> {
        match ReliableStationFrame::decode(&packet.bytes).map_err(ReliableStationError::Decode)? {
            ReliableStationFrame::Data { sequence, payload } => self
                .receiver
                .handle_data(transport, packet, sequence, payload),
            ReliableStationFrame::Ack { sequence } => {
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

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, ReliableStationDecodeError> {
        self.require(len)?;
        let bytes = self.input[self.offset..self.offset + len].to_vec();
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
        if let Some(old_station) = self.addr_to_station.insert(addr, station_id) {
            if old_station != station_id {
                self.stations.remove(&old_station);
            }
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
                Ok(Some(StationInboundPacket {
                    source_station,
                    target_station: self.local_station,
                    bytes: self.recv_buffer[..len].to_vec(),
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
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
/// expected to live outside the core SectorSync hot path.
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
        if let Some(old_client) = self.addr_to_client.insert(addr, client_id) {
            if old_client != client_id {
                self.clients.remove(&old_client);
            }
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
        match self.socket.recv_from(&mut self.recv_buffer) {
            Ok((len, remote_addr)) => Ok(Some(InboundPacket {
                client_id: self.addr_to_client.get(&remote_addr).copied(),
                remote_addr,
                bytes: self.recv_buffer[..len].to_vec(),
            })),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error.into()),
        }
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
    packets_sent: usize,
    bytes_sent: usize,
    batches_sent: usize,
}

impl FakeTransport {
    /// Returns sent packet count.
    pub const fn packets_sent(&self) -> usize {
        self.packets_sent
    }

    /// Returns sent byte count.
    pub const fn bytes_sent(&self) -> usize {
        self.bytes_sent
    }

    /// Returns sent batch count.
    pub const fn batches_sent(&self) -> usize {
        self.batches_sent
    }
}

impl TransportSink for FakeTransport {
    type Error = core::convert::Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.packets_sent += 1;
        self.bytes_sent += packet.bytes.len();
        Ok(())
    }

    fn send_batch(&mut self, batch: PacketBatch) -> Result<(), Self::Error> {
        self.batches_sent += 1;
        self.packets_sent += batch.packets.len();
        self.bytes_sent += batch.bytes_len();
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

    fn recv_with_retry(transport: &mut UdpTransport) -> InboundPacket {
        for _ in 0..50 {
            if let Some(packet) = transport.try_recv().expect("udp receive should work") {
                return packet;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("udp packet was not received");
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
        assert_eq!(
            ReliableStationFrame::decode(&bytes).expect("data frame should decode"),
            data
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
        let delivered = second
            .handle_inbound(&mut transport, raw)
            .expect("data packet should handle")
            .expect("first data packet should deliver");
        assert_eq!(delivered.source_station, station_one);
        assert_eq!(delivered.target_station, station_two);
        assert_eq!(delivered.bytes, b"event");
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
        let retry = first
            .retry_due(&mut transport, 2)
            .expect("retry should send");
        assert_eq!(retry.retried, 1);
        assert_eq!(retry.timed_out, 0);
        assert_eq!(first.sender.stats().retries_sent, 1);
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
        let inbound = recv_with_retry(&mut server);
        assert_eq!(inbound.client_id, Some(client_id));
        assert_eq!(inbound.remote_addr, client_addr);
        assert_eq!(inbound.bytes, b"command");

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
        let inbound = recv_station_with_retry(&mut second, station_two);
        assert_eq!(inbound.source_station, station_one);
        assert_eq!(inbound.target_station, station_two);
        assert_eq!(inbound.bytes, b"handoff-prepare");

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
        assert_eq!(first.stats().packets_sent, 1);
        assert_eq!(first.stats().packets_received, 1);
        assert_eq!(second.stats().packets_sent, 1);
        assert_eq!(second.stats().packets_received, 1);
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
