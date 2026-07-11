//! Wire frame traits and default frame shapes for `SectorSync`.

#![forbid(unsafe_code)]

use sectorsync_core::prelude::{
    BarrierId, BarrierState, ClientId, CommandEnvelope, CommandId, CommandPriority, ComponentId,
    ComponentStore, EntityHandle, EntityId, EventId, EventKind, EventPriority, OwnerEpoch,
    ReplicationPlan, Station, StationEvent, StationId, Tick,
};

/// Runtime frame kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameKind {
    /// Replication update frame.
    Replication = 0,
    /// Command acknowledgement frame.
    CommandAck = 1,
    /// Runtime barrier notification.
    Barrier = 2,
    /// Client command ingress frame.
    Command = 3,
    /// Cross-station event frame.
    StationEvent = 4,
    /// Gateway-to-station command dispatch frame.
    CommandDispatch = 5,
}

impl FrameKind {
    /// Converts a byte into a frame kind.
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Replication),
            1 => Some(Self::CommandAck),
            2 => Some(Self::Barrier),
            3 => Some(Self::Command),
            4 => Some(Self::StationEvent),
            5 => Some(Self::CommandDispatch),
            _ => None,
        }
    }
}

/// Replication frame metadata produced per client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Server tick represented by this frame.
    pub server_tick: Tick,
    /// Number of entity updates in this frame.
    pub entity_count: u32,
    /// Estimated payload bytes before transport overhead.
    pub estimated_payload_bytes: u32,
    /// Concrete entity/component deltas included in this frame.
    pub entities: Vec<EntityDelta>,
}

/// Entity delta included in a replication frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntityDelta {
    /// Entity being updated.
    pub entity_id: EntityId,
    /// Owner epoch observed by the sender.
    pub owner_epoch: OwnerEpoch,
    /// Component deltas for this entity.
    pub components: Vec<ComponentDelta>,
}

/// Component delta included in an entity delta.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentDelta {
    /// Component id.
    pub component_id: ComponentId,
    /// Component version.
    pub version: u64,
    /// Runtime-defined flags.
    pub flags: u8,
    /// Encoded component bytes.
    pub bytes: Vec<u8>,
}

/// Limits used by `ReplicationFrameBuilder`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationFrameLimits {
    /// Maximum entity deltas to materialize in one frame.
    pub max_entity_deltas: usize,
    /// Maximum component deltas to include per entity.
    pub max_components_per_entity: usize,
    /// Maximum component payload bytes to include per component.
    pub max_component_bytes: usize,
}

impl Default for ReplicationFrameLimits {
    fn default() -> Self {
        Self {
            max_entity_deltas: 256,
            max_components_per_entity: 16,
            max_component_bytes: 1024,
        }
    }
}

/// Component selection for frame building.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComponentSelection {
    /// Component ids to include when present and dirty.
    pub component_ids: Vec<ComponentId>,
}

/// Frame builder statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationFrameBuildStats {
    /// Entity handles selected by the replication plan.
    pub planned_entities: usize,
    /// Entity deltas materialized into the frame.
    pub encoded_entities: usize,
    /// Component deltas materialized into the frame.
    pub encoded_components: usize,
    /// Entity deltas skipped by builder limits.
    pub skipped_entities_by_limit: usize,
    /// Component deltas skipped by builder limits.
    pub skipped_components_by_limit: usize,
    /// Component payloads skipped because they exceed byte limits.
    pub skipped_components_by_size: usize,
}

/// Result of building a replication frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationFrameBuild {
    /// Built frame.
    pub frame: ReplicationFrame,
    /// Build statistics.
    pub stats: ReplicationFrameBuildStats,
}

/// Builds concrete replication frames from a core replication plan.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicationFrameBuilder {
    /// Builder limits.
    pub limits: ReplicationFrameLimits,
}

impl ReplicationFrameBuilder {
    const BINARY_HEADER_BYTES: usize = 1 + 8 + 8 + 4 + 4 + 4;
    const BINARY_ENTITY_METADATA_BYTES: usize = 8 + 8 + 2;
    const BINARY_COMPONENT_METADATA_BYTES: usize = 2 + 8 + 1 + 4;

    /// Creates a frame builder with explicit limits.
    pub const fn new(limits: ReplicationFrameLimits) -> Self {
        Self { limits }
    }

    /// Returns a bounded initial byte-capacity hint for direct binary encoding.
    ///
    /// The planner estimate supplements fixed wire metadata and is clamped by
    /// the active entity, component, and component-byte limits. The result is a
    /// hint rather than an encoded-size guarantee because dirty/missing
    /// components are resolved during encoding.
    pub fn binary_capacity_hint(
        &self,
        plan: &ReplicationPlan,
        selection: &ComponentSelection,
    ) -> usize {
        let entities = plan.entities.len().min(self.limits.max_entity_deltas);
        let components = selection
            .component_ids
            .len()
            .min(self.limits.max_components_per_entity);
        let fixed_bytes =
            Self::BINARY_HEADER_BYTES.saturating_add(entities.saturating_mul(
                Self::BINARY_ENTITY_METADATA_BYTES.saturating_add(
                    components.saturating_mul(Self::BINARY_COMPONENT_METADATA_BYTES),
                ),
            ));
        let maximum_payload_bytes = entities
            .saturating_mul(components)
            .saturating_mul(self.limits.max_component_bytes);
        fixed_bytes.saturating_add(plan.stats.estimated_bytes.min(maximum_payload_bytes))
    }

    /// Returns a dense-dirty-data initial capacity hint from at most four
    /// uniformly distributed entities in `plan`.
    ///
    /// Sampling avoids a full metadata prepass. If any sample has no encodable
    /// dirty component, this conservatively returns zero and lets the output
    /// buffer grow normally. Nonzero estimates remain bounded by active limits.
    pub fn sampled_binary_capacity_hint(
        &self,
        station: &Station,
        plan: &ReplicationPlan,
        components: &ComponentStore,
        selection: &ComponentSelection,
    ) -> usize {
        const MAX_SAMPLES: usize = 4;
        let candidates = plan.entities.len();
        let samples = candidates.min(MAX_SAMPLES);
        if samples == 0 {
            return 0;
        }
        let denominator = samples.saturating_mul(2);
        let mut sampled_bytes = 0_usize;
        for sample in 0..samples {
            let numerator = sample.saturating_mul(2).saturating_add(1);
            let index = numerator.saturating_mul(candidates) / denominator;
            let entity_bytes = self.binary_entity_bytes(
                station,
                plan.entities[index.min(candidates - 1)],
                components,
                selection,
            );
            if entity_bytes == 0 {
                return 0;
            }
            sampled_bytes = sampled_bytes.saturating_add(entity_bytes);
        }
        let estimated_entity_bytes = sampled_bytes.saturating_mul(candidates).div_ceil(samples);
        let maximum_entities = candidates.min(self.limits.max_entity_deltas);
        let maximum_components = selection
            .component_ids
            .len()
            .min(self.limits.max_components_per_entity);
        let maximum_entity_bytes = Self::BINARY_ENTITY_METADATA_BYTES.saturating_add(
            maximum_components.saturating_mul(
                Self::BINARY_COMPONENT_METADATA_BYTES
                    .saturating_add(self.limits.max_component_bytes),
            ),
        );
        Self::BINARY_HEADER_BYTES.saturating_add(
            estimated_entity_bytes.min(maximum_entities.saturating_mul(maximum_entity_bytes)),
        )
    }

    fn binary_entity_bytes(
        &self,
        station: &Station,
        handle: EntityHandle,
        components: &ComponentStore,
        selection: &ComponentSelection,
    ) -> usize {
        if station.get(handle).is_none() {
            return 0;
        }
        let mut bytes = Self::BINARY_ENTITY_METADATA_BYTES;
        let mut encoded_components = 0_usize;
        for component_id in &selection.component_ids {
            if encoded_components >= self.limits.max_components_per_entity {
                break;
            }
            let Some(blob) = components.get_blob(*component_id, handle) else {
                continue;
            };
            if !blob.dirty || blob.bytes.len() > self.limits.max_component_bytes {
                continue;
            }
            bytes = bytes
                .saturating_add(Self::BINARY_COMPONENT_METADATA_BYTES)
                .saturating_add(blob.bytes.len());
            encoded_components += 1;
        }
        if encoded_components == 0 { 0 } else { bytes }
    }

    /// Builds a frame from a station plan and component store.
    pub fn build(
        &self,
        client_id: ClientId,
        server_tick: Tick,
        station: &Station,
        plan: &ReplicationPlan,
        components: &ComponentStore,
        selection: &ComponentSelection,
    ) -> ReplicationFrameBuild {
        let mut stats = ReplicationFrameBuildStats {
            planned_entities: plan.entities.len(),
            ..ReplicationFrameBuildStats::default()
        };
        let mut entity_deltas =
            Vec::with_capacity(plan.entities.len().min(self.limits.max_entity_deltas));
        let mut estimated_payload_bytes = 0_usize;

        for handle in &plan.entities {
            if entity_deltas.len() >= self.limits.max_entity_deltas {
                stats.skipped_entities_by_limit += 1;
                continue;
            }
            let Some(entity) = station.get(*handle) else {
                continue;
            };

            let mut component_deltas = Vec::new();
            for component_id in &selection.component_ids {
                if component_deltas.len() >= self.limits.max_components_per_entity {
                    stats.skipped_components_by_limit += 1;
                    continue;
                }
                let Some(blob) = components.get_blob(*component_id, *handle) else {
                    continue;
                };
                if !blob.dirty {
                    continue;
                }
                if blob.bytes.len() > self.limits.max_component_bytes {
                    stats.skipped_components_by_size += 1;
                    continue;
                }
                estimated_payload_bytes = estimated_payload_bytes
                    .saturating_add(2 + 8 + 1 + 4)
                    .saturating_add(blob.bytes.len());
                component_deltas.push(ComponentDelta {
                    component_id: *component_id,
                    version: blob.version,
                    flags: 0,
                    bytes: blob.bytes.clone(),
                });
            }

            if component_deltas.is_empty() {
                continue;
            }

            stats.encoded_components += component_deltas.len();
            estimated_payload_bytes = estimated_payload_bytes.saturating_add(8 + 8 + 2);
            entity_deltas.push(EntityDelta {
                entity_id: entity.id,
                owner_epoch: entity.role.owner_epoch(),
                components: component_deltas,
            });
        }

        stats.encoded_entities = entity_deltas.len();
        ReplicationFrameBuild {
            frame: ReplicationFrame {
                client_id,
                server_tick,
                entity_count: u32::try_from(plan.entities.len()).unwrap_or(u32::MAX),
                estimated_payload_bytes: u32::try_from(estimated_payload_bytes).unwrap_or(u32::MAX),
                entities: entity_deltas,
            },
            stats,
        }
    }

    /// Builds and appends one binary replication frame directly into `out`.
    ///
    /// This preserves the same limits, dirty-component filtering, statistics,
    /// and wire shape as [`Self::build`] followed by [`BinaryFrameEncoder`],
    /// without allocating an intermediate entity/component delta tree or
    /// cloning component payloads. Existing bytes in `out` are retained.
    #[allow(clippy::too_many_arguments)]
    pub fn encode_binary_into(
        &self,
        client_id: ClientId,
        server_tick: Tick,
        station: &Station,
        plan: &ReplicationPlan,
        components: &ComponentStore,
        selection: &ComponentSelection,
        out: &mut Vec<u8>,
    ) -> Result<ReplicationFrameBuildStats, BinaryEncodeError> {
        let mut stats = ReplicationFrameBuildStats {
            planned_entities: plan.entities.len(),
            ..ReplicationFrameBuildStats::default()
        };
        let mut estimated_payload_bytes = 0_usize;

        out.push(FrameKind::Replication as u8);
        out.extend_from_slice(&client_id.get().to_le_bytes());
        out.extend_from_slice(&server_tick.get().to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(plan.entities.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        let estimated_payload_offset = out.len();
        out.extend_from_slice(&0_u32.to_le_bytes());
        let entity_count_offset = out.len();
        out.extend_from_slice(&0_u32.to_le_bytes());

        for handle in &plan.entities {
            if stats.encoded_entities >= self.limits.max_entity_deltas {
                stats.skipped_entities_by_limit += 1;
                continue;
            }
            let Some(entity) = station.get(*handle) else {
                continue;
            };

            let entity_start = out.len();
            out.extend_from_slice(&entity.id.get().to_le_bytes());
            out.extend_from_slice(&entity.role.owner_epoch().get().to_le_bytes());
            let component_count_offset = out.len();
            out.extend_from_slice(&0_u16.to_le_bytes());
            let mut component_count = 0_usize;

            for component_id in &selection.component_ids {
                if component_count >= self.limits.max_components_per_entity {
                    stats.skipped_components_by_limit += 1;
                    continue;
                }
                let Some(blob) = components.get_blob(*component_id, *handle) else {
                    continue;
                };
                if !blob.dirty {
                    continue;
                }
                if blob.bytes.len() > self.limits.max_component_bytes {
                    stats.skipped_components_by_size += 1;
                    continue;
                }

                out.extend_from_slice(&component_id.get().to_le_bytes());
                out.extend_from_slice(&blob.version.to_le_bytes());
                out.push(0);
                write_bytes("replication.component.bytes", &blob.bytes, out)?;
                estimated_payload_bytes = estimated_payload_bytes
                    .saturating_add(2 + 8 + 1 + 4)
                    .saturating_add(blob.bytes.len());
                component_count += 1;
            }

            if component_count == 0 {
                out.truncate(entity_start);
                continue;
            }

            let component_count =
                u16::try_from(component_count).map_err(|_| BinaryEncodeError::TooManyItems {
                    field: "replication.entity.components",
                    actual: component_count,
                })?;
            out[component_count_offset..component_count_offset + 2]
                .copy_from_slice(&component_count.to_le_bytes());
            stats.encoded_components += usize::from(component_count);
            stats.encoded_entities += 1;
            estimated_payload_bytes = estimated_payload_bytes.saturating_add(8 + 8 + 2);
        }

        let encoded_entities =
            u32::try_from(stats.encoded_entities).map_err(|_| BinaryEncodeError::TooManyItems {
                field: "replication.entities",
                actual: stats.encoded_entities,
            })?;
        out[estimated_payload_offset..estimated_payload_offset + 4].copy_from_slice(
            &u32::try_from(estimated_payload_bytes)
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        out[entity_count_offset..entity_count_offset + 4]
            .copy_from_slice(&encoded_entities.to_le_bytes());
        Ok(stats)
    }
}

/// Command acknowledgement frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandAckFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Acknowledged command.
    pub command_id: CommandId,
    /// Server tick at acknowledgement.
    pub server_tick: Tick,
    /// Whether the command was accepted by the runtime pipeline.
    pub accepted: bool,
    /// Game/runtime reject reason code.
    pub reason_code: u16,
}

/// Client command ingress frame.
///
/// The server stamps `received_at` when converting this into a
/// `CommandEnvelope`; game validation and anti-cheat checks remain outside the
/// wire codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandFrame {
    /// Client that submitted the command.
    pub client_id: ClientId,
    /// Command id used for replay and audit.
    pub command_id: CommandId,
    /// Entity the command intends to control.
    pub entity_id: EntityId,
    /// Client-side sequence number.
    pub sequence: u64,
    /// Game-defined command kind.
    pub kind: u32,
    /// Command priority.
    pub priority: CommandPriority,
    /// Opaque payload owned by the embedding game.
    pub payload: Vec<u8>,
}

impl CommandFrame {
    /// Converts an ingress frame into a runtime command envelope.
    pub fn into_envelope(self, received_at: Tick) -> CommandEnvelope {
        CommandEnvelope {
            id: self.command_id,
            client_id: self.client_id,
            entity_id: self.entity_id,
            sequence: self.sequence,
            received_at,
            kind: self.kind,
            priority: self.priority,
            payload: self.payload,
        }
    }

    /// Converts a command envelope into a wire frame, dropping server-only tick
    /// metadata.
    pub fn from_envelope(envelope: &CommandEnvelope) -> Self {
        Self {
            client_id: envelope.client_id,
            command_id: envelope.id,
            entity_id: envelope.entity_id,
            sequence: envelope.sequence,
            kind: envelope.kind,
            priority: envelope.priority,
            payload: envelope.payload.clone(),
        }
    }
}

/// Internal gateway-to-station command dispatch frame.
///
/// Unlike `CommandFrame`, this preserves the server `received_at` tick stamped
/// by the gateway pipeline before the command is forwarded to a station node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandDispatchFrame {
    /// Target station selected by gateway/deployment routing.
    pub station_id: StationId,
    /// Client that submitted the command.
    pub client_id: ClientId,
    /// Command id used for replay and audit.
    pub command_id: CommandId,
    /// Entity the command intends to control.
    pub entity_id: EntityId,
    /// Client-side sequence number.
    pub sequence: u64,
    /// Server tick observed when the command entered `SectorSync`.
    pub received_at: Tick,
    /// Game-defined command kind.
    pub kind: u32,
    /// Command priority.
    pub priority: CommandPriority,
    /// Opaque payload owned by the embedding game.
    pub payload: Vec<u8>,
}

impl CommandDispatchFrame {
    /// Converts a stamped command envelope into an internal dispatch frame.
    pub fn from_envelope(station_id: StationId, envelope: &CommandEnvelope) -> Self {
        Self {
            station_id,
            client_id: envelope.client_id,
            command_id: envelope.id,
            entity_id: envelope.entity_id,
            sequence: envelope.sequence,
            received_at: envelope.received_at,
            kind: envelope.kind,
            priority: envelope.priority,
            payload: envelope.payload.clone(),
        }
    }

    /// Converts an internal dispatch frame back into a command envelope.
    pub fn into_envelope(self) -> CommandEnvelope {
        CommandEnvelope {
            id: self.command_id,
            client_id: self.client_id,
            entity_id: self.entity_id,
            sequence: self.sequence,
            received_at: self.received_at,
            kind: self.kind,
            priority: self.priority,
            payload: self.payload,
        }
    }
}

/// Cross-station event frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StationEventFrame {
    /// Idempotency key.
    pub event_id: EventId,
    /// Source station.
    pub source_station: StationId,
    /// Target station.
    pub target_station: StationId,
    /// Tick observed at source.
    pub source_tick: Tick,
    /// Tick at which target should apply the event.
    pub target_tick: Tick,
    /// Priority class.
    pub priority: EventPriority,
    /// Event payload kind.
    pub kind: EventKind,
}

impl StationEventFrame {
    /// Converts a runtime station event into a wire frame.
    pub fn from_event(event: &StationEvent) -> Self {
        Self {
            event_id: event.id,
            source_station: event.source,
            target_station: event.target,
            source_tick: event.source_tick,
            target_tick: event.target_tick,
            priority: event.priority,
            kind: event.kind.clone(),
        }
    }

    /// Converts a wire frame into a runtime station event.
    pub fn into_event(self) -> StationEvent {
        StationEvent {
            id: self.event_id,
            source: self.source_station,
            target: self.target_station,
            source_tick: self.source_tick,
            target_tick: self.target_tick,
            priority: self.priority,
            kind: self.kind,
        }
    }
}

/// Runtime barrier notification frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarrierFrame {
    /// Target client.
    pub client_id: ClientId,
    /// Barrier id.
    pub barrier_id: BarrierId,
    /// Server tick associated with this barrier state.
    pub server_tick: Tick,
    /// Current barrier state.
    pub state: BarrierState,
}

/// Decoded runtime frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeFrame {
    /// Replication update.
    Replication(ReplicationFrame),
    /// Client command ingress.
    Command(CommandFrame),
    /// Gateway-to-station command dispatch.
    CommandDispatch(CommandDispatchFrame),
    /// Command acknowledgement.
    CommandAck(CommandAckFrame),
    /// Barrier notification.
    Barrier(BarrierFrame),
    /// Cross-station event.
    StationEvent(StationEventFrame),
}

/// Encodes frames into bytes.
pub trait FrameEncoder {
    /// Encoder error type.
    type Error;

    /// Encodes a replication frame into `out`.
    fn encode_replication(
        &mut self,
        frame: &ReplicationFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes a command acknowledgement frame into `out`.
    fn encode_command_ack(
        &mut self,
        frame: &CommandAckFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes a client command frame into `out`.
    fn encode_command(
        &mut self,
        frame: &CommandFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes an internal command dispatch frame into `out`.
    fn encode_command_dispatch(
        &mut self,
        frame: &CommandDispatchFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes a cross-station event frame into `out`.
    fn encode_station_event(
        &mut self,
        frame: &StationEventFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;

    /// Encodes a barrier frame into `out`.
    fn encode_barrier(
        &mut self,
        frame: &BarrierFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error>;
}

/// Decodes frames from bytes.
pub trait FrameDecoder {
    /// Decoder error type.
    type Error;

    /// Decodes one runtime frame.
    fn decode(&mut self, input: &[u8]) -> Result<RuntimeFrame, Self::Error>;
}

/// Binary decode error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryDecodeError {
    /// Input is empty.
    Empty,
    /// Unknown frame kind byte.
    UnknownFrameKind(u8),
    /// Frame ended before all fields were available.
    Truncated {
        /// Required bytes.
        needed: usize,
        /// Available bytes.
        available: usize,
    },
    /// Barrier state byte is invalid.
    InvalidBarrierState(u8),
    /// Command priority byte is invalid.
    InvalidCommandPriority(u8),
    /// Event priority byte is invalid.
    InvalidEventPriority(u8),
    /// Event kind byte is invalid.
    InvalidEventKind(u8),
    /// Trailing bytes were present after a complete frame.
    TrailingBytes(usize),
}

impl core::fmt::Display for BinaryDecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty frame"),
            Self::UnknownFrameKind(kind) => write!(f, "unknown frame kind {kind}"),
            Self::Truncated { needed, available } => {
                write!(f, "truncated frame: needed {needed}, available {available}")
            }
            Self::InvalidBarrierState(state) => write!(f, "invalid barrier state {state}"),
            Self::InvalidCommandPriority(priority) => {
                write!(f, "invalid command priority {priority}")
            }
            Self::InvalidEventPriority(priority) => write!(f, "invalid event priority {priority}"),
            Self::InvalidEventKind(kind) => write!(f, "invalid event kind {kind}"),
            Self::TrailingBytes(bytes) => write!(f, "frame has {bytes} trailing bytes"),
        }
    }
}

impl std::error::Error for BinaryDecodeError {}

/// Binary encode error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryEncodeError {
    /// A list length exceeded `u32::MAX`.
    TooManyItems {
        /// Field being encoded.
        field: &'static str,
        /// Actual item count.
        actual: usize,
    },
    /// A byte payload exceeded `u32::MAX`.
    PayloadTooLarge {
        /// Field being encoded.
        field: &'static str,
        /// Actual byte count.
        actual: usize,
    },
}

impl core::fmt::Display for BinaryEncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::TooManyItems { field, actual } => {
                write!(f, "{field} has too many items: {actual}")
            }
            Self::PayloadTooLarge { field, actual } => {
                write!(f, "{field} payload is too large: {actual} bytes")
            }
        }
    }
}

impl std::error::Error for BinaryEncodeError {}

/// Simple little-endian binary frame decoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinaryFrameDecoder;

impl FrameDecoder for BinaryFrameDecoder {
    type Error = BinaryDecodeError;

    fn decode(&mut self, input: &[u8]) -> Result<RuntimeFrame, Self::Error> {
        let mut cursor = Cursor::new(input);
        let kind_byte = cursor.read_u8()?;
        let kind = FrameKind::from_byte(kind_byte)
            .ok_or(BinaryDecodeError::UnknownFrameKind(kind_byte))?;
        let frame = match kind {
            FrameKind::Replication => RuntimeFrame::Replication(ReplicationFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                entity_count: cursor.read_u32()?,
                estimated_payload_bytes: cursor.read_u32()?,
                entities: {
                    let entity_delta_count = cursor.read_u32()? as usize;
                    let mut entities = Vec::with_capacity(entity_delta_count);
                    for _ in 0..entity_delta_count {
                        let entity_id = EntityId::new(cursor.read_u64()?);
                        let owner_epoch = OwnerEpoch::new(cursor.read_u64()?);
                        let component_count = cursor.read_u16()? as usize;
                        let mut components = Vec::with_capacity(component_count);
                        for _ in 0..component_count {
                            let component_id = ComponentId::new(cursor.read_u16()?);
                            let version = cursor.read_u64()?;
                            let flags = cursor.read_u8()?;
                            let byte_len = cursor.read_u32()? as usize;
                            let bytes = cursor.read_bytes(byte_len)?;
                            components.push(ComponentDelta {
                                component_id,
                                version,
                                flags,
                                bytes,
                            });
                        }
                        entities.push(EntityDelta {
                            entity_id,
                            owner_epoch,
                            components,
                        });
                    }
                    entities
                },
            }),
            FrameKind::CommandAck => RuntimeFrame::CommandAck(CommandAckFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                command_id: CommandId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                accepted: cursor.read_u8()? != 0,
                reason_code: cursor.read_u16()?,
            }),
            FrameKind::Command => RuntimeFrame::Command(CommandFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                command_id: CommandId::new(cursor.read_u64()?),
                entity_id: EntityId::new(cursor.read_u64()?),
                sequence: cursor.read_u64()?,
                kind: cursor.read_u32()?,
                priority: decode_command_priority(cursor.read_u8()?)?,
                payload: {
                    let byte_len = cursor.read_u32()? as usize;
                    cursor.read_bytes(byte_len)?
                },
            }),
            FrameKind::CommandDispatch => RuntimeFrame::CommandDispatch(CommandDispatchFrame {
                station_id: StationId::new(cursor.read_u32()?),
                client_id: ClientId::new(cursor.read_u64()?),
                command_id: CommandId::new(cursor.read_u64()?),
                entity_id: EntityId::new(cursor.read_u64()?),
                sequence: cursor.read_u64()?,
                received_at: Tick::new(cursor.read_u64()?),
                kind: cursor.read_u32()?,
                priority: decode_command_priority(cursor.read_u8()?)?,
                payload: {
                    let byte_len = cursor.read_u32()? as usize;
                    cursor.read_bytes(byte_len)?
                },
            }),
            FrameKind::Barrier => RuntimeFrame::Barrier(BarrierFrame {
                client_id: ClientId::new(cursor.read_u64()?),
                barrier_id: BarrierId::new(cursor.read_u64()?),
                server_tick: Tick::new(cursor.read_u64()?),
                state: decode_barrier_state(cursor.read_u8()?)?,
            }),
            FrameKind::StationEvent => RuntimeFrame::StationEvent(StationEventFrame {
                event_id: EventId::new(cursor.read_u64()?),
                source_station: StationId::new(cursor.read_u32()?),
                target_station: StationId::new(cursor.read_u32()?),
                source_tick: Tick::new(cursor.read_u64()?),
                target_tick: Tick::new(cursor.read_u64()?),
                priority: decode_event_priority(cursor.read_u8()?)?,
                kind: decode_event_kind(&mut cursor)?,
            }),
        };
        cursor.finish()?;
        Ok(frame)
    }
}

/// Simple little-endian binary frame encoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct BinaryFrameEncoder;

impl FrameEncoder for BinaryFrameEncoder {
    type Error = BinaryEncodeError;

    fn encode_replication(
        &mut self,
        frame: &ReplicationFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::Replication as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.extend_from_slice(&frame.entity_count.to_le_bytes());
        out.extend_from_slice(&frame.estimated_payload_bytes.to_le_bytes());
        write_len_u32("replication.entities", frame.entities.len(), out)?;
        for entity in &frame.entities {
            out.extend_from_slice(&entity.entity_id.get().to_le_bytes());
            out.extend_from_slice(&entity.owner_epoch.get().to_le_bytes());
            write_len_u16(
                "replication.entity.components",
                entity.components.len(),
                out,
            )?;
            for component in &entity.components {
                out.extend_from_slice(&component.component_id.get().to_le_bytes());
                out.extend_from_slice(&component.version.to_le_bytes());
                out.push(component.flags);
                write_bytes("replication.component.bytes", &component.bytes, out)?;
            }
        }
        Ok(())
    }

    fn encode_command_ack(
        &mut self,
        frame: &CommandAckFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::CommandAck as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.command_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.push(u8::from(frame.accepted));
        out.extend_from_slice(&frame.reason_code.to_le_bytes());
        Ok(())
    }

    fn encode_command(
        &mut self,
        frame: &CommandFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::Command as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.command_id.get().to_le_bytes());
        out.extend_from_slice(&frame.entity_id.get().to_le_bytes());
        out.extend_from_slice(&frame.sequence.to_le_bytes());
        out.extend_from_slice(&frame.kind.to_le_bytes());
        out.push(encode_command_priority(frame.priority));
        write_bytes("command.payload", &frame.payload, out)?;
        Ok(())
    }

    fn encode_command_dispatch(
        &mut self,
        frame: &CommandDispatchFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::CommandDispatch as u8);
        out.extend_from_slice(&frame.station_id.get().to_le_bytes());
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.command_id.get().to_le_bytes());
        out.extend_from_slice(&frame.entity_id.get().to_le_bytes());
        out.extend_from_slice(&frame.sequence.to_le_bytes());
        out.extend_from_slice(&frame.received_at.get().to_le_bytes());
        out.extend_from_slice(&frame.kind.to_le_bytes());
        out.push(encode_command_priority(frame.priority));
        write_bytes("command_dispatch.payload", &frame.payload, out)?;
        Ok(())
    }

    fn encode_station_event(
        &mut self,
        frame: &StationEventFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::StationEvent as u8);
        out.extend_from_slice(&frame.event_id.get().to_le_bytes());
        out.extend_from_slice(&frame.source_station.get().to_le_bytes());
        out.extend_from_slice(&frame.target_station.get().to_le_bytes());
        out.extend_from_slice(&frame.source_tick.get().to_le_bytes());
        out.extend_from_slice(&frame.target_tick.get().to_le_bytes());
        out.push(encode_event_priority(frame.priority));
        encode_event_kind(&frame.kind, out);
        Ok(())
    }

    fn encode_barrier(
        &mut self,
        frame: &BarrierFrame,
        out: &mut Vec<u8>,
    ) -> Result<(), Self::Error> {
        out.push(FrameKind::Barrier as u8);
        out.extend_from_slice(&frame.client_id.get().to_le_bytes());
        out.extend_from_slice(&frame.barrier_id.get().to_le_bytes());
        out.extend_from_slice(&frame.server_tick.get().to_le_bytes());
        out.push(encode_barrier_state(frame.state));
        Ok(())
    }
}

fn write_len_u16(
    field: &'static str,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), BinaryEncodeError> {
    let len =
        u16::try_from(len).map_err(|_| BinaryEncodeError::TooManyItems { field, actual: len })?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_len_u32(
    field: &'static str,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), BinaryEncodeError> {
    let len =
        u32::try_from(len).map_err(|_| BinaryEncodeError::TooManyItems { field, actual: len })?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_bytes(
    field: &'static str,
    bytes: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), BinaryEncodeError> {
    let len = u32::try_from(bytes.len()).map_err(|_| BinaryEncodeError::PayloadTooLarge {
        field,
        actual: bytes.len(),
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn encode_barrier_state(state: BarrierState) -> u8 {
    match state {
        BarrierState::Running => 0,
        BarrierState::Requested => 1,
        BarrierState::WaitingTickBoundary => 2,
        BarrierState::Frozen => 3,
        BarrierState::Resuming => 4,
    }
}

fn decode_barrier_state(state: u8) -> Result<BarrierState, BinaryDecodeError> {
    match state {
        0 => Ok(BarrierState::Running),
        1 => Ok(BarrierState::Requested),
        2 => Ok(BarrierState::WaitingTickBoundary),
        3 => Ok(BarrierState::Frozen),
        4 => Ok(BarrierState::Resuming),
        _ => Err(BinaryDecodeError::InvalidBarrierState(state)),
    }
}

fn encode_command_priority(priority: CommandPriority) -> u8 {
    match priority {
        CommandPriority::Normal => 0,
        CommandPriority::High => 1,
        CommandPriority::Low => 2,
    }
}

fn decode_command_priority(priority: u8) -> Result<CommandPriority, BinaryDecodeError> {
    match priority {
        0 => Ok(CommandPriority::Normal),
        1 => Ok(CommandPriority::High),
        2 => Ok(CommandPriority::Low),
        _ => Err(BinaryDecodeError::InvalidCommandPriority(priority)),
    }
}

fn encode_event_priority(priority: EventPriority) -> u8 {
    match priority {
        EventPriority::Critical => 0,
        EventPriority::Important => 1,
        EventPriority::BestEffort => 2,
    }
}

fn decode_event_priority(priority: u8) -> Result<EventPriority, BinaryDecodeError> {
    match priority {
        0 => Ok(EventPriority::Critical),
        1 => Ok(EventPriority::Important),
        2 => Ok(EventPriority::BestEffort),
        _ => Err(BinaryDecodeError::InvalidEventPriority(priority)),
    }
}

fn encode_event_kind(kind: &EventKind, out: &mut Vec<u8>) {
    match kind {
        EventKind::Custom(kind) => {
            out.push(0);
            out.extend_from_slice(&kind.to_le_bytes());
        }
        EventKind::HandoffPrepare { entity_id } => {
            out.push(1);
            out.extend_from_slice(&entity_id.get().to_le_bytes());
        }
        EventKind::HandoffCommit {
            entity_id,
            owner_epoch,
        } => {
            out.push(2);
            out.extend_from_slice(&entity_id.get().to_le_bytes());
            out.extend_from_slice(&owner_epoch.get().to_le_bytes());
        }
    }
}

fn decode_event_kind(cursor: &mut Cursor<'_>) -> Result<EventKind, BinaryDecodeError> {
    match cursor.read_u8()? {
        0 => Ok(EventKind::Custom(cursor.read_u32()?)),
        1 => Ok(EventKind::HandoffPrepare {
            entity_id: EntityId::new(cursor.read_u64()?),
        }),
        2 => Ok(EventKind::HandoffCommit {
            entity_id: EntityId::new(cursor.read_u64()?),
            owner_epoch: OwnerEpoch::new(cursor.read_u64()?),
        }),
        kind => Err(BinaryDecodeError::InvalidEventKind(kind)),
    }
}

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, BinaryDecodeError> {
        self.require(1)?;
        let value = self.input[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, BinaryDecodeError> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, BinaryDecodeError> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, BinaryDecodeError> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], BinaryDecodeError> {
        self.require(N)?;
        let mut out = [0_u8; N];
        out.copy_from_slice(&self.input[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, BinaryDecodeError> {
        self.require(len)?;
        let bytes = self.input[self.offset..self.offset + len].to_vec();
        self.offset += len;
        Ok(bytes)
    }

    fn require(&self, count: usize) -> Result<(), BinaryDecodeError> {
        let needed = self.offset.saturating_add(count);
        if needed > self.input.len() {
            Err(BinaryDecodeError::Truncated {
                needed,
                available: self.input.len(),
            })
        } else {
            Ok(())
        }
    }

    fn finish(&self) -> Result<(), BinaryDecodeError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(BinaryDecodeError::TrailingBytes(
                self.input.len().saturating_sub(self.offset),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_codec_roundtrips_replication_frame() {
        let frame = ReplicationFrame {
            client_id: ClientId::new(9),
            server_tick: Tick::new(42),
            entity_count: 17,
            estimated_payload_bytes: 544,
            entities: vec![EntityDelta {
                entity_id: EntityId::new(100),
                owner_epoch: OwnerEpoch::new(2),
                components: vec![ComponentDelta {
                    component_id: ComponentId::new(1),
                    version: 9,
                    flags: 0,
                    bytes: vec![1, 2, 3, 4],
                }],
            }],
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_replication(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::Replication(frame));
    }

    #[test]
    fn binary_codec_roundtrips_command_ack_frame() {
        let frame = CommandAckFrame {
            client_id: ClientId::new(1),
            command_id: CommandId::new(2),
            server_tick: Tick::new(3),
            accepted: false,
            reason_code: 7,
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_command_ack(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::CommandAck(frame));
    }

    #[test]
    fn binary_codec_roundtrips_command_frame() {
        let frame = CommandFrame {
            client_id: ClientId::new(1),
            command_id: CommandId::new(2),
            entity_id: EntityId::new(3),
            sequence: 4,
            kind: 5,
            priority: CommandPriority::High,
            payload: vec![9, 8, 7],
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_command(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::Command(frame));
    }

    #[test]
    fn binary_codec_roundtrips_command_dispatch_frame() {
        let frame = CommandDispatchFrame {
            station_id: StationId::new(10),
            client_id: ClientId::new(1),
            command_id: CommandId::new(2),
            entity_id: EntityId::new(3),
            sequence: 4,
            received_at: Tick::new(99),
            kind: 5,
            priority: CommandPriority::High,
            payload: vec![9, 8, 7],
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_command_dispatch(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::CommandDispatch(frame));
    }

    #[test]
    fn command_frame_converts_to_runtime_envelope() {
        let frame = CommandFrame {
            client_id: ClientId::new(1),
            command_id: CommandId::new(2),
            entity_id: EntityId::new(3),
            sequence: 4,
            kind: 5,
            priority: CommandPriority::Low,
            payload: vec![1, 2, 3],
        };

        let envelope = frame.clone().into_envelope(Tick::new(99));
        assert_eq!(envelope.id, frame.command_id);
        assert_eq!(envelope.received_at, Tick::new(99));
        assert_eq!(CommandFrame::from_envelope(&envelope), frame);
    }

    #[test]
    fn command_dispatch_frame_preserves_stamped_envelope_tick() {
        let envelope = CommandEnvelope {
            id: CommandId::new(2),
            client_id: ClientId::new(1),
            entity_id: EntityId::new(3),
            sequence: 4,
            received_at: Tick::new(99),
            kind: 5,
            priority: CommandPriority::Low,
            payload: vec![1, 2, 3],
        };

        let frame = CommandDispatchFrame::from_envelope(StationId::new(10), &envelope);
        assert_eq!(frame.station_id, StationId::new(10));
        assert_eq!(frame.received_at, Tick::new(99));
        assert_eq!(frame.into_envelope(), envelope);
    }

    #[test]
    fn binary_codec_roundtrips_barrier_frame() {
        let frame = BarrierFrame {
            client_id: ClientId::new(1),
            barrier_id: BarrierId::new(99),
            server_tick: Tick::new(11),
            state: BarrierState::Frozen,
        };
        let mut encoder = BinaryFrameEncoder;
        let mut bytes = Vec::new();
        encoder
            .encode_barrier(&frame, &mut bytes)
            .expect("encoder is infallible");

        let decoded = BinaryFrameDecoder
            .decode(&bytes)
            .expect("decode should work");
        assert_eq!(decoded, RuntimeFrame::Barrier(frame));
    }

    #[test]
    fn binary_codec_roundtrips_station_event_frame() {
        let frames = [
            StationEventFrame {
                event_id: EventId::new(1),
                source_station: StationId::new(10),
                target_station: StationId::new(11),
                source_tick: Tick::new(2),
                target_tick: Tick::new(3),
                priority: EventPriority::Critical,
                kind: EventKind::Custom(7),
            },
            StationEventFrame {
                event_id: EventId::new(2),
                source_station: StationId::new(10),
                target_station: StationId::new(11),
                source_tick: Tick::new(2),
                target_tick: Tick::new(3),
                priority: EventPriority::Important,
                kind: EventKind::HandoffPrepare {
                    entity_id: EntityId::new(99),
                },
            },
            StationEventFrame {
                event_id: EventId::new(3),
                source_station: StationId::new(10),
                target_station: StationId::new(11),
                source_tick: Tick::new(2),
                target_tick: Tick::new(3),
                priority: EventPriority::BestEffort,
                kind: EventKind::HandoffCommit {
                    entity_id: EntityId::new(99),
                    owner_epoch: OwnerEpoch::new(5),
                },
            },
        ];

        for frame in frames {
            let mut encoder = BinaryFrameEncoder;
            let mut bytes = Vec::new();
            encoder
                .encode_station_event(&frame, &mut bytes)
                .expect("encoder is infallible");

            let decoded = BinaryFrameDecoder
                .decode(&bytes)
                .expect("decode should work");
            assert_eq!(decoded, RuntimeFrame::StationEvent(frame));
        }
    }

    #[test]
    fn station_event_frame_converts_to_runtime_event() {
        let event = StationEvent {
            id: EventId::new(1),
            source: StationId::new(10),
            target: StationId::new(11),
            source_tick: Tick::new(2),
            target_tick: Tick::new(3),
            priority: EventPriority::Critical,
            kind: EventKind::HandoffPrepare {
                entity_id: EntityId::new(99),
            },
        };

        let frame = StationEventFrame::from_event(&event);
        assert_eq!(frame.event_id, event.id);
        assert_eq!(frame.clone().into_event(), event);
    }

    #[test]
    fn frame_builder_materializes_dirty_component_deltas() {
        use sectorsync_core::prelude::{
            Bounds, ComponentDescriptor, ComponentMigrationMode, ComponentSyncMode, InstanceId,
            NodeId, PolicyId, Position3, ReplicationPlan, StationConfig, Vec3, Vec3LeCodec,
        };

        let mut station = Station::new(StationConfig {
            station_id: sectorsync_core::prelude::StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        let handle = station
            .spawn_owned(
                EntityId::new(10),
                Position3::new(0.0, 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");

        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "velocity",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            12,
        );
        let mut components = ComponentStore::default();
        components
            .set_typed(
                &descriptor,
                handle,
                3,
                &Vec3LeCodec,
                &Vec3::new(1.0, 2.0, 3.0),
            )
            .expect("typed component should encode");

        let builder = ReplicationFrameBuilder::new(ReplicationFrameLimits {
            max_entity_deltas: 8,
            max_components_per_entity: 4,
            max_component_bytes: 64,
        });
        let plan = ReplicationPlan {
            entities: vec![handle],
            stats: sectorsync_core::prelude::ReplicationStats::default(),
        };
        let selection = ComponentSelection {
            component_ids: vec![ComponentId::new(1)],
        };
        let build = builder.build(
            ClientId::new(5),
            Tick::new(9),
            &station,
            &plan,
            &components,
            &selection,
        );

        assert_eq!(build.stats.encoded_entities, 1);
        assert_eq!(build.stats.encoded_components, 1);
        assert_eq!(build.frame.entities[0].entity_id, EntityId::new(10));
        assert_eq!(build.frame.entities[0].components[0].version, 3);

        let mut materialized_bytes = Vec::new();
        BinaryFrameEncoder
            .encode_replication(&build.frame, &mut materialized_bytes)
            .expect("materialized frame should encode");
        let mut direct_bytes = Vec::new();
        let direct_stats = builder
            .encode_binary_into(
                ClientId::new(5),
                Tick::new(9),
                &station,
                &plan,
                &components,
                &selection,
                &mut direct_bytes,
            )
            .expect("plan should encode directly");

        assert_eq!(direct_stats, build.stats);
        assert_eq!(direct_bytes, materialized_bytes);
        assert_eq!(
            builder.sampled_binary_capacity_hint(&station, &plan, &components, &selection),
            direct_bytes.len()
        );
    }

    #[test]
    fn binary_capacity_hint_is_bounded_by_active_builder_limits() {
        let builder = ReplicationFrameBuilder::new(ReplicationFrameLimits {
            max_entity_deltas: 2,
            max_components_per_entity: 2,
            max_component_bytes: 32,
        });
        let plan = ReplicationPlan {
            entities: vec![
                EntityHandle::new(1, 0),
                EntityHandle::new(2, 0),
                EntityHandle::new(3, 0),
            ],
            stats: sectorsync_core::prelude::ReplicationStats {
                estimated_bytes: usize::MAX,
                ..sectorsync_core::prelude::ReplicationStats::default()
            },
        };
        let selection = ComponentSelection {
            component_ids: vec![
                ComponentId::new(1),
                ComponentId::new(2),
                ComponentId::new(3),
            ],
        };

        let fixed_bytes = 29 + 2 * (18 + 2 * 15);
        let maximum_payload_bytes = 2 * 2 * 32;
        assert_eq!(
            builder.binary_capacity_hint(&plan, &selection),
            fixed_bytes + maximum_payload_bytes
        );
    }

    #[test]
    fn sampled_capacity_hint_falls_back_when_any_sample_has_no_dirty_data() {
        use sectorsync_core::prelude::{
            Bounds, ComponentDescriptor, ComponentMigrationMode, ComponentSyncMode, InstanceId,
            NodeId, PolicyId, Position3, ReplicationStats, StationConfig, U32LeCodec,
        };

        let mut station = Station::new(StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        });
        let descriptor = ComponentDescriptor::sparse_blob(
            ComponentId::new(1),
            "health",
            ComponentSyncMode::Delta,
            ComponentMigrationMode::Copy,
            4,
        );
        let mut components = ComponentStore::default();
        let mut handles = Vec::new();
        for entity in 0_u64..4 {
            let handle = station
                .spawn_owned(
                    EntityId::new(entity),
                    Position3::new(0.0, 0.0, 0.0),
                    Bounds::Point,
                    PolicyId::new(1),
                )
                .expect("entity ids are unique");
            if entity < 3 {
                components
                    .set_typed(&descriptor, handle, 1, &U32LeCodec, &100)
                    .expect("component should fit");
            }
            handles.push(handle);
        }
        let plan = ReplicationPlan {
            entities: handles,
            stats: ReplicationStats {
                selected: 4,
                estimated_bytes: 128,
                ..ReplicationStats::default()
            },
        };
        let selection = ComponentSelection {
            component_ids: vec![ComponentId::new(1)],
        };

        assert_eq!(
            ReplicationFrameBuilder::default().sampled_binary_capacity_hint(
                &station,
                &plan,
                &components,
                &selection,
            ),
            0
        );
    }
}
