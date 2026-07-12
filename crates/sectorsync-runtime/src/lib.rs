//! Multi-station orchestration helpers for `SectorSync`.

#![forbid(unsafe_code)]

pub mod deployment;
#[cfg(feature = "parallel")]
mod parallel;

use std::collections::{BTreeMap, HashMap, HashSet};

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, CellCoord3, CellIndex, CellLoadSample, CellOccupancy,
    ClientId, CommandEnvelope, CommandId, CommandIngress, CommandQueueError, CommandQueueMode,
    CommandQueues, ComponentStore, EntityHandle, EntityId, EventQueueError, EventQueueLimits,
    EventQueues, GatewayError, GatewaySessionTable, HandoffTransfer, HotspotDecision,
    HotspotPlanner, HotspotSeverity, HotspotSplitScratch, HotspotThresholds, NodeId, OwnerEpoch,
    PushOutcome, ReplicationBudget, ReplicationPlan, RuntimeBarrier, RuntimeUpgradeHook,
    SnapshotVersion, SplitProposal, Station, StationError, StationEvent, StationId,
    StationLoadSample, StationSnapshot, Tick,
};
use sectorsync_transport::{
    InboundPacket, OutboundPacket, StationOutboundPacket, StationTransportReceiver,
    StationTransportSink, TransportReceiver, TransportSink,
};
use sectorsync_wire::{
    BarrierFrame, BinaryDecodeError, BinaryEncodeError, BinaryFrameDecoder, BinaryFrameEncoder,
    CommandAckFrame, CommandDispatchFrame, CommandFrame, ComponentSelection, FrameDecoder,
    FrameEncoder, ReplicationFrame, ReplicationFrameBuildStats, ReplicationFrameBuilder,
    ReplicationFrameRef, ReplicationFrameRefDecodeError, RuntimeFrame, StationEventFrame,
};

pub use deployment::{
    DeploymentConfig, DeploymentError, DeploymentNodeRoute, DeploymentNodeState,
    DeploymentRouteTable, DeploymentStationMove, DeploymentStationRoute, DeploymentStats,
    GatewayDeliveryError, GatewayDeliveryRoute,
};
#[cfg(feature = "parallel")]
pub use parallel::{
    ParallelReplicationScratch, ParallelReplicationView, ReplicationThreadPool,
    ReplicationThreadPoolBuildError, ReplicationThreadPoolConfig, StationReplicationBatch,
    StationReplicationBatchSource,
};

/// Client replication transport bridge configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationTransportConfig {
    /// Planner budget used for every viewer unless the caller builds frames manually.
    pub budget: ReplicationBudget,
    /// Whether to send replication frames with no encoded entity deltas.
    pub send_empty_frames: bool,
}

/// Client replication transport bridge statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationTransportStats {
    /// Viewer queries planned.
    pub viewers_planned: usize,
    /// Frames skipped because they had no encoded entity deltas.
    pub frames_skipped_empty: usize,
    /// Frames encoded and sent to client transport.
    pub frames_sent: usize,
    /// Bytes sent through client transport.
    pub bytes_sent: usize,
    /// Initial packet byte capacity requested from bounded wire hints.
    pub packet_capacity_hint_bytes: usize,
    /// Entities selected by AOI planning.
    pub entities_selected: usize,
    /// Entities skipped by replication planner budget.
    pub entities_skipped_by_budget: usize,
    /// Entities skipped by replication planner cadence.
    pub entities_skipped_by_cadence: usize,
    /// Entity deltas encoded into replication frames.
    pub entities_encoded: usize,
    /// Component deltas encoded into replication frames.
    pub components_encoded: usize,
    /// Entities rolled back because the concrete frame byte budget filled.
    pub entities_skipped_by_frame_bytes: usize,
}

/// Result of one viewer replication send attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationTransportReport {
    /// Target client.
    pub client_id: ClientId,
    /// Candidate entities selected by the replication planner.
    pub selected_entities: usize,
    /// Entity deltas encoded into the frame.
    pub encoded_entities: usize,
    /// Component deltas encoded into the frame.
    pub encoded_components: usize,
    /// Estimated bytes from the replication planner.
    pub estimated_plan_bytes: usize,
    /// Candidate entities skipped because the planner budget was exhausted.
    pub skipped_by_budget: usize,
    /// Candidate entities skipped because cadence had not elapsed.
    pub skipped_by_cadence: usize,
    /// Entities rolled back because the concrete frame byte budget filled.
    pub skipped_by_frame_bytes: usize,
    /// Encoded wire bytes submitted to transport.
    pub bytes_sent: usize,
    /// Whether a frame was sent.
    pub sent: bool,
}

/// Error produced while planning, building, encoding, or sending replication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationTransportError<E> {
    /// Wire encoding failed.
    Encode(BinaryEncodeError),
    /// Underlying client transport failed.
    Transport(E),
}

impl<E: core::fmt::Display> core::fmt::Display for ReplicationTransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "{error}"),
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for ReplicationTransportError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
    }
}

impl<E> From<BinaryEncodeError> for ReplicationTransportError<E> {
    fn from(value: BinaryEncodeError) -> Self {
        Self::Encode(value)
    }
}

/// Bridge between replication planning/frame building and client packet transport.
#[derive(Clone, Debug)]
pub struct ReplicationTransportBridge {
    config: ReplicationTransportConfig,
    builder: ReplicationFrameBuilder,
    stats: ReplicationTransportStats,
}

impl ReplicationTransportBridge {
    /// Creates a replication transport bridge.
    pub const fn new(config: ReplicationTransportConfig, builder: ReplicationFrameBuilder) -> Self {
        Self {
            config,
            builder,
            stats: ReplicationTransportStats {
                viewers_planned: 0,
                frames_skipped_empty: 0,
                frames_sent: 0,
                bytes_sent: 0,
                packet_capacity_hint_bytes: 0,
                entities_selected: 0,
                entities_skipped_by_budget: 0,
                entities_skipped_by_cadence: 0,
                entities_encoded: 0,
                components_encoded: 0,
                entities_skipped_by_frame_bytes: 0,
            },
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> ReplicationTransportConfig {
        self.config
    }

    /// Returns frame builder configuration.
    pub const fn builder(&self) -> ReplicationFrameBuilder {
        self.builder
    }

    /// Returns accumulated statistics.
    pub const fn stats(&self) -> ReplicationTransportStats {
        self.stats
    }

    /// Builds, encodes, and sends a caller-provided replication plan.
    #[allow(clippy::too_many_arguments)]
    pub fn send_plan<T>(
        &mut self,
        transport: &mut T,
        client_id: ClientId,
        server_tick: Tick,
        station: &Station,
        components: &ComponentStore,
        selection: &ComponentSelection,
        plan: &ReplicationPlan,
    ) -> Result<ReplicationTransportReport, ReplicationTransportError<T::Error>>
    where
        T: TransportSink,
    {
        self.stats.viewers_planned = self.stats.viewers_planned.saturating_add(1);
        self.stats.entities_selected = self
            .stats
            .entities_selected
            .saturating_add(plan.stats.selected);
        self.stats.entities_skipped_by_budget = self
            .stats
            .entities_skipped_by_budget
            .saturating_add(plan.stats.skipped_by_budget);
        self.stats.entities_skipped_by_cadence = self
            .stats
            .entities_skipped_by_cadence
            .saturating_add(plan.stats.skipped_by_cadence);

        let capacity_hint = self
            .builder
            .sampled_binary_capacity_hint(station, plan, components, selection)
            .min(self.config.budget.max_bytes);
        let mut bytes = Vec::with_capacity(capacity_hint);
        self.stats.packet_capacity_hint_bytes = self
            .stats
            .packet_capacity_hint_bytes
            .saturating_add(capacity_hint);
        let build_stats = self.builder.encode_binary_bounded_into(
            client_id,
            server_tick,
            station,
            plan,
            components,
            selection,
            self.config.budget.max_bytes,
            &mut bytes,
        )?;
        self.stats.entities_encoded = self
            .stats
            .entities_encoded
            .saturating_add(build_stats.encoded_entities);
        self.stats.components_encoded = self
            .stats
            .components_encoded
            .saturating_add(build_stats.encoded_components);
        self.stats.entities_skipped_by_frame_bytes = self
            .stats
            .entities_skipped_by_frame_bytes
            .saturating_add(build_stats.skipped_entities_by_frame_bytes);

        if build_stats.encoded_entities == 0 && !self.config.send_empty_frames {
            self.stats.frames_skipped_empty = self.stats.frames_skipped_empty.saturating_add(1);
            return Ok(replication_report(client_id, plan, build_stats, 0, false));
        }

        let byte_len = bytes.len();
        transport
            .send(OutboundPacket { client_id, bytes })
            .map_err(ReplicationTransportError::Transport)?;
        self.stats.frames_sent = self.stats.frames_sent.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(byte_len);

        Ok(replication_report(
            client_id,
            plan,
            build_stats,
            byte_len,
            true,
        ))
    }
}

impl Default for ReplicationTransportBridge {
    fn default() -> Self {
        Self::new(
            ReplicationTransportConfig::default(),
            ReplicationFrameBuilder::default(),
        )
    }
}

fn replication_report(
    client_id: ClientId,
    plan: &ReplicationPlan,
    build_stats: ReplicationFrameBuildStats,
    bytes_sent: usize,
    sent: bool,
) -> ReplicationTransportReport {
    ReplicationTransportReport {
        client_id,
        selected_entities: plan.stats.selected,
        encoded_entities: build_stats.encoded_entities,
        encoded_components: build_stats.encoded_components,
        estimated_plan_bytes: plan.stats.estimated_bytes,
        skipped_by_budget: plan.stats.skipped_by_budget,
        skipped_by_cadence: plan.stats.skipped_by_cadence,
        skipped_by_frame_bytes: build_stats.skipped_entities_by_frame_bytes,
        bytes_sent,
        sent,
    }
}

/// Replication receive bridge configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationReceiveConfig {
    /// Local client id expected inside replication frames.
    pub client_id: ClientId,
    /// Expected remote sender identity when the transport can identify it.
    pub expected_source: Option<ClientId>,
}

impl ReplicationReceiveConfig {
    /// Creates receive configuration for a local client.
    pub const fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            expected_source: None,
        }
    }

    /// Returns a copy that expects packets from `source`.
    #[must_use]
    pub const fn with_expected_source(mut self, source: ClientId) -> Self {
        self.expected_source = Some(source);
        self
    }
}

/// Replication receive bridge statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationReceiveStats {
    /// Packets consumed from client transport.
    pub packets_received: usize,
    /// Bytes consumed from client transport.
    pub bytes_received: usize,
    /// Replication frames decoded and accepted.
    pub frames_received: usize,
    /// Packets rejected by wire decoding.
    pub frames_rejected_decode: usize,
    /// Packets rejected because they were not replication frames.
    pub frames_rejected_unexpected: usize,
    /// Packets rejected because the transport source did not match.
    pub frames_rejected_source: usize,
    /// Packets rejected because the frame target did not match this client.
    pub frames_rejected_target: usize,
    /// Entity deltas received.
    pub entities_received: usize,
    /// Component deltas received.
    pub components_received: usize,
}

/// Result of pumping replication packets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplicationReceivePump {
    /// Packets consumed from client transport.
    pub packets_received: usize,
    /// Bytes consumed from client transport.
    pub bytes_received: usize,
    /// Decoded replication frames.
    pub frames: Vec<ReplicationFrame>,
}

impl ReplicationReceivePump {
    /// Returns accepted frame count.
    pub fn frames_received(&self) -> usize {
        self.frames.len()
    }

    /// Returns received entity delta count.
    pub fn entities_received(&self) -> usize {
        self.frames.iter().map(|frame| frame.entities.len()).sum()
    }

    /// Returns received component delta count.
    pub fn components_received(&self) -> usize {
        self.frames
            .iter()
            .flat_map(|frame| &frame.entities)
            .map(|entity| entity.components.len())
            .sum()
    }
}

/// Allocation-free summary from visiting replication packets immediately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicationReceiveVisitReport {
    /// Packets consumed from client transport.
    pub packets_received: usize,
    /// Bytes consumed from client transport.
    pub bytes_received: usize,
    /// Replication frames accepted and passed to the visitor.
    pub frames_received: usize,
    /// Entity deltas observed across accepted frames.
    pub entities_received: usize,
    /// Component deltas observed across accepted frames.
    pub components_received: usize,
}

/// Error produced while receiving replication frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationReceiveError<E> {
    /// Underlying client transport failed.
    Transport(E),
    /// Wire decoding failed.
    Decode(BinaryDecodeError),
    /// Packet decoded as a non-replication frame.
    UnexpectedFrame,
    /// Packet source did not match expected remote.
    SourceMismatch {
        /// Expected source.
        expected: ClientId,
        /// Actual source if transport identified one.
        actual: Option<ClientId>,
    },
    /// Replication frame targeted another client.
    TargetMismatch {
        /// Expected local client id.
        expected: ClientId,
        /// Actual frame target.
        actual: ClientId,
    },
}

impl<E: core::fmt::Display> core::fmt::Display for ReplicationReceiveError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::UnexpectedFrame => f.write_str("client packet was not a replication frame"),
            Self::SourceMismatch { expected, actual } => write!(
                f,
                "replication source mismatch: expected {}, actual {:?}",
                expected.get(),
                actual.map(ClientId::get)
            ),
            Self::TargetMismatch { expected, actual } => write!(
                f,
                "replication target mismatch: expected {}, actual {}",
                expected.get(),
                actual.get()
            ),
        }
    }
}

impl<E> std::error::Error for ReplicationReceiveError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::UnexpectedFrame | Self::SourceMismatch { .. } | Self::TargetMismatch { .. } => {
                None
            }
        }
    }
}

impl<E> From<BinaryDecodeError> for ReplicationReceiveError<E> {
    fn from(value: BinaryDecodeError) -> Self {
        Self::Decode(value)
    }
}

/// Error produced while visiting borrowed replication frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationReceiveVisitError<T, V> {
    /// Packet receive, validation, or frame decoding failed.
    Receive(ReplicationReceiveError<T>),
    /// The caller-provided frame visitor failed.
    Visitor(V),
}

impl<T: core::fmt::Display, V: core::fmt::Display> core::fmt::Display
    for ReplicationReceiveVisitError<T, V>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Receive(error) => error.fmt(f),
            Self::Visitor(error) => write!(f, "replication frame visitor failed: {error}"),
        }
    }
}

impl<T, V> std::error::Error for ReplicationReceiveVisitError<T, V>
where
    T: std::error::Error + 'static,
    V: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Receive(error) => Some(error),
            Self::Visitor(error) => Some(error),
        }
    }
}

/// Bridge between client packet transport and decoded replication frames.
#[derive(Clone, Debug)]
pub struct ReplicationReceiveBridge {
    config: ReplicationReceiveConfig,
    stats: ReplicationReceiveStats,
}

impl ReplicationReceiveBridge {
    /// Creates a receive bridge.
    pub const fn new(config: ReplicationReceiveConfig) -> Self {
        Self {
            config,
            stats: ReplicationReceiveStats {
                packets_received: 0,
                bytes_received: 0,
                frames_received: 0,
                frames_rejected_decode: 0,
                frames_rejected_unexpected: 0,
                frames_rejected_source: 0,
                frames_rejected_target: 0,
                entities_received: 0,
                components_received: 0,
            },
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> ReplicationReceiveConfig {
        self.config
    }

    /// Returns accumulated statistics.
    pub const fn stats(&self) -> ReplicationReceiveStats {
        self.stats
    }

    /// Receives and decodes up to `max_packets` replication frames.
    pub fn pump_owned<T>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
    ) -> Result<ReplicationReceivePump, ReplicationReceiveError<T::Error>>
    where
        T: TransportReceiver,
    {
        let mut pump = ReplicationReceivePump::default();
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv()
                .map_err(ReplicationReceiveError::Transport)?
            else {
                break;
            };
            self.stats.packets_received = self.stats.packets_received.saturating_add(1);
            self.stats.bytes_received =
                self.stats.bytes_received.saturating_add(packet.bytes.len());
            pump.packets_received = pump.packets_received.saturating_add(1);
            pump.bytes_received = pump.bytes_received.saturating_add(packet.bytes.len());

            if let Some(expected) = self.config.expected_source
                && packet.client_id != Some(expected)
            {
                self.stats.frames_rejected_source =
                    self.stats.frames_rejected_source.saturating_add(1);
                return Err(ReplicationReceiveError::SourceMismatch {
                    expected,
                    actual: packet.client_id,
                });
            }

            let decoded = match BinaryFrameDecoder.decode(&packet.bytes) {
                Ok(decoded) => decoded,
                Err(error) => {
                    self.stats.frames_rejected_decode =
                        self.stats.frames_rejected_decode.saturating_add(1);
                    return Err(ReplicationReceiveError::Decode(error));
                }
            };
            let RuntimeFrame::Replication(frame) = decoded else {
                self.stats.frames_rejected_unexpected =
                    self.stats.frames_rejected_unexpected.saturating_add(1);
                return Err(ReplicationReceiveError::UnexpectedFrame);
            };
            if frame.client_id != self.config.client_id {
                self.stats.frames_rejected_target =
                    self.stats.frames_rejected_target.saturating_add(1);
                return Err(ReplicationReceiveError::TargetMismatch {
                    expected: self.config.client_id,
                    actual: frame.client_id,
                });
            }

            self.stats.frames_received = self.stats.frames_received.saturating_add(1);
            self.stats.entities_received = self
                .stats
                .entities_received
                .saturating_add(frame.entities.len());
            let components = frame
                .entities
                .iter()
                .map(|entity| entity.components.len())
                .sum::<usize>();
            self.stats.components_received =
                self.stats.components_received.saturating_add(components);
            pump.frames.push(frame);
        }
        Ok(pump)
    }

    /// Receives validated replication frames and visits borrowed deltas without
    /// materializing nested owned frame storage.
    ///
    /// The visitor must consume borrowed component bytes before returning. Its
    /// error is surfaced separately from transport and frame-validation errors.
    /// A frame is counted as accepted after source/wire/target validation and
    /// before visitor invocation; accumulated bridge statistics are not rolled
    /// back when the visitor returns an error.
    pub fn pump<T, F, V>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
        mut visitor: F,
    ) -> Result<ReplicationReceiveVisitReport, ReplicationReceiveVisitError<T::Error, V>>
    where
        T: TransportReceiver,
        F: for<'frame> FnMut(ReplicationFrameRef<'frame>) -> Result<(), V>,
    {
        let mut report = ReplicationReceiveVisitReport::default();
        for _ in 0..max_packets {
            let Some(packet) = transport.try_recv().map_err(|error| {
                ReplicationReceiveVisitError::Receive(ReplicationReceiveError::Transport(error))
            })?
            else {
                break;
            };
            self.stats.packets_received = self.stats.packets_received.saturating_add(1);
            self.stats.bytes_received =
                self.stats.bytes_received.saturating_add(packet.bytes.len());
            report.packets_received = report.packets_received.saturating_add(1);
            report.bytes_received = report.bytes_received.saturating_add(packet.bytes.len());

            if let Some(expected) = self.config.expected_source
                && packet.client_id != Some(expected)
            {
                self.stats.frames_rejected_source =
                    self.stats.frames_rejected_source.saturating_add(1);
                return Err(ReplicationReceiveVisitError::Receive(
                    ReplicationReceiveError::SourceMismatch {
                        expected,
                        actual: packet.client_id,
                    },
                ));
            }

            let frame = match BinaryFrameDecoder.decode_replication(&packet.bytes) {
                Ok(frame) => frame,
                Err(ReplicationFrameRefDecodeError::Binary(error)) => {
                    self.stats.frames_rejected_decode =
                        self.stats.frames_rejected_decode.saturating_add(1);
                    return Err(ReplicationReceiveVisitError::Receive(
                        ReplicationReceiveError::Decode(error),
                    ));
                }
                Err(ReplicationFrameRefDecodeError::UnexpectedFrameKind(_)) => {
                    self.stats.frames_rejected_unexpected =
                        self.stats.frames_rejected_unexpected.saturating_add(1);
                    return Err(ReplicationReceiveVisitError::Receive(
                        ReplicationReceiveError::UnexpectedFrame,
                    ));
                }
            };
            if frame.client_id != self.config.client_id {
                self.stats.frames_rejected_target =
                    self.stats.frames_rejected_target.saturating_add(1);
                return Err(ReplicationReceiveVisitError::Receive(
                    ReplicationReceiveError::TargetMismatch {
                        expected: self.config.client_id,
                        actual: frame.client_id,
                    },
                ));
            }

            let entities = frame.encoded_entity_count();
            let components = frame
                .entities()
                .map(sectorsync_wire::EntityDeltaRef::encoded_component_count)
                .sum::<usize>();
            self.stats.frames_received = self.stats.frames_received.saturating_add(1);
            self.stats.entities_received = self.stats.entities_received.saturating_add(entities);
            self.stats.components_received =
                self.stats.components_received.saturating_add(components);
            report.frames_received = report.frames_received.saturating_add(1);
            report.entities_received = report.entities_received.saturating_add(entities);
            report.components_received = report.components_received.saturating_add(components);
            visitor(frame).map_err(ReplicationReceiveVisitError::Visitor)?;
        }
        Ok(report)
    }
}

/// Low-level client transport bridge configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientTransportConfig {
    /// Local client id expected inside client-bound frames.
    pub client_id: ClientId,
    /// Remote server/gateway id used as the transport packet target for commands.
    pub server_id: ClientId,
    /// Expected remote sender identity when the transport can identify it.
    pub expected_source: Option<ClientId>,
}

impl ClientTransportConfig {
    /// Creates client transport configuration for a server/gateway target.
    pub const fn new(client_id: ClientId, server_id: ClientId) -> Self {
        Self {
            client_id,
            server_id,
            expected_source: None,
        }
    }

    /// Returns a copy that expects inbound packets from `source`.
    #[must_use]
    pub const fn with_expected_source(mut self, source: ClientId) -> Self {
        self.expected_source = Some(source);
        self
    }
}

/// Low-level client transport bridge statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientTransportStats {
    /// Command frames encoded and submitted to transport.
    pub commands_sent: usize,
    /// Command bytes submitted to transport.
    pub command_bytes_sent: usize,
    /// Packets consumed from transport.
    pub packets_received: usize,
    /// Bytes consumed from transport.
    pub bytes_received: usize,
    /// Command ACK frames decoded and accepted.
    pub command_acks_received: usize,
    /// Replication frames decoded and accepted.
    pub replication_frames_received: usize,
    /// Barrier frames decoded and accepted.
    pub barrier_frames_received: usize,
    /// Packets rejected by wire decoding.
    pub frames_rejected_decode: usize,
    /// Packets rejected because they were not client-bound frames.
    pub frames_rejected_unexpected: usize,
    /// Packets rejected because the transport source did not match.
    pub frames_rejected_source: usize,
    /// Packets rejected because the frame target did not match this client.
    pub frames_rejected_target: usize,
    /// Entity deltas received in replication frames.
    pub entities_received: usize,
    /// Component deltas received in replication frames.
    pub components_received: usize,
}

/// Result of sending one command frame.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientCommandSendReport {
    /// Submitted command id.
    pub command_id: CommandId,
    /// Encoded command bytes submitted to transport.
    pub bytes_sent: usize,
}

/// Client-bound frame categories accepted by `ClientTransportBridge`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientInboundFrameKind {
    /// Command acknowledgement.
    CommandAck,
    /// Replication update.
    Replication,
    /// Runtime barrier notification.
    Barrier,
}

/// Result of pumping client-bound frames.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClientTransportPump {
    /// Packets consumed from transport.
    pub packets_received: usize,
    /// Bytes consumed from transport.
    pub bytes_received: usize,
    /// Command acknowledgements decoded and accepted.
    pub command_acks: Vec<CommandAckFrame>,
    /// Replication frames decoded and accepted.
    pub replication_frames: Vec<ReplicationFrame>,
    /// Barrier frames decoded and accepted.
    pub barriers: Vec<BarrierFrame>,
}

impl ClientTransportPump {
    /// Returns received command ACK count.
    pub fn command_acks_received(&self) -> usize {
        self.command_acks.len()
    }

    /// Returns accepted replication frame count.
    pub fn replication_frames_received(&self) -> usize {
        self.replication_frames.len()
    }

    /// Returns accepted barrier frame count.
    pub fn barrier_frames_received(&self) -> usize {
        self.barriers.len()
    }

    /// Returns received entity delta count.
    pub fn entities_received(&self) -> usize {
        self.replication_frames
            .iter()
            .map(|frame| frame.entities.len())
            .sum()
    }

    /// Returns received component delta count.
    pub fn components_received(&self) -> usize {
        self.replication_frames
            .iter()
            .flat_map(|frame| &frame.entities)
            .map(|entity| entity.components.len())
            .sum()
    }
}

/// Client-bound frame visited without materializing replication delta storage.
#[derive(Clone, Debug)]
pub enum ClientInboundFrameRef<'a> {
    /// Decoded command acknowledgement value.
    CommandAck(CommandAckFrame),
    /// Validated replication frame borrowing entity/component payload bytes.
    Replication(ReplicationFrameRef<'a>),
    /// Decoded runtime barrier notification value.
    Barrier(BarrierFrame),
}

/// Fixed-size result of visiting client-bound transport frames.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientTransportVisitReport {
    /// Packets consumed from transport.
    pub packets_received: usize,
    /// Bytes consumed from transport.
    pub bytes_received: usize,
    /// Command acknowledgements decoded and accepted.
    pub command_acks_received: usize,
    /// Replication frames decoded and accepted.
    pub replication_frames_received: usize,
    /// Barrier frames decoded and accepted.
    pub barrier_frames_received: usize,
    /// Entity deltas visited in replication frames.
    pub entities_received: usize,
    /// Component deltas visited in replication frames.
    pub components_received: usize,
}

/// Error produced while visiting client-bound transport frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientTransportVisitError<T, V> {
    /// Packet receive, validation, or frame decoding failed.
    Receive(ClientTransportBridgeError<T>),
    /// The caller-provided frame visitor failed.
    Visitor(V),
}

impl<T: core::fmt::Display, V: core::fmt::Display> core::fmt::Display
    for ClientTransportVisitError<T, V>
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Receive(error) => error.fmt(formatter),
            Self::Visitor(error) => write!(formatter, "client frame visitor failed: {error}"),
        }
    }
}

impl<T, V> std::error::Error for ClientTransportVisitError<T, V>
where
    T: std::error::Error + 'static,
    V: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Receive(error) => Some(error),
            Self::Visitor(error) => Some(error),
        }
    }
}

/// Error produced by the low-level client transport bridge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientTransportBridgeError<E> {
    /// Outbound command used a different client id than the bridge config.
    CommandClientMismatch {
        /// Expected local client id.
        expected: ClientId,
        /// Actual command client id.
        actual: ClientId,
    },
    /// Wire encoding failed.
    Encode(BinaryEncodeError),
    /// Underlying client transport failed.
    Transport(E),
    /// Wire decoding failed.
    Decode(BinaryDecodeError),
    /// Packet decoded as a frame that is not client-bound.
    UnexpectedFrame,
    /// Packet source did not match expected remote.
    SourceMismatch {
        /// Expected source.
        expected: ClientId,
        /// Actual source if transport identified one.
        actual: Option<ClientId>,
    },
    /// Client-bound frame targeted another client.
    TargetMismatch {
        /// Frame category.
        kind: ClientInboundFrameKind,
        /// Expected local client id.
        expected: ClientId,
        /// Actual frame target.
        actual: ClientId,
    },
}

impl<E: core::fmt::Display> core::fmt::Display for ClientTransportBridgeError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CommandClientMismatch { expected, actual } => write!(
                f,
                "command client mismatch: expected {}, actual {}",
                expected.get(),
                actual.get()
            ),
            Self::Encode(error) => write!(f, "{error}"),
            Self::Transport(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::UnexpectedFrame => f.write_str("packet was not a client-bound frame"),
            Self::SourceMismatch { expected, actual } => write!(
                f,
                "client packet source mismatch: expected {}, actual {:?}",
                expected.get(),
                actual.map(ClientId::get)
            ),
            Self::TargetMismatch {
                kind,
                expected,
                actual,
            } => write!(
                f,
                "client {:?} frame target mismatch: expected {}, actual {}",
                kind,
                expected.get(),
                actual.get()
            ),
        }
    }
}

impl<E> std::error::Error for ClientTransportBridgeError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Transport(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::CommandClientMismatch { .. }
            | Self::UnexpectedFrame
            | Self::SourceMismatch { .. }
            | Self::TargetMismatch { .. } => None,
        }
    }
}

impl<E> From<BinaryEncodeError> for ClientTransportBridgeError<E> {
    fn from(value: BinaryEncodeError) -> Self {
        Self::Encode(value)
    }
}

impl<E> From<BinaryDecodeError> for ClientTransportBridgeError<E> {
    fn from(value: BinaryDecodeError) -> Self {
        Self::Decode(value)
    }
}

/// Low-level bridge for client command send and client-bound frame receive.
#[derive(Clone, Debug)]
pub struct ClientTransportBridge {
    config: ClientTransportConfig,
    stats: ClientTransportStats,
}

impl ClientTransportBridge {
    /// Creates a client transport bridge.
    pub const fn new(config: ClientTransportConfig) -> Self {
        Self {
            config,
            stats: ClientTransportStats {
                commands_sent: 0,
                command_bytes_sent: 0,
                packets_received: 0,
                bytes_received: 0,
                command_acks_received: 0,
                replication_frames_received: 0,
                barrier_frames_received: 0,
                frames_rejected_decode: 0,
                frames_rejected_unexpected: 0,
                frames_rejected_source: 0,
                frames_rejected_target: 0,
                entities_received: 0,
                components_received: 0,
            },
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> ClientTransportConfig {
        self.config
    }

    /// Returns accumulated statistics.
    pub const fn stats(&self) -> ClientTransportStats {
        self.stats
    }

    /// Encodes and sends a client command frame to the configured server id.
    pub fn send_command_frame<T>(
        &mut self,
        transport: &mut T,
        frame: &CommandFrame,
    ) -> Result<ClientCommandSendReport, ClientTransportBridgeError<T::Error>>
    where
        T: TransportSink,
    {
        if frame.client_id != self.config.client_id {
            return Err(ClientTransportBridgeError::CommandClientMismatch {
                expected: self.config.client_id,
                actual: frame.client_id,
            });
        }

        let mut bytes = Vec::new();
        BinaryFrameEncoder.encode_command(frame, &mut bytes)?;
        let bytes_sent = bytes.len();
        transport
            .send(OutboundPacket {
                client_id: self.config.server_id,
                bytes,
            })
            .map_err(ClientTransportBridgeError::Transport)?;
        self.stats.commands_sent = self.stats.commands_sent.saturating_add(1);
        self.stats.command_bytes_sent = self.stats.command_bytes_sent.saturating_add(bytes_sent);

        Ok(ClientCommandSendReport {
            command_id: frame.command_id,
            bytes_sent,
        })
    }

    /// Receives and decodes up to `max_packets` client-bound frames.
    pub fn pump_owned<T>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
    ) -> Result<ClientTransportPump, ClientTransportBridgeError<T::Error>>
    where
        T: TransportReceiver,
    {
        let mut pump = ClientTransportPump::default();
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv()
                .map_err(ClientTransportBridgeError::Transport)?
            else {
                break;
            };
            self.stats.packets_received = self.stats.packets_received.saturating_add(1);
            self.stats.bytes_received =
                self.stats.bytes_received.saturating_add(packet.bytes.len());
            pump.packets_received = pump.packets_received.saturating_add(1);
            pump.bytes_received = pump.bytes_received.saturating_add(packet.bytes.len());

            if let Some(expected) = self.config.expected_source
                && packet.client_id != Some(expected)
            {
                self.stats.frames_rejected_source =
                    self.stats.frames_rejected_source.saturating_add(1);
                return Err(ClientTransportBridgeError::SourceMismatch {
                    expected,
                    actual: packet.client_id,
                });
            }

            let decoded = match BinaryFrameDecoder.decode(&packet.bytes) {
                Ok(decoded) => decoded,
                Err(error) => {
                    self.stats.frames_rejected_decode =
                        self.stats.frames_rejected_decode.saturating_add(1);
                    return Err(ClientTransportBridgeError::Decode(error));
                }
            };
            match decoded {
                RuntimeFrame::CommandAck(frame) => {
                    self.validate_client_target(
                        ClientInboundFrameKind::CommandAck,
                        frame.client_id,
                    )?;
                    self.stats.command_acks_received =
                        self.stats.command_acks_received.saturating_add(1);
                    pump.command_acks.push(frame);
                }
                RuntimeFrame::Replication(frame) => {
                    self.validate_client_target(
                        ClientInboundFrameKind::Replication,
                        frame.client_id,
                    )?;
                    self.stats.replication_frames_received =
                        self.stats.replication_frames_received.saturating_add(1);
                    self.stats.entities_received = self
                        .stats
                        .entities_received
                        .saturating_add(frame.entities.len());
                    let components = frame
                        .entities
                        .iter()
                        .map(|entity| entity.components.len())
                        .sum::<usize>();
                    self.stats.components_received =
                        self.stats.components_received.saturating_add(components);
                    pump.replication_frames.push(frame);
                }
                RuntimeFrame::Barrier(frame) => {
                    self.validate_client_target(ClientInboundFrameKind::Barrier, frame.client_id)?;
                    self.stats.barrier_frames_received =
                        self.stats.barrier_frames_received.saturating_add(1);
                    pump.barriers.push(frame);
                }
                RuntimeFrame::Command(_)
                | RuntimeFrame::CommandDispatch(_)
                | RuntimeFrame::StationEvent(_) => {
                    self.stats.frames_rejected_unexpected =
                        self.stats.frames_rejected_unexpected.saturating_add(1);
                    return Err(ClientTransportBridgeError::UnexpectedFrame);
                }
            }
        }
        Ok(pump)
    }

    /// Receives mixed client-bound frames and visits them without materializing
    /// nested replication frame storage.
    ///
    /// ACK and barrier values are copied into the visitor enum. Replication
    /// component bytes borrow the current transport packet and must be consumed
    /// before the visitor returns. Accepted statistics are recorded before
    /// visitor invocation and are not rolled back if the visitor fails.
    #[allow(clippy::too_many_lines)]
    pub fn pump<T, F, V>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
        mut visitor: F,
    ) -> Result<ClientTransportVisitReport, ClientTransportVisitError<T::Error, V>>
    where
        T: TransportReceiver,
        F: for<'frame> FnMut(ClientInboundFrameRef<'frame>) -> Result<(), V>,
    {
        let mut report = ClientTransportVisitReport::default();
        for _ in 0..max_packets {
            let Some(packet) = transport.try_recv().map_err(|error| {
                ClientTransportVisitError::Receive(ClientTransportBridgeError::Transport(error))
            })?
            else {
                break;
            };
            self.stats.packets_received = self.stats.packets_received.saturating_add(1);
            self.stats.bytes_received =
                self.stats.bytes_received.saturating_add(packet.bytes.len());
            report.packets_received = report.packets_received.saturating_add(1);
            report.bytes_received = report.bytes_received.saturating_add(packet.bytes.len());

            if let Some(expected) = self.config.expected_source
                && packet.client_id != Some(expected)
            {
                self.stats.frames_rejected_source =
                    self.stats.frames_rejected_source.saturating_add(1);
                return Err(ClientTransportVisitError::Receive(
                    ClientTransportBridgeError::SourceMismatch {
                        expected,
                        actual: packet.client_id,
                    },
                ));
            }

            match BinaryFrameDecoder.decode_replication(&packet.bytes) {
                Ok(frame) => {
                    self.validate_client_target::<T::Error>(
                        ClientInboundFrameKind::Replication,
                        frame.client_id,
                    )
                    .map_err(ClientTransportVisitError::Receive)?;
                    let entities = frame.encoded_entity_count();
                    let components = frame
                        .entities()
                        .map(sectorsync_wire::EntityDeltaRef::encoded_component_count)
                        .sum::<usize>();
                    self.stats.replication_frames_received =
                        self.stats.replication_frames_received.saturating_add(1);
                    self.stats.entities_received =
                        self.stats.entities_received.saturating_add(entities);
                    self.stats.components_received =
                        self.stats.components_received.saturating_add(components);
                    report.replication_frames_received =
                        report.replication_frames_received.saturating_add(1);
                    report.entities_received = report.entities_received.saturating_add(entities);
                    report.components_received =
                        report.components_received.saturating_add(components);
                    visitor(ClientInboundFrameRef::Replication(frame))
                        .map_err(ClientTransportVisitError::Visitor)?;
                }
                Err(ReplicationFrameRefDecodeError::Binary(error)) => {
                    self.stats.frames_rejected_decode =
                        self.stats.frames_rejected_decode.saturating_add(1);
                    return Err(ClientTransportVisitError::Receive(
                        ClientTransportBridgeError::Decode(error),
                    ));
                }
                Err(ReplicationFrameRefDecodeError::UnexpectedFrameKind(_)) => {
                    let decoded = BinaryFrameDecoder.decode(&packet.bytes).map_err(|error| {
                        self.stats.frames_rejected_decode =
                            self.stats.frames_rejected_decode.saturating_add(1);
                        ClientTransportVisitError::Receive(ClientTransportBridgeError::Decode(
                            error,
                        ))
                    })?;
                    match decoded {
                        RuntimeFrame::CommandAck(frame) => {
                            self.validate_client_target::<T::Error>(
                                ClientInboundFrameKind::CommandAck,
                                frame.client_id,
                            )
                            .map_err(ClientTransportVisitError::Receive)?;
                            self.stats.command_acks_received =
                                self.stats.command_acks_received.saturating_add(1);
                            report.command_acks_received =
                                report.command_acks_received.saturating_add(1);
                            visitor(ClientInboundFrameRef::CommandAck(frame))
                                .map_err(ClientTransportVisitError::Visitor)?;
                        }
                        RuntimeFrame::Barrier(frame) => {
                            self.validate_client_target::<T::Error>(
                                ClientInboundFrameKind::Barrier,
                                frame.client_id,
                            )
                            .map_err(ClientTransportVisitError::Receive)?;
                            self.stats.barrier_frames_received =
                                self.stats.barrier_frames_received.saturating_add(1);
                            report.barrier_frames_received =
                                report.barrier_frames_received.saturating_add(1);
                            visitor(ClientInboundFrameRef::Barrier(frame))
                                .map_err(ClientTransportVisitError::Visitor)?;
                        }
                        RuntimeFrame::Replication(_)
                        | RuntimeFrame::Command(_)
                        | RuntimeFrame::CommandDispatch(_)
                        | RuntimeFrame::StationEvent(_) => {
                            self.stats.frames_rejected_unexpected =
                                self.stats.frames_rejected_unexpected.saturating_add(1);
                            return Err(ClientTransportVisitError::Receive(
                                ClientTransportBridgeError::UnexpectedFrame,
                            ));
                        }
                    }
                }
            }
        }
        Ok(report)
    }

    fn validate_client_target<E>(
        &mut self,
        kind: ClientInboundFrameKind,
        actual: ClientId,
    ) -> Result<(), ClientTransportBridgeError<E>> {
        if actual == self.config.client_id {
            return Ok(());
        }
        self.stats.frames_rejected_target = self.stats.frames_rejected_target.saturating_add(1);
        Err(ClientTransportBridgeError::TargetMismatch {
            kind,
            expected: self.config.client_id,
            actual,
        })
    }
}

/// Runtime barrier notification transport statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BarrierTransportStats {
    /// Barrier frames encoded and submitted to client transport.
    pub notifications_sent: usize,
    /// Client targets submitted to transport.
    pub clients_notified: usize,
    /// Encoded bytes submitted to transport.
    pub bytes_sent: usize,
}

/// Result of one barrier notification broadcast.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierTransportReport {
    /// Barrier id.
    pub barrier_id: BarrierId,
    /// Barrier state sent to clients.
    pub state: BarrierState,
    /// Server tick associated with the notification.
    pub server_tick: Tick,
    /// Client targets requested by the caller.
    pub clients_requested: usize,
    /// Client targets successfully submitted to transport.
    pub clients_sent: usize,
    /// Encoded bytes submitted to transport.
    pub bytes_sent: usize,
}

/// Error produced while encoding or sending barrier notifications.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BarrierTransportError<E> {
    /// Wire encoding failed.
    Encode(BinaryEncodeError),
    /// Underlying client transport failed.
    Transport(E),
}

impl<E: core::fmt::Display> core::fmt::Display for BarrierTransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "{error}"),
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for BarrierTransportError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
    }
}

impl<E> From<BinaryEncodeError> for BarrierTransportError<E> {
    fn from(value: BinaryEncodeError) -> Self {
        Self::Encode(value)
    }
}

/// Low-level bridge for sending runtime barrier notifications to clients.
#[derive(Clone, Debug, Default)]
pub struct BarrierTransportBridge {
    stats: BarrierTransportStats,
}

impl BarrierTransportBridge {
    /// Creates a barrier notification transport bridge.
    pub const fn new() -> Self {
        Self {
            stats: BarrierTransportStats {
                notifications_sent: 0,
                clients_notified: 0,
                bytes_sent: 0,
            },
        }
    }

    /// Returns accumulated statistics.
    pub const fn stats(&self) -> BarrierTransportStats {
        self.stats
    }

    /// Sends one barrier notification to one client.
    pub fn send_state<T>(
        &mut self,
        transport: &mut T,
        client_id: ClientId,
        barrier_id: BarrierId,
        server_tick: Tick,
        state: BarrierState,
    ) -> Result<usize, BarrierTransportError<T::Error>>
    where
        T: TransportSink,
    {
        let frame = BarrierFrame {
            client_id,
            barrier_id,
            server_tick,
            state,
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder.encode_barrier(&frame, &mut bytes)?;
        let bytes_sent = bytes.len();
        transport
            .send(OutboundPacket { client_id, bytes })
            .map_err(BarrierTransportError::Transport)?;
        self.stats.notifications_sent = self.stats.notifications_sent.saturating_add(1);
        self.stats.clients_notified = self.stats.clients_notified.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(bytes_sent);
        Ok(bytes_sent)
    }

    /// Sends one runtime barrier notification to one client.
    pub fn send_barrier<T>(
        &mut self,
        transport: &mut T,
        client_id: ClientId,
        barrier: RuntimeBarrier,
    ) -> Result<usize, BarrierTransportError<T::Error>>
    where
        T: TransportSink,
    {
        self.send_state(
            transport,
            client_id,
            barrier.id,
            barrier.target_tick,
            barrier.state,
        )
    }

    /// Broadcasts one barrier state to a bounded caller-provided client list.
    pub fn broadcast_state<T, I>(
        &mut self,
        transport: &mut T,
        clients: I,
        barrier_id: BarrierId,
        server_tick: Tick,
        state: BarrierState,
    ) -> Result<BarrierTransportReport, BarrierTransportError<T::Error>>
    where
        T: TransportSink,
        I: IntoIterator<Item = ClientId>,
    {
        let mut report = BarrierTransportReport {
            barrier_id,
            state,
            server_tick,
            clients_requested: 0,
            clients_sent: 0,
            bytes_sent: 0,
        };
        for client_id in clients {
            report.clients_requested = report.clients_requested.saturating_add(1);
            let bytes_sent =
                self.send_state(transport, client_id, barrier_id, server_tick, state)?;
            report.clients_sent = report.clients_sent.saturating_add(1);
            report.bytes_sent = report.bytes_sent.saturating_add(bytes_sent);
        }
        Ok(report)
    }

    /// Broadcasts one runtime barrier to a bounded caller-provided client list.
    pub fn broadcast_barrier<T, I>(
        &mut self,
        transport: &mut T,
        clients: I,
        barrier: RuntimeBarrier,
    ) -> Result<BarrierTransportReport, BarrierTransportError<T::Error>>
    where
        T: TransportSink,
        I: IntoIterator<Item = ClientId>,
    {
        self.broadcast_state(
            transport,
            clients,
            barrier.id,
            barrier.target_tick,
            barrier.state,
        )
    }
}

/// Accepted command ACK reason code.
pub const GATEWAY_COMMAND_ACK_ACCEPTED: u16 = 0;
/// Command was rejected by generic gateway/session state.
pub const GATEWAY_COMMAND_ACK_GATEWAY_REJECTED: u16 = 1;
/// Command was rejected by gateway rate limiting.
pub const GATEWAY_COMMAND_ACK_RATE_LIMITED: u16 = 2;
/// Command was rejected as stale or replayed.
pub const GATEWAY_COMMAND_ACK_REPLAY_OR_STALE: u16 = 3;
/// Command could not be queued because a target station queue was full.
pub const GATEWAY_COMMAND_ACK_QUEUE_FULL: u16 = 4;
/// Command was rejected by the station barrier ingress policy.
pub const GATEWAY_COMMAND_ACK_BARRIER_REJECTED: u16 = 5;
/// Command route pointed at a station queue that was not registered.
pub const GATEWAY_COMMAND_ACK_MISSING_QUEUE: u16 = 6;
/// Command could not be resolved through deployment metadata.
pub const GATEWAY_COMMAND_ACK_DEPLOYMENT_REJECTED: u16 = 7;

/// Gateway command pipeline configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayCommandPipelineConfig {
    /// Encode negative ACKs for gateway/queue rejections.
    pub ack_rejections: bool,
}

impl Default for GatewayCommandPipelineConfig {
    fn default() -> Self {
        Self {
            ack_rejections: true,
        }
    }
}

/// Gateway command pipeline statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GatewayCommandPipelineStats {
    /// Command frames decoded.
    pub command_frames_decoded: usize,
    /// Frames rejected by the binary decoder.
    pub frames_rejected_decode: usize,
    /// Non-command frames rejected by this pipeline.
    pub frames_rejected_non_command: usize,
    /// Commands admitted by gateway/session metadata.
    pub commands_admitted: usize,
    /// Commands enqueued into target station queues.
    pub commands_enqueued: usize,
    /// Commands rejected by gateway/session metadata.
    pub commands_rejected_gateway: usize,
    /// Commands rejected by station queue or station queue lookup.
    pub commands_rejected_queue: usize,
    /// Commands resolved to deployment node delivery routes.
    pub commands_routed_deployment: usize,
    /// Commands rejected by deployment node/station route metadata.
    pub commands_rejected_deployment: usize,
    /// ACK frames encoded.
    pub acks_encoded: usize,
}

/// Gateway command pipeline error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayCommandPipelineError {
    /// Wire decode failed.
    Decode(BinaryDecodeError),
    /// Frame decoded correctly but was not a command frame.
    NonCommandFrame,
    /// Gateway/session metadata rejected the command.
    Gateway(GatewayError),
    /// Gateway route pointed at a missing station queue.
    MissingQueue(StationId),
    /// Target station queue rejected the command.
    Queue(CommandQueueError),
    /// Deployment route metadata rejected command delivery.
    Deployment(DeploymentError),
    /// ACK encode failed.
    Encode(BinaryEncodeError),
}

impl core::fmt::Display for GatewayCommandPipelineError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "{error}"),
            Self::NonCommandFrame => f.write_str("gateway command pipeline expected command frame"),
            Self::Gateway(error) => write!(f, "{error}"),
            Self::MissingQueue(station_id) => write!(
                f,
                "gateway command route target station {} has no queue",
                station_id.get()
            ),
            Self::Queue(error) => write!(f, "{error}"),
            Self::Deployment(error) => write!(f, "{error}"),
            Self::Encode(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for GatewayCommandPipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Gateway(error) => Some(error),
            Self::Queue(error) => Some(error),
            Self::Deployment(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::NonCommandFrame | Self::MissingQueue(_) => None,
        }
    }
}

/// Gateway command pipeline result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GatewayCommandPipelineReport {
    /// Client id when a command frame was decoded.
    pub client_id: Option<ClientId>,
    /// Command id when a command frame was decoded.
    pub command_id: Option<CommandId>,
    /// Target station when gateway routing succeeded.
    pub station_id: Option<StationId>,
    /// Target node when deployment routing succeeded.
    pub node_id: Option<NodeId>,
    /// Resolved deployment delivery route.
    pub delivery: Option<GatewayDeliveryRoute>,
    /// Stamped command envelope for external dispatch, when not enqueued locally.
    pub command: Option<CommandEnvelope>,
    /// Whether the command was queued for station application.
    pub accepted: bool,
    /// ACK reason code. Zero means accepted.
    pub reason_code: u16,
    /// Encoded ACK bytes, when an ACK was produced.
    pub ack_bytes: Option<Vec<u8>>,
    /// Decode, gateway, queue, or encode error detail.
    pub error: Option<GatewayCommandPipelineError>,
}

/// Business-agnostic gateway command frame pipeline.
#[derive(Clone, Debug)]
pub struct GatewayCommandPipeline {
    config: GatewayCommandPipelineConfig,
    decoder: BinaryFrameDecoder,
    encoder: BinaryFrameEncoder,
    stats: GatewayCommandPipelineStats,
}

impl GatewayCommandPipeline {
    /// Creates a pipeline.
    pub fn new(config: GatewayCommandPipelineConfig) -> Self {
        Self {
            config,
            decoder: BinaryFrameDecoder,
            encoder: BinaryFrameEncoder,
            stats: GatewayCommandPipelineStats::default(),
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> GatewayCommandPipelineConfig {
        self.config
    }

    /// Returns statistics.
    pub const fn stats(&self) -> GatewayCommandPipelineStats {
        self.stats
    }

    /// Processes one decoded-transport command packet and optionally produces
    /// an encoded command ACK.
    pub fn process(
        &mut self,
        gateway: &mut GatewaySessionTable,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        input: &[u8],
        now: Tick,
        ingress: CommandIngress,
    ) -> GatewayCommandPipelineReport {
        let command_frame = match self.decode_command_frame(input) {
            Ok(command_frame) => command_frame,
            Err(error) => {
                return GatewayCommandPipelineReport {
                    error: Some(error),
                    ..GatewayCommandPipelineReport::default()
                };
            }
        };

        self.process_command_frame(gateway, station_queues, command_frame, now, ingress)
    }

    /// Decodes and admits one command packet, then resolves a deployment route
    /// for external node dispatch without touching local station queues.
    pub fn dispatch(
        &mut self,
        gateway: &mut GatewaySessionTable,
        deployment: &DeploymentRouteTable,
        input: &[u8],
        now: Tick,
    ) -> GatewayCommandPipelineReport {
        let command_frame = match self.decode_command_frame(input) {
            Ok(command_frame) => command_frame,
            Err(error) => {
                return GatewayCommandPipelineReport {
                    error: Some(error),
                    ..GatewayCommandPipelineReport::default()
                };
            }
        };

        self.dispatch_command_frame(gateway, deployment, command_frame, now)
    }

    fn decode_command_frame(
        &mut self,
        input: &[u8],
    ) -> Result<CommandFrame, GatewayCommandPipelineError> {
        let frame = match self.decoder.decode(input) {
            Ok(frame) => frame,
            Err(error) => {
                self.stats.frames_rejected_decode =
                    self.stats.frames_rejected_decode.saturating_add(1);
                return Err(GatewayCommandPipelineError::Decode(error));
            }
        };

        let RuntimeFrame::Command(command_frame) = frame else {
            self.stats.frames_rejected_non_command =
                self.stats.frames_rejected_non_command.saturating_add(1);
            return Err(GatewayCommandPipelineError::NonCommandFrame);
        };
        self.stats.command_frames_decoded = self.stats.command_frames_decoded.saturating_add(1);

        Ok(command_frame)
    }

    fn process_command_frame(
        &mut self,
        gateway: &mut GatewaySessionTable,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        command_frame: CommandFrame,
        now: Tick,
        ingress: CommandIngress,
    ) -> GatewayCommandPipelineReport {
        let client_id = command_frame.client_id;
        let command_id = command_frame.command_id;
        let command = command_frame.into_envelope(now);
        let admission = match gateway.admit_command(&command) {
            Ok(admission) => {
                self.stats.commands_admitted = self.stats.commands_admitted.saturating_add(1);
                admission
            }
            Err(error) => {
                self.stats.commands_rejected_gateway =
                    self.stats.commands_rejected_gateway.saturating_add(1);
                return self.rejected_report(
                    client_id,
                    command_id,
                    None,
                    now,
                    gateway_reject_reason_code(error),
                    GatewayCommandPipelineError::Gateway(error),
                );
            }
        };

        let station_id = admission.route.station_id;
        let Some(queue) = station_queues.get_mut(&station_id) else {
            self.stats.commands_rejected_queue =
                self.stats.commands_rejected_queue.saturating_add(1);
            return self.rejected_report(
                client_id,
                command_id,
                Some(station_id),
                now,
                GATEWAY_COMMAND_ACK_MISSING_QUEUE,
                GatewayCommandPipelineError::MissingQueue(station_id),
            );
        };

        if let Err(error) = queue.push(command, ingress) {
            self.stats.commands_rejected_queue =
                self.stats.commands_rejected_queue.saturating_add(1);
            return self.rejected_report(
                client_id,
                command_id,
                Some(station_id),
                now,
                queue_reject_reason_code(error),
                GatewayCommandPipelineError::Queue(error),
            );
        }

        self.stats.commands_enqueued = self.stats.commands_enqueued.saturating_add(1);
        self.accepted_report(client_id, command_id, station_id, now)
    }

    fn dispatch_command_frame(
        &mut self,
        gateway: &mut GatewaySessionTable,
        deployment: &DeploymentRouteTable,
        command_frame: CommandFrame,
        now: Tick,
    ) -> GatewayCommandPipelineReport {
        let client_id = command_frame.client_id;
        let command_id = command_frame.command_id;
        let command = command_frame.into_envelope(now);
        let admission = match gateway.admit_command(&command) {
            Ok(admission) => {
                self.stats.commands_admitted = self.stats.commands_admitted.saturating_add(1);
                admission
            }
            Err(error) => {
                self.stats.commands_rejected_gateway =
                    self.stats.commands_rejected_gateway.saturating_add(1);
                return self.rejected_report(
                    client_id,
                    command_id,
                    None,
                    now,
                    gateway_reject_reason_code(error),
                    GatewayCommandPipelineError::Gateway(error),
                );
            }
        };

        let delivery = match deployment.resolve_gateway_route(admission.route) {
            Ok(delivery) => {
                self.stats.commands_routed_deployment =
                    self.stats.commands_routed_deployment.saturating_add(1);
                delivery
            }
            Err(error) => {
                self.stats.commands_rejected_deployment =
                    self.stats.commands_rejected_deployment.saturating_add(1);
                return self.rejected_report(
                    client_id,
                    command_id,
                    Some(admission.route.station_id),
                    now,
                    GATEWAY_COMMAND_ACK_DEPLOYMENT_REJECTED,
                    GatewayCommandPipelineError::Deployment(error),
                );
            }
        };

        self.dispatch_report(command, delivery, now)
    }

    fn accepted_report(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        station_id: StationId,
        now: Tick,
    ) -> GatewayCommandPipelineReport {
        let ack = CommandAckFrame {
            client_id,
            command_id,
            server_tick: now,
            accepted: true,
            reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
        };
        match self.encode_ack(&ack) {
            Ok(ack_bytes) => GatewayCommandPipelineReport {
                client_id: Some(client_id),
                command_id: Some(command_id),
                station_id: Some(station_id),
                node_id: None,
                delivery: None,
                command: None,
                accepted: true,
                reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
                ack_bytes: Some(ack_bytes),
                error: None,
            },
            Err(error) => GatewayCommandPipelineReport {
                client_id: Some(client_id),
                command_id: Some(command_id),
                station_id: Some(station_id),
                node_id: None,
                delivery: None,
                command: None,
                accepted: false,
                reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
                ack_bytes: None,
                error: Some(GatewayCommandPipelineError::Encode(error)),
            },
        }
    }

    fn dispatch_report(
        &mut self,
        command: CommandEnvelope,
        delivery: GatewayDeliveryRoute,
        now: Tick,
    ) -> GatewayCommandPipelineReport {
        let ack = CommandAckFrame {
            client_id: command.client_id,
            command_id: command.id,
            server_tick: now,
            accepted: true,
            reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
        };
        match self.encode_ack(&ack) {
            Ok(ack_bytes) => GatewayCommandPipelineReport {
                client_id: Some(command.client_id),
                command_id: Some(command.id),
                station_id: Some(delivery.station_id),
                node_id: Some(delivery.node_id),
                delivery: Some(delivery),
                command: Some(command),
                accepted: true,
                reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
                ack_bytes: Some(ack_bytes),
                error: None,
            },
            Err(error) => GatewayCommandPipelineReport {
                client_id: Some(command.client_id),
                command_id: Some(command.id),
                station_id: Some(delivery.station_id),
                node_id: Some(delivery.node_id),
                delivery: Some(delivery),
                command: None,
                accepted: false,
                reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
                ack_bytes: None,
                error: Some(GatewayCommandPipelineError::Encode(error)),
            },
        }
    }

    fn rejected_report(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        station_id: Option<StationId>,
        now: Tick,
        reason_code: u16,
        error: GatewayCommandPipelineError,
    ) -> GatewayCommandPipelineReport {
        let ack_bytes = if self.config.ack_rejections {
            let ack = CommandAckFrame {
                client_id,
                command_id,
                server_tick: now,
                accepted: false,
                reason_code,
            };
            match self.encode_ack(&ack) {
                Ok(bytes) => Some(bytes),
                Err(encode_error) => {
                    return GatewayCommandPipelineReport {
                        client_id: Some(client_id),
                        command_id: Some(command_id),
                        station_id,
                        node_id: None,
                        delivery: None,
                        command: None,
                        accepted: false,
                        reason_code,
                        ack_bytes: None,
                        error: Some(GatewayCommandPipelineError::Encode(encode_error)),
                    };
                }
            }
        } else {
            None
        };

        GatewayCommandPipelineReport {
            client_id: Some(client_id),
            command_id: Some(command_id),
            station_id,
            node_id: None,
            delivery: None,
            command: None,
            accepted: false,
            reason_code,
            ack_bytes,
            error: Some(error),
        }
    }

    fn encode_ack(&mut self, ack: &CommandAckFrame) -> Result<Vec<u8>, BinaryEncodeError> {
        let mut out = Vec::new();
        self.encoder.encode_command_ack(ack, &mut out)?;
        self.stats.acks_encoded = self.stats.acks_encoded.saturating_add(1);
        Ok(out)
    }
}

impl Default for GatewayCommandPipeline {
    fn default() -> Self {
        Self::new(GatewayCommandPipelineConfig::default())
    }
}

const fn gateway_reject_reason_code(error: GatewayError) -> u16 {
    match error {
        GatewayError::ReplayOrStale { .. } => GATEWAY_COMMAND_ACK_REPLAY_OR_STALE,
        GatewayError::RateLimited { .. } => GATEWAY_COMMAND_ACK_RATE_LIMITED,
        GatewayError::MissingSession(_)
        | GatewayError::SessionDisconnected { .. }
        | GatewayError::BadGeneration { .. }
        | GatewayError::CapacityFull { .. } => GATEWAY_COMMAND_ACK_GATEWAY_REJECTED,
    }
}

const fn queue_reject_reason_code(error: CommandQueueError) -> u16 {
    match error {
        CommandQueueError::QueueFull(_) => GATEWAY_COMMAND_ACK_QUEUE_FULL,
        CommandQueueError::RejectedByBarrier(_) => GATEWAY_COMMAND_ACK_BARRIER_REJECTED,
    }
}

/// Gateway-side client command transport bridge statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GatewayClientTransportStats {
    /// Client packets consumed from transport.
    pub packets_received: usize,
    /// Client packet bytes consumed from transport.
    pub bytes_received: usize,
    /// Command frames decoded from client packets.
    pub command_frames_received: usize,
    /// Packets rejected because transport source and command client differed.
    pub source_mismatches: usize,
    /// Commands accepted by the gateway command pipeline.
    pub commands_accepted: usize,
    /// Commands rejected by the gateway command pipeline.
    pub commands_rejected: usize,
    /// Command ACK frames submitted to client transport.
    pub acks_sent: usize,
    /// Command ACK bytes submitted to client transport.
    pub ack_bytes_sent: usize,
}

/// Result of pumping gateway-side client command packets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GatewayClientTransportPump {
    /// Client packets consumed from transport.
    pub packets_received: usize,
    /// Client packet bytes consumed from transport.
    pub bytes_received: usize,
    /// Gateway pipeline reports produced from accepted or rejected commands.
    pub reports: Vec<GatewayCommandPipelineReport>,
    /// Command ACK frames submitted to client transport.
    pub acks_sent: usize,
    /// Command ACK bytes submitted to client transport.
    pub ack_bytes_sent: usize,
}

impl GatewayClientTransportPump {
    /// Returns processed command count.
    pub fn commands_processed(&self) -> usize {
        self.reports.len()
    }

    /// Returns accepted command count.
    pub fn commands_accepted(&self) -> usize {
        self.reports.iter().filter(|report| report.accepted).count()
    }

    /// Returns rejected command count.
    pub fn commands_rejected(&self) -> usize {
        self.reports
            .iter()
            .filter(|report| !report.accepted)
            .count()
    }
}

/// Compact result of pumping gateway commands without retaining per-command reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GatewayClientTransportSummary {
    /// Client packets consumed from transport.
    pub packets_received: usize,
    /// Client packet bytes consumed from transport.
    pub bytes_received: usize,
    /// Commands accepted by the gateway pipeline.
    pub commands_accepted: usize,
    /// Commands rejected by the gateway pipeline.
    pub commands_rejected: usize,
    /// Command ACK frames submitted to client transport.
    pub acks_sent: usize,
    /// Command ACK bytes submitted to client transport.
    pub ack_bytes_sent: usize,
}

/// Error produced while pumping gateway-side client command packets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GatewayClientTransportError<E> {
    /// Underlying client transport failed while receiving or sending acknowledgements.
    Transport(E),
    /// Wire decoding failed.
    Decode(BinaryDecodeError),
    /// Packet decoded as a non-command frame.
    NonCommandFrame,
    /// Transport source client and command frame client disagreed.
    SourceMismatch {
        /// Client identified by the transport.
        packet_client_id: ClientId,
        /// Client encoded inside the command frame.
        frame_client_id: ClientId,
    },
}

impl<E: core::fmt::Display> core::fmt::Display for GatewayClientTransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::NonCommandFrame => f.write_str("gateway client transport expected command frame"),
            Self::SourceMismatch {
                packet_client_id,
                frame_client_id,
            } => write!(
                f,
                "gateway client source mismatch: packet {}, frame {}",
                packet_client_id.get(),
                frame_client_id.get()
            ),
        }
    }
}

impl<E> std::error::Error for GatewayClientTransportError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::NonCommandFrame | Self::SourceMismatch { .. } => None,
        }
    }
}

/// Low-level bridge from client packet transport into the gateway command pipeline.
#[derive(Clone, Debug, Default)]
pub struct GatewayClientTransportBridge {
    stats: GatewayClientTransportStats,
}

impl GatewayClientTransportBridge {
    /// Creates a gateway client transport bridge.
    pub const fn new() -> Self {
        Self {
            stats: GatewayClientTransportStats {
                packets_received: 0,
                bytes_received: 0,
                command_frames_received: 0,
                source_mismatches: 0,
                commands_accepted: 0,
                commands_rejected: 0,
                acks_sent: 0,
                ack_bytes_sent: 0,
            },
        }
    }

    /// Returns accumulated statistics.
    pub const fn stats(&self) -> GatewayClientTransportStats {
        self.stats
    }

    /// Pumps up to `max_packets` client command packets into station queues and
    /// sends produced ACKs back through the same bounded client transport.
    #[allow(clippy::too_many_arguments)]
    pub fn pump_ingress<T, E>(
        &mut self,
        transport: &mut T,
        pipeline: &mut GatewayCommandPipeline,
        gateway: &mut GatewaySessionTable,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        now: Tick,
        ingress: CommandIngress,
        max_packets: usize,
    ) -> Result<GatewayClientTransportPump, GatewayClientTransportError<E>>
    where
        T: TransportReceiver<Error = E> + TransportSink<Error = E>,
    {
        let mut pump = GatewayClientTransportPump::default();
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv()
                .map_err(GatewayClientTransportError::Transport)?
            else {
                break;
            };
            let (ack_client_id, packet_bytes, report) = self.process_ingress_packet::<E>(
                pipeline,
                gateway,
                station_queues,
                &packet,
                now,
                ingress,
            )?;
            pump.packets_received = pump.packets_received.saturating_add(1);
            pump.bytes_received = pump.bytes_received.saturating_add(packet_bytes);

            if let Some(bytes) = &report.ack_bytes {
                let ack_len = bytes.len();
                transport
                    .send(OutboundPacket {
                        client_id: ack_client_id,
                        bytes: bytes.clone(),
                    })
                    .map_err(GatewayClientTransportError::Transport)?;
                self.stats.acks_sent = self.stats.acks_sent.saturating_add(1);
                self.stats.ack_bytes_sent = self.stats.ack_bytes_sent.saturating_add(ack_len);
                pump.acks_sent = pump.acks_sent.saturating_add(1);
                pump.ack_bytes_sent = pump.ack_bytes_sent.saturating_add(ack_len);
            }
            pump.reports.push(report);
        }
        Ok(pump)
    }

    /// Pumps commands and moves ACK buffers directly into transport without
    /// retaining per-command pipeline reports.
    ///
    /// Use [`Self::pump_ingress`] when the caller needs detailed error reports
    /// or encoded ACK bytes after sending.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn pump_ingress_compact<T, E>(
        &mut self,
        transport: &mut T,
        pipeline: &mut GatewayCommandPipeline,
        gateway: &mut GatewaySessionTable,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        now: Tick,
        ingress: CommandIngress,
        max_packets: usize,
    ) -> Result<GatewayClientTransportSummary, GatewayClientTransportError<E>>
    where
        T: TransportReceiver<Error = E> + TransportSink<Error = E>,
    {
        let mut summary = GatewayClientTransportSummary::default();
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv()
                .map_err(GatewayClientTransportError::Transport)?
            else {
                break;
            };
            let (ack_client_id, packet_bytes, mut report) = self.process_ingress_packet::<E>(
                pipeline,
                gateway,
                station_queues,
                &packet,
                now,
                ingress,
            )?;
            summary.packets_received = summary.packets_received.saturating_add(1);
            summary.bytes_received = summary.bytes_received.saturating_add(packet_bytes);
            if report.accepted {
                summary.commands_accepted = summary.commands_accepted.saturating_add(1);
            } else {
                summary.commands_rejected = summary.commands_rejected.saturating_add(1);
            }

            if let Some(bytes) = report.ack_bytes.take() {
                let ack_len = bytes.len();
                transport
                    .send(OutboundPacket {
                        client_id: ack_client_id,
                        bytes,
                    })
                    .map_err(GatewayClientTransportError::Transport)?;
                self.stats.acks_sent = self.stats.acks_sent.saturating_add(1);
                self.stats.ack_bytes_sent = self.stats.ack_bytes_sent.saturating_add(ack_len);
                summary.acks_sent = summary.acks_sent.saturating_add(1);
                summary.ack_bytes_sent = summary.ack_bytes_sent.saturating_add(ack_len);
            }
        }
        Ok(summary)
    }

    #[allow(clippy::too_many_arguments)]
    fn process_ingress_packet<E>(
        &mut self,
        pipeline: &mut GatewayCommandPipeline,
        gateway: &mut GatewaySessionTable,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        packet: &InboundPacket,
        now: Tick,
        ingress: CommandIngress,
    ) -> Result<(ClientId, usize, GatewayCommandPipelineReport), GatewayClientTransportError<E>>
    {
        let packet_bytes = packet.bytes.len();
        self.stats.packets_received = self.stats.packets_received.saturating_add(1);
        self.stats.bytes_received = self.stats.bytes_received.saturating_add(packet_bytes);
        let command_frame = match pipeline.decode_command_frame(&packet.bytes) {
            Ok(command_frame) => command_frame,
            Err(GatewayCommandPipelineError::Decode(error)) => {
                return Err(GatewayClientTransportError::Decode(error));
            }
            Err(GatewayCommandPipelineError::NonCommandFrame) => {
                return Err(GatewayClientTransportError::NonCommandFrame);
            }
            Err(error) => {
                unreachable!(
                    "decode_command_frame only returns decode/non-command errors: {error}"
                );
            }
        };
        self.stats.command_frames_received = self.stats.command_frames_received.saturating_add(1);
        if let Some(packet_client_id) = packet.client_id
            && packet_client_id != command_frame.client_id
        {
            self.stats.source_mismatches = self.stats.source_mismatches.saturating_add(1);
            return Err(GatewayClientTransportError::SourceMismatch {
                packet_client_id,
                frame_client_id: command_frame.client_id,
            });
        }
        let ack_client_id = command_frame.client_id;
        let report =
            pipeline.process_command_frame(gateway, station_queues, command_frame, now, ingress);
        if report.accepted {
            self.stats.commands_accepted = self.stats.commands_accepted.saturating_add(1);
        } else {
            self.stats.commands_rejected = self.stats.commands_rejected.saturating_add(1);
        }
        Ok((ack_client_id, packet_bytes, report))
    }
}

// Linear scans win for small registries; larger sets amortize an ID index.
const STATION_LOOKUP_INDEX_THRESHOLD: usize = 64;

/// Small ordered in-process Station collection for simulations and embedders.
#[derive(Clone, Debug, Default)]
pub struct StationSet {
    stations: Vec<Station>,
    positions: HashMap<StationId, usize>,
}

impl StationSet {
    /// Creates an empty collection with capacity for `capacity` Stations.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            stations: Vec::with_capacity(capacity),
            positions: if capacity >= STATION_LOOKUP_INDEX_THRESHOLD {
                HashMap::with_capacity(capacity)
            } else {
                HashMap::new()
            },
        }
    }

    /// Reserves capacity for at least `additional` more Stations and lookup entries.
    pub fn reserve(&mut self, additional: usize) {
        self.stations.reserve(additional);
        if !self.positions.is_empty() {
            self.positions.reserve(additional);
        } else if self.stations.len().saturating_add(additional) >= STATION_LOOKUP_INDEX_THRESHOLD {
            self.positions.reserve(self.stations.len() + additional);
        }
    }

    /// Adds a station to the collection.
    pub fn push(&mut self, station: Station) {
        let station_id = station.config().station_id;
        self.activate_lookup_for(self.stations.len().saturating_add(1));
        if !self.positions.is_empty() {
            self.positions
                .entry(station_id)
                .or_insert(self.stations.len());
        }
        self.stations.push(station);
    }

    /// Removes and returns the first Station with `station_id`.
    ///
    /// Remaining Stations retain their iteration order. Lookup storage remains
    /// allocated for reuse when the indexed path is active.
    pub fn remove(&mut self, station_id: StationId) -> Option<Station> {
        let position = self.position(station_id)?;
        let station = self.stations.remove(position);
        self.rebuild_positions();
        Some(station)
    }

    /// Gets a station by id.
    pub fn get(&self, station_id: StationId) -> Option<&Station> {
        self.position(station_id)
            .and_then(|index| self.stations.get(index))
    }

    /// Gets a mutable station by id.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut Station> {
        let index = self.position(station_id)?;
        self.stations.get_mut(index)
    }

    /// Gets two distinct mutable stations by id.
    pub fn get_pair_mut(
        &mut self,
        left_id: StationId,
        right_id: StationId,
    ) -> Option<(&mut Station, &mut Station)> {
        if left_id == right_id {
            return None;
        }

        let left_index = self.position(left_id)?;
        let right_index = self.position(right_id)?;

        if left_index < right_index {
            let (left, right) = self.stations.split_at_mut(right_index);
            Some((&mut left[left_index], &mut right[0]))
        } else {
            let (left, right) = self.stations.split_at_mut(left_index);
            Some((&mut right[0], &mut left[right_index]))
        }
    }

    /// Iterates over stations.
    pub fn iter(&self) -> impl Iterator<Item = &Station> {
        self.stations.iter()
    }

    /// Iterates mutably over stations.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Station> {
        self.stations.iter_mut()
    }

    /// Returns station ids matching a barrier scope.
    pub fn station_ids_in_scope(&self, scope: BarrierScope) -> Vec<StationId> {
        self.stations
            .iter()
            .filter(|station| match scope {
                BarrierScope::Instance(instance_id) => station.config().instance_id == instance_id,
                BarrierScope::Station(station_id) => station.config().station_id == station_id,
            })
            .map(|station| station.config().station_id)
            .collect()
    }

    /// Number of stations.
    pub fn len(&self) -> usize {
        self.stations.len()
    }

    /// Station slots retained without growing the ordered collection.
    pub fn station_capacity(&self) -> usize {
        self.stations.capacity()
    }

    /// Lookup entries retained without rehashing the Station index.
    pub fn lookup_capacity(&self) -> usize {
        self.positions.capacity()
    }

    /// Returns whether Station lookup currently uses the indexed path.
    pub fn lookup_index_active(&self) -> bool {
        !self.positions.is_empty()
    }

    /// Returns whether no stations are registered.
    pub fn is_empty(&self) -> bool {
        self.stations.is_empty()
    }

    fn position(&self, station_id: StationId) -> Option<usize> {
        if self.positions.is_empty() {
            self.stations
                .iter()
                .position(|station| station.config().station_id == station_id)
        } else {
            self.positions.get(&station_id).copied()
        }
    }

    fn activate_lookup_for(&mut self, new_len: usize) {
        if new_len < STATION_LOOKUP_INDEX_THRESHOLD || !self.positions.is_empty() {
            return;
        }
        self.positions.reserve(new_len);
        for (index, station) in self.stations.iter().enumerate() {
            self.positions
                .entry(station.config().station_id)
                .or_insert(index);
        }
    }

    fn rebuild_positions(&mut self) {
        if self.positions.is_empty() {
            return;
        }
        self.positions.clear();
        for (index, station) in self.stations.iter().enumerate() {
            self.positions
                .entry(station.config().station_id)
                .or_insert(index);
        }
    }
}

/// Station-local spatial indexes keyed by station id.
#[derive(Clone, Debug, Default)]
pub struct StationIndexSet {
    indexes: Vec<(StationId, CellIndex)>,
    positions: HashMap<StationId, usize>,
}

impl StationIndexSet {
    /// Creates an empty collection with capacity for `capacity` Station indexes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            indexes: Vec::with_capacity(capacity),
            positions: if capacity >= STATION_LOOKUP_INDEX_THRESHOLD {
                HashMap::with_capacity(capacity)
            } else {
                HashMap::new()
            },
        }
    }

    /// Reserves capacity for at least `additional` more indexes and lookup entries.
    pub fn reserve(&mut self, additional: usize) {
        self.indexes.reserve(additional);
        if !self.positions.is_empty() {
            self.positions.reserve(additional);
        } else if self.indexes.len().saturating_add(additional) >= STATION_LOOKUP_INDEX_THRESHOLD {
            self.positions.reserve(self.indexes.len() + additional);
        }
    }

    /// Adds or replaces one station index.
    pub fn insert(&mut self, station_id: StationId, index: CellIndex) {
        if let Some(position) = self.position(station_id) {
            self.indexes[position].1 = index;
        } else {
            self.activate_lookup_for(self.indexes.len().saturating_add(1));
            if !self.positions.is_empty() {
                self.positions.insert(station_id, self.indexes.len());
            }
            self.indexes.push((station_id, index));
        }
    }

    /// Removes and returns one Station-local spatial index.
    ///
    /// Remaining indexes retain their iteration order and indexed lookup
    /// storage remains allocated for later registrations.
    pub fn remove(&mut self, station_id: StationId) -> Option<CellIndex> {
        let position = self.position(station_id)?;
        let (_, index) = self.indexes.remove(position);
        self.rebuild_positions();
        Some(index)
    }

    /// Gets one station index.
    pub fn get(&self, station_id: StationId) -> Option<&CellIndex> {
        self.position(station_id)
            .and_then(|position| self.indexes.get(position))
            .map(|(_, index)| index)
    }

    /// Gets one mutable station index.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut CellIndex> {
        let position = self.position(station_id)?;
        self.indexes.get_mut(position).map(|(_, index)| index)
    }

    /// Gets two distinct mutable station indexes.
    pub fn get_pair_mut(
        &mut self,
        left_id: StationId,
        right_id: StationId,
    ) -> Option<(&mut CellIndex, &mut CellIndex)> {
        if left_id == right_id {
            return None;
        }

        let left_index = self.position(left_id)?;
        let right_index = self.position(right_id)?;

        if left_index < right_index {
            let (left, right) = self.indexes.split_at_mut(right_index);
            Some((&mut left[left_index].1, &mut right[0].1))
        } else {
            let (left, right) = self.indexes.split_at_mut(left_index);
            Some((&mut right[0].1, &mut left[right_index].1))
        }
    }

    /// Number of indexes.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Index slots retained without growing the ordered collection.
    pub fn index_capacity(&self) -> usize {
        self.indexes.capacity()
    }

    /// Lookup entries retained without rehashing the Station index.
    pub fn lookup_capacity(&self) -> usize {
        self.positions.capacity()
    }

    /// Returns whether Station-index lookup currently uses the indexed path.
    pub fn lookup_index_active(&self) -> bool {
        !self.positions.is_empty()
    }

    /// Iterates over registered indexes.
    pub fn iter(&self) -> impl Iterator<Item = (StationId, &CellIndex)> {
        self.indexes
            .iter()
            .map(|(station_id, index)| (*station_id, index))
    }

    /// Returns whether no indexes are registered.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    fn position(&self, station_id: StationId) -> Option<usize> {
        if self.positions.is_empty() {
            self.indexes.iter().position(|(id, _)| *id == station_id)
        } else {
            self.positions.get(&station_id).copied()
        }
    }

    fn activate_lookup_for(&mut self, new_len: usize) {
        if new_len < STATION_LOOKUP_INDEX_THRESHOLD || !self.positions.is_empty() {
            return;
        }
        self.positions.reserve(new_len);
        for (index, (station_id, _)) in self.indexes.iter().enumerate() {
            self.positions.entry(*station_id).or_insert(index);
        }
    }

    fn rebuild_positions(&mut self) {
        if self.positions.is_empty() {
            return;
        }
        self.positions.clear();
        for (index, (station_id, _)) in self.indexes.iter().enumerate() {
            self.positions.entry(*station_id).or_insert(index);
        }
    }
}

/// Lightweight coefficients used to derive hotspot/scheduler load samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationLoadSamplerConfig {
    /// Estimated bytes contributed by one stored entity in the measurement window.
    pub estimated_bytes_per_entity: usize,
    /// Estimated bytes contributed by one subscriber routed to a station.
    pub estimated_bytes_per_subscriber: usize,
    /// Estimated bytes contributed by one queued station event.
    pub estimated_bytes_per_event: usize,
    /// Runtime cost units assigned to one authoritative entity.
    pub tick_cost_per_owned_entity: u64,
    /// Runtime cost units assigned to one read-only ghost entity.
    pub tick_cost_per_ghost_entity: u64,
    /// Runtime cost units assigned to one occupied spatial cell.
    pub tick_cost_per_occupied_cell: u64,
    /// Runtime cost units assigned to one queued station event.
    pub tick_cost_per_queued_event: u64,
}

impl Default for StationLoadSamplerConfig {
    fn default() -> Self {
        Self {
            estimated_bytes_per_entity: 48,
            estimated_bytes_per_subscriber: 16,
            estimated_bytes_per_event: 32,
            tick_cost_per_owned_entity: 2,
            tick_cost_per_ghost_entity: 1,
            tick_cost_per_occupied_cell: 1,
            tick_cost_per_queued_event: 1,
        }
    }
}

/// Runtime helper that derives `StationLoadSample` from existing low-level state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationLoadSampler {
    config: StationLoadSamplerConfig,
}

/// Caller-owned reusable storage for periodic station load sampling.
#[derive(Clone, Debug, Default)]
pub struct StationLoadSamplerScratch {
    subscribers_by_station: HashMap<StationId, usize>,
    occupancy: Vec<CellOccupancy>,
    samples: Vec<StationLoadSample>,
}

impl StationLoadSamplerScratch {
    /// Creates empty sampling storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Hash-table capacity retained for subscriber aggregation.
    pub fn retained_subscriber_capacity(&self) -> usize {
        self.subscribers_by_station.capacity()
    }

    /// Temporary occupancy capacity retained across station scans.
    pub fn retained_occupancy_capacity(&self) -> usize {
        self.occupancy.capacity()
    }

    /// Station output slots retained across sampling passes.
    pub fn retained_sample_slots(&self) -> usize {
        self.samples.len()
    }

    /// Total cell output capacity retained across station slots.
    pub fn retained_cell_capacity(&self) -> usize {
        self.samples
            .iter()
            .map(|sample| sample.cells.capacity())
            .sum()
    }
}

impl StationLoadSampler {
    /// Creates a station load sampler.
    pub const fn new(config: StationLoadSamplerConfig) -> Self {
        Self { config }
    }

    /// Returns the sampler configuration.
    pub const fn config(&self) -> StationLoadSamplerConfig {
        self.config
    }

    /// Samples one station from its storage, optional index, queued event count,
    /// and caller-provided subscriber estimate.
    pub fn sample_station(
        &self,
        station: &Station,
        index: Option<&CellIndex>,
        queued_events: usize,
        subscribers: usize,
    ) -> StationLoadSample {
        let mut sample = StationLoadSample::default();
        let mut occupancy = Vec::new();
        self.sample_station_into(
            station,
            index,
            queued_events,
            subscribers,
            &mut occupancy,
            &mut sample,
        );
        sample
    }

    /// Samples every station in deterministic station-set order.
    ///
    /// `subscriber_counts` is explicit integration input: `SectorSync` can use it
    /// for load decisions, but does not own gateway/client/session business state.
    /// Counts with the same station id are aggregated with saturating arithmetic.
    /// Because the event router and subscriber input are station-scoped, their
    /// pressure stays on the station sample instead of being invented per cell.
    pub fn sample_all(
        &self,
        stations: &StationSet,
        indexes: &StationIndexSet,
        router: &EventRouter,
        subscriber_counts: &[(StationId, usize)],
    ) -> Vec<StationLoadSample> {
        let subscribers_by_station = station_count_map(subscriber_counts);
        stations
            .iter()
            .map(|station| {
                let station_id = station.config().station_id;
                self.sample_station(
                    station,
                    indexes.get(station_id),
                    router.queued_len(station_id).unwrap_or(0),
                    subscribers_by_station
                        .get(&station_id)
                        .copied()
                        .unwrap_or(0),
                )
            })
            .collect()
    }

    /// Samples every station into caller-owned storage and returns its active prefix.
    ///
    /// Station order and aggregation semantics match [`Self::sample_all`]. Reusing
    /// the same scratch retains subscriber, occupancy, station, and per-cell storage.
    pub fn sample_all_into<'a>(
        &self,
        stations: &StationSet,
        indexes: &StationIndexSet,
        router: &EventRouter,
        subscriber_counts: &[(StationId, usize)],
        scratch: &'a mut StationLoadSamplerScratch,
    ) -> &'a [StationLoadSample] {
        scratch.subscribers_by_station.clear();
        for (station_id, count) in subscriber_counts {
            let entry = scratch
                .subscribers_by_station
                .entry(*station_id)
                .or_insert(0);
            *entry = entry.saturating_add(*count);
        }

        let station_count = stations.len();
        if scratch.samples.len() < station_count {
            scratch
                .samples
                .resize_with(station_count, StationLoadSample::default);
        }
        for (station, sample) in stations
            .iter()
            .zip(scratch.samples[..station_count].iter_mut())
        {
            let station_id = station.config().station_id;
            self.sample_station_into(
                station,
                indexes.get(station_id),
                router.queued_len(station_id).unwrap_or(0),
                scratch
                    .subscribers_by_station
                    .get(&station_id)
                    .copied()
                    .unwrap_or(0),
                &mut scratch.occupancy,
                sample,
            );
        }
        &scratch.samples[..station_count]
    }

    fn sample_station_into(
        &self,
        station: &Station,
        index: Option<&CellIndex>,
        queued_events: usize,
        subscribers: usize,
        occupancy: &mut Vec<CellOccupancy>,
        sample: &mut StationLoadSample,
    ) {
        let (owned_entities, ghost_entities) = count_station_roles(station);
        sample.cells.clear();
        if let Some(index) = index {
            index.cell_occupancy_into(occupancy);
            for occupancy in occupancy.iter() {
                let mut cell_owned_entities = 0usize;
                let mut cell_ghost_entities = 0usize;
                for handle in index.handles_in_cell_slice(occupancy.cell) {
                    if let Some(record) = station.get(*handle) {
                        if record.is_owned() {
                            cell_owned_entities = cell_owned_entities.saturating_add(1);
                        } else {
                            cell_ghost_entities = cell_ghost_entities.saturating_add(1);
                        }
                    }
                }
                let entities = cell_owned_entities.saturating_add(cell_ghost_entities);
                sample.cells.push(CellLoadSample {
                    cell: occupancy.cell,
                    owned_entities: cell_owned_entities,
                    ghost_entities: cell_ghost_entities,
                    subscribers: 0,
                    estimated_updates: entities,
                    estimated_bytes: entities
                        .saturating_mul(self.config.estimated_bytes_per_entity),
                    tick_cost_units: self.estimate_tick_cost(
                        cell_owned_entities,
                        cell_ghost_entities,
                        1,
                        0,
                    ),
                    event_pressure: 0,
                });
            }
        } else {
            occupancy.clear();
        }
        sample.station_id = station.config().station_id;
        sample.owned_entities = owned_entities;
        sample.ghost_entities = ghost_entities;
        sample.subscribers = subscribers;
        sample.queued_events = queued_events;
        sample.estimated_bytes =
            self.estimate_station_bytes(owned_entities, ghost_entities, subscribers, queued_events);
        sample.tick_cost_units = self.estimate_tick_cost(
            owned_entities,
            ghost_entities,
            sample.cells.len(),
            queued_events,
        );
    }

    fn estimate_station_bytes(
        &self,
        owned_entities: usize,
        ghost_entities: usize,
        subscribers: usize,
        queued_events: usize,
    ) -> usize {
        owned_entities
            .saturating_add(ghost_entities)
            .saturating_mul(self.config.estimated_bytes_per_entity)
            .saturating_add(subscribers.saturating_mul(self.config.estimated_bytes_per_subscriber))
            .saturating_add(queued_events.saturating_mul(self.config.estimated_bytes_per_event))
    }

    fn estimate_tick_cost(
        &self,
        owned_entities: usize,
        ghost_entities: usize,
        occupied_cells: usize,
        queued_events: usize,
    ) -> u64 {
        (owned_entities as u64)
            .saturating_mul(self.config.tick_cost_per_owned_entity)
            .saturating_add(
                (ghost_entities as u64).saturating_mul(self.config.tick_cost_per_ghost_entity),
            )
            .saturating_add(
                (occupied_cells as u64).saturating_mul(self.config.tick_cost_per_occupied_cell),
            )
            .saturating_add(
                (queued_events as u64).saturating_mul(self.config.tick_cost_per_queued_event),
            )
    }
}

impl Default for StationLoadSampler {
    fn default() -> Self {
        Self::new(StationLoadSamplerConfig::default())
    }
}

fn count_station_roles(station: &Station) -> (usize, usize) {
    let mut owned_entities = 0usize;
    let mut ghost_entities = 0usize;
    for record in station.iter() {
        if record.is_owned() {
            owned_entities = owned_entities.saturating_add(1);
        } else {
            ghost_entities = ghost_entities.saturating_add(1);
        }
    }
    (owned_entities, ghost_entities)
}

fn station_count_map(counts: &[(StationId, usize)]) -> BTreeMap<StationId, usize> {
    let mut map = BTreeMap::new();
    for (station_id, count) in counts {
        let entry = map.entry(*station_id).or_insert(0usize);
        *entry = entry.saturating_add(*count);
    }
    map
}

/// Result of an in-process entity owner migration.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityMigrationReport {
    /// Transfer payload used for the migration.
    pub transfer: HandoffTransfer,
    /// Source-side ghost handle after commit.
    pub source_ghost: EntityHandle,
    /// Target-side authoritative handle after commit.
    pub target_owner: EntityHandle,
}

/// Entity migration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityMigrationError {
    /// Source and target station ids must differ.
    SameSourceAndTarget(StationId),
    /// Source station was not found.
    MissingSource(StationId),
    /// Target station was not found.
    MissingTarget(StationId),
    /// Station-level operation failed.
    Station(StationError),
}

impl core::fmt::Display for EntityMigrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SameSourceAndTarget(id) => {
                write!(f, "source and target station are both {}", id.get())
            }
            Self::MissingSource(id) => write!(f, "source station {} is missing", id.get()),
            Self::MissingTarget(id) => write!(f, "target station {} is missing", id.get()),
            Self::Station(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EntityMigrationError {}

impl From<StationError> for EntityMigrationError {
    fn from(value: StationError) -> Self {
        Self::Station(value)
    }
}

/// Runtime helper for in-process station-to-station owner migration.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntityMigrationExecutor;

impl EntityMigrationExecutor {
    /// Migrates one authoritative entity from source station to target station.
    pub fn migrate_entity(
        stations: &mut StationSet,
        entity_id: EntityId,
        source_station: StationId,
        target_station: StationId,
        ghost_ttl_ticks: u64,
    ) -> Result<EntityMigrationReport, EntityMigrationError> {
        if source_station == target_station {
            return Err(EntityMigrationError::SameSourceAndTarget(source_station));
        }

        if stations.get(source_station).is_none() {
            return Err(EntityMigrationError::MissingSource(source_station));
        }
        if stations.get(target_station).is_none() {
            return Err(EntityMigrationError::MissingTarget(target_station));
        }

        let (source, target) = stations
            .get_pair_mut(source_station, target_station)
            .expect("stations were checked above");
        let target_epoch = next_target_epoch(target);
        let source_ghost_expires_at =
            Tick::new(source.tick().get().saturating_add(ghost_ttl_ticks));
        let transfer = source.prepare_outgoing_handoff(
            entity_id,
            target_station,
            target_epoch,
            source_ghost_expires_at,
        )?;
        target.prewarm_handoff_ghost(&transfer)?;
        let target_owner = target.commit_incoming_handoff(transfer.clone())?;
        let source_ghost = source.commit_outgoing_handoff(&transfer)?;

        Ok(EntityMigrationReport {
            transfer,
            source_ghost,
            target_owner,
        })
    }
}

fn next_target_epoch(station: &mut Station) -> OwnerEpoch {
    station.next_owner_epoch()
}

/// Dynamic ownership table for fixed 3D cells.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellOwnershipTable {
    owners: BTreeMap<CellCoord3, StationId>,
}

impl CellOwnershipTable {
    /// Assigns one cell to a station and returns the previous owner.
    pub fn assign(&mut self, cell: CellCoord3, station_id: StationId) -> Option<StationId> {
        self.owners.insert(cell, station_id)
    }

    /// Returns the current owner for one cell.
    pub fn owner_of(&self, cell: CellCoord3) -> Option<StationId> {
        self.owners.get(&cell).copied()
    }

    /// Applies a split proposal by assigning all proposed cells to `target_station`.
    pub fn apply_split(
        &mut self,
        proposal: &SplitProposal,
        target_station: StationId,
    ) -> CellOwnershipUpdate {
        let mut update = CellOwnershipUpdate::default();
        self.apply_split_into(proposal, target_station, &mut update);
        update
    }

    /// Applies a split into caller-owned reusable update storage.
    pub fn apply_split_into(
        &mut self,
        proposal: &SplitProposal,
        target_station: StationId,
        update: &mut CellOwnershipUpdate,
    ) {
        update.source_station = proposal.source_station;
        update.target_station = target_station;
        update.moved_cells.clear();
        for cell in &proposal.cells_to_move {
            let previous = self.assign(*cell, target_station);
            if previous != Some(target_station) {
                update.moved_cells.push(*cell);
            }
        }
    }

    /// Number of explicitly assigned cells.
    pub fn len(&self) -> usize {
        self.owners.len()
    }

    /// Returns whether no cells are explicitly assigned.
    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}

/// Result of applying cell ownership changes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CellOwnershipUpdate {
    /// Previous/source station.
    pub source_station: StationId,
    /// New/target station.
    pub target_station: StationId,
    /// Cells whose owner changed.
    pub moved_cells: Vec<CellCoord3>,
}

/// Result of migrating entities indexed by moved cells.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CellMigrationReport {
    /// Source station.
    pub source_station: StationId,
    /// Target station.
    pub target_station: StationId,
    /// Cells scanned for owner entities.
    pub scanned_cells: Vec<CellCoord3>,
    /// Entity migrations that were committed.
    pub entity_migrations: Vec<EntityMigrationReport>,
    /// Candidate handles that no longer resolved to an entity.
    pub skipped_missing_handles: usize,
    /// Candidate entities skipped because they were ghosts or non-authoritative.
    pub skipped_non_owned: usize,
    /// Duplicate candidate entities skipped after first occurrence.
    pub skipped_duplicate_entities: usize,
}

/// Caller-owned working storage for repeated cell migration passes.
#[derive(Clone, Debug, Default)]
pub struct CellMigrationScratch {
    seen_handles: HashSet<EntityHandle>,
    seen_entities: HashSet<EntityId>,
    entity_ids: Vec<EntityId>,
}

impl CellMigrationScratch {
    /// Creates empty migration scratch storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves deduplication and candidate storage for an expected pass size.
    pub fn reserve(&mut self, handles: usize, entities: usize) {
        if self.seen_handles.capacity() < handles {
            self.seen_handles
                .reserve(handles.saturating_sub(self.seen_handles.len()));
        }
        if self.seen_entities.capacity() < entities {
            self.seen_entities
                .reserve(entities.saturating_sub(self.seen_entities.len()));
        }
        if self.entity_ids.capacity() < entities {
            self.entity_ids
                .reserve(entities.saturating_sub(self.entity_ids.len()));
        }
    }

    /// Retained handle-deduplication capacity.
    pub fn handle_capacity(&self) -> usize {
        self.seen_handles.capacity()
    }

    /// Retained entity-deduplication capacity.
    pub fn entity_capacity(&self) -> usize {
        self.seen_entities.capacity()
    }

    /// Retained candidate entity capacity.
    pub fn candidate_capacity(&self) -> usize {
        self.entity_ids.capacity()
    }

    fn clear(&mut self) {
        self.seen_handles.clear();
        self.seen_entities.clear();
        self.entity_ids.clear();
    }
}

/// Cell-level migration error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellMigrationError {
    /// Entity migration failed.
    Entity(EntityMigrationError),
    /// Target owner record was not found after a successful migration.
    MissingTargetRecord(EntityId),
    /// Source ghost record was not found after a successful migration.
    MissingSourceRecord(EntityId),
}

impl core::fmt::Display for CellMigrationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Entity(error) => write!(f, "{error}"),
            Self::MissingTargetRecord(id) => {
                write!(f, "target owner record for entity {} is missing", id.get())
            }
            Self::MissingSourceRecord(id) => {
                write!(f, "source ghost record for entity {} is missing", id.get())
            }
        }
    }
}

impl std::error::Error for CellMigrationError {}

impl From<EntityMigrationError> for CellMigrationError {
    fn from(value: EntityMigrationError) -> Self {
        Self::Entity(value)
    }
}

/// Executes cell-level ownership migration using station-local indexes.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellMigrationExecutor;

impl CellMigrationExecutor {
    /// Migrates owned entities found in `cells` from source station to target station.
    pub fn migrate_cells(
        stations: &mut StationSet,
        source_index: &mut CellIndex,
        target_index: &mut CellIndex,
        source_station: StationId,
        target_station: StationId,
        cells: &[CellCoord3],
        ghost_ttl_ticks: u64,
    ) -> Result<CellMigrationReport, CellMigrationError> {
        let mut report = CellMigrationReport::default();
        let mut scratch = CellMigrationScratch::new();
        Self::migrate_cells_into(
            stations,
            source_index,
            target_index,
            source_station,
            target_station,
            cells,
            ghost_ttl_ticks,
            &mut scratch,
            &mut report,
        )?;
        Ok(report)
    }

    /// Migrates cells using caller-owned reusable working and report storage.
    ///
    /// The report is reset before processing. As with [`Self::migrate_cells`],
    /// an error may occur after earlier entities have already migrated.
    #[allow(clippy::too_many_arguments)]
    pub fn migrate_cells_into(
        stations: &mut StationSet,
        source_index: &mut CellIndex,
        target_index: &mut CellIndex,
        source_station: StationId,
        target_station: StationId,
        cells: &[CellCoord3],
        ghost_ttl_ticks: u64,
        scratch: &mut CellMigrationScratch,
        report: &mut CellMigrationReport,
    ) -> Result<(), CellMigrationError> {
        report.source_station = source_station;
        report.target_station = target_station;
        report.scanned_cells.clear();
        report.scanned_cells.extend_from_slice(cells);
        report.entity_migrations.clear();
        report.skipped_missing_handles = 0;
        report.skipped_non_owned = 0;
        report.skipped_duplicate_entities = 0;
        scratch.clear();

        {
            let source = stations
                .get(source_station)
                .ok_or(EntityMigrationError::MissingSource(source_station))?;
            for cell in cells {
                for &handle in source_index.handles_in_cell_slice(*cell) {
                    if !scratch.seen_handles.insert(handle) {
                        report.skipped_duplicate_entities += 1;
                        continue;
                    }
                    let Some(record) = source.get(handle) else {
                        report.skipped_missing_handles += 1;
                        continue;
                    };
                    if record.is_owned() {
                        scratch.entity_ids.push(record.id);
                    } else {
                        report.skipped_non_owned += 1;
                    }
                }
            }
        }

        for &entity_id in &scratch.entity_ids {
            if !scratch.seen_entities.insert(entity_id) {
                report.skipped_duplicate_entities += 1;
                continue;
            }
            let migration = EntityMigrationExecutor::migrate_entity(
                stations,
                entity_id,
                source_station,
                target_station,
                ghost_ttl_ticks,
            )?;

            {
                let target = stations
                    .get(target_station)
                    .ok_or(EntityMigrationError::MissingTarget(target_station))?;
                let target_record = target
                    .get(migration.target_owner)
                    .ok_or(CellMigrationError::MissingTargetRecord(entity_id))?;
                target_index.upsert(
                    migration.target_owner,
                    target_record.position,
                    target_record.bounds,
                );
            }

            {
                let source = stations
                    .get(source_station)
                    .ok_or(EntityMigrationError::MissingSource(source_station))?;
                let source_record = source
                    .get(migration.source_ghost)
                    .ok_or(CellMigrationError::MissingSourceRecord(entity_id))?;
                source_index.upsert(
                    migration.source_ghost,
                    source_record.position,
                    source_record.bounds,
                );
            }

            report.entity_migrations.push(migration);
        }

        Ok(())
    }
}

/// Automatic split scheduler configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SplitSchedulerConfig {
    /// Hotspot thresholds.
    pub thresholds: HotspotThresholds,
    /// Maximum split actions to create per scheduling pass.
    pub max_actions_per_pass: usize,
    /// Maximum cells to move in each split action.
    pub max_cells_per_action: usize,
    /// Source ghost TTL used during migration execution.
    pub ghost_ttl_ticks: u64,
    /// Minimum load-score gap required between source and target.
    pub min_score_improvement: u64,
    /// Maximum permitted target load score after moved cell pressure is added.
    pub max_target_score_after_move: u64,
    /// Ticks a source station must wait before another split can be planned.
    pub split_cooldown_ticks: u64,
    /// Whether warm target stations may receive split cells.
    pub allow_warm_targets: bool,
}

impl Default for SplitSchedulerConfig {
    fn default() -> Self {
        Self {
            thresholds: HotspotThresholds::default(),
            max_actions_per_pass: 4,
            max_cells_per_action: 4,
            ghost_ttl_ticks: 4,
            min_score_improvement: 1,
            max_target_score_after_move: u64::MAX,
            split_cooldown_ticks: 0,
            allow_warm_targets: true,
        }
    }
}

/// One scheduled split action.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SplitAction {
    /// Source station selected for split.
    pub source_station: StationId,
    /// Target station selected to receive cells.
    pub target_station: StationId,
    /// Cell split proposal.
    pub proposal: SplitProposal,
    /// Source load score observed when planning.
    pub source_score: u64,
    /// Target load score observed when planning.
    pub target_score: u64,
    /// Estimated target score after moving proposed cell pressure.
    pub estimated_target_score_after_move: u64,
}

/// Split schedule produced from a load snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SplitSchedule {
    /// Hotspot decisions produced for every input station.
    pub decisions: Vec<HotspotDecision>,
    /// Actions selected for execution.
    pub actions: Vec<SplitAction>,
    /// Hot stations skipped because no distinct target existed.
    pub skipped_no_target: usize,
    /// Hot stations skipped because no cells were proposed.
    pub skipped_no_cells: usize,
    /// Hot stations skipped because source station is inside split cooldown.
    pub skipped_cooldown: usize,
    /// Hot stations skipped because all targets were too warm or hot.
    pub skipped_target_severity: usize,
    /// Hot stations skipped because target capacity would be exceeded.
    pub skipped_target_capacity: usize,
    /// Hot stations skipped because target score improvement was too small.
    pub skipped_insufficient_improvement: usize,
}

/// Borrowed split schedule produced from reusable scheduler output slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SplitScheduleView<'a> {
    /// Hotspot decisions in input sample order.
    pub decisions: &'a [HotspotDecision],
    /// Admitted split actions in deterministic source order.
    pub actions: &'a [SplitAction],
    /// Hot stations skipped because no distinct target existed.
    pub skipped_no_target: usize,
    /// Hot stations skipped because no cells were proposed.
    pub skipped_no_cells: usize,
    /// Hot stations skipped because source station is inside split cooldown.
    pub skipped_cooldown: usize,
    /// Hot stations skipped because all targets were too warm or hot.
    pub skipped_target_severity: usize,
    /// Hot stations skipped because target capacity would be exceeded.
    pub skipped_target_capacity: usize,
    /// Hot stations skipped because target score improvement was too small.
    pub skipped_insufficient_improvement: usize,
}

impl From<SplitScheduleView<'_>> for SplitSchedule {
    fn from(view: SplitScheduleView<'_>) -> Self {
        Self {
            decisions: view.decisions.to_vec(),
            actions: view.actions.to_vec(),
            skipped_no_target: view.skipped_no_target,
            skipped_no_cells: view.skipped_no_cells,
            skipped_cooldown: view.skipped_cooldown,
            skipped_target_severity: view.skipped_target_severity,
            skipped_target_capacity: view.skipped_target_capacity,
            skipped_insufficient_improvement: view.skipped_insufficient_improvement,
        }
    }
}

/// Caller-owned reusable output and working storage for split scheduling.
#[derive(Clone, Debug, Default)]
pub struct SplitSchedulerScratch {
    decisions: Vec<HotspotDecision>,
    active_decisions: usize,
    actions: Vec<SplitAction>,
    active_actions: usize,
    hotspot: HotspotSplitScratch,
    proposal: SplitProposal,
    skipped_no_target: usize,
    skipped_no_cells: usize,
    skipped_cooldown: usize,
    skipped_target_severity: usize,
    skipped_target_capacity: usize,
    skipped_insufficient_improvement: usize,
}

impl SplitSchedulerScratch {
    /// Creates empty scheduler storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decision slots retained across passes.
    pub fn retained_decision_slots(&self) -> usize {
        self.decisions.len()
    }

    /// Action slots retained across passes.
    pub fn retained_action_slots(&self) -> usize {
        self.actions.len()
    }

    /// Total reason capacity retained across decision slots.
    pub fn retained_reason_capacity(&self) -> usize {
        self.decisions
            .iter()
            .map(|decision| decision.reasons.capacity())
            .sum()
    }

    /// Total cell-coordinate capacity retained across action proposal slots.
    pub fn retained_action_cell_capacity(&self) -> usize {
        self.actions
            .iter()
            .map(|action| action.proposal.cells_to_move.capacity())
            .sum()
    }

    /// Hotspot candidate capacity retained across passes.
    pub fn retained_candidate_capacity(&self) -> usize {
        self.hotspot.candidate_capacity()
    }

    fn prepare(&mut self, decisions: usize) {
        if self.decisions.len() < decisions {
            self.decisions
                .resize_with(decisions, HotspotDecision::default);
        }
        self.active_decisions = decisions;
        self.active_actions = 0;
        self.skipped_no_target = 0;
        self.skipped_no_cells = 0;
        self.skipped_cooldown = 0;
        self.skipped_target_severity = 0;
        self.skipped_target_capacity = 0;
        self.skipped_insufficient_improvement = 0;
    }

    fn view(&self) -> SplitScheduleView<'_> {
        SplitScheduleView {
            decisions: &self.decisions[..self.active_decisions],
            actions: &self.actions[..self.active_actions],
            skipped_no_target: self.skipped_no_target,
            skipped_no_cells: self.skipped_no_cells,
            skipped_cooldown: self.skipped_cooldown,
            skipped_target_severity: self.skipped_target_severity,
            skipped_target_capacity: self.skipped_target_capacity,
            skipped_insufficient_improvement: self.skipped_insufficient_improvement,
        }
    }
}

/// Mutable planning state for conservative split scheduling.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SplitSchedulerState {
    last_split_at: BTreeMap<StationId, Tick>,
}

impl SplitSchedulerState {
    /// Returns the last split tick for a source station.
    pub fn last_split_at(&self, station_id: StationId) -> Option<Tick> {
        self.last_split_at.get(&station_id).copied()
    }

    /// Records one executed or externally accepted split action.
    pub fn record_action(&mut self, action: &SplitAction, tick: Tick) {
        self.last_split_at.insert(action.source_station, tick);
    }

    /// Records all actions in a schedule at the same tick.
    pub fn record_schedule(&mut self, schedule: &SplitSchedule, tick: Tick) {
        for action in &schedule.actions {
            self.record_action(action, tick);
        }
    }

    /// Records all actions from a borrowed reusable schedule view.
    pub fn record_schedule_view(&mut self, schedule: SplitScheduleView<'_>, tick: Tick) {
        for action in schedule.actions {
            self.record_action(action, tick);
        }
    }

    /// Returns whether a station is inside split cooldown.
    pub fn is_in_cooldown(
        &self,
        station_id: StationId,
        current_tick: Tick,
        cooldown_ticks: u64,
    ) -> bool {
        if cooldown_ticks == 0 {
            return false;
        }
        let Some(last_split) = self.last_split_at(station_id) else {
            return false;
        };
        current_tick.get().saturating_sub(last_split.get()) < cooldown_ticks
    }
}

/// Result of executing a split schedule.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SplitScheduleExecutionReport {
    /// Ownership changes applied.
    pub ownership_updates: Vec<CellOwnershipUpdate>,
    /// Cell migration reports.
    pub cell_migrations: Vec<CellMigrationReport>,
}

/// Borrowed result of executing a split schedule into reusable storage.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SplitScheduleExecutionView<'a> {
    /// Ownership changes applied during this pass.
    pub ownership_updates: &'a [CellOwnershipUpdate],
    /// Cell migration reports produced during this pass.
    pub cell_migrations: &'a [CellMigrationReport],
}

/// Caller-owned reusable output and working storage for split execution.
#[derive(Clone, Debug, Default)]
pub struct SplitScheduleExecutionScratch {
    ownership_updates: Vec<CellOwnershipUpdate>,
    cell_migrations: Vec<CellMigrationReport>,
    active_actions: usize,
    migration: CellMigrationScratch,
}

impl SplitScheduleExecutionScratch {
    /// Creates empty split-execution storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves outer action slots and per-action cell/entity report capacity.
    pub fn reserve(&mut self, actions: usize, cells_per_action: usize, entities_per_action: usize) {
        while self.ownership_updates.len() < actions {
            self.ownership_updates.push(CellOwnershipUpdate::default());
            self.cell_migrations.push(CellMigrationReport::default());
        }
        for update in &mut self.ownership_updates[..actions] {
            if update.moved_cells.capacity() < cells_per_action {
                update
                    .moved_cells
                    .reserve(cells_per_action.saturating_sub(update.moved_cells.len()));
            }
        }
        for report in &mut self.cell_migrations[..actions] {
            if report.scanned_cells.capacity() < cells_per_action {
                report
                    .scanned_cells
                    .reserve(cells_per_action.saturating_sub(report.scanned_cells.len()));
            }
            if report.entity_migrations.capacity() < entities_per_action {
                report
                    .entity_migrations
                    .reserve(entities_per_action.saturating_sub(report.entity_migrations.len()));
            }
        }
        self.migration
            .reserve(entities_per_action, entities_per_action);
    }

    /// Ownership-update slots retained across passes.
    pub fn retained_ownership_slots(&self) -> usize {
        self.ownership_updates.len()
    }

    /// Cell-migration report slots retained across passes.
    pub fn retained_migration_slots(&self) -> usize {
        self.cell_migrations.len()
    }

    /// Total moved-cell capacity retained across ownership updates.
    pub fn retained_update_cell_capacity(&self) -> usize {
        self.ownership_updates
            .iter()
            .map(|update| update.moved_cells.capacity())
            .sum()
    }

    /// Total entity-report capacity retained across cell migrations.
    pub fn retained_entity_migration_capacity(&self) -> usize {
        self.cell_migrations
            .iter()
            .map(|report| report.entity_migrations.capacity())
            .sum()
    }

    /// Retained candidate entity capacity in the shared migration scratch.
    pub fn retained_candidate_capacity(&self) -> usize {
        self.migration.candidate_capacity()
    }

    fn prepare(&mut self, actions: usize) {
        while self.ownership_updates.len() < actions {
            self.ownership_updates.push(CellOwnershipUpdate::default());
            self.cell_migrations.push(CellMigrationReport::default());
        }
        self.active_actions = 0;
    }

    fn view(&self) -> SplitScheduleExecutionView<'_> {
        SplitScheduleExecutionView {
            ownership_updates: &self.ownership_updates[..self.active_actions],
            cell_migrations: &self.cell_migrations[..self.active_actions],
        }
    }
}

/// Split schedule execution error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitScheduleExecutionError {
    /// Source index is missing.
    MissingSourceIndex(StationId),
    /// Target index is missing.
    MissingTargetIndex(StationId),
    /// Cell migration failed.
    CellMigration(CellMigrationError),
}

impl core::fmt::Display for SplitScheduleExecutionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingSourceIndex(id) => write!(f, "source index {} is missing", id.get()),
            Self::MissingTargetIndex(id) => write!(f, "target index {} is missing", id.get()),
            Self::CellMigration(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for SplitScheduleExecutionError {}

impl From<CellMigrationError> for SplitScheduleExecutionError {
    fn from(value: CellMigrationError) -> Self {
        Self::CellMigration(value)
    }
}

/// Conservative automatic split scheduler.
#[derive(Clone, Copy, Debug)]
pub struct SplitScheduler {
    /// Scheduler configuration.
    pub config: SplitSchedulerConfig,
}

impl SplitScheduler {
    /// Creates a split scheduler.
    pub const fn new(config: SplitSchedulerConfig) -> Self {
        Self { config }
    }

    /// Plans split actions from station load samples.
    pub fn plan(&self, samples: &[StationLoadSample]) -> SplitSchedule {
        let mut scratch = SplitSchedulerScratch::new();
        self.plan_into(samples, &mut scratch).into()
    }

    /// Plans into fully reusable caller-owned output and working storage.
    pub fn plan_into<'a>(
        &self,
        samples: &[StationLoadSample],
        scratch: &'a mut SplitSchedulerScratch,
    ) -> SplitScheduleView<'a> {
        self.plan_with_state_into(samples, None, Tick::new(0), scratch)
    }

    /// Plans split actions using reusable hotspot cell candidate storage.
    pub fn plan_with_scratch(
        &self,
        samples: &[StationLoadSample],
        scratch: &mut HotspotSplitScratch,
    ) -> SplitSchedule {
        self.plan_with_state_and_scratch(samples, None, Tick::new(0), scratch)
    }

    /// Plans split actions using optional cooldown state.
    pub fn plan_with_state(
        &self,
        samples: &[StationLoadSample],
        state: Option<&SplitSchedulerState>,
        current_tick: Tick,
    ) -> SplitSchedule {
        let mut scratch = SplitSchedulerScratch::new();
        self.plan_with_state_into(samples, state, current_tick, &mut scratch)
            .into()
    }

    /// Plans with optional cooldown state and reusable hotspot candidate storage.
    pub fn plan_with_state_and_scratch(
        &self,
        samples: &[StationLoadSample],
        state: Option<&SplitSchedulerState>,
        current_tick: Tick,
        scratch: &mut HotspotSplitScratch,
    ) -> SplitSchedule {
        let mut scheduler_scratch = SplitSchedulerScratch::new();
        core::mem::swap(&mut scheduler_scratch.hotspot, scratch);
        let schedule = self
            .plan_with_state_into(samples, state, current_tick, &mut scheduler_scratch)
            .into();
        core::mem::swap(&mut scheduler_scratch.hotspot, scratch);
        schedule
    }

    /// Plans with optional cooldown state into fully reusable scheduler storage.
    pub fn plan_with_state_into<'a>(
        &self,
        samples: &[StationLoadSample],
        state: Option<&SplitSchedulerState>,
        current_tick: Tick,
        scratch: &'a mut SplitSchedulerScratch,
    ) -> SplitScheduleView<'a> {
        scratch.prepare(samples.len());
        for (decision, sample) in scratch.decisions[..samples.len()].iter_mut().zip(samples) {
            HotspotPlanner::evaluate_into(sample, self.config.thresholds, decision);
        }

        for (source_index, source) in samples.iter().enumerate() {
            if scratch.active_actions >= self.config.max_actions_per_pass {
                break;
            }
            let source_decision = &scratch.decisions[source_index];
            if source_decision.severity != HotspotSeverity::Hot {
                continue;
            }
            if state.is_some_and(|state| {
                state.is_in_cooldown(
                    source.station_id,
                    current_tick,
                    self.config.split_cooldown_ticks,
                )
            }) {
                scratch.skipped_cooldown = scratch.skipped_cooldown.saturating_add(1);
                continue;
            }

            HotspotPlanner::propose_cell_split_into(
                source,
                self.config.max_cells_per_action,
                &mut scratch.hotspot,
                &mut scratch.proposal,
            );
            if scratch.proposal.cells_to_move.is_empty() {
                scratch.skipped_no_cells = scratch.skipped_no_cells.saturating_add(1);
                continue;
            }
            let target_selection = select_split_target(
                source,
                &scratch.proposal,
                samples,
                &scratch.decisions[..scratch.active_decisions],
                self.config,
            );
            let Some(target) = target_selection.target else {
                if target_selection.considered_targets == 0 {
                    scratch.skipped_no_target = scratch.skipped_no_target.saturating_add(1);
                } else {
                    scratch.skipped_target_severity = scratch
                        .skipped_target_severity
                        .saturating_add(usize::from(target_selection.rejected_by_severity > 0));
                    scratch.skipped_target_capacity = scratch
                        .skipped_target_capacity
                        .saturating_add(usize::from(target_selection.rejected_by_capacity > 0));
                    scratch.skipped_insufficient_improvement = scratch
                        .skipped_insufficient_improvement
                        .saturating_add(usize::from(target_selection.rejected_by_improvement > 0));
                }
                continue;
            };
            let target_score = station_load_score(target);
            let estimated_target_score_after_move =
                target_score.saturating_add(scratch.proposal.moved_pressure_score);
            let action_index = scratch.active_actions;
            if action_index == scratch.actions.len() {
                scratch.actions.push(SplitAction::default());
            }
            let action = &mut scratch.actions[action_index];
            action.source_station = source.station_id;
            action.target_station = target.station_id;
            action.proposal.source_station = scratch.proposal.source_station;
            action.proposal.cells_to_move.clear();
            action
                .proposal
                .cells_to_move
                .extend_from_slice(&scratch.proposal.cells_to_move);
            action.proposal.moved_pressure_score = scratch.proposal.moved_pressure_score;
            action.source_score = station_load_score(source);
            action.target_score = target_score;
            action.estimated_target_score_after_move = estimated_target_score_after_move;
            scratch.active_actions = scratch.active_actions.saturating_add(1);
        }

        scratch.view()
    }

    /// Executes a split schedule by applying ownership updates and migrating entities.
    pub fn execute(
        &self,
        schedule: &SplitSchedule,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionReport, SplitScheduleExecutionError> {
        self.execute_actions(&schedule.actions, stations, indexes, ownership)
    }

    /// Executes actions directly from a borrowed reusable schedule view.
    pub fn execute_view(
        &self,
        schedule: SplitScheduleView<'_>,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionReport, SplitScheduleExecutionError> {
        self.execute_actions(schedule.actions, stations, indexes, ownership)
    }

    /// Executes an owned schedule into fully reusable caller-owned storage.
    pub fn execute_into<'a>(
        &self,
        schedule: &SplitSchedule,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
        scratch: &'a mut SplitScheduleExecutionScratch,
    ) -> Result<SplitScheduleExecutionView<'a>, SplitScheduleExecutionError> {
        self.execute_actions_into(&schedule.actions, stations, indexes, ownership, scratch)
    }

    /// Executes a borrowed schedule into fully reusable caller-owned storage.
    pub fn execute_view_into<'a>(
        &self,
        schedule: SplitScheduleView<'_>,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
        scratch: &'a mut SplitScheduleExecutionScratch,
    ) -> Result<SplitScheduleExecutionView<'a>, SplitScheduleExecutionError> {
        self.execute_actions_into(schedule.actions, stations, indexes, ownership, scratch)
    }

    fn execute_actions_into<'a>(
        &self,
        actions: &[SplitAction],
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
        scratch: &'a mut SplitScheduleExecutionScratch,
    ) -> Result<SplitScheduleExecutionView<'a>, SplitScheduleExecutionError> {
        scratch.prepare(actions.len());

        for action in actions {
            if indexes.get(action.source_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingSourceIndex(
                    action.source_station,
                ));
            }
            if indexes.get(action.target_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingTargetIndex(
                    action.target_station,
                ));
            }

            let action_index = scratch.active_actions;
            let update = &mut scratch.ownership_updates[action_index];
            ownership.apply_split_into(&action.proposal, action.target_station, update);
            let (source_index, target_index) = indexes
                .get_pair_mut(action.source_station, action.target_station)
                .expect("indexes were checked above");
            CellMigrationExecutor::migrate_cells_into(
                stations,
                source_index,
                target_index,
                action.source_station,
                action.target_station,
                &update.moved_cells,
                self.config.ghost_ttl_ticks,
                &mut scratch.migration,
                &mut scratch.cell_migrations[action_index],
            )?;
            scratch.active_actions = scratch.active_actions.saturating_add(1);
        }

        Ok(scratch.view())
    }

    fn execute_actions(
        &self,
        actions: &[SplitAction],
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionReport, SplitScheduleExecutionError> {
        let mut report = SplitScheduleExecutionReport::default();

        for action in actions {
            if indexes.get(action.source_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingSourceIndex(
                    action.source_station,
                ));
            }
            if indexes.get(action.target_station).is_none() {
                return Err(SplitScheduleExecutionError::MissingTargetIndex(
                    action.target_station,
                ));
            }

            let update = ownership.apply_split(&action.proposal, action.target_station);
            let (source_index, target_index) = indexes
                .get_pair_mut(action.source_station, action.target_station)
                .expect("indexes were checked above");
            let migration = CellMigrationExecutor::migrate_cells(
                stations,
                source_index,
                target_index,
                action.source_station,
                action.target_station,
                &update.moved_cells,
                self.config.ghost_ttl_ticks,
            )?;
            report.ownership_updates.push(update);
            report.cell_migrations.push(migration);
        }

        Ok(report)
    }
}

impl Default for SplitScheduler {
    fn default() -> Self {
        Self::new(SplitSchedulerConfig::default())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SplitTargetSelection<'a> {
    target: Option<&'a StationLoadSample>,
    target_key: Option<(u8, u64, u32)>,
    considered_targets: usize,
    rejected_by_severity: usize,
    rejected_by_capacity: usize,
    rejected_by_improvement: usize,
}

fn select_split_target<'a>(
    source: &StationLoadSample,
    proposal: &SplitProposal,
    samples: &'a [StationLoadSample],
    decisions: &[HotspotDecision],
    config: SplitSchedulerConfig,
) -> SplitTargetSelection<'a> {
    let mut selection = SplitTargetSelection::default();
    let source_score = station_load_score(source);

    for (target, decision) in samples.iter().zip(decisions) {
        if target.station_id == source.station_id {
            continue;
        }
        selection.considered_targets += 1;

        debug_assert_eq!(decision.station_id, target.station_id);
        let severity = decision.severity;
        if severity == HotspotSeverity::Hot
            || (severity == HotspotSeverity::Warm && !config.allow_warm_targets)
        {
            selection.rejected_by_severity += 1;
            continue;
        }

        let target_score = station_load_score(target);
        if source_score.saturating_sub(target_score) < config.min_score_improvement {
            selection.rejected_by_improvement += 1;
            continue;
        }
        if target_score.saturating_add(proposal.moved_pressure_score)
            > config.max_target_score_after_move
        {
            selection.rejected_by_capacity += 1;
            continue;
        }

        let target_key = (
            severity_rank(severity),
            target_score,
            target.station_id.get(),
        );
        if selection
            .target_key
            .is_none_or(|current_key| target_key < current_key)
        {
            selection.target = Some(target);
            selection.target_key = Some(target_key);
        }
    }

    selection
}

fn severity_rank(severity: HotspotSeverity) -> u8 {
    match severity {
        HotspotSeverity::Normal => 0,
        HotspotSeverity::Warm => 1,
        HotspotSeverity::Hot => 2,
    }
}

fn station_load_score(sample: &StationLoadSample) -> u64 {
    (sample.total_entities() as u64)
        .saturating_mul(8)
        .saturating_add((sample.subscribers as u64).saturating_mul(4))
        .saturating_add(sample.queued_events as u64)
        .saturating_add((sample.estimated_bytes / 256) as u64)
        .saturating_add(sample.tick_cost_units)
}

/// Event router statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventRouterStats {
    /// Events accepted by target queues.
    pub routed_events: usize,
    /// Ready events drained for station application.
    pub drained_events: usize,
    /// Best-effort events dropped by bounded target queues.
    pub dropped_best_effort_events: usize,
}

/// Event router error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRouterError {
    /// Target station was not registered with the router.
    MissingTarget(StationId),
    /// Underlying target queue rejected the event.
    Queue(EventQueueError),
}

impl core::fmt::Display for EventRouterError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingTarget(id) => write!(f, "event target station {} is missing", id.get()),
            Self::Queue(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for EventRouterError {}

impl From<EventQueueError> for EventRouterError {
    fn from(value: EventQueueError) -> Self {
        Self::Queue(value)
    }
}

/// In-process station event router.
#[derive(Clone, Debug)]
pub struct EventRouter {
    limits: EventQueueLimits,
    queues: BTreeMap<StationId, EventQueues>,
    stats: EventRouterStats,
}

impl EventRouter {
    /// Creates an empty event router.
    pub fn new(limits: EventQueueLimits) -> Self {
        Self {
            limits,
            queues: BTreeMap::new(),
            stats: EventRouterStats::default(),
        }
    }

    /// Registers a station target queue.
    pub fn register_station(&mut self, station_id: StationId) {
        self.queues
            .entry(station_id)
            .or_insert_with(|| EventQueues::new(self.limits));
    }

    /// Registers all stations in a set.
    pub fn register_stations(&mut self, stations: &StationSet) {
        for station in stations.iter() {
            self.register_station(station.config().station_id);
        }
    }

    /// Unregisters a station queue and returns the number of queued events dropped.
    pub fn unregister_station(&mut self, station_id: StationId) -> Option<usize> {
        self.queues.remove(&station_id).map(|queue| queue.len())
    }

    /// Routes an event to its target station queue.
    pub fn route(&mut self, event: StationEvent) -> Result<PushOutcome, EventRouterError> {
        let queue = self
            .queues
            .get_mut(&event.target)
            .ok_or(EventRouterError::MissingTarget(event.target))?;
        let outcome = queue.push(event)?;
        self.stats.routed_events += 1;
        if outcome == PushOutcome::DroppedOldestBestEffort {
            self.stats.dropped_best_effort_events += 1;
        }
        Ok(outcome)
    }

    /// Drains events whose `target_tick` is ready for application.
    pub fn drain_ready(
        &mut self,
        station_id: StationId,
        current_tick: Tick,
    ) -> Result<Vec<StationEvent>, EventRouterError> {
        let mut ready = Vec::new();
        self.drain_ready_into(station_id, current_tick, &mut ready)?;
        Ok(ready)
    }

    /// Drains ready events into caller-owned storage while retaining its capacity.
    pub fn drain_ready_into(
        &mut self,
        station_id: StationId,
        current_tick: Tick,
        ready: &mut Vec<StationEvent>,
    ) -> Result<(), EventRouterError> {
        ready.clear();
        self.append_ready(station_id, current_tick, ready)?;
        Ok(())
    }

    fn append_ready(
        &mut self,
        station_id: StationId,
        current_tick: Tick,
        ready: &mut Vec<StationEvent>,
    ) -> Result<(), EventRouterError> {
        let queue = self
            .queues
            .get_mut(&station_id)
            .ok_or(EventRouterError::MissingTarget(station_id))?;
        let drained = queue.drain_ready_into(current_tick, ready);
        self.stats.drained_events = self.stats.drained_events.saturating_add(drained);
        Ok(())
    }

    /// Returns queued event count for one station.
    pub fn queued_len(&self, station_id: StationId) -> Option<usize> {
        self.queues.get(&station_id).map(EventQueues::len)
    }

    /// Returns router statistics.
    pub const fn stats(&self) -> EventRouterStats {
        self.stats
    }
}

impl Default for EventRouter {
    fn default() -> Self {
        Self::new(EventQueueLimits::default())
    }
}

/// Statistics for station event transport bridging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StationEventTransportStats {
    /// Events encoded and submitted to station transport.
    pub events_sent: usize,
    /// Bytes submitted to station transport.
    pub bytes_sent: usize,
    /// Packets received from station transport.
    pub packets_received: usize,
    /// Bytes received from station transport.
    pub bytes_received: usize,
    /// Events decoded and accepted by the target router.
    pub events_routed: usize,
}

/// Result of pumping station event packets for one target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StationEventPumpReport {
    /// Target station pumped.
    pub target_station: StationId,
    /// Packets consumed from the station transport.
    pub packets_received: usize,
    /// Bytes consumed from the station transport.
    pub bytes_received: usize,
    /// Events accepted by the target router.
    pub events_routed: usize,
}

/// Error produced while bridging station events through packet transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StationEventTransportError<E> {
    /// Underlying station transport failed.
    Transport(E),
    /// Wire encoding failed.
    Encode(BinaryEncodeError),
    /// Wire decoding failed.
    Decode(BinaryDecodeError),
    /// Packet decoded as a non-event frame.
    UnexpectedFrame,
    /// Packet envelope and decoded event disagreed about endpoints.
    EndpointMismatch {
        /// Packet source station.
        packet_source: StationId,
        /// Packet target station.
        packet_target: StationId,
        /// Decoded event source station.
        event_source: StationId,
        /// Decoded event target station.
        event_target: StationId,
    },
    /// Event router rejected the decoded event.
    Router(EventRouterError),
}

impl<E: core::fmt::Display> core::fmt::Display for StationEventTransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::Encode(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::UnexpectedFrame => f.write_str("station transport packet was not an event frame"),
            Self::EndpointMismatch {
                packet_source,
                packet_target,
                event_source,
                event_target,
            } => write!(
                f,
                "station event endpoint mismatch: packet {}->{}, event {}->{}",
                packet_source.get(),
                packet_target.get(),
                event_source.get(),
                event_target.get()
            ),
            Self::Router(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for StationEventTransportError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::UnexpectedFrame | Self::EndpointMismatch { .. } => None,
            Self::Router(error) => Some(error),
        }
    }
}

impl<E> From<BinaryEncodeError> for StationEventTransportError<E> {
    fn from(value: BinaryEncodeError) -> Self {
        Self::Encode(value)
    }
}

impl<E> From<BinaryDecodeError> for StationEventTransportError<E> {
    fn from(value: BinaryDecodeError) -> Self {
        Self::Decode(value)
    }
}

impl<E> From<EventRouterError> for StationEventTransportError<E> {
    fn from(value: EventRouterError) -> Self {
        Self::Router(value)
    }
}

/// Bridge between typed station events and bounded station packet transport.
#[derive(Clone, Debug, Default)]
pub struct StationEventTransportBridge {
    stats: StationEventTransportStats,
}

impl StationEventTransportBridge {
    /// Returns bridge statistics.
    pub const fn stats(&self) -> StationEventTransportStats {
        self.stats
    }

    /// Encodes and sends one station event through the station transport.
    pub fn send_event<T>(
        &mut self,
        transport: &mut T,
        event: &StationEvent,
    ) -> Result<(), StationEventTransportError<T::Error>>
    where
        T: StationTransportSink,
    {
        let frame = StationEventFrame::from_event(event);
        let mut bytes = Vec::with_capacity(64);
        BinaryFrameEncoder.encode_station_event(&frame, &mut bytes)?;
        let byte_len = bytes.len();
        transport
            .send_station(StationOutboundPacket {
                source_station: event.source,
                target_station: event.target,
                bytes,
            })
            .map_err(StationEventTransportError::Transport)?;
        self.stats.events_sent = self.stats.events_sent.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(byte_len);
        Ok(())
    }

    /// Receives up to `max_packets` for `target_station`, decodes station
    /// events, and routes them into `router`.
    pub fn pump_target<T>(
        &mut self,
        transport: &mut T,
        router: &mut EventRouter,
        target_station: StationId,
        max_packets: usize,
    ) -> Result<StationEventPumpReport, StationEventTransportError<T::Error>>
    where
        T: StationTransportReceiver,
    {
        let mut report = StationEventPumpReport {
            target_station,
            ..StationEventPumpReport::default()
        };
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv_station(target_station)
                .map_err(StationEventTransportError::Transport)?
            else {
                break;
            };
            report.packets_received = report.packets_received.saturating_add(1);
            report.bytes_received = report.bytes_received.saturating_add(packet.bytes.len());

            let decoded = BinaryFrameDecoder.decode(&packet.bytes)?;
            let RuntimeFrame::StationEvent(frame) = decoded else {
                return Err(StationEventTransportError::UnexpectedFrame);
            };
            if frame.source_station != packet.source_station
                || frame.target_station != packet.target_station
            {
                return Err(StationEventTransportError::EndpointMismatch {
                    packet_source: packet.source_station,
                    packet_target: packet.target_station,
                    event_source: frame.source_station,
                    event_target: frame.target_station,
                });
            }

            router.route(frame.into_event())?;
            report.events_routed = report.events_routed.saturating_add(1);
        }

        self.stats.packets_received = self
            .stats
            .packets_received
            .saturating_add(report.packets_received);
        self.stats.bytes_received = self
            .stats
            .bytes_received
            .saturating_add(report.bytes_received);
        self.stats.events_routed = self
            .stats
            .events_routed
            .saturating_add(report.events_routed);
        Ok(report)
    }
}

/// Statistics for command dispatch transport bridging.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandDispatchTransportStats {
    /// Commands encoded and submitted to station transport.
    pub commands_sent: usize,
    /// Bytes submitted to station transport.
    pub bytes_sent: usize,
    /// Packets received from station transport.
    pub packets_received: usize,
    /// Bytes received from station transport.
    pub bytes_received: usize,
    /// Commands decoded and enqueued at the target station.
    pub commands_enqueued: usize,
    /// Commands rejected by target station queues.
    pub commands_rejected_queue: usize,
}

/// Result of pumping command dispatch packets for one target.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandDispatchPumpReport {
    /// Target station pumped.
    pub target_station: StationId,
    /// Packets consumed from station transport.
    pub packets_received: usize,
    /// Bytes consumed from station transport.
    pub bytes_received: usize,
    /// Commands enqueued into the target queue.
    pub commands_enqueued: usize,
}

/// Error produced while bridging command dispatch frames through station packet transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandDispatchTransportError<E> {
    /// Underlying station transport failed.
    Transport(E),
    /// Wire encoding failed.
    Encode(BinaryEncodeError),
    /// Wire decoding failed.
    Decode(BinaryDecodeError),
    /// Packet decoded as a non-command-dispatch frame.
    UnexpectedFrame,
    /// Packet envelope and decoded dispatch frame disagreed about the target.
    EndpointMismatch {
        /// Packet source station.
        packet_source: StationId,
        /// Packet target station.
        packet_target: StationId,
        /// Decoded command dispatch target station.
        dispatch_target: StationId,
    },
    /// Target station queue was not registered.
    MissingQueue(StationId),
    /// Target station queue rejected the command.
    Queue(CommandQueueError),
}

impl<E: core::fmt::Display> core::fmt::Display for CommandDispatchTransportError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "{error}"),
            Self::Encode(error) => write!(f, "{error}"),
            Self::Decode(error) => write!(f, "{error}"),
            Self::UnexpectedFrame => {
                f.write_str("station transport packet was not a command dispatch frame")
            }
            Self::EndpointMismatch {
                packet_source,
                packet_target,
                dispatch_target,
            } => write!(
                f,
                "command dispatch endpoint mismatch: packet {}->{}, dispatch target {}",
                packet_source.get(),
                packet_target.get(),
                dispatch_target.get()
            ),
            Self::MissingQueue(station_id) => {
                write!(
                    f,
                    "command dispatch target station {} has no queue",
                    station_id.get()
                )
            }
            Self::Queue(error) => write!(f, "{error}"),
        }
    }
}

impl<E> std::error::Error for CommandDispatchTransportError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Encode(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::UnexpectedFrame | Self::EndpointMismatch { .. } | Self::MissingQueue(_) => None,
            Self::Queue(error) => Some(error),
        }
    }
}

impl<E> From<BinaryEncodeError> for CommandDispatchTransportError<E> {
    fn from(value: BinaryEncodeError) -> Self {
        Self::Encode(value)
    }
}

impl<E> From<BinaryDecodeError> for CommandDispatchTransportError<E> {
    fn from(value: BinaryDecodeError) -> Self {
        Self::Decode(value)
    }
}

impl<E> From<CommandQueueError> for CommandDispatchTransportError<E> {
    fn from(value: CommandQueueError) -> Self {
        Self::Queue(value)
    }
}

/// Bridge between stamped command envelopes and bounded station packet transport.
#[derive(Clone, Debug, Default)]
pub struct CommandDispatchTransportBridge {
    stats: CommandDispatchTransportStats,
}

impl CommandDispatchTransportBridge {
    /// Returns bridge statistics.
    pub const fn stats(&self) -> CommandDispatchTransportStats {
        self.stats
    }

    /// Encodes and sends a stamped command envelope to a station node.
    pub fn send_envelope<T>(
        &mut self,
        transport: &mut T,
        source_station: StationId,
        target_station: StationId,
        command: &CommandEnvelope,
    ) -> Result<(), CommandDispatchTransportError<T::Error>>
    where
        T: StationTransportSink,
    {
        let mut bytes = Vec::with_capacity(64_usize.saturating_add(command.payload.len()));
        BinaryFrameEncoder.encode_command_dispatch_envelope(target_station, command, &mut bytes)?;
        self.send_encoded(transport, source_station, target_station, bytes)
    }

    /// Encodes and sends a command dispatch frame to its target station.
    pub fn send_frame<T>(
        &mut self,
        transport: &mut T,
        source_station: StationId,
        frame: &CommandDispatchFrame,
    ) -> Result<(), CommandDispatchTransportError<T::Error>>
    where
        T: StationTransportSink,
    {
        let mut bytes = Vec::with_capacity(64);
        BinaryFrameEncoder.encode_command_dispatch(frame, &mut bytes)?;
        self.send_encoded(transport, source_station, frame.station_id, bytes)
    }

    fn send_encoded<T>(
        &mut self,
        transport: &mut T,
        source_station: StationId,
        target_station: StationId,
        bytes: Vec<u8>,
    ) -> Result<(), CommandDispatchTransportError<T::Error>>
    where
        T: StationTransportSink,
    {
        let byte_len = bytes.len();
        transport
            .send_station(StationOutboundPacket {
                source_station,
                target_station,
                bytes,
            })
            .map_err(CommandDispatchTransportError::Transport)?;
        self.stats.commands_sent = self.stats.commands_sent.saturating_add(1);
        self.stats.bytes_sent = self.stats.bytes_sent.saturating_add(byte_len);
        Ok(())
    }

    /// Receives up to `max_packets` for `target_station`, decodes command
    /// dispatch frames, and enqueues stamped commands into `station_queues`.
    pub fn pump_target<T>(
        &mut self,
        transport: &mut T,
        station_queues: &mut BTreeMap<StationId, CommandQueues>,
        target_station: StationId,
        max_packets: usize,
        ingress: CommandIngress,
    ) -> Result<CommandDispatchPumpReport, CommandDispatchTransportError<T::Error>>
    where
        T: StationTransportReceiver,
    {
        let mut report = CommandDispatchPumpReport {
            target_station,
            ..CommandDispatchPumpReport::default()
        };
        for _ in 0..max_packets {
            let Some(packet) = transport
                .try_recv_station(target_station)
                .map_err(CommandDispatchTransportError::Transport)?
            else {
                break;
            };
            report.packets_received = report.packets_received.saturating_add(1);
            report.bytes_received = report.bytes_received.saturating_add(packet.bytes.len());

            let decoded = BinaryFrameDecoder.decode(&packet.bytes)?;
            let RuntimeFrame::CommandDispatch(frame) = decoded else {
                return Err(CommandDispatchTransportError::UnexpectedFrame);
            };
            if frame.station_id != packet.target_station {
                return Err(CommandDispatchTransportError::EndpointMismatch {
                    packet_source: packet.source_station,
                    packet_target: packet.target_station,
                    dispatch_target: frame.station_id,
                });
            }

            let queue = station_queues.get_mut(&frame.station_id).ok_or(
                CommandDispatchTransportError::MissingQueue(frame.station_id),
            )?;
            match queue.push(frame.into_envelope(), ingress) {
                Ok(_) => {
                    report.commands_enqueued = report.commands_enqueued.saturating_add(1);
                }
                Err(error) => {
                    self.stats.commands_rejected_queue =
                        self.stats.commands_rejected_queue.saturating_add(1);
                    return Err(CommandDispatchTransportError::Queue(error));
                }
            }
        }

        self.stats.packets_received = self
            .stats
            .packets_received
            .saturating_add(report.packets_received);
        self.stats.bytes_received = self
            .stats
            .bytes_received
            .saturating_add(report.bytes_received);
        self.stats.commands_enqueued = self
            .stats
            .commands_enqueued
            .saturating_add(report.commands_enqueued);
        Ok(report)
    }
}

/// Budget for a load-aware scheduler step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationScheduleConfig {
    /// Maximum stations that may advance during one scheduler step.
    pub max_station_advances_per_step: usize,
}

impl Default for StationScheduleConfig {
    fn default() -> Self {
        Self {
            max_station_advances_per_step: usize::MAX,
        }
    }
}

/// Candidate selected by the load-aware station scheduler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationScheduleCandidate {
    /// Station selected for advancement.
    pub station_id: StationId,
    /// Deterministic pressure score derived from the latest load sample.
    pub load_score: u64,
    /// How far this station is behind the most advanced station in the set.
    pub tick_lag: u64,
}

/// Result of one load-aware station scheduling pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StationSchedulePlan {
    /// Stations considered by this pass.
    pub candidates_considered: usize,
    /// Stations selected by this pass.
    pub stations_selected: usize,
    /// Station tick advances requested by this pass.
    pub total_advances: usize,
    /// Selected stations in deterministic execution order.
    pub selected: Vec<StationScheduleCandidate>,
}

/// Borrowed deterministic result from reusable Station scheduling storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StationScheduleView<'a> {
    /// Stations considered by this pass.
    pub candidates_considered: usize,
    /// Stations selected by this pass.
    pub stations_selected: usize,
    /// Station tick advances requested by this pass.
    pub total_advances: usize,
    /// Selected stations in deterministic execution order.
    pub selected: &'a [StationScheduleCandidate],
}

/// Caller-owned reusable storage for load-aware Station scheduling.
///
/// Storage contains only derived pressure scores and stateless candidates. It
/// does not retain scheduling decisions, cooldowns, or gameplay state.
#[derive(Clone, Debug, Default)]
pub struct StationScheduleScratch {
    scores: HashMap<StationId, u64>,
    candidates: Vec<StationScheduleCandidate>,
}

impl StationScheduleScratch {
    /// Creates empty scheduling scratch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Hash-table capacity retained for sampled Station scores.
    pub fn score_capacity(&self) -> usize {
        self.scores.capacity()
    }

    /// Candidate capacity retained across scheduling passes.
    pub fn candidate_capacity(&self) -> usize {
        self.candidates.capacity()
    }
}

/// Basic in-process station scheduler.
#[derive(Clone, Debug, Default)]
pub struct StationScheduler {
    /// Total station ticks advanced by this scheduler.
    pub advanced_ticks: u64,
}

impl StationScheduler {
    /// Advances every station by one tick.
    pub fn advance_all(&mut self, stations: &mut StationSet) {
        for station in stations.iter_mut() {
            station.advance_tick();
            self.advanced_ticks = self.advanced_ticks.saturating_add(1);
        }
    }

    /// Plans a bounded station advancement pass from load samples.
    pub fn plan_loaded(
        &self,
        stations: &StationSet,
        samples: &[StationLoadSample],
        config: StationScheduleConfig,
    ) -> StationSchedulePlan {
        let mut scratch = StationScheduleScratch::default();
        let view = self.plan_loaded_into(stations, samples, config, &mut scratch);
        StationSchedulePlan {
            candidates_considered: view.candidates_considered,
            stations_selected: view.stations_selected,
            total_advances: view.total_advances,
            selected: view.selected.to_vec(),
        }
    }

    /// Plans into caller-owned storage and returns a borrowed deterministic top-k view.
    pub fn plan_loaded_into<'a>(
        &self,
        stations: &StationSet,
        samples: &[StationLoadSample],
        config: StationScheduleConfig,
        scratch: &'a mut StationScheduleScratch,
    ) -> StationScheduleView<'a> {
        let candidates_considered = stations.len();
        let limit = config
            .max_station_advances_per_step
            .min(candidates_considered);
        let max_tick = stations
            .iter()
            .map(|station| station.tick().get())
            .max()
            .unwrap_or(0);
        scratch.scores.clear();
        scratch.scores.reserve(samples.len());
        for sample in samples {
            scratch
                .scores
                .insert(sample.station_id, station_schedule_score(sample));
        }
        scratch.candidates.clear();
        scratch.candidates.reserve(candidates_considered);
        scratch.candidates.extend(stations.iter().map(|station| {
            let station_id = station.config().station_id;
            StationScheduleCandidate {
                station_id,
                load_score: scratch.scores.get(&station_id).copied().unwrap_or(0),
                tick_lag: max_tick.saturating_sub(station.tick().get()),
            }
        }));
        prioritize_station_candidates(&mut scratch.candidates, limit);
        let selected = &scratch.candidates[..limit];

        StationScheduleView {
            candidates_considered,
            stations_selected: selected.len(),
            total_advances: selected.len(),
            selected,
        }
    }

    /// Advances a bounded set of high-load stations by one tick each.
    pub fn advance_loaded(
        &mut self,
        stations: &mut StationSet,
        samples: &[StationLoadSample],
        config: StationScheduleConfig,
    ) -> StationSchedulePlan {
        let plan = self.plan_loaded(stations, samples, config);
        for candidate in &plan.selected {
            if let Some(station) = stations.get_mut(candidate.station_id) {
                station.advance_tick();
                self.advanced_ticks = self.advanced_ticks.saturating_add(1);
            }
        }
        plan
    }

    /// Advances a bounded top-k set using reusable caller-owned scheduling storage.
    pub fn advance_loaded_into<'a>(
        &mut self,
        stations: &mut StationSet,
        samples: &[StationLoadSample],
        config: StationScheduleConfig,
        scratch: &'a mut StationScheduleScratch,
    ) -> StationScheduleView<'a> {
        let plan = self.plan_loaded_into(stations, samples, config, scratch);
        for candidate in plan.selected {
            if let Some(station) = stations.get_mut(candidate.station_id) {
                station.advance_tick();
                self.advanced_ticks = self.advanced_ticks.saturating_add(1);
            }
        }
        plan
    }

    /// Drains router events ready for each station's current tick.
    pub fn drain_ready_events(
        &mut self,
        stations: &StationSet,
        router: &mut EventRouter,
    ) -> Result<Vec<StationEvent>, EventRouterError> {
        let mut events = Vec::new();
        self.drain_ready_events_into(stations, router, &mut events)?;
        Ok(events)
    }

    /// Drains all Station-ready events into reusable caller-owned output.
    pub fn drain_ready_events_into(
        &mut self,
        stations: &StationSet,
        router: &mut EventRouter,
        events: &mut Vec<StationEvent>,
    ) -> Result<(), EventRouterError> {
        events.clear();
        for station in stations.iter() {
            router.append_ready(station.config().station_id, station.tick(), events)?;
        }
        Ok(())
    }
}

fn compare_station_schedule_candidates(
    left: &StationScheduleCandidate,
    right: &StationScheduleCandidate,
) -> core::cmp::Ordering {
    right
        .load_score
        .cmp(&left.load_score)
        .then_with(|| right.tick_lag.cmp(&left.tick_lag))
        .then_with(|| left.station_id.cmp(&right.station_id))
}

fn prioritize_station_candidates(candidates: &mut [StationScheduleCandidate], limit: usize) {
    if limit == 0 {
        return;
    }
    if limit.saturating_mul(2) < candidates.len() {
        candidates.select_nth_unstable_by(limit, compare_station_schedule_candidates);
        candidates[..limit].sort_by(compare_station_schedule_candidates);
    } else {
        candidates.sort_by(compare_station_schedule_candidates);
    }
}

fn station_schedule_score(sample: &StationLoadSample) -> u64 {
    station_load_score(sample).saturating_add(sample.max_cell_pressure())
}

/// Per-station progress inside a full runtime barrier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StationBarrierPhase {
    /// Station is part of the barrier but has not reached the target tick.
    WaitingTick,
    /// Station reached the target tick and is frozen.
    Frozen,
    /// Station has resumed.
    Resumed,
}

/// Barrier progress summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BarrierProgress {
    /// Barrier state.
    pub state: BarrierState,
    /// Number of stations covered by the barrier.
    pub station_count: usize,
    /// Number of stations frozen.
    pub frozen_count: usize,
    /// Target tick selected for the barrier.
    pub target_tick: Tick,
}

/// Runtime barrier metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BarrierMetrics {
    /// Number of stations covered by this barrier.
    pub station_count: usize,
    /// Number of snapshots exported while frozen.
    pub snapshots_exported: usize,
    /// Number of times polling observed at least one station still waiting.
    pub waiting_polls: u64,
    /// Number of times polling observed a fully frozen barrier.
    pub frozen_polls: u64,
}

/// Runtime barrier execution error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierRuntimeError {
    /// A barrier is already active.
    AlreadyActive(BarrierId),
    /// No barrier is active.
    NoActiveBarrier,
    /// Barrier scope matched no stations.
    EmptyScope(BarrierScope),
    /// Requested operation requires frozen state.
    NotFrozen(BarrierState),
    /// A station covered by the barrier is missing.
    MissingStation(StationId),
}

/// Caller-owned reusable station snapshot slots for frozen barrier exports.
#[derive(Clone, Debug, Default)]
pub struct BarrierSnapshotScratch {
    snapshots: Vec<StationSnapshot>,
    active_snapshots: usize,
}

impl BarrierSnapshotScratch {
    /// Creates empty barrier snapshot storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves Station slots and per-Station entity capacity.
    pub fn reserve(&mut self, stations: usize, entities_per_station: usize) {
        if self.snapshots.len() < stations {
            self.snapshots
                .resize_with(stations, StationSnapshot::default);
        }
        for snapshot in &mut self.snapshots[..stations] {
            if snapshot.entities.capacity() < entities_per_station {
                snapshot
                    .entities
                    .reserve(entities_per_station.saturating_sub(snapshot.entities.len()));
            }
        }
    }

    /// Snapshot slots retained across exports.
    pub fn retained_snapshot_slots(&self) -> usize {
        self.snapshots.len()
    }

    /// Total entity capacity retained across Station snapshot slots.
    pub fn retained_entity_capacity(&self) -> usize {
        self.snapshots
            .iter()
            .map(|snapshot| snapshot.entities.capacity())
            .sum()
    }

    fn prepare(&mut self, stations: usize) {
        if self.snapshots.len() < stations {
            self.snapshots
                .resize_with(stations, StationSnapshot::default);
        }
        self.active_snapshots = stations;
    }

    fn active(&self) -> &[StationSnapshot] {
        &self.snapshots[..self.active_snapshots]
    }
}

impl core::fmt::Display for BarrierRuntimeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::AlreadyActive(id) => write!(f, "barrier {} is already active", id.get()),
            Self::NoActiveBarrier => f.write_str("no active barrier"),
            Self::EmptyScope(scope) => write!(f, "barrier scope {scope:?} contains no stations"),
            Self::NotFrozen(state) => {
                write!(f, "barrier operation requires Frozen state, got {state:?}")
            }
            Self::MissingStation(id) => write!(f, "barrier station {} is missing", id.get()),
        }
    }
}

impl std::error::Error for BarrierRuntimeError {}

/// Full runtime barrier executor for in-process station sets.
#[derive(Clone, Debug, Default)]
pub struct BarrierController {
    active: Option<RuntimeBarrier>,
    phases: BTreeMap<StationId, StationBarrierPhase>,
    metrics: BarrierMetrics,
}

impl BarrierController {
    /// Returns the active barrier, if any.
    pub const fn active(&self) -> Option<RuntimeBarrier> {
        self.active
    }

    /// Requests a barrier over stations matching `scope`.
    pub fn request(
        &mut self,
        stations: &StationSet,
        id: BarrierId,
        scope: BarrierScope,
        target_tick: Tick,
        command_mode: CommandQueueMode,
    ) -> Result<BarrierProgress, BarrierRuntimeError> {
        if let Some(active) = self.active {
            return Err(BarrierRuntimeError::AlreadyActive(active.id));
        }

        let station_ids = stations.station_ids_in_scope(scope);
        if station_ids.is_empty() {
            return Err(BarrierRuntimeError::EmptyScope(scope));
        }

        let requested_at = station_ids
            .iter()
            .filter_map(|station_id| stations.get(*station_id).map(Station::tick))
            .map(Tick::get)
            .max()
            .map_or(Tick::new(0), Tick::new);

        let mut barrier =
            RuntimeBarrier::requested(id, scope, requested_at, target_tick, command_mode);
        barrier.wait_for_tick_boundary();

        self.metrics = BarrierMetrics {
            station_count: station_ids.len(),
            ..BarrierMetrics::default()
        };
        self.phases.clear();
        for station_id in station_ids {
            self.phases
                .insert(station_id, StationBarrierPhase::WaitingTick);
        }
        self.active = Some(barrier);

        Ok(self.progress())
    }

    /// Polls station ticks and freezes the barrier once all covered stations are aligned.
    pub fn poll(&mut self, stations: &StationSet) -> Result<BarrierProgress, BarrierRuntimeError> {
        let Some(mut barrier) = self.active else {
            return Err(BarrierRuntimeError::NoActiveBarrier);
        };

        if matches!(barrier.state, BarrierState::Frozen) {
            self.metrics.frozen_polls = self.metrics.frozen_polls.saturating_add(1);
            return Ok(self.progress());
        }

        let mut all_ready = true;
        for (station_id, phase) in &mut self.phases {
            let station = stations
                .get(*station_id)
                .ok_or(BarrierRuntimeError::MissingStation(*station_id))?;
            if station.tick() >= barrier.target_tick {
                *phase = StationBarrierPhase::Frozen;
            } else {
                all_ready = false;
            }
        }

        if all_ready {
            barrier.freeze();
            self.active = Some(barrier);
            self.metrics.frozen_polls = self.metrics.frozen_polls.saturating_add(1);
        } else {
            self.metrics.waiting_polls = self.metrics.waiting_polls.saturating_add(1);
        }

        Ok(self.progress())
    }

    /// Exports station snapshots while the barrier is frozen.
    pub fn export_snapshots(
        &mut self,
        stations: &StationSet,
        version: SnapshotVersion,
    ) -> Result<Vec<StationSnapshot>, BarrierRuntimeError> {
        let barrier = self.active.ok_or(BarrierRuntimeError::NoActiveBarrier)?;
        if barrier.state != BarrierState::Frozen {
            return Err(BarrierRuntimeError::NotFrozen(barrier.state));
        }

        let mut snapshots = Vec::with_capacity(self.phases.len());
        for station_id in self.phases.keys().copied() {
            let station = stations
                .get(station_id)
                .ok_or(BarrierRuntimeError::MissingStation(station_id))?;
            snapshots.push(station.snapshot(version));
        }
        self.metrics.snapshots_exported = self
            .metrics
            .snapshots_exported
            .saturating_add(snapshots.len());
        Ok(snapshots)
    }

    /// Exports frozen snapshots into caller-owned reusable Station slots.
    pub fn export_snapshots_into<'a>(
        &mut self,
        stations: &StationSet,
        version: SnapshotVersion,
        scratch: &'a mut BarrierSnapshotScratch,
    ) -> Result<&'a [StationSnapshot], BarrierRuntimeError> {
        let barrier = self.active.ok_or(BarrierRuntimeError::NoActiveBarrier)?;
        if barrier.state != BarrierState::Frozen {
            return Err(BarrierRuntimeError::NotFrozen(barrier.state));
        }

        scratch.prepare(self.phases.len());
        for (snapshot, station_id) in scratch.snapshots[..scratch.active_snapshots]
            .iter_mut()
            .zip(self.phases.keys().copied())
        {
            let station = stations
                .get(station_id)
                .ok_or(BarrierRuntimeError::MissingStation(station_id))?;
            station.snapshot_into(version, snapshot);
        }
        self.metrics.snapshots_exported = self
            .metrics
            .snapshots_exported
            .saturating_add(scratch.active_snapshots);
        Ok(scratch.active())
    }

    /// Resumes all stations covered by the barrier and returns final metrics.
    pub fn resume(&mut self) -> Result<BarrierMetrics, BarrierRuntimeError> {
        let Some(mut barrier) = self.active else {
            return Err(BarrierRuntimeError::NoActiveBarrier);
        };
        if barrier.state != BarrierState::Frozen {
            return Err(BarrierRuntimeError::NotFrozen(barrier.state));
        }

        barrier.resume();
        for phase in self.phases.values_mut() {
            *phase = StationBarrierPhase::Resumed;
        }
        barrier.finish();
        let metrics = self.metrics;
        self.active = None;
        self.phases.clear();
        self.metrics = BarrierMetrics::default();
        Ok(metrics)
    }

    /// Returns current barrier progress.
    pub fn progress(&self) -> BarrierProgress {
        let state = self
            .active
            .map_or(BarrierState::Running, |barrier| barrier.state);
        let target_tick = self
            .active
            .map_or(Tick::new(0), |barrier| barrier.target_tick);
        let frozen_count = self
            .phases
            .values()
            .filter(|phase| matches!(phase, StationBarrierPhase::Frozen))
            .count();

        BarrierProgress {
            state,
            station_count: self.phases.len(),
            frozen_count,
            target_tick,
        }
    }
}

/// Report produced after applying an external upgrade hook to frozen snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BarrierUpgradeReport {
    /// Snapshot version requested for export.
    pub version: SnapshotVersion,
    /// Snapshots passed through the upgrade hook.
    pub snapshots_migrated: usize,
    /// Stations restored from migrated snapshots.
    pub stations_restored: usize,
    /// Entity records restored across all stations.
    pub entities_restored: usize,
}

/// Error produced while applying an upgrade hook around a frozen barrier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BarrierUpgradeError {
    /// Barrier was missing or not frozen.
    Barrier(BarrierRuntimeError),
    /// Station disappeared between snapshot export and restore.
    MissingStation(StationId),
    /// Restoring a migrated snapshot failed.
    Restore {
        /// Station being restored.
        station_id: StationId,
        /// Restore error.
        error: StationError,
    },
}

impl core::fmt::Display for BarrierUpgradeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Barrier(error) => write!(f, "{error}"),
            Self::MissingStation(station_id) => {
                write!(f, "upgrade station {} is missing", station_id.get())
            }
            Self::Restore { station_id, error } => {
                write!(
                    f,
                    "upgrade restore for station {} failed: {error}",
                    station_id.get()
                )
            }
        }
    }
}

impl std::error::Error for BarrierUpgradeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Barrier(error) => Some(error),
            Self::Restore { error, .. } => Some(error),
            Self::MissingStation(_) => None,
        }
    }
}

impl From<BarrierRuntimeError> for BarrierUpgradeError {
    fn from(value: BarrierRuntimeError) -> Self {
        Self::Barrier(value)
    }
}

/// Applies an external in-memory upgrade hook while a runtime barrier is frozen.
#[derive(Clone, Copy, Debug, Default)]
pub struct BarrierUpgradeExecutor;

impl BarrierUpgradeExecutor {
    /// Exports frozen station snapshots, lets `hook` migrate them, and restores
    /// every station only after all migrated snapshots are valid.
    pub fn migrate_frozen<H>(
        controller: &mut BarrierController,
        stations: &mut StationSet,
        version: SnapshotVersion,
        hook: &mut H,
    ) -> Result<BarrierUpgradeReport, BarrierUpgradeError>
    where
        H: RuntimeUpgradeHook,
    {
        let report_version = version;
        let snapshots = controller.export_snapshots(stations, version)?;
        let mut restored = Vec::with_capacity(snapshots.len());
        let mut entities_restored = 0usize;

        for snapshot in snapshots {
            let station_id = snapshot.meta.station_id;
            let config = stations
                .get(station_id)
                .ok_or(BarrierUpgradeError::MissingStation(station_id))?
                .config();
            hook.pre_upgrade(&snapshot.meta);
            let migrated = hook.migrate_state(snapshot);
            let migrated_meta = migrated.meta.clone();
            let restored_station = Station::restore(config, migrated)
                .map_err(|error| BarrierUpgradeError::Restore { station_id, error })?;
            entities_restored = entities_restored.saturating_add(restored_station.len());
            hook.post_upgrade(&migrated_meta);
            restored.push((station_id, restored_station));
        }

        let stations_restored = restored.len();
        for (station_id, restored_station) in restored {
            let station = stations
                .get_mut(station_id)
                .ok_or(BarrierUpgradeError::MissingStation(station_id))?;
            *station = restored_station;
        }

        Ok(BarrierUpgradeReport {
            version: report_version,
            snapshots_migrated: stations_restored,
            stations_restored,
            entities_restored,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sectorsync_core::prelude::{
        Bounds, CellCoord3, CellLoadSample, CommandEnvelope, CommandPriority, CommandQueueLimits,
        ComponentId, EventId, EventKind, EventPriority, GatewayConfig, GridSpec, HotspotThresholds,
        InstanceId, NodeId, PolicyId, Position3, SnapshotMeta, StationConfig, StationLoadSample,
    };
    use sectorsync_transport::{
        ClientTransportLimits, InMemoryStationTransport, InMemoryTransportHub, OutboundPacket,
        StationOutboundPacket, StationTransportSink, TransportReceiver, TransportSink,
    };
    use sectorsync_wire::{
        BarrierFrame, BinaryFrameDecoder, BinaryFrameEncoder, CommandAckFrame,
        CommandDispatchFrame, CommandFrame, ComponentDelta, EntityDelta, FrameDecoder,
        FrameEncoder, ReplicationFrame,
    };

    fn station(station_id: u32, instance_id: u64) -> Station {
        Station::new(StationConfig {
            station_id: StationId::new(station_id),
            node_id: NodeId::new(0),
            instance_id: InstanceId::new(instance_id),
            tick_rate_hz: 20,
        })
    }

    fn encode_command_frame(sequence: u64) -> Vec<u8> {
        let frame = CommandFrame {
            client_id: ClientId::new(7),
            command_id: CommandId::new(sequence),
            entity_id: EntityId::new(100),
            sequence,
            kind: 1,
            priority: CommandPriority::High,
            payload: b"move:north".to_vec(),
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command(&frame, &mut bytes)
            .expect("command should encode");
        bytes
    }

    fn command_queues() -> CommandQueues {
        CommandQueues::new(CommandQueueLimits {
            high: 4,
            normal: 4,
            low: 4,
        })
    }

    fn gateway(max_commands_per_tick: usize) -> GatewaySessionTable {
        GatewaySessionTable::new(GatewayConfig {
            max_sessions: 8,
            reconnect_grace_ticks: 10,
            max_commands_per_tick,
        })
    }

    #[test]
    fn station_set_indexes_first_slot_and_reserves_both_storage_classes() {
        let mut stations = StationSet::with_capacity(3);
        let mut duplicate = station(1, 99);
        duplicate.advance_tick();
        stations.push(station(1, 10));
        stations.push(duplicate);
        stations.push(station(2, 10));

        assert!(stations.station_capacity() >= 3);
        assert!(!stations.lookup_index_active());
        assert_eq!(
            stations
                .get(StationId::new(1))
                .expect("first exists")
                .tick(),
            Tick::new(0)
        );
        let (first, second) = stations
            .get_pair_mut(StationId::new(1), StationId::new(2))
            .expect("distinct indexed Stations should borrow");
        first.advance_tick();
        second.advance_tick();
        assert_eq!(
            stations
                .get(StationId::new(1))
                .expect("first exists")
                .tick(),
            Tick::new(1)
        );
        assert_eq!(
            stations
                .get(StationId::new(2))
                .expect("second exists")
                .tick(),
            Tick::new(1)
        );

        let lookup_capacity = stations.lookup_capacity();
        stations.reserve(4);
        assert!(stations.station_capacity() >= stations.len().saturating_add(4));
        assert!(stations.lookup_capacity() >= lookup_capacity);
    }

    #[test]
    fn station_index_set_replaces_in_place_and_indexes_mutable_pairs() {
        let grid = GridSpec::new(10.0).expect("grid should build");
        let first_id = StationId::new(1);
        let second_id = StationId::new(2);
        let first_handle = EntityHandle::new(1, 0);
        let second_handle = EntityHandle::new(2, 0);
        let mut indexes = StationIndexSet::with_capacity(2);
        indexes.insert(first_id, CellIndex::new(grid));
        indexes.insert(second_id, CellIndex::new(grid));

        let mut replacement = CellIndex::new(grid);
        replacement.upsert(first_handle, Position3::new(1.0, 0.0, 0.0), Bounds::Point);
        indexes.insert(first_id, replacement);
        assert_eq!(indexes.len(), 2);
        assert_eq!(
            indexes.iter().map(|(id, _)| id).collect::<Vec<_>>(),
            vec![first_id, second_id]
        );
        assert_eq!(
            indexes
                .get(first_id)
                .expect("first index exists")
                .entity_count(),
            1
        );

        let (first, second) = indexes
            .get_pair_mut(first_id, second_id)
            .expect("distinct indexed cells should borrow");
        first.remove(first_handle);
        second.upsert(second_handle, Position3::new(11.0, 0.0, 0.0), Bounds::Point);
        assert_eq!(
            indexes
                .get(first_id)
                .expect("first index exists")
                .entity_count(),
            0
        );
        assert_eq!(
            indexes
                .get(second_id)
                .expect("second index exists")
                .entity_count(),
            1
        );
        assert!(indexes.index_capacity() >= 2);
        assert!(!indexes.lookup_index_active());
    }

    #[test]
    fn station_registries_activate_lookup_index_at_adaptive_threshold() {
        let grid = GridSpec::new(10.0).expect("grid should build");
        let mut stations = StationSet::with_capacity(STATION_LOOKUP_INDEX_THRESHOLD);
        let mut indexes = StationIndexSet::with_capacity(STATION_LOOKUP_INDEX_THRESHOLD);
        for raw_id in 1..=STATION_LOOKUP_INDEX_THRESHOLD {
            let station_id = StationId::new(u32::try_from(raw_id).expect("threshold fits u32"));
            stations.push(station(station_id.get(), 10));
            indexes.insert(station_id, CellIndex::new(grid));
            if raw_id < STATION_LOOKUP_INDEX_THRESHOLD {
                assert!(!stations.lookup_index_active());
                assert!(!indexes.lookup_index_active());
            }
        }

        assert!(stations.lookup_index_active());
        assert!(indexes.lookup_index_active());
        assert!(stations.lookup_capacity() >= STATION_LOOKUP_INDEX_THRESHOLD);
        assert!(indexes.lookup_capacity() >= STATION_LOOKUP_INDEX_THRESHOLD);
        let last = StationId::new(
            u32::try_from(STATION_LOOKUP_INDEX_THRESHOLD).expect("threshold fits u32"),
        );
        assert_eq!(
            stations
                .get(last)
                .expect("last Station exists")
                .config()
                .station_id,
            last
        );
        assert!(indexes.get(last).is_some());

        let removed_id = StationId::new(2);
        let removed_station = stations.remove(removed_id).expect("Station should remove");
        let removed_index = indexes.remove(removed_id).expect("index should remove");
        assert_eq!(removed_station.config().station_id, removed_id);
        assert_eq!(removed_index.entity_count(), 0);
        assert!(stations.get(removed_id).is_none());
        assert!(indexes.get(removed_id).is_none());
        assert_eq!(
            stations
                .get(last)
                .expect("shifted Station resolves")
                .config()
                .station_id,
            last
        );
        assert!(indexes.get(last).is_some());
        assert_eq!(
            stations
                .iter()
                .map(|station| station.config().station_id)
                .nth(1),
            Some(StationId::new(3))
        );
        assert_eq!(
            indexes.iter().nth(1).map(|(id, _)| id),
            Some(StationId::new(3))
        );
        assert!(stations.lookup_index_active());
        assert!(indexes.lookup_index_active());
    }

    #[test]
    fn barrier_freezes_snapshots_and_resumes_instance_scope() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));

        for station in stations.iter_mut() {
            station.advance_tick();
            station.advance_tick();
        }

        let mut controller = BarrierController::default();
        let requested = controller
            .request(
                &stations,
                BarrierId::new(7),
                BarrierScope::Instance(InstanceId::new(10)),
                Tick::new(2),
                CommandQueueMode::Buffer,
            )
            .expect("request should work");
        assert_eq!(requested.state, BarrierState::WaitingTickBoundary);

        let frozen = controller.poll(&stations).expect("poll should work");
        assert_eq!(frozen.state, BarrierState::Frozen);
        assert_eq!(frozen.frozen_count, 2);

        let mut scratch = BarrierSnapshotScratch::new();
        scratch.reserve(2, 1);
        let snapshots = controller
            .export_snapshots_into(&stations, SnapshotVersion::default(), &mut scratch)
            .expect("reusable snapshot should work while frozen");
        assert_eq!(snapshots.len(), 2);
        let retained_slots = scratch.retained_snapshot_slots();
        let retained_entities = scratch.retained_entity_capacity();
        scratch.reserve(2, 1);
        let snapshots = controller
            .export_snapshots_into(&stations, SnapshotVersion::default(), &mut scratch)
            .expect("second reusable snapshot should work while frozen");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(scratch.retained_snapshot_slots(), retained_slots);
        assert_eq!(scratch.retained_entity_capacity(), retained_entities);

        let metrics = controller.resume().expect("resume should work");
        assert_eq!(metrics.station_count, 2);
        assert_eq!(metrics.snapshots_exported, 4);
        assert_eq!(controller.progress().state, BarrierState::Running);
    }

    #[derive(Default)]
    struct MoveSnapshotUpgrade {
        pre: usize,
        migrations: usize,
        post: usize,
    }

    impl RuntimeUpgradeHook for MoveSnapshotUpgrade {
        fn pre_upgrade(&mut self, meta: &SnapshotMeta) {
            self.pre = self.pre.saturating_add(1);
            assert_eq!(meta.version.runtime_version, 2);
        }

        fn migrate_state(&mut self, mut snapshot: StationSnapshot) -> StationSnapshot {
            self.migrations = self.migrations.saturating_add(1);
            for entity in &mut snapshot.entities {
                entity.position.x += 10.0;
            }
            snapshot
        }

        fn post_upgrade(&mut self, meta: &SnapshotMeta) {
            self.post = self.post.saturating_add(1);
            assert_eq!(meta.version.runtime_version, 2);
        }
    }

    #[test]
    fn barrier_upgrade_executor_migrates_and_restores_frozen_snapshots() {
        let mut first = station(1, 10);
        first
            .spawn_owned(
                EntityId::new(100),
                Position3::new(1.0, 2.0, 3.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        let mut stations = StationSet::default();
        stations.push(first);
        stations.push(station(2, 10));

        for station in stations.iter_mut() {
            station.advance_tick();
            station.advance_tick();
        }

        let mut controller = BarrierController::default();
        controller
            .request(
                &stations,
                BarrierId::new(8),
                BarrierScope::Instance(InstanceId::new(10)),
                Tick::new(2),
                CommandQueueMode::Buffer,
            )
            .expect("request should work");
        assert_eq!(
            controller.poll(&stations).expect("poll should work").state,
            BarrierState::Frozen
        );

        let mut hook = MoveSnapshotUpgrade::default();
        let version = SnapshotVersion {
            runtime_version: 2,
            ..SnapshotVersion::default()
        };
        let report = BarrierUpgradeExecutor::migrate_frozen(
            &mut controller,
            &mut stations,
            version,
            &mut hook,
        )
        .expect("upgrade should migrate frozen snapshots");

        assert_eq!(report.version, version);
        assert_eq!(report.snapshots_migrated, 2);
        assert_eq!(report.stations_restored, 2);
        assert_eq!(report.entities_restored, 1);
        assert_eq!(hook.pre, 2);
        assert_eq!(hook.migrations, 2);
        assert_eq!(hook.post, 2);
        let moved = stations
            .get(StationId::new(1))
            .expect("station should exist")
            .get_by_id(EntityId::new(100))
            .expect("entity should restore");
        assert_eq!(moved.position, Position3::new(11.0, 2.0, 3.0));
        assert_eq!(controller.progress().state, BarrierState::Frozen);

        let metrics = controller.resume().expect("resume should work");
        assert_eq!(metrics.snapshots_exported, 2);
        assert_eq!(controller.progress().state, BarrierState::Running);
    }

    #[test]
    fn barrier_transport_bridge_broadcasts_client_notifications() {
        let server_id = ClientId::new(0);
        let clients = [ClientId::new(7), ClientId::new(8)];
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 4,
            max_packet_bytes: 512,
        });
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23400".parse().expect("server addr"))
            .expect("server endpoint should register");
        let mut client_transports = clients
            .into_iter()
            .enumerate()
            .map(|(index, client_id)| {
                hub.endpoint(
                    client_id,
                    format!("127.0.0.1:{}", 23407 + index)
                        .parse()
                        .expect("client addr"),
                )
                .expect("client endpoint should register")
            })
            .collect::<Vec<_>>();
        let mut barrier = RuntimeBarrier::requested(
            BarrierId::new(5),
            BarrierScope::Instance(InstanceId::new(10)),
            Tick::new(10),
            Tick::new(12),
            CommandQueueMode::Buffer,
        );
        barrier.wait_for_tick_boundary();
        barrier.freeze();

        let mut bridge = BarrierTransportBridge::default();
        let report = bridge
            .broadcast_barrier(&mut server_transport, clients, barrier)
            .expect("barrier should broadcast");

        assert_eq!(report.barrier_id, barrier.id);
        assert_eq!(report.state, BarrierState::Frozen);
        assert_eq!(report.server_tick, Tick::new(12));
        assert_eq!(report.clients_requested, 2);
        assert_eq!(report.clients_sent, 2);
        assert!(report.bytes_sent > 0);
        assert_eq!(bridge.stats().notifications_sent, 2);
        assert_eq!(bridge.stats().clients_notified, 2);
        assert_eq!(bridge.stats().bytes_sent, report.bytes_sent);

        for (index, client_id) in clients.into_iter().enumerate() {
            let mut client_bridge = ClientTransportBridge::new(
                ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
            );
            let pump = client_bridge
                .pump_owned(&mut client_transports[index], 2)
                .expect("client should receive barrier");
            assert_eq!(pump.barrier_frames_received(), 1);
            assert_eq!(
                pump.barriers[0],
                BarrierFrame {
                    client_id,
                    barrier_id: barrier.id,
                    server_tick: barrier.target_tick,
                    state: BarrierState::Frozen,
                }
            );
        }
    }

    #[test]
    fn replication_receive_bridge_decodes_target_frames() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 4,
            max_packet_bytes: 512,
        });
        let mut client_transport = hub
            .endpoint(client_id, "127.0.0.1:23007".parse().expect("client addr"))
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23000".parse().expect("server addr"))
            .expect("server endpoint should register");
        let frame = ReplicationFrame {
            client_id,
            server_tick: Tick::new(12),
            entity_count: 1,
            estimated_payload_bytes: 4,
            entities: vec![EntityDelta {
                entity_id: EntityId::new(100),
                owner_epoch: OwnerEpoch::new(1),
                components: vec![ComponentDelta {
                    component_id: ComponentId::new(1),
                    version: 1,
                    flags: 0,
                    bytes: 100_u32.to_le_bytes().to_vec(),
                }],
            }],
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_replication(&frame, &mut bytes)
            .expect("replication should encode");
        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: bytes.clone(),
            })
            .expect("replication packet should send");

        let mut receive = ReplicationReceiveBridge::new(
            ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
        );
        let pump = receive
            .pump_owned(&mut client_transport, 4)
            .expect("replication packet should receive");

        assert_eq!(pump.frames_received(), 1);
        assert_eq!(pump.entities_received(), 1);
        assert_eq!(pump.components_received(), 1);
        assert_eq!(pump.frames[0].client_id, client_id);
        assert_eq!(receive.stats().packets_received, 1);
        assert_eq!(receive.stats().frames_received, 1);
        assert_eq!(receive.stats().entities_received, 1);
        assert_eq!(receive.stats().components_received, 1);
        assert!(receive.stats().bytes_received > 0);

        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: bytes.clone(),
            })
            .expect("visitor packet should send");
        let mut visited_payload = 0_u32;
        let visit = receive
            .pump(&mut client_transport, 4, |borrowed| {
                assert_eq!(borrowed.client_id, client_id);
                for entity in borrowed.entities() {
                    assert_eq!(entity.entity_id, EntityId::new(100));
                    for component in entity.components() {
                        visited_payload = u32::from_le_bytes(
                            component
                                .bytes
                                .try_into()
                                .expect("health payload should be four bytes"),
                        );
                    }
                }
                Ok::<_, core::convert::Infallible>(())
            })
            .expect("borrowed replication packet should visit");
        assert_eq!(visited_payload, 100);
        assert_eq!(visit.packets_received, 1);
        assert_eq!(visit.frames_received, 1);
        assert_eq!(visit.entities_received, 1);
        assert_eq!(visit.components_received, 1);

        server_transport
            .send(OutboundPacket { client_id, bytes })
            .expect("visitor failure packet should send");
        let error = receive
            .pump(&mut client_transport, 4, |_| Err("apply failed"))
            .expect_err("visitor failure should surface separately");
        assert!(matches!(
            error,
            ReplicationReceiveVisitError::Visitor("apply failed")
        ));
        assert_eq!(receive.stats().packets_received, 3);
        assert_eq!(receive.stats().frames_received, 3);
        assert_eq!(receive.stats().entities_received, 3);
        assert_eq!(receive.stats().components_received, 3);
    }

    #[test]
    fn replication_receive_bridge_rejects_wrong_target() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let wrong_client_id = ClientId::new(99);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 4,
            max_packet_bytes: 512,
        });
        let mut client_transport = hub
            .endpoint(client_id, "127.0.0.1:23107".parse().expect("client addr"))
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23100".parse().expect("server addr"))
            .expect("server endpoint should register");
        let frame = ReplicationFrame {
            client_id: wrong_client_id,
            server_tick: Tick::new(12),
            entity_count: 1,
            estimated_payload_bytes: 4,
            entities: vec![EntityDelta {
                entity_id: EntityId::new(100),
                owner_epoch: OwnerEpoch::new(1),
                components: vec![ComponentDelta {
                    component_id: ComponentId::new(1),
                    version: 1,
                    flags: 0,
                    bytes: 100_u32.to_le_bytes().to_vec(),
                }],
            }],
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_replication(&frame, &mut bytes)
            .expect("replication should encode");
        server_transport
            .send(OutboundPacket { client_id, bytes })
            .expect("replication packet should send");

        let mut receive = ReplicationReceiveBridge::new(
            ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
        );
        let error = receive
            .pump_owned(&mut client_transport, 4)
            .expect_err("wrong target should be rejected");

        assert!(matches!(
            error,
            ReplicationReceiveError::TargetMismatch {
                expected,
                actual,
            } if expected == client_id && actual == wrong_client_id
        ));
        assert_eq!(receive.stats().packets_received, 1);
        assert_eq!(receive.stats().frames_received, 0);
        assert_eq!(receive.stats().frames_rejected_target, 1);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn client_transport_bridge_sends_command_and_receives_client_frames() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 8,
            max_packet_bytes: 512,
        });
        let mut client_transport = hub
            .endpoint(client_id, "127.0.0.1:23207".parse().expect("client addr"))
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23200".parse().expect("server addr"))
            .expect("server endpoint should register");
        let mut bridge = ClientTransportBridge::new(
            ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
        );
        let command = CommandFrame {
            client_id,
            command_id: CommandId::new(42),
            entity_id: EntityId::new(100),
            sequence: 9,
            kind: 1,
            priority: CommandPriority::High,
            payload: b"move:north".to_vec(),
        };

        let send = bridge
            .send_command_frame(&mut client_transport, &command)
            .expect("command should send");
        assert_eq!(send.command_id, command.command_id);
        assert!(send.bytes_sent > 0);
        assert_eq!(bridge.stats().commands_sent, 1);
        assert_eq!(bridge.stats().command_bytes_sent, send.bytes_sent);
        let inbound = server_transport
            .try_recv()
            .expect("server receive should work")
            .expect("command packet should arrive");
        assert_eq!(inbound.client_id, Some(client_id));
        let RuntimeFrame::Command(decoded) = BinaryFrameDecoder
            .decode(&inbound.bytes)
            .expect("command should decode")
        else {
            panic!("expected command frame");
        };
        assert_eq!(decoded, command);

        let ack = CommandAckFrame {
            client_id,
            command_id: command.command_id,
            server_tick: Tick::new(12),
            accepted: true,
            reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
        };
        let mut ack_bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command_ack(&ack, &mut ack_bytes)
            .expect("ACK should encode");
        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: ack_bytes.clone(),
            })
            .expect("ACK should send");

        let replication = ReplicationFrame {
            client_id,
            server_tick: Tick::new(12),
            entity_count: 1,
            estimated_payload_bytes: 4,
            entities: vec![EntityDelta {
                entity_id: EntityId::new(100),
                owner_epoch: OwnerEpoch::new(1),
                components: vec![ComponentDelta {
                    component_id: ComponentId::new(1),
                    version: 1,
                    flags: 0,
                    bytes: 100_u32.to_le_bytes().to_vec(),
                }],
            }],
        };
        let mut replication_bytes = Vec::new();
        BinaryFrameEncoder
            .encode_replication(&replication, &mut replication_bytes)
            .expect("replication should encode");
        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: replication_bytes.clone(),
            })
            .expect("replication should send");

        let barrier = BarrierFrame {
            client_id,
            barrier_id: BarrierId::new(5),
            server_tick: Tick::new(12),
            state: BarrierState::Frozen,
        };
        let mut barrier_bytes = Vec::new();
        BinaryFrameEncoder
            .encode_barrier(&barrier, &mut barrier_bytes)
            .expect("barrier should encode");
        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: barrier_bytes.clone(),
            })
            .expect("barrier should send");

        let pump = bridge
            .pump_owned(&mut client_transport, 8)
            .expect("client frames should receive");

        assert_eq!(pump.packets_received, 3);
        assert_eq!(pump.command_acks_received(), 1);
        assert_eq!(pump.replication_frames_received(), 1);
        assert_eq!(pump.barrier_frames_received(), 1);
        assert_eq!(pump.entities_received(), 1);
        assert_eq!(pump.components_received(), 1);
        assert_eq!(pump.command_acks[0], ack);
        assert_eq!(pump.replication_frames[0], replication);
        assert_eq!(pump.barriers[0], barrier);
        assert_eq!(bridge.stats().packets_received, 3);
        assert_eq!(bridge.stats().command_acks_received, 1);
        assert_eq!(bridge.stats().replication_frames_received, 1);
        assert_eq!(bridge.stats().barrier_frames_received, 1);
        assert_eq!(bridge.stats().entities_received, 1);
        assert_eq!(bridge.stats().components_received, 1);

        for bytes in [ack_bytes.clone(), replication_bytes, barrier_bytes] {
            server_transport
                .send(OutboundPacket { client_id, bytes })
                .expect("visitor packet should send");
        }
        let mut visited_ack = 0_usize;
        let mut visited_replication = 0_usize;
        let mut visited_barrier = 0_usize;
        let mut payload_checksum = 0_u64;
        let visit = bridge
            .pump(&mut client_transport, 8, |frame| {
                match frame {
                    ClientInboundFrameRef::CommandAck(frame) => {
                        assert_eq!(frame, ack);
                        visited_ack = visited_ack.saturating_add(1);
                    }
                    ClientInboundFrameRef::Replication(frame) => {
                        assert_eq!(frame.client_id, client_id);
                        assert_eq!(frame.encoded_entity_count(), 1);
                        for entity in frame.entities() {
                            for component in entity.components() {
                                payload_checksum = payload_checksum.saturating_add(
                                    component.bytes.iter().map(|byte| u64::from(*byte)).sum(),
                                );
                            }
                        }
                        visited_replication = visited_replication.saturating_add(1);
                    }
                    ClientInboundFrameRef::Barrier(frame) => {
                        assert_eq!(frame, barrier);
                        visited_barrier = visited_barrier.saturating_add(1);
                    }
                }
                Ok::<(), &'static str>(())
            })
            .expect("mixed visitor pump should work");
        assert_eq!(visit.packets_received, 3);
        assert_eq!(visit.command_acks_received, 1);
        assert_eq!(visit.replication_frames_received, 1);
        assert_eq!(visit.barrier_frames_received, 1);
        assert_eq!(visit.entities_received, 1);
        assert_eq!(visit.components_received, 1);
        assert_eq!(
            (visited_ack, visited_replication, visited_barrier),
            (1, 1, 1)
        );
        assert_eq!(payload_checksum, 100);
        assert_eq!(bridge.stats().packets_received, 6);

        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: ack_bytes,
            })
            .expect("failing visitor packet should send");
        let visitor_error = bridge
            .pump(&mut client_transport, 1, |_| Err("apply failed"))
            .expect_err("visitor failure should propagate");
        assert_eq!(
            visitor_error,
            ClientTransportVisitError::Visitor("apply failed")
        );
        assert_eq!(bridge.stats().packets_received, 7);
        assert_eq!(bridge.stats().command_acks_received, 3);
    }

    #[test]
    fn client_transport_bridge_rejects_wrong_ack_target() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let wrong_client_id = ClientId::new(99);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 4,
            max_packet_bytes: 512,
        });
        let mut client_transport = hub
            .endpoint(client_id, "127.0.0.1:23307".parse().expect("client addr"))
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23300".parse().expect("server addr"))
            .expect("server endpoint should register");
        let mut bridge = ClientTransportBridge::new(
            ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
        );
        let ack = CommandAckFrame {
            client_id: wrong_client_id,
            command_id: CommandId::new(42),
            server_tick: Tick::new(12),
            accepted: true,
            reason_code: GATEWAY_COMMAND_ACK_ACCEPTED,
        };
        let mut ack_bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command_ack(&ack, &mut ack_bytes)
            .expect("ACK should encode");
        server_transport
            .send(OutboundPacket {
                client_id,
                bytes: ack_bytes,
            })
            .expect("ACK should send");

        let error = bridge
            .pump_owned(&mut client_transport, 4)
            .expect_err("wrong target should be rejected");

        assert!(matches!(
            error,
            ClientTransportBridgeError::TargetMismatch {
                kind: ClientInboundFrameKind::CommandAck,
                expected,
                actual,
            } if expected == client_id && actual == wrong_client_id
        ));
        assert_eq!(bridge.stats().packets_received, 1);
        assert_eq!(bridge.stats().command_acks_received, 0);
        assert_eq!(bridge.stats().frames_rejected_target, 1);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn gateway_client_transport_bridge_queues_command_and_sends_ack() {
        let client_id = ClientId::new(7);
        let server_id = ClientId::new(0);
        let station_id = StationId::new(1);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 8,
            max_packet_bytes: 512,
        });
        let mut client_transport = hub
            .endpoint(client_id, "127.0.0.1:23507".parse().expect("client addr"))
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23500".parse().expect("server addr"))
            .expect("server endpoint should register");
        let mut client_bridge = ClientTransportBridge::new(
            ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
        );
        let command = CommandFrame {
            client_id,
            command_id: CommandId::new(42),
            entity_id: EntityId::new(100),
            sequence: 9,
            kind: 1,
            priority: CommandPriority::High,
            payload: b"move:north".to_vec(),
        };
        client_bridge
            .send_command_frame(&mut client_transport, &command)
            .expect("client command should send");

        let mut gateway = gateway(4);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let mut station_queues = BTreeMap::from([(station_id, command_queues())]);
        let mut pipeline = GatewayCommandPipeline::default();
        let mut gateway_bridge = GatewayClientTransportBridge::default();

        let pump = gateway_bridge
            .pump_ingress(
                &mut server_transport,
                &mut pipeline,
                &mut gateway,
                &mut station_queues,
                Tick::new(10),
                CommandIngress::RUNNING,
                4,
            )
            .expect("gateway client transport should pump");

        assert_eq!(pump.packets_received, 1);
        assert_eq!(pump.commands_processed(), 1);
        assert_eq!(pump.commands_accepted(), 1);
        assert_eq!(pump.acks_sent, 1);
        assert_eq!(gateway_bridge.stats().packets_received, 1);
        assert_eq!(gateway_bridge.stats().command_frames_received, 1);
        assert_eq!(gateway_bridge.stats().commands_accepted, 1);
        assert_eq!(gateway_bridge.stats().acks_sent, 1);
        let queued = station_queues
            .get_mut(&station_id)
            .expect("station queue should exist")
            .pop_next()
            .expect("command should queue");
        assert_eq!(queued.id, command.command_id);

        let ack_pump = client_bridge
            .pump_owned(&mut client_transport, 4)
            .expect("client should receive ACK");
        assert_eq!(ack_pump.command_acks_received(), 1);
        assert!(ack_pump.command_acks[0].accepted);
        assert_eq!(ack_pump.command_acks[0].command_id, command.command_id);

        let compact_command = CommandFrame {
            command_id: CommandId::new(43),
            sequence: 10,
            ..command
        };
        client_bridge
            .send_command_frame(&mut client_transport, &compact_command)
            .expect("second client command should send");
        let summary = gateway_bridge
            .pump_ingress_compact(
                &mut server_transport,
                &mut pipeline,
                &mut gateway,
                &mut station_queues,
                Tick::new(11),
                CommandIngress::RUNNING,
                4,
            )
            .expect("compact gateway transport should pump");
        assert_eq!(summary.packets_received, 1);
        assert_eq!(summary.commands_accepted, 1);
        assert_eq!(summary.commands_rejected, 0);
        assert_eq!(summary.acks_sent, 1);
        assert!(summary.ack_bytes_sent > 0);
        let compact_queued = station_queues
            .get_mut(&station_id)
            .expect("station queue should exist")
            .pop_next()
            .expect("compact command should queue");
        assert_eq!(compact_queued.id, compact_command.command_id);
        let compact_ack = client_bridge
            .pump_owned(&mut client_transport, 4)
            .expect("client should receive compact ACK");
        assert_eq!(compact_ack.command_acks_received(), 1);
        assert_eq!(
            compact_ack.command_acks[0].command_id,
            compact_command.command_id
        );
        assert_eq!(gateway_bridge.stats().packets_received, 2);
        assert_eq!(gateway_bridge.stats().commands_accepted, 2);
        assert_eq!(gateway_bridge.stats().acks_sent, 2);
    }

    #[test]
    fn gateway_client_transport_bridge_rejects_source_mismatch_before_admission() {
        let packet_client_id = ClientId::new(7);
        let frame_client_id = ClientId::new(8);
        let server_id = ClientId::new(0);
        let station_id = StationId::new(1);
        let hub = InMemoryTransportHub::new(ClientTransportLimits {
            max_queued_packets_per_client: 4,
            max_packet_bytes: 512,
        });
        let mut packet_client_transport = hub
            .endpoint(
                packet_client_id,
                "127.0.0.1:23607".parse().expect("client addr"),
            )
            .expect("client endpoint should register");
        let mut server_transport = hub
            .endpoint(server_id, "127.0.0.1:23600".parse().expect("server addr"))
            .expect("server endpoint should register");
        let command = CommandFrame {
            client_id: frame_client_id,
            command_id: CommandId::new(42),
            entity_id: EntityId::new(100),
            sequence: 9,
            kind: 1,
            priority: CommandPriority::High,
            payload: b"move:north".to_vec(),
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command(&command, &mut bytes)
            .expect("command should encode");
        packet_client_transport
            .send(OutboundPacket {
                client_id: server_id,
                bytes,
            })
            .expect("packet should send");

        let mut gateway = gateway(4);
        gateway
            .connect(frame_client_id, station_id, Tick::new(10))
            .expect("frame client should connect");
        let mut station_queues = BTreeMap::from([(station_id, command_queues())]);
        let mut pipeline = GatewayCommandPipeline::default();
        let mut gateway_bridge = GatewayClientTransportBridge::default();

        let error = gateway_bridge
            .pump_ingress(
                &mut server_transport,
                &mut pipeline,
                &mut gateway,
                &mut station_queues,
                Tick::new(10),
                CommandIngress::RUNNING,
                4,
            )
            .expect_err("source mismatch should reject before admission");

        assert!(matches!(
            error,
            GatewayClientTransportError::SourceMismatch {
                packet_client_id: actual_packet,
                frame_client_id: actual_frame,
            } if actual_packet == packet_client_id && actual_frame == frame_client_id
        ));
        assert_eq!(gateway_bridge.stats().source_mismatches, 1);
        assert_eq!(gateway_bridge.stats().commands_accepted, 0);
        assert_eq!(pipeline.stats().commands_admitted, 0);
        assert_eq!(
            station_queues
                .get(&station_id)
                .expect("station queue should exist")
                .total_len(),
            0
        );
    }

    #[test]
    fn gateway_command_pipeline_queues_command_and_encodes_ack() {
        let client_id = ClientId::new(7);
        let station_id = StationId::new(1);
        let mut gateway = gateway(4);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let mut station_queues = BTreeMap::from([(station_id, command_queues())]);
        let mut pipeline = GatewayCommandPipeline::default();

        let report = pipeline.process(
            &mut gateway,
            &mut station_queues,
            &encode_command_frame(1),
            Tick::new(10),
            CommandIngress::RUNNING,
        );

        assert!(report.accepted);
        assert_eq!(report.reason_code, GATEWAY_COMMAND_ACK_ACCEPTED);
        assert_eq!(report.station_id, Some(station_id));
        assert!(report.error.is_none());
        let ack_bytes = report.ack_bytes.expect("ACK should encode");
        let RuntimeFrame::CommandAck(ack) = BinaryFrameDecoder
            .decode(&ack_bytes)
            .expect("ACK should decode")
        else {
            panic!("expected command ACK");
        };
        assert!(ack.accepted);
        assert_eq!(ack.command_id, CommandId::new(1));
        let queued = station_queues
            .get_mut(&station_id)
            .expect("queue should exist")
            .pop_next()
            .expect("command should queue");
        assert_eq!(queued.id, CommandId::new(1));
        assert_eq!(pipeline.stats().commands_admitted, 1);
        assert_eq!(pipeline.stats().commands_enqueued, 1);
        assert_eq!(pipeline.stats().acks_encoded, 1);
    }

    #[test]
    fn gateway_command_pipeline_negative_acks_rate_limit() {
        let client_id = ClientId::new(7);
        let station_id = StationId::new(1);
        let mut gateway = gateway(1);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let mut station_queues = BTreeMap::from([(station_id, command_queues())]);
        let mut pipeline = GatewayCommandPipeline::default();

        assert!(
            pipeline
                .process(
                    &mut gateway,
                    &mut station_queues,
                    &encode_command_frame(1),
                    Tick::new(10),
                    CommandIngress::RUNNING,
                )
                .accepted
        );
        let rejected = pipeline.process(
            &mut gateway,
            &mut station_queues,
            &encode_command_frame(2),
            Tick::new(10),
            CommandIngress::RUNNING,
        );

        assert!(!rejected.accepted);
        assert_eq!(rejected.reason_code, GATEWAY_COMMAND_ACK_RATE_LIMITED);
        assert!(matches!(
            rejected.error,
            Some(GatewayCommandPipelineError::Gateway(
                GatewayError::RateLimited { .. }
            ))
        ));
        let RuntimeFrame::CommandAck(ack) = BinaryFrameDecoder
            .decode(&rejected.ack_bytes.expect("rejection ACK should encode"))
            .expect("ACK should decode")
        else {
            panic!("expected command ACK");
        };
        assert!(!ack.accepted);
        assert_eq!(ack.reason_code, GATEWAY_COMMAND_ACK_RATE_LIMITED);
        assert_eq!(pipeline.stats().commands_rejected_gateway, 1);
    }

    #[test]
    fn gateway_command_pipeline_rejects_missing_station_queue() {
        let client_id = ClientId::new(7);
        let station_id = StationId::new(1);
        let mut gateway = gateway(4);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let mut station_queues = BTreeMap::new();
        let mut pipeline = GatewayCommandPipeline::default();

        let report = pipeline.process(
            &mut gateway,
            &mut station_queues,
            &encode_command_frame(1),
            Tick::new(10),
            CommandIngress::RUNNING,
        );

        assert!(!report.accepted);
        assert_eq!(report.station_id, Some(station_id));
        assert_eq!(report.reason_code, GATEWAY_COMMAND_ACK_MISSING_QUEUE);
        assert!(matches!(
            report.error,
            Some(GatewayCommandPipelineError::MissingQueue(id)) if id == station_id
        ));
        assert_eq!(pipeline.stats().commands_admitted, 1);
        assert_eq!(pipeline.stats().commands_rejected_queue, 1);
    }

    #[test]
    fn gateway_command_pipeline_dispatches_to_deployment_route() {
        let client_id = ClientId::new(7);
        let station_id = StationId::new(1);
        let node_id = NodeId::new(9);
        let mut gateway = gateway(4);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let mut deployment = DeploymentRouteTable::new(DeploymentConfig {
            max_nodes: 4,
            max_stations_per_node: 4,
            stale_after_ticks: 10,
        });
        deployment
            .register_node(node_id, 4, Tick::new(10))
            .expect("node should register");
        deployment
            .assign_station(station_id, node_id, Tick::new(10))
            .expect("station should assign");
        let mut pipeline = GatewayCommandPipeline::default();

        let report = pipeline.dispatch(
            &mut gateway,
            &deployment,
            &encode_command_frame(1),
            Tick::new(12),
        );

        assert!(report.accepted);
        assert_eq!(report.station_id, Some(station_id));
        assert_eq!(report.node_id, Some(node_id));
        let delivery = report.delivery.expect("delivery should resolve");
        assert_eq!(delivery.client_id, client_id);
        assert_eq!(delivery.station_id, station_id);
        assert_eq!(delivery.node_id, node_id);
        assert_eq!(delivery.station_route_epoch, 1);
        assert_eq!(
            report
                .command
                .expect("command should be returned")
                .received_at,
            Tick::new(12)
        );
        let RuntimeFrame::CommandAck(ack) = BinaryFrameDecoder
            .decode(&report.ack_bytes.expect("ACK should encode"))
            .expect("ACK should decode")
        else {
            panic!("expected command ACK");
        };
        assert!(ack.accepted);
        assert_eq!(pipeline.stats().commands_routed_deployment, 1);
    }

    #[test]
    fn gateway_command_pipeline_negative_acks_missing_deployment_route() {
        let client_id = ClientId::new(7);
        let station_id = StationId::new(1);
        let mut gateway = gateway(4);
        gateway
            .connect(client_id, station_id, Tick::new(10))
            .expect("client should connect");
        let deployment = DeploymentRouteTable::default();
        let mut pipeline = GatewayCommandPipeline::default();

        let report = pipeline.dispatch(
            &mut gateway,
            &deployment,
            &encode_command_frame(1),
            Tick::new(12),
        );

        assert!(!report.accepted);
        assert_eq!(report.station_id, Some(station_id));
        assert_eq!(report.reason_code, GATEWAY_COMMAND_ACK_DEPLOYMENT_REJECTED);
        assert!(matches!(
            report.error,
            Some(GatewayCommandPipelineError::Deployment(
                DeploymentError::MissingStation(id)
            )) if id == station_id
        ));
        let RuntimeFrame::CommandAck(ack) = BinaryFrameDecoder
            .decode(&report.ack_bytes.expect("rejection ACK should encode"))
            .expect("ACK should decode")
        else {
            panic!("expected command ACK");
        };
        assert!(!ack.accepted);
        assert_eq!(ack.reason_code, GATEWAY_COMMAND_ACK_DEPLOYMENT_REJECTED);
        assert_eq!(pipeline.stats().commands_rejected_deployment, 1);
    }

    #[test]
    fn gateway_command_pipeline_rejects_non_command_frame() {
        let ack = CommandAckFrame {
            client_id: ClientId::new(7),
            command_id: CommandId::new(1),
            server_tick: Tick::new(10),
            accepted: true,
            reason_code: 0,
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command_ack(&ack, &mut bytes)
            .expect("ACK should encode");
        let mut gateway = gateway(4);
        let mut station_queues = BTreeMap::new();
        let mut pipeline = GatewayCommandPipeline::default();

        let report = pipeline.process(
            &mut gateway,
            &mut station_queues,
            &bytes,
            Tick::new(10),
            CommandIngress::RUNNING,
        );

        assert!(!report.accepted);
        assert!(report.ack_bytes.is_none());
        assert_eq!(
            report.error,
            Some(GatewayCommandPipelineError::NonCommandFrame)
        );
        assert_eq!(pipeline.stats().frames_rejected_non_command, 1);
    }

    #[test]
    fn migration_executor_moves_owner_and_leaves_source_ghost() {
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        source
            .spawn_owned(
                EntityId::new(99),
                Position3::new(1.0, 2.0, 3.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let report = EntityMigrationExecutor::migrate_entity(
            &mut stations,
            EntityId::new(99),
            StationId::new(1),
            StationId::new(2),
            4,
        )
        .expect("migration should work");

        assert_eq!(report.transfer.target_station, StationId::new(2));
        assert!(
            !stations
                .get(StationId::new(1))
                .expect("source")
                .get_by_id(EntityId::new(99))
                .expect("source ghost")
                .is_owned()
        );
        assert!(
            stations
                .get(StationId::new(2))
                .expect("target")
                .get_by_id(EntityId::new(99))
                .expect("target owner")
                .is_owned()
        );
    }

    #[test]
    fn event_router_delays_until_target_tick_and_scheduler_drains() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));

        let mut router = EventRouter::default();
        router.register_stations(&stations);
        router
            .route(StationEvent {
                id: EventId::new(1),
                source: StationId::new(1),
                target: StationId::new(2),
                source_tick: Tick::new(0),
                target_tick: Tick::new(2),
                priority: EventPriority::Critical,
                kind: EventKind::Custom(7),
            })
            .expect("route should work");

        let mut scheduler = StationScheduler::default();
        let mut drained = Vec::new();
        scheduler.advance_all(&mut stations);
        scheduler
            .drain_ready_events_into(&stations, &mut router, &mut drained)
            .expect("drain should work");
        assert!(drained.is_empty());

        scheduler.advance_all(&mut stations);
        scheduler
            .drain_ready_events_into(&stations, &mut router, &mut drained)
            .expect("drain should work");
        assert_eq!(drained.len(), 1);
        let retained_capacity = drained.capacity();

        scheduler
            .drain_ready_events_into(&stations, &mut router, &mut drained)
            .expect("empty drain should work");
        assert!(drained.is_empty());
        assert_eq!(drained.capacity(), retained_capacity);
        assert_eq!(router.stats().routed_events, 1);
        assert_eq!(router.stats().drained_events, 1);
    }

    #[test]
    fn event_router_unregisters_station_and_discards_queued_events() {
        let station_id = StationId::new(2);
        let mut router = EventRouter::default();
        router.register_station(station_id);
        router
            .route(StationEvent {
                id: EventId::new(1),
                source: StationId::new(1),
                target: station_id,
                source_tick: Tick::new(0),
                target_tick: Tick::new(10),
                priority: EventPriority::Important,
                kind: EventKind::Custom(1),
            })
            .expect("event should queue");

        assert_eq!(router.unregister_station(station_id), Some(1));
        assert_eq!(router.unregister_station(station_id), None);
        assert_eq!(router.queued_len(station_id), None);
        assert_eq!(
            router.drain_ready(station_id, Tick::new(10)),
            Err(EventRouterError::MissingTarget(station_id))
        );
    }

    #[test]
    fn station_scheduler_prioritizes_loaded_stations_with_budget() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));
        stations.push(station(3, 10));

        let samples = vec![
            StationLoadSample {
                station_id: StationId::new(1),
                owned_entities: 1,
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(2),
                owned_entities: 100,
                subscribers: 40,
                queued_events: 20,
                tick_cost_units: 500,
                cells: vec![CellLoadSample {
                    cell: CellCoord3::new(0, 0, 0),
                    owned_entities: 90,
                    subscribers: 40,
                    event_pressure: 10,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(3),
                owned_entities: 25,
                subscribers: 10,
                queued_events: 5,
                tick_cost_units: 50,
                ..StationLoadSample::default()
            },
        ];

        let mut scheduler = StationScheduler::default();
        let plan = scheduler.advance_loaded(
            &mut stations,
            &samples,
            StationScheduleConfig {
                max_station_advances_per_step: 2,
            },
        );

        assert_eq!(plan.candidates_considered, 3);
        assert_eq!(plan.stations_selected, 2);
        assert_eq!(plan.total_advances, 2);
        assert_eq!(
            plan.selected
                .iter()
                .map(|candidate| candidate.station_id)
                .collect::<Vec<_>>(),
            vec![StationId::new(2), StationId::new(3)]
        );
        assert_eq!(scheduler.advanced_ticks, 2);
        assert_eq!(
            stations.get(StationId::new(1)).expect("station").tick(),
            Tick::new(0)
        );
        assert_eq!(
            stations.get(StationId::new(2)).expect("station").tick(),
            Tick::new(1)
        );
        assert_eq!(
            stations.get(StationId::new(3)).expect("station").tick(),
            Tick::new(1)
        );
    }

    #[test]
    fn station_scheduler_top_k_matches_full_sort_for_budget_edges() {
        let candidates = (0_u32..257)
            .map(|index| StationScheduleCandidate {
                station_id: StationId::new(index),
                load_score: u64::from(index.wrapping_mul(37) % 23),
                tick_lag: u64::from(index.wrapping_mul(19) % 11),
            })
            .collect::<Vec<_>>();

        for requested in [0, 1, 7, 64, 128, 129, 256, 257, 300] {
            let limit = requested.min(candidates.len());
            let mut expected = candidates.clone();
            expected.sort_by(compare_station_schedule_candidates);
            expected.truncate(limit);
            let mut actual = candidates.clone();
            prioritize_station_candidates(&mut actual, limit);

            assert_eq!(&actual[..limit], expected.as_slice());
        }
    }

    #[test]
    fn station_schedule_scratch_reuses_capacity_and_last_sample_wins() {
        let mut stations = StationSet::default();
        for station_id in 1..=8 {
            stations.push(station(station_id, 10));
        }
        let samples = [
            StationLoadSample {
                station_id: StationId::new(3),
                owned_entities: 1,
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(3),
                owned_entities: 500,
                ..StationLoadSample::default()
            },
        ];
        let scheduler = StationScheduler::default();
        let mut scratch = StationScheduleScratch::new();

        {
            let plan = scheduler.plan_loaded_into(
                &stations,
                &samples,
                StationScheduleConfig {
                    max_station_advances_per_step: 2,
                },
                &mut scratch,
            );
            assert_eq!(plan.candidates_considered, 8);
            assert_eq!(plan.selected[0].station_id, StationId::new(3));
            assert_eq!(
                plan.selected[0].load_score,
                station_schedule_score(&samples[1])
            );
        }
        let score_capacity = scratch.score_capacity();
        let candidate_capacity = scratch.candidate_capacity();

        let plan = scheduler.plan_loaded_into(
            &stations,
            &samples[..1],
            StationScheduleConfig {
                max_station_advances_per_step: 1,
            },
            &mut scratch,
        );
        assert_eq!(plan.selected.len(), 1);
        assert_eq!(scratch.score_capacity(), score_capacity);
        assert_eq!(scratch.candidate_capacity(), candidate_capacity);
    }

    #[test]
    fn station_load_sampler_derives_cells_router_and_subscribers() {
        let station_id = StationId::new(1);
        let owner_position = Position3::new(1.0, 0.0, 0.0);
        let ghost_position = Position3::new(12.0, 0.0, 0.0);
        let policy_id = PolicyId::new(1);
        let mut station = station(1, 10);
        let owner = station
            .spawn_owned(EntityId::new(10), owner_position, Bounds::Point, policy_id)
            .expect("owner should spawn");
        let ghost = station.upsert_ghost(
            EntityId::new(20),
            ghost_position,
            Bounds::Point,
            policy_id,
            StationId::new(9),
            OwnerEpoch::new(3),
            Tick::new(30),
        );

        let grid = GridSpec::new(10.0).expect("grid should build");
        let mut index = CellIndex::new(grid);
        index.upsert(owner, owner_position, Bounds::Point);
        index.upsert(ghost, ghost_position, Bounds::Point);
        let mut indexes = StationIndexSet::default();
        indexes.insert(station_id, index);

        let mut stations = StationSet::default();
        stations.push(station);
        let mut router = EventRouter::default();
        router.register_station(station_id);
        for (event_id, kind) in [(1_u64, 1_u32), (2, 2)] {
            router
                .route(StationEvent {
                    id: EventId::new(event_id),
                    source: StationId::new(9),
                    target: station_id,
                    source_tick: Tick::new(0),
                    target_tick: Tick::new(4),
                    priority: EventPriority::Important,
                    kind: EventKind::Custom(kind),
                })
                .expect("event should queue");
        }

        assert_eq!(indexes.iter().count(), 1);
        let load_sampler = StationLoadSampler::default();
        let samples = load_sampler.sample_all(
            &stations,
            &indexes,
            &router,
            &[(station_id, 2), (station_id, 3)],
        );

        assert_eq!(samples.len(), 1);
        let sample = &samples[0];
        assert_eq!(sample.station_id, station_id);
        assert_eq!(sample.owned_entities, 1);
        assert_eq!(sample.ghost_entities, 1);
        assert_eq!(sample.subscribers, 5);
        assert_eq!(sample.queued_events, 2);
        assert_eq!(sample.estimated_bytes, 240);
        assert_eq!(sample.tick_cost_units, 7);
        assert_eq!(
            sample.cells,
            vec![
                CellLoadSample {
                    cell: grid.cell_at(owner_position),
                    owned_entities: 1,
                    ghost_entities: 0,
                    subscribers: 0,
                    estimated_updates: 1,
                    estimated_bytes: 48,
                    tick_cost_units: 3,
                    event_pressure: 0,
                },
                CellLoadSample {
                    cell: grid.cell_at(ghost_position),
                    owned_entities: 0,
                    ghost_entities: 1,
                    subscribers: 0,
                    estimated_updates: 1,
                    estimated_bytes: 48,
                    tick_cost_units: 2,
                    event_pressure: 0,
                },
            ]
        );

        assert_load_sampler_scratch_reuse(
            &load_sampler,
            &stations,
            &indexes,
            &router,
            station_id,
            &samples,
        );
    }

    fn assert_load_sampler_scratch_reuse(
        load_sampler: &StationLoadSampler,
        stations: &StationSet,
        indexes: &StationIndexSet,
        router: &EventRouter,
        station_id: StationId,
        samples: &[StationLoadSample],
    ) {
        let mut scratch = StationLoadSamplerScratch::new();
        let (sample_ptr, cell_ptr) = {
            let reused = load_sampler.sample_all_into(
                stations,
                indexes,
                router,
                &[(station_id, 2), (station_id, 3)],
                &mut scratch,
            );
            assert_eq!(reused, samples);
            (reused.as_ptr(), reused[0].cells.as_ptr())
        };
        let subscriber_capacity = scratch.retained_subscriber_capacity();
        let occupancy_capacity = scratch.retained_occupancy_capacity();
        let cell_capacity = scratch.retained_cell_capacity();

        let reused = load_sampler.sample_all_into(
            stations,
            indexes,
            router,
            &[(station_id, 2), (station_id, 3)],
            &mut scratch,
        );
        assert_eq!(reused, samples);
        assert_eq!(reused.as_ptr(), sample_ptr);
        assert_eq!(reused[0].cells.as_ptr(), cell_ptr);
        assert_eq!(scratch.retained_sample_slots(), 1);
        assert_eq!(scratch.retained_subscriber_capacity(), subscriber_capacity);
        assert_eq!(scratch.retained_occupancy_capacity(), occupancy_capacity);
        assert_eq!(scratch.retained_cell_capacity(), cell_capacity);
    }

    #[test]
    fn station_event_transport_bridge_routes_events_through_bounded_packets() {
        let mut stations = StationSet::default();
        stations.push(station(1, 10));
        stations.push(station(2, 10));

        let mut router = EventRouter::default();
        router.register_stations(&stations);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(StationId::new(2));
        let mut bridge = StationEventTransportBridge::default();
        let event = StationEvent {
            id: EventId::new(7),
            source: StationId::new(1),
            target: StationId::new(2),
            source_tick: Tick::new(0),
            target_tick: Tick::new(1),
            priority: EventPriority::Important,
            kind: EventKind::Custom(99),
        };

        bridge
            .send_event(&mut transport, &event)
            .expect("event should encode and send");
        assert_eq!(transport.queued_len(StationId::new(2)), Some(1));

        let report = bridge
            .pump_target(&mut transport, &mut router, StationId::new(2), 4)
            .expect("event should pump into router");
        assert_eq!(report.packets_received, 1);
        assert_eq!(report.events_routed, 1);
        assert_eq!(router.queued_len(StationId::new(2)), Some(1));

        let mut scheduler = StationScheduler::default();
        scheduler.advance_all(&mut stations);
        let drained = scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("drain should work");
        assert_eq!(drained, vec![event]);
        assert_eq!(bridge.stats().events_sent, 1);
        assert_eq!(bridge.stats().events_routed, 1);
        assert_eq!(transport.stats().packets_sent, 1);
        assert_eq!(transport.stats().packets_received, 1);
    }

    #[test]
    fn command_dispatch_transport_bridge_enqueues_stamped_command() {
        let gateway_station = StationId::new(0);
        let target_station = StationId::new(2);
        let command = CommandEnvelope {
            id: CommandId::new(42),
            client_id: ClientId::new(7),
            entity_id: EntityId::new(100),
            sequence: 42,
            received_at: Tick::new(12),
            kind: 1,
            priority: CommandPriority::High,
            payload: b"move:north".to_vec(),
        };
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(target_station);
        let mut queues = BTreeMap::from([(target_station, command_queues())]);
        let mut bridge = CommandDispatchTransportBridge::default();

        bridge
            .send_envelope(&mut transport, gateway_station, target_station, &command)
            .expect("command dispatch should send");
        assert_eq!(transport.queued_len(target_station), Some(1));
        let report = bridge
            .pump_target(
                &mut transport,
                &mut queues,
                target_station,
                4,
                CommandIngress::RUNNING,
            )
            .expect("command dispatch should pump");

        assert_eq!(report.packets_received, 1);
        assert_eq!(report.commands_enqueued, 1);
        let queued_command = queues
            .get_mut(&target_station)
            .expect("queue should exist")
            .pop_next()
            .expect("command should queue");
        assert_eq!(queued_command, command);
        assert_eq!(bridge.stats().commands_sent, 1);
        assert_eq!(bridge.stats().commands_enqueued, 1);
        assert_eq!(transport.stats().packets_sent, 1);
        assert_eq!(transport.stats().packets_received, 1);
    }

    #[test]
    fn command_dispatch_transport_bridge_rejects_endpoint_mismatch() {
        let packet_target = StationId::new(2);
        let frame_target = StationId::new(3);
        let mut transport = InMemoryStationTransport::default();
        transport.register_station(packet_target);
        let frame = CommandDispatchFrame {
            station_id: frame_target,
            client_id: ClientId::new(7),
            command_id: CommandId::new(42),
            entity_id: EntityId::new(100),
            sequence: 42,
            received_at: Tick::new(12),
            kind: 1,
            priority: CommandPriority::High,
            payload: Vec::new(),
        };
        let mut bytes = Vec::new();
        BinaryFrameEncoder
            .encode_command_dispatch(&frame, &mut bytes)
            .expect("frame should encode");
        transport
            .send_station(StationOutboundPacket {
                source_station: StationId::new(0),
                target_station: packet_target,
                bytes,
            })
            .expect("bad packet should enter transport");
        let mut queues = BTreeMap::from([(packet_target, command_queues())]);
        let mut bridge = CommandDispatchTransportBridge::default();

        let error = bridge
            .pump_target(
                &mut transport,
                &mut queues,
                packet_target,
                4,
                CommandIngress::RUNNING,
            )
            .expect_err("endpoint mismatch should reject");

        assert!(matches!(
            error,
            CommandDispatchTransportError::EndpointMismatch {
                packet_source,
                packet_target: observed_packet_target,
                dispatch_target,
            } if packet_source == StationId::new(0)
                && observed_packet_target == packet_target
                && dispatch_target == frame_target
        ));
        assert!(
            queues
                .get_mut(&packet_target)
                .expect("queue should exist")
                .pop_next()
                .is_none()
        );
    }

    #[test]
    fn cell_migration_moves_owned_entities_and_updates_indexes() {
        let grid = GridSpec::new(16.0).expect("valid grid");
        let cell = CellCoord3::new(0, 0, 0);
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        let first = source
            .spawn_owned(
                EntityId::new(1),
                Position3::new(1.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("first spawn should work");
        let second = source
            .spawn_owned(
                EntityId::new(2),
                Position3::new(2.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("second spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let mut source_index = CellIndex::new(grid);
        source_index.upsert(first, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
        source_index.upsert(second, Position3::new(2.0, 1.0, 1.0), Bounds::Point);
        let mut target_index = CellIndex::new(grid);

        let mut ownership = CellOwnershipTable::default();
        ownership.assign(cell, StationId::new(1));
        let update = ownership.apply_split(
            &SplitProposal {
                source_station: StationId::new(1),
                cells_to_move: vec![cell],
                moved_pressure_score: 10,
            },
            StationId::new(2),
        );
        assert_eq!(ownership.owner_of(cell), Some(StationId::new(2)));
        assert_eq!(update.moved_cells, vec![cell]);

        let mut scratch = CellMigrationScratch::new();
        scratch.reserve(2, 2);
        let mut report = CellMigrationReport::default();
        report.scanned_cells.reserve(1);
        report.entity_migrations.reserve(2);
        CellMigrationExecutor::migrate_cells_into(
            &mut stations,
            &mut source_index,
            &mut target_index,
            StationId::new(1),
            StationId::new(2),
            &update.moved_cells,
            4,
            &mut scratch,
            &mut report,
        )
        .expect("cell migration should work");

        assert_eq!(report.entity_migrations.len(), 2);
        assert_eq!(target_index.entity_count(), 2);
        assert!(
            !stations
                .get(StationId::new(1))
                .expect("source")
                .get_by_id(EntityId::new(1))
                .expect("source ghost")
                .is_owned()
        );
        assert!(
            stations
                .get(StationId::new(2))
                .expect("target")
                .get_by_id(EntityId::new(1))
                .expect("target owner")
                .is_owned()
        );

        let retained_handle_capacity = scratch.handle_capacity();
        let retained_entity_capacity = scratch.entity_capacity();
        let retained_candidate_capacity = scratch.candidate_capacity();
        let retained_scanned_capacity = report.scanned_cells.capacity();
        let retained_migration_capacity = report.entity_migrations.capacity();
        CellMigrationExecutor::migrate_cells_into(
            &mut stations,
            &mut source_index,
            &mut target_index,
            StationId::new(1),
            StationId::new(2),
            &[],
            4,
            &mut scratch,
            &mut report,
        )
        .expect("empty reusable migration should work");
        assert!(report.scanned_cells.is_empty());
        assert!(report.entity_migrations.is_empty());
        assert_eq!(report.scanned_cells.capacity(), retained_scanned_capacity);
        assert_eq!(
            report.entity_migrations.capacity(),
            retained_migration_capacity
        );
        assert_eq!(scratch.handle_capacity(), retained_handle_capacity);
        assert_eq!(scratch.entity_capacity(), retained_entity_capacity);
        assert_eq!(scratch.candidate_capacity(), retained_candidate_capacity);
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn split_scheduler_plans_and_executes_hot_cell_move() {
        let grid = GridSpec::new(16.0).expect("valid grid");
        let hot_cell = CellCoord3::new(0, 0, 0);
        let mut stations = StationSet::default();
        let mut source = station(1, 10);
        let handle = source
            .spawn_owned(
                EntityId::new(1),
                Position3::new(1.0, 1.0, 1.0),
                Bounds::Point,
                PolicyId::new(0),
            )
            .expect("spawn should work");
        stations.push(source);
        stations.push(station(2, 10));

        let mut source_index = CellIndex::new(grid);
        source_index.upsert(handle, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
        let mut indexes = StationIndexSet::default();
        indexes.insert(StationId::new(1), source_index);
        indexes.insert(StationId::new(2), CellIndex::new(grid));

        let samples = vec![
            StationLoadSample {
                station_id: StationId::new(1),
                owned_entities: 100,
                subscribers: 100,
                tick_cost_units: 1000,
                cells: vec![CellLoadSample {
                    cell: hot_cell,
                    owned_entities: 100,
                    subscribers: 100,
                    event_pressure: 10,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(2),
                owned_entities: 1,
                cells: vec![CellLoadSample {
                    cell: CellCoord3::new(10, 0, 0),
                    owned_entities: 1,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
        ];
        let scheduler = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: HotspotThresholds {
                max_station_entities: 10,
                max_station_subscribers: 10,
                max_cell_pressure: 10,
                ..HotspotThresholds::default()
            },
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            ghost_ttl_ticks: 4,
            ..SplitSchedulerConfig::default()
        });
        let schedule = scheduler.plan(&samples);
        assert_eq!(schedule.actions.len(), 1);
        assert_eq!(schedule.actions[0].target_station, StationId::new(2));

        let mut ownership = CellOwnershipTable::default();
        ownership.assign(hot_cell, StationId::new(1));
        let mut execution_scratch = SplitScheduleExecutionScratch::new();
        execution_scratch.reserve(1, 1, 1);
        {
            let report = scheduler
                .execute_into(
                    &schedule,
                    &mut stations,
                    &mut indexes,
                    &mut ownership,
                    &mut execution_scratch,
                )
                .expect("reusable execute should work");
            assert_eq!(report.cell_migrations.len(), 1);
            assert_eq!(report.cell_migrations[0].entity_migrations.len(), 1);
        }

        assert_eq!(ownership.owner_of(hot_cell), Some(StationId::new(2)));
        assert_eq!(
            indexes
                .get(StationId::new(2))
                .expect("target index")
                .entity_count(),
            1
        );

        let retained_ownership_slots = execution_scratch.retained_ownership_slots();
        let retained_migration_slots = execution_scratch.retained_migration_slots();
        let retained_update_cells = execution_scratch.retained_update_cell_capacity();
        let retained_entity_migrations = execution_scratch.retained_entity_migration_capacity();
        let retained_candidates = execution_scratch.retained_candidate_capacity();
        execution_scratch.reserve(1, 1, 1);
        assert_eq!(
            execution_scratch.retained_update_cell_capacity(),
            retained_update_cells
        );
        assert_eq!(
            execution_scratch.retained_entity_migration_capacity(),
            retained_entity_migrations
        );
        assert_eq!(
            execution_scratch.retained_candidate_capacity(),
            retained_candidates
        );
        let empty = SplitSchedule::default();
        let empty_report = scheduler
            .execute_into(
                &empty,
                &mut stations,
                &mut indexes,
                &mut ownership,
                &mut execution_scratch,
            )
            .expect("empty reusable execute should work");
        assert!(empty_report.ownership_updates.is_empty());
        assert!(empty_report.cell_migrations.is_empty());
        assert_eq!(
            execution_scratch.retained_ownership_slots(),
            retained_ownership_slots
        );
        assert_eq!(
            execution_scratch.retained_migration_slots(),
            retained_migration_slots
        );
        assert_eq!(
            execution_scratch.retained_update_cell_capacity(),
            retained_update_cells
        );
        assert_eq!(
            execution_scratch.retained_entity_migration_capacity(),
            retained_entity_migrations
        );
        assert_eq!(
            execution_scratch.retained_candidate_capacity(),
            retained_candidates
        );
    }

    #[test]
    fn split_scheduler_respects_source_cooldown() {
        let hot_cell = CellCoord3::new(0, 0, 0);
        let samples = split_test_samples(hot_cell);
        let scheduler = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: split_test_thresholds(),
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            split_cooldown_ticks: 10,
            ..SplitSchedulerConfig::default()
        });
        let mut state = SplitSchedulerState::default();

        let initial = scheduler.plan_with_state(&samples, Some(&state), Tick::new(5));
        assert_eq!(initial.actions.len(), 1);
        state.record_schedule(&initial, Tick::new(5));

        let cooled_down = scheduler.plan_with_state(&samples, Some(&state), Tick::new(8));
        assert!(cooled_down.actions.is_empty());
        assert_eq!(cooled_down.skipped_cooldown, 1);

        let after_cooldown = scheduler.plan_with_state(&samples, Some(&state), Tick::new(16));
        assert_eq!(after_cooldown.actions.len(), 1);
    }

    #[test]
    fn split_scheduler_reports_capacity_and_improvement_skips() {
        let hot_cell = CellCoord3::new(0, 0, 0);
        let samples = split_test_samples(hot_cell);

        let capacity_guard = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: split_test_thresholds(),
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            max_target_score_after_move: 1,
            ..SplitSchedulerConfig::default()
        });
        let capacity_schedule = capacity_guard.plan(&samples);
        assert!(capacity_schedule.actions.is_empty());
        assert_eq!(capacity_schedule.skipped_target_capacity, 1);

        let improvement_guard = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: split_test_thresholds(),
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            min_score_improvement: u64::MAX,
            ..SplitSchedulerConfig::default()
        });
        let improvement_schedule = improvement_guard.plan(&samples);
        assert!(improvement_schedule.actions.is_empty());
        assert_eq!(improvement_schedule.skipped_insufficient_improvement, 1);
    }

    #[test]
    fn split_scheduler_view_matches_owned_and_retains_nested_capacity() {
        let hot_cell = CellCoord3::new(0, 0, 0);
        let samples = split_test_samples(hot_cell);
        let scheduler = SplitScheduler::new(SplitSchedulerConfig {
            thresholds: split_test_thresholds(),
            max_actions_per_pass: 1,
            max_cells_per_action: 1,
            ..SplitSchedulerConfig::default()
        });
        let expected = scheduler.plan(&samples);
        let mut scratch = SplitSchedulerScratch::new();

        {
            let view = scheduler.plan_into(&samples, &mut scratch);
            assert_eq!(SplitSchedule::from(view), expected);
        }
        let decision_slots = scratch.retained_decision_slots();
        let action_slots = scratch.retained_action_slots();
        let reason_capacity = scratch.retained_reason_capacity();
        let action_cell_capacity = scratch.retained_action_cell_capacity();
        let candidate_capacity = scratch.retained_candidate_capacity();
        assert_eq!(decision_slots, samples.len());
        assert_eq!(action_slots, 1);
        assert!(reason_capacity > 0);
        assert!(action_cell_capacity > 0);
        assert!(candidate_capacity > 0);

        let reduced = scheduler.plan_into(&samples[1..], &mut scratch);
        assert_eq!(reduced.decisions.len(), 1);
        assert!(reduced.actions.is_empty());
        assert_eq!(scratch.retained_decision_slots(), decision_slots);
        assert_eq!(scratch.retained_action_slots(), action_slots);
        assert_eq!(scratch.retained_reason_capacity(), reason_capacity);
        assert_eq!(
            scratch.retained_action_cell_capacity(),
            action_cell_capacity
        );
        assert_eq!(scratch.retained_candidate_capacity(), candidate_capacity);
    }

    fn split_test_thresholds() -> HotspotThresholds {
        HotspotThresholds {
            max_station_entities: 10,
            max_station_subscribers: 10,
            max_cell_pressure: 10,
            ..HotspotThresholds::default()
        }
    }

    fn split_test_samples(hot_cell: CellCoord3) -> Vec<StationLoadSample> {
        vec![
            StationLoadSample {
                station_id: StationId::new(1),
                owned_entities: 100,
                subscribers: 100,
                tick_cost_units: 1000,
                cells: vec![CellLoadSample {
                    cell: hot_cell,
                    owned_entities: 100,
                    subscribers: 100,
                    event_pressure: 10,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
            StationLoadSample {
                station_id: StationId::new(2),
                owned_entities: 1,
                cells: vec![CellLoadSample {
                    cell: CellCoord3::new(10, 0, 0),
                    owned_entities: 1,
                    ..CellLoadSample::default()
                }],
                ..StationLoadSample::default()
            },
        ]
    }
}
