//! Multi-station orchestration helpers for SectorSync.

#![forbid(unsafe_code)]

pub mod deployment;

use std::collections::{BTreeMap, BTreeSet};

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, CellCoord3, CellIndex, ClientId, CommandEnvelope,
    CommandId, CommandIngress, CommandQueueError, CommandQueueMode, CommandQueues, EntityHandle,
    EntityId, EventQueueError, EventQueueLimits, EventQueues, GatewayError, GatewaySessionTable,
    HandoffTransfer, HotspotDecision, HotspotPlanner, HotspotSeverity, HotspotThresholds, NodeId,
    OwnerEpoch, PushOutcome, RuntimeBarrier, SnapshotVersion, SplitProposal, Station, StationError,
    StationEvent, StationId, StationLoadSample, StationSnapshot, Tick,
};
use sectorsync_transport::{StationOutboundPacket, StationTransportReceiver, StationTransportSink};
use sectorsync_wire::{
    BinaryDecodeError, BinaryEncodeError, BinaryFrameDecoder, BinaryFrameEncoder, CommandAckFrame,
    FrameDecoder, FrameEncoder, RuntimeFrame, StationEventFrame,
};

pub use deployment::{
    DeploymentConfig, DeploymentError, DeploymentNodeRoute, DeploymentNodeState,
    DeploymentRouteTable, DeploymentStationMove, DeploymentStationRoute, DeploymentStats,
    GatewayDeliveryError, GatewayDeliveryRoute,
};

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
        let frame = match self.decoder.decode(input) {
            Ok(frame) => frame,
            Err(error) => {
                self.stats.frames_rejected_decode =
                    self.stats.frames_rejected_decode.saturating_add(1);
                return GatewayCommandPipelineReport {
                    error: Some(GatewayCommandPipelineError::Decode(error)),
                    ..GatewayCommandPipelineReport::default()
                };
            }
        };

        let RuntimeFrame::Command(command_frame) = frame else {
            self.stats.frames_rejected_non_command =
                self.stats.frames_rejected_non_command.saturating_add(1);
            return GatewayCommandPipelineReport {
                error: Some(GatewayCommandPipelineError::NonCommandFrame),
                ..GatewayCommandPipelineReport::default()
            };
        };
        self.stats.command_frames_decoded = self.stats.command_frames_decoded.saturating_add(1);

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

    /// Decodes and admits one command packet, then resolves a deployment route
    /// for external node dispatch without touching local station queues.
    pub fn dispatch(
        &mut self,
        gateway: &mut GatewaySessionTable,
        deployment: &DeploymentRouteTable,
        input: &[u8],
        now: Tick,
    ) -> GatewayCommandPipelineReport {
        let frame = match self.decoder.decode(input) {
            Ok(frame) => frame,
            Err(error) => {
                self.stats.frames_rejected_decode =
                    self.stats.frames_rejected_decode.saturating_add(1);
                return GatewayCommandPipelineReport {
                    error: Some(GatewayCommandPipelineError::Decode(error)),
                    ..GatewayCommandPipelineReport::default()
                };
            }
        };

        let RuntimeFrame::Command(command_frame) = frame else {
            self.stats.frames_rejected_non_command =
                self.stats.frames_rejected_non_command.saturating_add(1);
            return GatewayCommandPipelineReport {
                error: Some(GatewayCommandPipelineError::NonCommandFrame),
                ..GatewayCommandPipelineReport::default()
            };
        };
        self.stats.command_frames_decoded = self.stats.command_frames_decoded.saturating_add(1);

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

/// Small in-process station collection for simulations and embedders.
#[derive(Clone, Debug, Default)]
pub struct StationSet {
    stations: Vec<Station>,
}

impl StationSet {
    /// Adds a station to the collection.
    pub fn push(&mut self, station: Station) {
        self.stations.push(station);
    }

    /// Gets a station by id.
    pub fn get(&self, station_id: StationId) -> Option<&Station> {
        self.stations
            .iter()
            .find(|station| station.config().station_id == station_id)
    }

    /// Gets a mutable station by id.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut Station> {
        self.stations
            .iter_mut()
            .find(|station| station.config().station_id == station_id)
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

        let left_index = self
            .stations
            .iter()
            .position(|station| station.config().station_id == left_id)?;
        let right_index = self
            .stations
            .iter()
            .position(|station| station.config().station_id == right_id)?;

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

    /// Returns whether no stations are registered.
    pub fn is_empty(&self) -> bool {
        self.stations.is_empty()
    }
}

/// Station-local spatial indexes keyed by station id.
#[derive(Clone, Debug, Default)]
pub struct StationIndexSet {
    indexes: Vec<(StationId, CellIndex)>,
}

impl StationIndexSet {
    /// Adds or replaces one station index.
    pub fn insert(&mut self, station_id: StationId, index: CellIndex) {
        if let Some((_, existing)) = self.indexes.iter_mut().find(|(id, _)| *id == station_id) {
            *existing = index;
        } else {
            self.indexes.push((station_id, index));
        }
    }

    /// Gets one station index.
    pub fn get(&self, station_id: StationId) -> Option<&CellIndex> {
        self.indexes
            .iter()
            .find(|(id, _)| *id == station_id)
            .map(|(_, index)| index)
    }

    /// Gets one mutable station index.
    pub fn get_mut(&mut self, station_id: StationId) -> Option<&mut CellIndex> {
        self.indexes
            .iter_mut()
            .find(|(id, _)| *id == station_id)
            .map(|(_, index)| index)
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

        let left_index = self.indexes.iter().position(|(id, _)| *id == left_id)?;
        let right_index = self.indexes.iter().position(|(id, _)| *id == right_id)?;

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

    /// Returns whether no indexes are registered.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
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
        let mut moved_cells = Vec::new();
        for cell in &proposal.cells_to_move {
            let previous = self.assign(*cell, target_station);
            if previous != Some(target_station) {
                moved_cells.push(*cell);
            }
        }
        CellOwnershipUpdate {
            source_station: proposal.source_station,
            target_station,
            moved_cells,
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
        let mut report = CellMigrationReport {
            source_station,
            target_station,
            scanned_cells: cells.to_vec(),
            ..CellMigrationReport::default()
        };
        let mut seen_handles = BTreeSet::new();
        let mut entity_ids = Vec::new();

        {
            let source = stations
                .get(source_station)
                .ok_or(EntityMigrationError::MissingSource(source_station))?;
            for cell in cells {
                for handle in source_index.handles_in_cell(*cell) {
                    if !seen_handles.insert(handle) {
                        report.skipped_duplicate_entities += 1;
                        continue;
                    }
                    let Some(record) = source.get(handle) else {
                        report.skipped_missing_handles += 1;
                        continue;
                    };
                    if record.is_owned() {
                        entity_ids.push(record.id);
                    } else {
                        report.skipped_non_owned += 1;
                    }
                }
            }
        }

        let mut seen_entities = BTreeSet::new();
        for entity_id in entity_ids {
            if !seen_entities.insert(entity_id) {
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

        Ok(report)
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
        self.plan_with_state(samples, None, Tick::new(0))
    }

    /// Plans split actions using optional cooldown state.
    pub fn plan_with_state(
        &self,
        samples: &[StationLoadSample],
        state: Option<&SplitSchedulerState>,
        current_tick: Tick,
    ) -> SplitSchedule {
        let decisions = samples
            .iter()
            .map(|sample| HotspotPlanner::evaluate(sample, self.config.thresholds))
            .collect::<Vec<_>>();
        let mut schedule = SplitSchedule {
            decisions,
            ..SplitSchedule::default()
        };

        for source in samples {
            if schedule.actions.len() >= self.config.max_actions_per_pass {
                break;
            }
            let Some(source_decision) = schedule
                .decisions
                .iter()
                .find(|decision| decision.station_id == source.station_id)
            else {
                continue;
            };
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
                schedule.skipped_cooldown += 1;
                continue;
            }

            let proposal =
                HotspotPlanner::propose_cell_split(source, self.config.max_cells_per_action);
            if proposal.cells_to_move.is_empty() {
                schedule.skipped_no_cells += 1;
                continue;
            }
            let target_selection =
                select_split_target(source, &proposal, samples, &schedule.decisions, self.config);
            let Some(target) = target_selection.target else {
                if target_selection.considered_targets == 0 {
                    schedule.skipped_no_target += 1;
                } else {
                    schedule.skipped_target_severity +=
                        usize::from(target_selection.rejected_by_severity > 0);
                    schedule.skipped_target_capacity +=
                        usize::from(target_selection.rejected_by_capacity > 0);
                    schedule.skipped_insufficient_improvement +=
                        usize::from(target_selection.rejected_by_improvement > 0);
                }
                continue;
            };
            let target_score = station_load_score(target);
            let estimated_target_score_after_move =
                target_score.saturating_add(proposal.moved_pressure_score);
            schedule.actions.push(SplitAction {
                source_station: source.station_id,
                target_station: target.station_id,
                proposal,
                source_score: station_load_score(source),
                target_score,
                estimated_target_score_after_move,
            });
        }

        schedule
    }

    /// Executes a split schedule by applying ownership updates and migrating entities.
    pub fn execute(
        &self,
        schedule: &SplitSchedule,
        stations: &mut StationSet,
        indexes: &mut StationIndexSet,
        ownership: &mut CellOwnershipTable,
    ) -> Result<SplitScheduleExecutionReport, SplitScheduleExecutionError> {
        let mut report = SplitScheduleExecutionReport::default();

        for action in &schedule.actions {
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

    for target in samples {
        if target.station_id == source.station_id {
            continue;
        }
        selection.considered_targets += 1;

        let severity = decisions
            .iter()
            .find(|decision| decision.station_id == target.station_id)
            .map_or(HotspotSeverity::Normal, |decision| decision.severity);
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
        let current_key = selection.target.map(|current| {
            let current_severity = decisions
                .iter()
                .find(|decision| decision.station_id == current.station_id)
                .map_or(HotspotSeverity::Normal, |decision| decision.severity);
            (
                severity_rank(current_severity),
                station_load_score(current),
                current.station_id.get(),
            )
        });
        if current_key.is_none_or(|current_key| target_key < current_key) {
            selection.target = Some(target);
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
        let queue = self
            .queues
            .get_mut(&station_id)
            .ok_or(EventRouterError::MissingTarget(station_id))?;
        let mut ready = Vec::new();
        let mut delayed = Vec::new();

        while let Some(event) = queue.pop_next() {
            if event.target_tick <= current_tick {
                ready.push(event);
            } else {
                delayed.push(event);
            }
        }

        for event in delayed {
            queue.push(event)?;
        }
        self.stats.drained_events += ready.len();
        Ok(ready)
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

    /// Drains router events ready for each station's current tick.
    pub fn drain_ready_events(
        &mut self,
        stations: &StationSet,
        router: &mut EventRouter,
    ) -> Result<Vec<StationEvent>, EventRouterError> {
        let mut events = Vec::new();
        for station in stations.iter() {
            events.extend(router.drain_ready(station.config().station_id, station.tick())?);
        }
        Ok(events)
    }
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
            .map(Tick::new)
            .unwrap_or(Tick::new(0));

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
            snapshots.push(station.snapshot(version.clone()));
        }
        self.metrics.snapshots_exported = self
            .metrics
            .snapshots_exported
            .saturating_add(snapshots.len());
        Ok(snapshots)
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

#[cfg(test)]
mod tests {
    use super::*;
    use sectorsync_core::prelude::{
        Bounds, CellCoord3, CellLoadSample, CommandPriority, CommandQueueLimits, EventId,
        EventKind, EventPriority, GatewayConfig, GridSpec, HotspotThresholds, InstanceId, NodeId,
        PolicyId, Position3, StationConfig, StationLoadSample,
    };
    use sectorsync_transport::InMemoryStationTransport;
    use sectorsync_wire::{
        BinaryFrameDecoder, BinaryFrameEncoder, CommandFrame, FrameDecoder, FrameEncoder,
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

        let snapshots = controller
            .export_snapshots(&stations, SnapshotVersion::default())
            .expect("snapshot should work while frozen");
        assert_eq!(snapshots.len(), 2);

        let metrics = controller.resume().expect("resume should work");
        assert_eq!(metrics.station_count, 2);
        assert_eq!(metrics.snapshots_exported, 2);
        assert_eq!(controller.progress().state, BarrierState::Running);
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
        scheduler.advance_all(&mut stations);
        let drained = scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("drain should work");
        assert!(drained.is_empty());

        scheduler.advance_all(&mut stations);
        let drained = scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("drain should work");
        assert_eq!(drained.len(), 1);
        assert_eq!(router.stats().routed_events, 1);
        assert_eq!(router.stats().drained_events, 1);
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

        let report = CellMigrationExecutor::migrate_cells(
            &mut stations,
            &mut source_index,
            &mut target_index,
            StationId::new(1),
            StationId::new(2),
            &update.moved_cells,
            4,
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
    }

    #[test]
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
        let report = scheduler
            .execute(&schedule, &mut stations, &mut indexes, &mut ownership)
            .expect("execute should work");

        assert_eq!(ownership.owner_of(hot_cell), Some(StationId::new(2)));
        assert_eq!(report.cell_migrations.len(), 1);
        assert_eq!(report.cell_migrations[0].entity_migrations.len(), 1);
        assert_eq!(
            indexes
                .get(StationId::new(2))
                .expect("target index")
                .entity_count(),
            1
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
