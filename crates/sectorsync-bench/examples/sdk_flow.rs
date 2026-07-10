//! Cohesive command-to-replication SDK flow.

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    BarrierState, Bounds, CellIndex, ClientId, CommandEnvelope, CommandId, CommandIngress,
    CommandPriority, CommandQueueLimits, CommandQueueMode, CommandQueues, CompiledSyncPolicy,
    ComponentDescriptor, ComponentId, ComponentMigrationMode, ComponentStore, ComponentSyncMode,
    EntityId, GatewayConfig, GatewaySessionTable, GridSpec, InstanceId, NodeId, PolicyId,
    PolicyTable, Position3, RangeOnlyVisibility, Station, StationConfig, StationId, Tick,
    U32LeCodec, ViewerQuery,
};
use sectorsync_runtime::{
    GATEWAY_COMMAND_ACK_ACCEPTED, GATEWAY_COMMAND_ACK_BARRIER_REJECTED, GatewayCommandPipeline,
    ReplicationReceiveBridge, ReplicationReceiveConfig, ReplicationTransportBridge,
};
use sectorsync_transport::{ClientTransportLimits, InMemoryTransportHub};
use sectorsync_wire::{BinaryFrameEncoder, CommandFrame, ComponentSelection, FrameEncoder};

const SET_HEALTH_KIND: u32 = 1;
const HEALTH_COMPONENT_ID: ComponentId = ComponentId::new(1);

/// Observable handoff from the cohesive SDK flow to an external metrics system.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SdkFlowReport {
    /// Requests rejected before entering `SectorSync`.
    pub external_rejections: usize,
    /// Commands accepted into bounded station queues.
    pub commands_enqueued: usize,
    /// Commands applied by the external station-local system.
    pub commands_applied: usize,
    /// Commands rejected by the active barrier ingress policy.
    pub barrier_rejections: usize,
    /// Positive and negative ACK frames encoded by the pipeline.
    pub acks_encoded: usize,
    /// Replication frames submitted to bounded transport.
    pub replication_frames_sent: usize,
    /// Replication frames accepted by the client receive bridge.
    pub replication_frames_received: usize,
    /// Entity deltas delivered to the client receive bridge.
    pub entities_received: usize,
    /// Component deltas delivered to the client receive bridge.
    pub components_received: usize,
    /// Business component value observed after command application.
    pub final_health: u32,
}

#[cfg(not(test))]
fn main() {
    let report = run();
    println!(
        "sdk_flow external_rejections={} enqueued={} applied={} barrier_rejections={} acks={} replication_sent={} replication_received={} entities={} components={} final_health={}",
        report.external_rejections,
        report.commands_enqueued,
        report.commands_applied,
        report.barrier_rejections,
        report.acks_encoded,
        report.replication_frames_sent,
        report.replication_frames_received,
        report.entities_received,
        report.components_received,
        report.final_health,
    );
}

/// Runs the recommended single-station integration order.
#[allow(clippy::too_many_lines)]
pub fn run() -> SdkFlowReport {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let station_id = StationId::new(1);
    let entity_id = EntityId::new(100);
    let mut station = station(station_id);
    let mut index = CellIndex::new(GridSpec::new(64.0).expect("grid is valid"));
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(0), 1, 20, 256.0));

    let position = Position3::new(0.0, 0.0, 0.0);
    let handle = station
        .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(0))
        .expect("authoritative entity should spawn");
    index.upsert(handle, position, Bounds::Point);

    let health = ComponentDescriptor::sparse_blob(
        HEALTH_COMPONENT_ID,
        "health",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        4,
    );
    let mut components = ComponentStore::default();
    components
        .set_typed(&health, handle, 1, &U32LeCodec, &100)
        .expect("initial health should encode");

    // Authentication, anti-cheat, and game-rule validation happen before this frame exists.
    let external_rejections = usize::from(
        validate_health_request(client_id, CommandId::new(0), 0, entity_id, 101).is_err(),
    );
    let command = validate_health_request(client_id, CommandId::new(1), 1, entity_id, 99)
        .expect("valid external request should translate");

    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 4,
        reconnect_grace_ticks: 20,
        max_commands_per_tick: 4,
    });
    gateway
        .connect(client_id, station_id, Tick::new(0))
        .expect("validated client route should connect");
    let mut station_queues = BTreeMap::from([(
        station_id,
        CommandQueues::new(CommandQueueLimits {
            high: 4,
            normal: 4,
            low: 2,
        }),
    )]);
    let mut pipeline = GatewayCommandPipeline::default();
    let accepted = pipeline.process(
        &mut gateway,
        &mut station_queues,
        &encode_command(&command),
        Tick::new(0),
        CommandIngress::RUNNING,
    );
    assert!(accepted.accepted);
    assert_eq!(accepted.reason_code, GATEWAY_COMMAND_ACK_ACCEPTED);
    assert!(accepted.ack_bytes.is_some());

    let queued = station_queues
        .get_mut(&station_id)
        .expect("station queue should be registered")
        .pop_next()
        .expect("accepted command should be queued");
    let final_health = apply_external_business_command(&station, &mut components, &health, &queued)
        .expect("external station-local system should apply command");

    let frozen_command = validate_health_request(client_id, CommandId::new(2), 2, entity_id, 98)
        .expect("second request should validate externally");
    let barrier_rejected = pipeline.process(
        &mut gateway,
        &mut station_queues,
        &encode_command(&frozen_command),
        Tick::new(0),
        CommandIngress {
            barrier_state: BarrierState::Frozen,
            command_mode: CommandQueueMode::Reject,
        },
    );
    assert!(!barrier_rejected.accepted);
    assert_eq!(
        barrier_rejected.reason_code,
        GATEWAY_COMMAND_ACK_BARRIER_REJECTED
    );

    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 4,
        max_packet_bytes: 1_024,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:24007".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:24000".parse().expect("server addr"))
        .expect("server endpoint should register");
    let viewer = ViewerQuery {
        client_id,
        position,
        radius: 256.0,
        max_entities: 32,
    };
    let selection = ComponentSelection {
        component_ids: vec![HEALTH_COMPONENT_ID],
    };
    let mut replication = ReplicationTransportBridge::default();
    let send = replication
        .send_viewer(
            &mut server_transport,
            &station,
            &index,
            &policies,
            &components,
            &selection,
            &viewer,
            &RangeOnlyVisibility,
        )
        .expect("bounded replication transport should accept frame");
    assert!(send.sent);

    let mut receive = ReplicationReceiveBridge::new(
        ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
    );
    let pump = receive
        .pump(&mut client_transport, 4)
        .expect("client should receive validated replication frame");
    let component_bytes = &pump.frames[0].entities[0].components[0].bytes;
    assert_eq!(component_bytes.as_slice(), final_health.to_le_bytes());

    let pipeline_stats = pipeline.stats();
    SdkFlowReport {
        external_rejections,
        commands_enqueued: pipeline_stats.commands_enqueued,
        commands_applied: 1,
        barrier_rejections: pipeline_stats.commands_rejected_queue,
        acks_encoded: pipeline_stats.acks_encoded,
        replication_frames_sent: replication.stats().frames_sent,
        replication_frames_received: receive.stats().frames_received,
        entities_received: pump.entities_received(),
        components_received: pump.components_received(),
        final_health,
    }
}

fn validate_health_request(
    client_id: ClientId,
    command_id: CommandId,
    sequence: u64,
    entity_id: EntityId,
    requested_health: u32,
) -> Result<CommandFrame, &'static str> {
    if requested_health > 100 {
        return Err("external game rule rejected health above maximum");
    }
    Ok(CommandFrame {
        client_id,
        command_id,
        entity_id,
        sequence,
        kind: SET_HEALTH_KIND,
        priority: CommandPriority::High,
        payload: requested_health.to_le_bytes().to_vec(),
    })
}

fn apply_external_business_command(
    station: &Station,
    components: &mut ComponentStore,
    health: &ComponentDescriptor,
    command: &CommandEnvelope,
) -> Result<u32, &'static str> {
    if command.kind != SET_HEALTH_KIND {
        return Err("unsupported external command kind");
    }
    let record = station
        .get_by_id(command.entity_id)
        .ok_or("command entity is missing")?;
    if !record.is_owned() {
        return Err("command entity is not authoritative here");
    }
    let value = u32::from_le_bytes(
        command
            .payload
            .as_slice()
            .try_into()
            .map_err(|_| "invalid external command payload")?,
    );
    components
        .set_typed(health, record.handle, 2, &U32LeCodec, &value)
        .map_err(|_| "component write was rejected")?;
    Ok(value)
}

fn encode_command(command: &CommandFrame) -> Vec<u8> {
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_command(command, &mut bytes)
        .expect("validated command should encode");
    bytes
}

fn station(station_id: StationId) -> Station {
    Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}
