//! Low-level client transport bridge SDK example.

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CommandId, CommandIngress, CommandPriority, CommandQueueLimits,
    CommandQueues, CompiledSyncPolicy, ComponentDescriptor, ComponentId, ComponentMigrationMode,
    ComponentStore, ComponentSyncMode, EntityId, GatewayConfig, GatewaySessionTable, GridSpec,
    InstanceId, NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    ReplicationPlan, ReplicationPlanner, ReplicationScratch, Station, StationConfig, StationId,
    U32LeCodec, ViewerQuery,
};
use sectorsync_runtime::{
    ClientTransportBridge, ClientTransportConfig, GatewayClientTransportBridge,
    GatewayCommandPipeline, ReplicationTransportBridge,
};
use sectorsync_transport::{ClientTransportLimits, InMemoryTransportHub};
use sectorsync_wire::{CommandFrame, ComponentSelection};

#[allow(clippy::too_many_lines)]
fn main() {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let station_id = StationId::new(1);
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 8,
        max_packet_bytes: 512,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:24007".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:24000".parse().expect("server addr"))
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
    let send = client_bridge
        .send_command_frame(&mut client_transport, &command)
        .expect("client command should send");

    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 8,
        reconnect_grace_ticks: 20,
        max_commands_per_tick: 4,
    });
    gateway
        .connect(
            client_id,
            station_id,
            sectorsync_core::prelude::Tick::new(10),
        )
        .expect("client should connect");
    let mut station_queues = BTreeMap::from([(
        station_id,
        CommandQueues::new(CommandQueueLimits {
            high: 4,
            normal: 4,
            low: 4,
        }),
    )]);
    let mut gateway_pipeline = GatewayCommandPipeline::default();
    let mut gateway_transport = GatewayClientTransportBridge::default();
    let ingress = gateway_transport
        .pump_ingress_compact(
            &mut server_transport,
            &mut gateway_pipeline,
            &mut gateway,
            &mut station_queues,
            sectorsync_core::prelude::Tick::new(10),
            CommandIngress::RUNNING,
            4,
        )
        .expect("gateway transport should pump command");
    assert_eq!(ingress.commands_accepted, 1);
    assert_eq!(ingress.acks_sent, 1);
    let applied = station_queues
        .get_mut(&station_id)
        .expect("station queue should exist")
        .pop_next()
        .expect("accepted command should queue");
    assert_eq!(applied.id, command.command_id);

    let mut station = Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let mut index = CellIndex::new(GridSpec::new(64.0).expect("grid is valid"));
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(0), 1, 20, 256.0));
    let handle = station
        .spawn_owned(
            EntityId::new(100),
            Position3::new(0.0, 0.0, 0.0),
            Bounds::Point,
            PolicyId::new(0),
        )
        .expect("spawn should work");
    index.upsert(handle, Position3::new(0.0, 0.0, 0.0), Bounds::Point);

    let health = ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "health",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        4,
    );
    let mut components = ComponentStore::default();
    components
        .set_typed(&health, handle, 1, &U32LeCodec, &100)
        .expect("health should encode");
    let selection = ComponentSelection {
        component_ids: vec![ComponentId::new(1)],
    };
    let viewer = ViewerQuery {
        client_id,
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 32,
    };
    let mut replication_bridge = ReplicationTransportBridge::default();
    let mut replication_scratch = ReplicationScratch::default();
    let mut plan = ReplicationPlan::default();
    ReplicationPlanner::plan_for_viewer_into(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
        &mut replication_scratch,
        &mut plan,
    );
    let replication = replication_bridge
        .send_plan(
            &mut server_transport,
            viewer.client_id,
            station.tick(),
            &station,
            &components,
            &selection,
            &plan,
        )
        .expect("replication should send");
    assert!(replication.sent);

    let pump = client_bridge
        .pump_owned(&mut client_transport, 8)
        .expect("client frames should receive");
    assert_eq!(pump.command_acks_received(), 1);
    assert_eq!(pump.replication_frames_received(), 1);
    assert!(pump.command_acks[0].accepted);
    assert_eq!(pump.replication_frames[0].entities.len(), 1);

    println!(
        "client_bridge command_bytes={} acked={} replication_frames={} entities={} components={} applied_command={}",
        send.bytes_sent,
        pump.command_acks_received(),
        pump.replication_frames_received(),
        pump.entities_received(),
        pump.components_received(),
        applied.id.get()
    );
}
