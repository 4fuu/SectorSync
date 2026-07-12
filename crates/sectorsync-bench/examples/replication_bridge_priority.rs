//! Priority-aware replication transport bridge SDK example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, ComponentDescriptor, ComponentId,
    ComponentMigrationMode, ComponentStore, ComponentSyncMode, EntityId, GridSpec, InstanceId,
    NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    ReplicationPlan, ReplicationPlanner, ReplicationScratch, ReplicationSelectionMode, Station,
    StationConfig, StationId, U32LeCodec, ViewerQuery,
};
use sectorsync_runtime::{ReplicationReceiveBridge, ReplicationReceiveConfig};
use sectorsync_runtime::{ReplicationTransportBridge, ReplicationTransportConfig};
use sectorsync_transport::{ClientTransportLimits, InMemoryTransportHub};
use sectorsync_wire::{ComponentSelection, ReplicationFrameBuilder};

#[allow(clippy::too_many_lines)]
fn main() {
    let client_id = ClientId::new(7);
    let server_id = ClientId::new(0);
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let mut index = CellIndex::new(GridSpec::new(64.0).expect("grid is valid"));
    let mut policies = PolicyTable::default();
    let mut ambient = CompiledSyncPolicy::new(PolicyId::new(1), 1, 10, 256.0);
    ambient.priority_weight = 1;
    let mut critical = CompiledSyncPolicy::new(PolicyId::new(2), 10, 20, 256.0);
    critical.priority_weight = 10;
    policies.set(ambient);
    policies.set(critical);

    let ambient_handle = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(100),
        Position3::new(0.0, 0.0, 0.0),
        PolicyId::new(1),
    );
    let critical_handle = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(200),
        Position3::new(128.0, 0.0, 0.0),
        PolicyId::new(2),
    );

    let health = ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "health",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        4,
    );
    let mut components = ComponentStore::default();
    components
        .set_typed(&health, ambient_handle, 1, &U32LeCodec, &100)
        .expect("ambient should encode");
    components
        .set_typed(&health, critical_handle, 1, &U32LeCodec, &200)
        .expect("critical should encode");

    let viewer = ViewerQuery {
        client_id,
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 1,
    };
    let selection = ComponentSelection {
        component_ids: vec![ComponentId::new(1)],
    };
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 4,
        max_packet_bytes: 512,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:23207".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:23200".parse().expect("server addr"))
        .expect("server endpoint should register");
    let mut bridge = ReplicationTransportBridge::new(
        ReplicationTransportConfig {
            budget: ReplicationBudget {
                max_entities: 1,
                max_bytes: 32,
                estimated_entity_bytes: 32,
            },
            send_empty_frames: false,
        },
        ReplicationFrameBuilder::default(),
    );

    let mut scratch = ReplicationScratch::default();
    let mut plan = ReplicationPlan::default();
    ReplicationPlanner::plan_for_viewer_configured_into(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        bridge.config().budget,
        ReplicationSelectionMode::Prioritized,
        |_, _, _| true,
        |_, _| None,
        &mut scratch,
        &mut plan,
    );
    let report = bridge
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
    assert!(report.sent);

    let mut receive_bridge = ReplicationReceiveBridge::new(
        ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
    );
    let pump = receive_bridge
        .pump_owned(&mut client_transport, 4)
        .expect("replication should receive");
    assert_eq!(pump.frames_received(), 1);
    assert_eq!(pump.frames[0].entities[0].entity_id, EntityId::new(200));

    println!(
        "replication_bridge_priority selected={} skipped_by_budget={} sent={} received_entity={}",
        report.selected_entities,
        report.skipped_by_budget,
        bridge.stats().frames_sent,
        pump.frames[0].entities[0].entity_id.get()
    );
}

fn spawn_indexed(
    station: &mut Station,
    index: &mut CellIndex,
    entity_id: EntityId,
    position: Position3,
    policy_id: PolicyId,
) -> sectorsync_core::prelude::EntityHandle {
    let handle = station
        .spawn_owned(entity_id, position, Bounds::Point, policy_id)
        .expect("spawn should work");
    index.upsert(handle, position, Bounds::Point);
    handle
}
