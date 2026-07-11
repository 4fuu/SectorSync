//! Replication transport bridge SDK example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, ComponentDescriptor, ComponentId,
    ComponentMigrationMode, ComponentStore, ComponentSyncMode, EntityId, GridSpec, InstanceId,
    NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, Station, StationConfig,
    StationId, U32LeCodec, ViewerQuery,
};
use sectorsync_runtime::{
    ReplicationReceiveBridge, ReplicationReceiveConfig, ReplicationTransportBridge,
};
use sectorsync_transport::{ClientTransportLimits, InMemoryTransportHub};
use sectorsync_wire::ComponentSelection;

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

    let viewer = ViewerQuery {
        client_id,
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 32,
    };
    let selection = ComponentSelection {
        component_ids: vec![ComponentId::new(1)],
    };

    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 4,
        max_packet_bytes: 512,
    });
    let mut client_transport = hub
        .endpoint(client_id, "127.0.0.1:22007".parse().expect("client addr"))
        .expect("client endpoint should register");
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:22000".parse().expect("server addr"))
        .expect("server endpoint should register");

    let mut bridge = ReplicationTransportBridge::default();
    let report = bridge
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
        .expect("replication should send");
    assert!(report.sent);

    let mut receive_bridge = ReplicationReceiveBridge::new(
        ReplicationReceiveConfig::new(client_id).with_expected_source(server_id),
    );
    let mut entities = 0_usize;
    let mut received_components = 0_usize;
    let visit = receive_bridge
        .pump_visit(&mut client_transport, 4, |frame| {
            assert_eq!(frame.client_id, client_id);
            for entity in frame.entities() {
                entities = entities.saturating_add(1);
                for component in entity.components() {
                    assert_eq!(component.bytes, 100_u32.to_le_bytes());
                    received_components = received_components.saturating_add(1);
                }
            }
            Ok::<_, core::convert::Infallible>(())
        })
        .expect("replication should receive and visit");
    assert_eq!(visit.frames_received, 1);
    assert_eq!(entities, 1);
    assert_eq!(received_components, 1);

    println!(
        "replication_bridge sent={} recv={} bytes={} selected={} entities={} components={}",
        bridge.stats().frames_sent,
        receive_bridge.stats().frames_received,
        bridge.stats().bytes_sent,
        report.selected_entities,
        visit.entities_received,
        visit.components_received
    );
}
