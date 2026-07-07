//! End-to-end SDK flow integration test.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, ComponentDescriptor, ComponentId,
    ComponentMigrationMode, ComponentStore, ComponentSyncMode, EntityId, GridSpec, InstanceId,
    NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    ReplicationPlanner, Station, StationConfig, StationId, U32LeCodec, ViewerQuery,
};
use sectorsync_transport::{FakeTransport, OutboundPacket, TransportSink};
use sectorsync_wire::{
    BinaryFrameDecoder, BinaryFrameEncoder, ComponentSelection, FrameDecoder, FrameEncoder,
    ReplicationFrameBuilder, RuntimeFrame,
};

#[test]
fn sdk_flow_builds_encodes_decodes_and_sends_replication_frame() {
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
            EntityId::new(10),
            Position3::new(1.0, 2.0, 3.0),
            Bounds::Point,
            PolicyId::new(0),
        )
        .expect("spawn should work");
    index.upsert(handle, Position3::new(1.0, 2.0, 3.0), Bounds::Point);

    let health = ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "health",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        4,
    );
    let mut components = ComponentStore::default();
    components
        .set_typed(&health, handle, 1, &U32LeCodec, &99)
        .expect("typed component should write");

    let viewer = ViewerQuery {
        client_id: ClientId::new(1),
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 32,
    };
    let plan = ReplicationPlanner::plan_for_viewer(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
    );
    assert_eq!(plan.stats.selected, 1);

    let build = ReplicationFrameBuilder::default().build(
        viewer.client_id,
        station.tick(),
        &station,
        &plan,
        &components,
        &ComponentSelection {
            component_ids: vec![ComponentId::new(1)],
        },
    );
    assert_eq!(build.stats.encoded_entities, 1);
    assert_eq!(build.stats.encoded_components, 1);

    let mut encoder = BinaryFrameEncoder;
    let mut bytes = Vec::new();
    encoder
        .encode_replication(&build.frame, &mut bytes)
        .expect("encode should work");

    let decoded = BinaryFrameDecoder
        .decode(&bytes)
        .expect("decode should work");
    let RuntimeFrame::Replication(frame) = decoded else {
        panic!("expected replication frame");
    };
    assert_eq!(frame.entities[0].components[0].bytes, 99_u32.to_le_bytes());

    let mut transport = FakeTransport::default();
    transport
        .send(OutboundPacket {
            client_id: viewer.client_id,
            bytes,
        })
        .expect("fake send should work");
    assert_eq!(transport.packets_sent(), 1);
}
