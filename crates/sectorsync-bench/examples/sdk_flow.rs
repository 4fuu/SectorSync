//! End-to-end SDK flow example.

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

fn main() {
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let grid = GridSpec::new(64.0).expect("grid is valid");
    let mut index = CellIndex::new(grid);
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
        client_id: ClientId::new(7),
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

    let mut encoder = BinaryFrameEncoder;
    let mut bytes = Vec::new();
    encoder
        .encode_replication(&build.frame, &mut bytes)
        .expect("frame should encode");
    let decoded = BinaryFrameDecoder
        .decode(&bytes)
        .expect("frame should decode");
    assert!(matches!(decoded, RuntimeFrame::Replication(_)));

    let mut transport = FakeTransport::default();
    transport
        .send(OutboundPacket {
            client_id: viewer.client_id,
            bytes,
        })
        .expect("fake transport should send");

    println!(
        "sdk_flow packets={} bytes={} entities={} components={}",
        transport.packets_sent(),
        transport.bytes_sent(),
        build.stats.encoded_entities,
        build.stats.encoded_components
    );
}
