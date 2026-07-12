//! Generated component schema SDK example.

use sectorsync_bench::plan_viewer_owned;
use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, ComponentEncodeScratch,
    ComponentFieldDescriptor, ComponentFieldType, ComponentId, ComponentMigrationMode,
    ComponentRegistry, ComponentStorageKind, ComponentStore, ComponentSyncMode, EntityId,
    GeneratedComponentSchema, GridSpec, InstanceId, NodeId, PolicyId, PolicyTable, Position3,
    RangeOnlyVisibility, ReplicationBudget, Station, StationConfig, StationId, Vec3, Vec3LeCodec,
    ViewerQuery,
};
use sectorsync_wire::{
    BinaryFrameEncoder, ComponentSelection, FrameEncoder, ReplicationFrameBuilder,
};

const TRANSFORM_FIELDS: &[ComponentFieldDescriptor] = &[ComponentFieldDescriptor::new(
    "position",
    ComponentFieldType::Vec3,
    0,
)];

const TRANSFORM_SCHEMA: GeneratedComponentSchema = GeneratedComponentSchema::new(
    ComponentId::new(20),
    "transform",
    ComponentStorageKind::SparseBlob,
    ComponentSyncMode::Delta,
    ComponentMigrationMode::Copy,
    12,
    TRANSFORM_FIELDS,
);

fn main() {
    let mut registry = ComponentRegistry::default();
    let schema = registry
        .register_generated_schema(&TRANSFORM_SCHEMA)
        .expect("generated schema should validate and register");
    assert_eq!(schema.fixed_size, Some(12));
    assert_eq!(
        registry
            .get(ComponentId::new(20))
            .expect("generated descriptor should exist")
            .schema_hash,
        TRANSFORM_SCHEMA.schema_hash()
    );

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
            EntityId::new(300),
            Position3::new(4.0, 5.0, 6.0),
            Bounds::Point,
            PolicyId::new(0),
        )
        .expect("spawn should work");
    index.upsert(handle, Position3::new(4.0, 5.0, 6.0), Bounds::Point);

    let mut components = ComponentStore::default();
    let mut encode_scratch = ComponentEncodeScratch::with_capacity(12);
    components
        .set_typed_with_scratch(
            &schema.descriptor,
            handle,
            1,
            &Vec3LeCodec,
            &Vec3::new(4.0, 5.0, 6.0),
            &mut encode_scratch,
        )
        .expect("generated component should encode");
    let position = Vec3::new(7.0, 8.0, 9.0);
    components
        .set_typed_with_scratch(
            &schema.descriptor,
            handle,
            2,
            &Vec3LeCodec,
            &position,
            &mut encode_scratch,
        )
        .expect("repeated component update should reuse scratch");
    let decoded = components
        .get_typed(ComponentId::new(20), handle, &Vec3LeCodec)
        .expect("generated component should decode");
    assert_eq!(decoded, position);

    let viewer = ViewerQuery {
        client_id: ClientId::new(3),
        position: Position3::new(4.0, 5.0, 6.0),
        radius: 128.0,
        max_entities: 16,
    };
    let plan = plan_viewer_owned(
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
            component_ids: vec![ComponentId::new(20)],
        },
    );
    assert_eq!(build.stats.encoded_components, 1);

    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_replication(&build.frame, &mut bytes)
        .expect("frame should encode");

    println!(
        "generated_schema hash={} fixed_size={} fields={} encode_scratch_capacity={} frame_bytes={}",
        schema.descriptor.schema_hash,
        schema.fixed_size.expect("fixed size"),
        TRANSFORM_SCHEMA.fields.len(),
        encode_scratch.retained_capacity(),
        bytes.len()
    );
}
