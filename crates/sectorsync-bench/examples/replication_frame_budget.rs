//! Focused concrete replication frame byte-budget benchmark.

use sectorsync_core::prelude::{
    Bounds, ClientId, ComponentDescriptor, ComponentId, ComponentMigrationMode, ComponentStore,
    ComponentSyncMode, EntityId, InstanceId, NodeId, PolicyId, Position3, ReplicationPlan, Station,
    StationConfig, StationId, Tick,
};
use sectorsync_wire::{ComponentSelection, ReplicationFrameBuilder, ReplicationFrameLimits};

const ENTITIES: usize = 32;
const FRAME_BUDGET: usize = 512;
const COMPONENT_BYTES: usize = 64;

fn main() {
    let mut station = Station::with_capacity(
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 30,
        },
        ENTITIES,
    );
    let descriptor = ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "benchmark.payload",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        COMPONENT_BYTES,
    );
    let mut components = ComponentStore::default();
    components.reserve_component(descriptor.id, ENTITIES);
    let mut plan = ReplicationPlan::default();
    plan.entities.reserve(ENTITIES);
    for entity in 0_u16..u16::try_from(ENTITIES).expect("guarded count fits u16") {
        let handle = station
            .spawn_owned(
                EntityId::new(u64::from(entity) + 1),
                Position3::new(f32::from(entity), 0.0, 0.0),
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("benchmark entity should spawn");
        components
            .set_blob(
                &descriptor,
                handle,
                1,
                vec![u8::try_from(entity).expect("guarded byte fits u8"); COMPONENT_BYTES],
            )
            .expect("benchmark component should fit");
        plan.entities.push(handle);
    }
    plan.stats.selected = plan.entities.len();
    plan.stats.estimated_bytes = ENTITIES * 8;

    let builder = ReplicationFrameBuilder::new(ReplicationFrameLimits {
        max_entity_deltas: ENTITIES,
        max_components_per_entity: 1,
        max_component_bytes: COMPONENT_BYTES,
    });
    let selection = ComponentSelection {
        component_ids: vec![descriptor.id],
    };
    let mut bytes = Vec::new();
    let build = builder
        .encode_binary_bounded_into(
            ClientId::new(1),
            Tick::new(1),
            &station,
            &plan,
            &components,
            &selection,
            FRAME_BUDGET,
            &mut bytes,
        )
        .expect("bounded benchmark frame should encode");
    let benchmark_ok = plan.stats.estimated_bytes <= FRAME_BUDGET
        && bytes.len() <= FRAME_BUDGET
        && build.encoded_entities > 0
        && build.encoded_entities < ENTITIES
        && build.skipped_entities_by_frame_bytes == 1;

    println!("SectorSync replication frame budget benchmark");
    println!("planned_entities={ENTITIES}");
    println!("estimated_plan_bytes={}", plan.stats.estimated_bytes);
    println!("frame_budget_bytes={FRAME_BUDGET}");
    println!("encoded_frame_bytes={}", bytes.len());
    println!("encoded_entities={}", build.encoded_entities);
    println!("encoded_components={}", build.encoded_components);
    println!(
        "skipped_entities_by_frame_bytes={}",
        build.skipped_entities_by_frame_bytes
    );
    println!(
        "threshold_concrete_frame_budget_ok={}",
        bytes.len() <= FRAME_BUDGET
    );
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}
