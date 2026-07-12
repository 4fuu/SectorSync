//! Tag-aware visibility SDK example.

use sectorsync_bench::plan_viewer_owned;
use sectorsync_core::prelude::{
    AndVisibility, Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, EntityTags, GridSpec,
    InstanceId, NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    Station, StationConfig, StationId, TagVisibility, ViewerQuery,
};

const TAG_STATIC: EntityTags = EntityTags::from_bits(1 << 0);
const TAG_FAST_MOVER: EntityTags = EntityTags::from_bits(1 << 1);

fn main() {
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let grid = GridSpec::new(32.0).expect("grid is valid");
    let mut index = CellIndex::new(grid);
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 20, 256.0));

    let crate_handle = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(1),
        Position3::new(48.0, 0.0, 0.0),
    );
    let projectile_handle = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(2),
        Position3::new(56.0, 0.0, 0.0),
    );
    station
        .set_tags(crate_handle, TAG_STATIC)
        .expect("tag static entity");
    station
        .set_tags(projectile_handle, TAG_FAST_MOVER)
        .expect("tag fast mover");

    let viewer = ViewerQuery {
        client_id: ClientId::new(7),
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 16,
    };
    let visibility = AndVisibility::new(
        RangeOnlyVisibility,
        TagVisibility::new(TAG_STATIC, TAG_FAST_MOVER),
    );
    let plan = plan_viewer_owned(
        &station,
        &index,
        &policies,
        &viewer,
        &visibility,
        ReplicationBudget::default(),
    );

    let selected_ids = plan
        .entities
        .iter()
        .filter_map(|handle| station.get(*handle))
        .map(|entity| entity.id.get().to_string())
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "tag_visibility candidates={} selected={} entity_ids=[{}]",
        plan.stats.candidates, plan.stats.selected, selected_ids
    );
}

fn spawn_indexed(
    station: &mut Station,
    index: &mut CellIndex,
    entity_id: EntityId,
    position: Position3,
) -> sectorsync_core::prelude::EntityHandle {
    let handle = station
        .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(1))
        .expect("spawn should work");
    index.upsert(handle, position, Bounds::Point);
    handle
}
