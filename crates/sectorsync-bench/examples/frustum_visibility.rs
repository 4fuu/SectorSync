//! 3D frustum visibility SDK example.

use sectorsync_bench::plan_viewer_owned;
use sectorsync_core::prelude::{
    Aabb3, AndVisibility, Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, Frustum3,
    FrustumVisibility, GridSpec, InstanceId, NodeId, PolicyId, PolicyTable, Position3,
    RangeOnlyVisibility, ReplicationBudget, Station, StationConfig, StationId, ViewerQuery,
};

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

    spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(1),
        Position3::new(48.0, 0.0, 0.0),
    );
    spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(2),
        Position3::new(-32.0, 0.0, 0.0),
    );
    spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(3),
        Position3::new(80.0, 96.0, 0.0),
    );

    let viewer = ViewerQuery {
        client_id: ClientId::new(7),
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 256.0,
        max_entities: 16,
    };
    let frustum = Frustum3::from_aabb(Aabb3::new(
        Position3::new(0.0, -64.0, -64.0),
        Position3::new(160.0, 64.0, 64.0),
    ));
    let visibility = AndVisibility::new(RangeOnlyVisibility, FrustumVisibility::new(frustum));
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
        "frustum_visibility candidates={} selected={} entity_ids=[{}]",
        plan.stats.candidates, plan.stats.selected, selected_ids
    );
}

fn spawn_indexed(
    station: &mut Station,
    index: &mut CellIndex,
    entity_id: EntityId,
    position: Position3,
) {
    let handle = station
        .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(1))
        .expect("spawn should work");
    index.upsert(handle, position, Bounds::Point);
}
