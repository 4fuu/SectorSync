//! Bounded replication send/ACK tracker SDK example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, ComponentDescriptor, ComponentId, ComponentMigrationMode,
    ComponentStore, ComponentSyncMode, DirtyMask, EntityId, GridSpec, InstanceId, NodeId, PolicyId,
    PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlanner,
    ReplicationTracker, ReplicationTrackerConfig, Station, StationConfig, StationId, U32LeCodec,
    ViewerQuery,
};

fn main() {
    let client_id = ClientId::new(7);
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let grid = GridSpec::new(32.0).expect("grid is valid");
    let mut index = CellIndex::new(grid);
    let mut policies = PolicyTable::default();
    policies.set(sectorsync_core::prelude::CompiledSyncPolicy::new(
        PolicyId::new(1),
        2,
        20,
        128.0,
    ));

    let handle = station
        .spawn_owned(
            EntityId::new(100),
            Position3::new(0.0, 0.0, 0.0),
            Bounds::Point,
            PolicyId::new(1),
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
        .expect("component should encode");

    let viewer = ViewerQuery {
        client_id,
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 128.0,
        max_entities: 16,
    };
    let mut tracker = ReplicationTracker::new(ReplicationTrackerConfig { max_entries: 16 });

    let first = ReplicationPlanner::plan_for_viewer_with_cadence(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
        |entity| tracker.last_sent(client_id, entity),
    );
    tracker
        .record_plan_sent(client_id, &first, station.tick())
        .expect("tracker has capacity");
    let acked = tracker.acknowledge_plan(client_id, &first, station.tick());
    station
        .clear_dirty(handle, DirtyMask::TRANSFORM)
        .expect("clear entity dirty");
    let cleared_components = components.clear_dirty_for_entity(handle);

    let second = ReplicationPlanner::plan_for_viewer_with_cadence(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
        |entity| tracker.last_sent(client_id, entity),
    );
    let component_dirty = components
        .get_blob(ComponentId::new(1), handle)
        .expect("component exists")
        .dirty;
    let transform_dirty = station
        .get(handle)
        .expect("entity exists")
        .dirty
        .contains(DirtyMask::TRANSFORM);

    println!(
        "replication_tracker first_selected={} second_selected={} acked={} cleared_components={} component_dirty={} transform_dirty={}",
        first.stats.selected,
        second.stats.selected,
        acked,
        cleared_components,
        component_dirty,
        transform_dirty
    );
}
