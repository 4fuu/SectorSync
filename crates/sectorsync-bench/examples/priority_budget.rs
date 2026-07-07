//! Budget-aware replication priority SDK example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlanner,
    Station, StationConfig, StationId, ViewerQuery,
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
    let mut ambient = CompiledSyncPolicy::new(PolicyId::new(1), 1, 10, 128.0);
    ambient.priority_weight = 1;
    let mut critical = CompiledSyncPolicy::new(PolicyId::new(2), 10, 20, 128.0);
    critical.priority_weight = 10;
    policies.set(ambient);
    policies.set(critical);

    let nearby_ambient = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(1),
        Position3::new(0.0, 0.0, 0.0),
        PolicyId::new(1),
    );
    let farther_critical = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(2),
        Position3::new(96.0, 0.0, 0.0),
        PolicyId::new(2),
    );

    let viewer = ViewerQuery {
        client_id: ClientId::new(7),
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 128.0,
        max_entities: 1,
    };
    let plan = ReplicationPlanner::plan_for_viewer_prioritized(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget {
            max_entities: 1,
            max_bytes: 32,
            estimated_entity_bytes: 32,
        },
    );
    assert_eq!(plan.entities, vec![farther_critical]);

    let selected_id = station
        .get(plan.entities[0])
        .expect("selected entity")
        .id
        .get();
    println!(
        "priority_budget nearby_ambient_handle={} selected_entity={} skipped_by_budget={}",
        nearby_ambient.index(),
        selected_id,
        plan.stats.skipped_by_budget
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
