//! Distance-based adaptive replication cadence SDK example.

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityHandle, EntityId, GridSpec, InstanceId,
    NodeId, PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget,
    ReplicationCadence, ReplicationPlanner, Station, StationConfig, StationId, Tick, ViewerQuery,
};

fn main() {
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    for _ in 0..10 {
        station.advance_tick();
    }

    let grid = GridSpec::new(32.0).expect("grid is valid");
    let mut index = CellIndex::new(grid);
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 2, 20, 128.0));

    let near = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(1),
        Position3::new(0.0, 0.0, 0.0),
    );
    let far = spawn_indexed(
        &mut station,
        &mut index,
        EntityId::new(2),
        Position3::new(120.0, 0.0, 0.0),
    );

    let mut last_sent = BTreeMap::new();
    last_sent.insert(near, Tick::new(9));
    last_sent.insert(far, Tick::new(9));

    let viewer = ViewerQuery {
        client_id: ClientId::new(7),
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 128.0,
        max_entities: 16,
    };
    let plan = ReplicationPlanner::plan_for_viewer_with_cadence(
        &station,
        &index,
        &policies,
        &viewer,
        &RangeOnlyVisibility,
        ReplicationBudget::default(),
        |handle| last_sent.get(&handle).copied(),
    );

    let policy = policies.get(PolicyId::new(1)).expect("policy exists");
    println!(
        "adaptive_cadence near_interval={} far_interval={} selected={} skipped_by_cadence={}",
        ReplicationCadence::interval_ticks(policy, station.config().tick_rate_hz, 0.0),
        ReplicationCadence::interval_ticks(policy, station.config().tick_rate_hz, 120.0 * 120.0),
        plan.stats.selected,
        plan.stats.skipped_by_cadence
    );
}

fn spawn_indexed(
    station: &mut Station,
    index: &mut CellIndex,
    entity_id: EntityId,
    position: Position3,
) -> EntityHandle {
    let handle = station
        .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(1))
        .expect("spawn should work");
    index.upsert(handle, position, Bounds::Point);
    handle
}
