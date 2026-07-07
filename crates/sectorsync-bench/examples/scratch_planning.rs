//! Allocation-aware replication planning scratch SDK example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlanner,
    ReplicationScratch, Station, StationConfig, StationId, ViewerQuery,
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

    for entity_index in 0..64_u64 {
        let x = (entity_index % 8) as f32 * 16.0;
        let z = (entity_index / 8) as f32 * 16.0;
        let position = Position3::new(x, 0.0, z);
        let handle = station
            .spawn_owned(
                EntityId::new(entity_index + 1),
                position,
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("spawn should work");
        index.upsert(handle, position, Bounds::Point);
    }

    let viewers = [
        ViewerQuery {
            client_id: ClientId::new(1),
            position: Position3::new(0.0, 0.0, 0.0),
            radius: 128.0,
            max_entities: 16,
        },
        ViewerQuery {
            client_id: ClientId::new(2),
            position: Position3::new(96.0, 0.0, 96.0),
            radius: 128.0,
            max_entities: 16,
        },
    ];

    let mut scratch = ReplicationScratch::default();
    let mut selected = 0_usize;
    let mut last_candidates = 0_usize;
    for viewer in &viewers {
        let plan = ReplicationPlanner::plan_for_viewer_with_scratch(
            &station,
            &index,
            &policies,
            viewer,
            &RangeOnlyVisibility,
            ReplicationBudget::default(),
            &mut scratch,
        );
        selected += plan.stats.selected;
        last_candidates = scratch.candidate_count();
    }

    println!(
        "scratch_planning viewers={} selected={} last_candidates={} priority_capacity={}",
        viewers.len(),
        selected,
        last_candidates,
        scratch.prioritized_capacity()
    );
}
