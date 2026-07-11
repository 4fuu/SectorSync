//! Explicit bounded thread-pool and deterministic station-batch example.

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, ReplicationBudget, Station, StationConfig, StationId,
    ViewerQuery,
};
use sectorsync_runtime::{
    ParallelReplicationScratch, ReplicationThreadPool, ReplicationThreadPoolConfig,
    StationReplicationBatch,
};

fn main() {
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 32, 128.0));
    let mut stations = Vec::new();
    let mut indexes = Vec::new();
    let mut viewer_groups = Vec::new();

    for station_number in 0_u32..12 {
        let mut station = Station::new(StationConfig {
            station_id: StationId::new(station_number),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 128,
        });
        let mut index = CellIndex::new(GridSpec::new(32.0).expect("valid grid"));
        for entity_number in 0_u16..32 {
            let position = Position3::new(f32::from(entity_number) * 4.0, 0.0, 0.0);
            let handle = station
                .spawn_owned(
                    EntityId::new(u64::from(station_number) * 1_000 + u64::from(entity_number)),
                    position,
                    Bounds::Point,
                    PolicyId::new(1),
                )
                .expect("unique entity id");
            index.upsert(handle, position, Bounds::Point);
        }
        stations.push(station);
        indexes.push(index);
        viewer_groups.push(vec![ViewerQuery {
            client_id: ClientId::new(u64::from(station_number)),
            position: Position3::new(64.0, 0.0, 0.0),
            radius: 96.0,
            max_entities: 64,
        }]);
    }

    let batches = stations
        .iter()
        .zip(&indexes)
        .zip(&viewer_groups)
        .map(|((station, index), viewers)| StationReplicationBatch::new(station, index, viewers))
        .collect::<Vec<_>>();
    let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
        .expect("explicit pool should build");
    let mut scratch = ParallelReplicationScratch::new();
    let result = pool.plan_station_range_batches(
        &batches,
        &policies,
        ReplicationBudget::default(),
        &mut scratch,
    );

    assert_eq!(result.batches.len(), 12);
    assert_eq!(result.stats.viewers, 12);
    assert_eq!(scratch.lanes(), 2);
    assert!(result.stats.selected > 0);
    println!("threads={}", pool.threads());
    println!("scratch_lanes={}", scratch.lanes());
    println!("viewers={}", result.stats.viewers);
    println!("selected={}", result.stats.selected);
}
