//! Cell split and migration SDK example.

use sectorsync_core::prelude::{
    Bounds, CellCoord3, CellIndex, CellLoadSample, EntityId, GridSpec, HotspotThresholds,
    InstanceId, NodeId, PolicyId, Position3, Station, StationConfig, StationId, StationLoadSample,
};
use sectorsync_runtime::{
    CellOwnershipTable, SplitScheduler, SplitSchedulerConfig, StationIndexSet, StationSet,
};

fn main() {
    let grid = GridSpec::new(16.0).expect("grid is valid");
    let cell = CellCoord3::new(0, 0, 0);
    let mut stations = StationSet::default();
    let mut source = Station::new(StationConfig {
        station_id: StationId::new(1),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let handle = source
        .spawn_owned(
            EntityId::new(42),
            Position3::new(1.0, 1.0, 1.0),
            Bounds::Point,
            PolicyId::new(0),
        )
        .expect("spawn should work");
    stations.push(source);
    stations.push(Station::new(StationConfig {
        station_id: StationId::new(2),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    }));

    let mut indexes = StationIndexSet::default();
    let mut source_index = CellIndex::new(grid);
    source_index.upsert(handle, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
    indexes.insert(StationId::new(1), source_index);
    indexes.insert(StationId::new(2), CellIndex::new(grid));

    let mut ownership = CellOwnershipTable::default();
    ownership.assign(cell, StationId::new(1));

    let samples = vec![
        StationLoadSample {
            station_id: StationId::new(1),
            owned_entities: 100,
            subscribers: 100,
            tick_cost_units: 1000,
            cells: vec![CellLoadSample {
                cell,
                owned_entities: 100,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(2),
            owned_entities: 1,
            ..StationLoadSample::default()
        },
    ];
    let scheduler = SplitScheduler::new(SplitSchedulerConfig {
        thresholds: HotspotThresholds {
            max_station_entities: 10,
            max_station_subscribers: 10,
            max_cell_pressure: 10,
            ..HotspotThresholds::default()
        },
        max_actions_per_pass: 1,
        max_cells_per_action: 1,
        ghost_ttl_ticks: 4,
        ..SplitSchedulerConfig::default()
    });
    let schedule = scheduler.plan(&samples);
    let report = scheduler
        .execute(&schedule, &mut stations, &mut indexes, &mut ownership)
        .expect("split schedule should execute");
    let action = schedule
        .actions
        .first()
        .expect("hot source should produce one split action");
    let source_pressure_after = action
        .source_score
        .saturating_sub(action.proposal.moved_pressure_score);
    let migrated_cells = report
        .ownership_updates
        .iter()
        .map(|update| update.moved_cells.len())
        .sum::<usize>();
    let migrated_entities = report
        .cell_migrations
        .iter()
        .map(|migration| migration.entity_migrations.len())
        .sum::<usize>();

    let target_index_entities = indexes
        .get(StationId::new(2))
        .expect("target index exists")
        .entity_count();

    println!(
        "split_migration actions={} source_pressure_before={} source_pressure_after={} target_pressure_before={} target_pressure_after={} migrated_cells={} migrated_entities={} target_index_entities={}",
        schedule.actions.len(),
        action.source_score,
        source_pressure_after,
        action.target_score,
        action.estimated_target_score_after_move,
        migrated_cells,
        migrated_entities,
        target_index_entities
    );
}
