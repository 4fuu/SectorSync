//! Cell split and migration SDK example.

use sectorsync_core::prelude::{
    Bounds, CellCoord3, CellIndex, EntityId, GridSpec, InstanceId, NodeId, PolicyId, Position3,
    SplitProposal, Station, StationConfig, StationId,
};
use sectorsync_runtime::{CellMigrationExecutor, CellOwnershipTable, StationSet};

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

    let mut source_index = CellIndex::new(grid);
    source_index.upsert(handle, Position3::new(1.0, 1.0, 1.0), Bounds::Point);
    let mut target_index = CellIndex::new(grid);

    let proposal = SplitProposal {
        source_station: StationId::new(1),
        cells_to_move: vec![cell],
        moved_pressure_score: 100,
    };
    let mut ownership = CellOwnershipTable::default();
    ownership.assign(cell, StationId::new(1));
    let update = ownership.apply_split(&proposal, StationId::new(2));

    let report = CellMigrationExecutor::migrate_cells(
        &mut stations,
        &mut source_index,
        &mut target_index,
        update.source_station,
        update.target_station,
        &update.moved_cells,
        4,
    )
    .expect("cell migration should work");

    println!(
        "split_migration cells={} migrated_entities={} target_index_entities={}",
        update.moved_cells.len(),
        report.entity_migrations.len(),
        target_index.entity_count()
    );
}
