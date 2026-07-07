//! Load-aware station scheduler SDK example.

use sectorsync_core::prelude::{
    CellCoord3, CellLoadSample, InstanceId, NodeId, Station, StationConfig, StationId,
    StationLoadSample,
};
use sectorsync_runtime::{StationScheduleConfig, StationScheduler, StationSet};

fn main() {
    let mut stations = StationSet::default();
    stations.push(station(1));
    stations.push(station(2));
    stations.push(station(3));

    let samples = vec![
        StationLoadSample {
            station_id: StationId::new(1),
            owned_entities: 4,
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(2),
            owned_entities: 120,
            subscribers: 80,
            queued_events: 24,
            tick_cost_units: 2_000,
            cells: vec![CellLoadSample {
                cell: CellCoord3::new(0, 0, 0),
                owned_entities: 100,
                subscribers: 80,
                estimated_updates: 240,
                event_pressure: 12,
                ..CellLoadSample::default()
            }],
            ..StationLoadSample::default()
        },
        StationLoadSample {
            station_id: StationId::new(3),
            owned_entities: 40,
            subscribers: 20,
            queued_events: 6,
            tick_cost_units: 300,
            ..StationLoadSample::default()
        },
    ];

    let mut scheduler = StationScheduler::default();
    let plan = scheduler.advance_loaded(
        &mut stations,
        &samples,
        StationScheduleConfig {
            max_station_advances_per_step: 2,
        },
    );

    let selected = plan
        .selected
        .iter()
        .map(|candidate| candidate.station_id.get().to_string())
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "load_scheduler considered={} selected={} advances={} station_ids=[{}]",
        plan.candidates_considered, plan.stations_selected, plan.total_advances, selected
    );
}

fn station(station_id: u32) -> Station {
    Station::new(StationConfig {
        station_id: StationId::new(station_id),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}
