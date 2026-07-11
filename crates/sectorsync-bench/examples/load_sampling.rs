//! Runtime load sampling feeding bounded station scheduling.

use sectorsync_core::prelude::{
    Bounds, CellIndex, EntityId, EventId, EventKind, EventPriority, GridSpec, InstanceId, NodeId,
    OwnerEpoch, PolicyId, Position3, Station, StationConfig, StationEvent, StationId, Tick,
};
use sectorsync_runtime::{
    EventRouter, StationIndexSet, StationLoadSampler, StationLoadSamplerScratch,
    StationScheduleConfig, StationScheduler, StationSet,
};

fn main() {
    let mut stations = StationSet::with_capacity(3);
    let mut indexes = StationIndexSet::with_capacity(3);
    for (station_id, owned_entities, ghost_entities) in [(1, 1, 0), (2, 5, 1), (3, 2, 0)] {
        let (station, index) = populated_station(station_id, owned_entities, ghost_entities);
        stations.push(station);
        indexes.insert(StationId::new(station_id), index);
    }

    let busy_station = StationId::new(2);
    let mut router = EventRouter::default();
    router.register_stations(&stations);
    for (event_id, kind) in [(1_u64, 1_u32), (2, 2)] {
        router
            .route(StationEvent {
                id: EventId::new(event_id),
                source: StationId::new(1),
                target: busy_station,
                source_tick: Tick::new(0),
                target_tick: Tick::new(2),
                priority: EventPriority::Important,
                kind: EventKind::Custom(kind),
            })
            .expect("bounded event queue should accept sample event");
    }

    let load_sampler = StationLoadSampler::default();
    let mut load_scratch = StationLoadSamplerScratch::new();
    let samples = load_sampler.sample_all_into(
        &stations,
        &indexes,
        &router,
        &[
            (busy_station, 24),
            (busy_station, 16),
            (StationId::new(3), 4),
        ],
        &mut load_scratch,
    );
    let busy_sample = samples
        .iter()
        .find(|sample| sample.station_id == busy_station)
        .expect("busy station should be sampled");
    assert_eq!(busy_sample.owned_entities, 5);
    assert_eq!(busy_sample.ghost_entities, 1);
    assert_eq!(busy_sample.subscribers, 40);
    assert_eq!(busy_sample.queued_events, 2);
    assert_eq!(busy_sample.cells.len(), 6);

    let mut scheduler = StationScheduler::default();
    let plan = scheduler.advance_loaded(
        &mut stations,
        samples,
        StationScheduleConfig {
            max_station_advances_per_step: 1,
        },
    );
    let selected = plan
        .selected
        .first()
        .expect("one station should be selected");
    assert_eq!(selected.station_id, busy_station);

    println!(
        "load_sampling samples={} station={} owned={} ghosts={} subscribers={} queued_events={} cells={} selected={} load_score={}",
        samples.len(),
        busy_sample.station_id.get(),
        busy_sample.owned_entities,
        busy_sample.ghost_entities,
        busy_sample.subscribers,
        busy_sample.queued_events,
        busy_sample.cells.len(),
        selected.station_id.get(),
        selected.load_score,
    );
}

#[allow(clippy::cast_precision_loss)]
fn populated_station(
    station_id: u32,
    owned_entities: usize,
    ghost_entities: usize,
) -> (Station, CellIndex) {
    let mut station = Station::new(StationConfig {
        station_id: StationId::new(station_id),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    });
    let grid = GridSpec::new(10.0).expect("grid should build");
    let mut index = CellIndex::new(grid);
    let policy_id = PolicyId::new(1);
    let base = station_id as f32 * 1_000.0;

    for offset in 0..owned_entities {
        let position = Position3::new(base + offset as f32 * 11.0, 0.0, 0.0);
        let handle = station
            .spawn_owned(
                EntityId::new(
                    u64::from(station_id) * 1_000
                        + u64::try_from(offset).expect("entity count must fit in u64"),
                ),
                position,
                Bounds::Point,
                policy_id,
            )
            .expect("owned entity should spawn");
        index.upsert(handle, position, Bounds::Point);
    }
    for offset in 0..ghost_entities {
        let position = Position3::new(base + (owned_entities + offset) as f32 * 11.0, 0.0, 0.0);
        let handle = station.upsert_ghost(
            EntityId::new(
                100_000
                    + u64::from(station_id) * 1_000
                    + u64::try_from(offset).expect("entity count must fit in u64"),
            ),
            position,
            Bounds::Point,
            policy_id,
            StationId::new(99),
            OwnerEpoch::new(1),
            Tick::new(20),
        );
        index.upsert(handle, position, Bounds::Point);
    }

    (station, index)
}
