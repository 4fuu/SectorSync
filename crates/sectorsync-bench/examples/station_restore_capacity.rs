//! Guarded benchmark for capacity-aware multi-room Station restore.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, EntityId, InstanceId, NodeId, PolicyId, Position3, SnapshotVersion, Station,
    StationConfig, StationId,
};

const DEFAULT_ROOMS: usize = 20;
const DEFAULT_ENTITIES_PER_ROOM: usize = 512;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_ROOMS: usize = 100;
const GUARD_MAX_ENTITIES_PER_ROOM: usize = 2_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_ENTITY_RESTORES: usize = 500_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    rooms: usize,
    entities_per_room: usize,
    ticks: usize,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    stations_restored: usize,
    entities_restored: usize,
    restore_checksum: u64,
    initial_entity_capacity_shortfalls: usize,
    initial_id_index_capacity_shortfalls: usize,
    entity_capacity_growth_restores: usize,
    id_index_capacity_growth_restores: usize,
    retained_entity_capacity: usize,
    retained_id_index_capacity: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    capacity_ok: bool,
    workload_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

fn main() {
    let config = parse_config();
    let result = run(config);
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run(config: Config) -> ResultSummary {
    let mut base = Vec::with_capacity(config.rooms);
    let mut next_entity_id = 1_u64;
    for room in 0..config.rooms {
        let station_config = station_config(room);
        let mut station = Station::with_capacity(station_config, config.entities_per_room);
        for _ in 0..config.entities_per_room {
            station
                .spawn_owned(
                    EntityId::new(next_entity_id),
                    Position3::new(0.0, 0.0, 0.0),
                    Bounds::Point,
                    PolicyId::new(0),
                )
                .expect("guarded entity should spawn");
            next_entity_id = next_entity_id.saturating_add(1);
        }
        base.push((station_config, station.snapshot(SnapshotVersion::default())));
    }

    let mut workloads = Vec::with_capacity(config.ticks);
    for _ in 0..config.ticks {
        workloads.push(base.clone());
    }
    drop(base);

    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut ticks_completed = 0_usize;
    let mut stations_restored = 0_usize;
    let mut entities_restored = 0_usize;
    let mut restore_checksum = 0_u64;
    let mut initial_entity_capacity_shortfalls = 0_usize;
    let mut initial_id_index_capacity_shortfalls = 0_usize;
    let mut entity_capacity_growth_restores = 0_usize;
    let mut id_index_capacity_growth_restores = 0_usize;
    let mut retained_entity_capacity = 0_usize;
    let mut retained_id_index_capacity = 0_usize;
    let mut time_budget_exhausted = false;

    for workload in &mut workloads {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        let mut restored_batch = Vec::with_capacity(config.rooms);
        for (station_config, snapshot) in core::mem::take(workload) {
            let expected_entities = snapshot.entities.len();
            let (station, stats) = Station::restore_tracked(station_config, snapshot)
                .expect("guarded snapshot should restore");
            initial_entity_capacity_shortfalls = initial_entity_capacity_shortfalls.saturating_add(
                usize::from(stats.initial_entity_capacity < expected_entities),
            );
            initial_id_index_capacity_shortfalls = initial_id_index_capacity_shortfalls
                .saturating_add(usize::from(
                    stats.initial_id_index_capacity < expected_entities,
                ));
            entity_capacity_growth_restores = entity_capacity_growth_restores
                .saturating_add(usize::from(stats.entity_capacity_grew));
            id_index_capacity_growth_restores = id_index_capacity_growth_restores
                .saturating_add(usize::from(stats.id_index_capacity_grew));
            retained_entity_capacity =
                retained_entity_capacity.saturating_add(station.entity_capacity());
            retained_id_index_capacity =
                retained_id_index_capacity.saturating_add(station.id_index_capacity());
            entities_restored = entities_restored.saturating_add(station.len());
            for entity in station.iter() {
                restore_checksum = restore_checksum.saturating_add(entity.id.get());
            }
            restored_batch.push(station);
        }
        black_box(&restored_batch);
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        stations_restored = stations_restored.saturating_add(restored_batch.len());
        ticks_completed = ticks_completed.saturating_add(1);
    }

    black_box(restore_checksum);
    latencies.sort_by(f64::total_cmp);
    let expected_stations = config.rooms.saturating_mul(config.ticks);
    let expected_entities = expected_stations.saturating_mul(config.entities_per_room);
    let capacity_ok = initial_entity_capacity_shortfalls == 0
        && initial_id_index_capacity_shortfalls == 0
        && entity_capacity_growth_restores == 0
        && id_index_capacity_growth_restores == 0;
    let workload_ok = ticks_completed == config.ticks
        && stations_restored == expected_stations
        && entities_restored == expected_entities;
    let benchmark_ok = capacity_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        ticks_completed,
        stations_restored,
        entities_restored,
        restore_checksum,
        initial_entity_capacity_shortfalls,
        initial_id_index_capacity_shortfalls,
        entity_capacity_growth_restores,
        id_index_capacity_growth_restores,
        retained_entity_capacity,
        retained_id_index_capacity,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        capacity_ok,
        workload_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn station_config(room: usize) -> StationConfig {
    StationConfig {
        station_id: StationId::new(
            u32::try_from(room.saturating_add(1)).expect("guarded room id fits u32"),
        ),
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(
            u64::try_from(room.saturating_add(1)).expect("guarded room id fits u64"),
        ),
        tick_rate_hz: 20,
    }
}

fn parse_config() -> Config {
    let mut rooms = DEFAULT_ROOMS;
    let mut entities_per_room = DEFAULT_ENTITIES_PER_ROOM;
    let mut ticks = DEFAULT_TICKS;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--rooms=") {
            rooms = value.parse().unwrap_or(rooms);
        } else if let Some(value) = arg.strip_prefix("--entities-per-room=") {
            entities_per_room = value.parse().unwrap_or(entities_per_room);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    rooms = rooms.max(1);
    entities_per_room = entities_per_room.max(1);
    ticks = ticks.max(1);
    let requested = (rooms, entities_per_room, ticks);
    if !allow_heavy {
        rooms = rooms.min(GUARD_MAX_ROOMS);
        entities_per_room = entities_per_room.min(GUARD_MAX_ENTITIES_PER_ROOM);
        ticks = ticks.min(GUARD_MAX_TICKS);
        let passes = rooms.saturating_mul(ticks).max(1);
        if passes.saturating_mul(entities_per_room) > GUARD_MAX_ENTITY_RESTORES {
            entities_per_room = (GUARD_MAX_ENTITY_RESTORES / passes).max(1);
        }
    }
    Config {
        rooms,
        entities_per_room,
        ticks,
        allow_heavy,
        guard_applied: requested != (rooms, entities_per_room, ticks),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile(values: &[f64], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).ceil() as usize;
    values[index.min(values.len() - 1)]
}

#[allow(clippy::too_many_lines)]
fn print_result(config: Config, result: &ResultSummary) {
    println!("SectorSync Station restore capacity benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_entities_per_room={GUARD_MAX_ENTITIES_PER_ROOM}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_entity_restores={GUARD_MAX_ENTITY_RESTORES}");
    println!("rooms={}", config.rooms);
    println!("entities_per_room={}", config.entities_per_room);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("stations_restored={}", result.stations_restored);
    println!("entities_restored={}", result.entities_restored);
    println!("restore_checksum={}", result.restore_checksum);
    println!(
        "initial_entity_capacity_shortfalls={}",
        result.initial_entity_capacity_shortfalls
    );
    println!(
        "initial_id_index_capacity_shortfalls={}",
        result.initial_id_index_capacity_shortfalls
    );
    println!(
        "entity_capacity_growth_restores={}",
        result.entity_capacity_growth_restores
    );
    println!(
        "id_index_capacity_growth_restores={}",
        result.id_index_capacity_growth_restores
    );
    println!(
        "retained_entity_capacity={}",
        result.retained_entity_capacity
    );
    println!(
        "retained_id_index_capacity={}",
        result.retained_id_index_capacity
    );
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_capacity_ok={}", result.capacity_ok);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
