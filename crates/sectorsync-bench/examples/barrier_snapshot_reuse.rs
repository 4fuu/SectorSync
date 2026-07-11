//! Guarded A/B benchmark for reusable multi-room barrier snapshots.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, Bounds, CommandQueueMode, EntityId, InstanceId, NodeId,
    PolicyId, Position3, SnapshotVersion, Station, StationConfig, StationId, StationSnapshot, Tick,
};
use sectorsync_runtime::{BarrierController, BarrierSnapshotScratch, StationSet};

const DEFAULT_ROOMS: usize = 50;
const DEFAULT_ENTITIES_PER_ROOM: usize = 256;
const DEFAULT_EXPORTS: usize = 10;
const GUARD_MAX_ROOMS: usize = 100;
const GUARD_MAX_ENTITIES_PER_ROOM: usize = 1_000;
const GUARD_MAX_EXPORTS: usize = 20;
const GUARD_MAX_ENTITY_COPIES: usize = 1_000_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    rooms: usize,
    entities_per_room: usize,
    exports: usize,
    reuse: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    exports_completed: usize,
    snapshots_exported: usize,
    entities_snapshotted: usize,
    snapshot_checksum: u64,
    fresh_snapshot_batches: usize,
    retained_snapshot_slots: usize,
    retained_entity_capacity: usize,
    barrier_snapshots_exported: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    path_ok: bool,
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
    let instance_id = InstanceId::new(1);
    let total_entities = config.rooms.saturating_mul(config.entities_per_room);
    let mut stations = StationSet::with_capacity(config.rooms);
    let mut next_entity_id = 1_u64;
    for room in 0..config.rooms {
        let station_id = StationId::new(
            u32::try_from(room.saturating_add(1)).expect("guarded room id fits u32"),
        );
        let mut station = Station::with_capacity(
            StationConfig {
                station_id,
                node_id: NodeId::new(1),
                instance_id,
                tick_rate_hz: 20,
            },
            config.entities_per_room,
        );
        for _ in 0..config.entities_per_room {
            let entity_id = EntityId::new(next_entity_id);
            next_entity_id = next_entity_id.saturating_add(1);
            station
                .spawn_owned(
                    entity_id,
                    Position3::new(0.0, 0.0, 0.0),
                    Bounds::Point,
                    PolicyId::new(0),
                )
                .expect("guarded entity should spawn");
        }
        station.advance_tick();
        station.advance_tick();
        stations.push(station);
    }

    let mut controller = BarrierController::default();
    controller
        .request(
            &stations,
            BarrierId::new(1),
            BarrierScope::Instance(instance_id),
            Tick::new(2),
            CommandQueueMode::Buffer,
        )
        .expect("benchmark barrier should request");
    let progress = controller
        .poll(&stations)
        .expect("benchmark barrier should poll");
    assert_eq!(progress.state, BarrierState::Frozen);

    let version = SnapshotVersion::default();
    let mut scratch = BarrierSnapshotScratch::new();
    if config.reuse {
        scratch.reserve(config.rooms, config.entities_per_room);
    }
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.exports);
    let mut exports_completed = 0_usize;
    let mut snapshots_exported = 0_usize;
    let mut entities_snapshotted = 0_usize;
    let mut snapshot_checksum = 0_u64;
    let mut fresh_snapshot_batches = 0_usize;
    let mut time_budget_exhausted = false;

    for _ in 0..config.exports {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        if config.reuse {
            let snapshots = controller
                .export_snapshots_into(&stations, version, &mut scratch)
                .expect("reusable snapshots should export");
            consume_snapshots(
                snapshots,
                &mut snapshots_exported,
                &mut entities_snapshotted,
                &mut snapshot_checksum,
            );
        } else {
            let snapshots = controller
                .export_snapshots(&stations, version)
                .expect("owned snapshots should export");
            fresh_snapshot_batches = fresh_snapshot_batches.saturating_add(1);
            consume_snapshots(
                &snapshots,
                &mut snapshots_exported,
                &mut entities_snapshotted,
                &mut snapshot_checksum,
            );
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        exports_completed = exports_completed.saturating_add(1);
    }

    black_box(snapshot_checksum);
    latencies.sort_by(f64::total_cmp);
    let metrics = controller
        .resume()
        .expect("benchmark barrier should resume");
    let expected_snapshots = config.rooms.saturating_mul(config.exports);
    let expected_entity_copies = total_entities.saturating_mul(config.exports);
    let path_ok = if config.reuse {
        fresh_snapshot_batches == 0
            && scratch.retained_snapshot_slots() >= config.rooms
            && scratch.retained_entity_capacity() >= total_entities
    } else {
        fresh_snapshot_batches == exports_completed
            && scratch.retained_snapshot_slots() == 0
            && scratch.retained_entity_capacity() == 0
    };
    let workload_ok = exports_completed == config.exports
        && snapshots_exported == expected_snapshots
        && entities_snapshotted == expected_entity_copies
        && metrics.snapshots_exported == expected_snapshots;
    let benchmark_ok = path_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        exports_completed,
        snapshots_exported,
        entities_snapshotted,
        snapshot_checksum,
        fresh_snapshot_batches,
        retained_snapshot_slots: scratch.retained_snapshot_slots(),
        retained_entity_capacity: scratch.retained_entity_capacity(),
        barrier_snapshots_exported: metrics.snapshots_exported,
        p50: percentile(&latencies, 0.50),
        p95: percentile(&latencies, 0.95),
        p99: percentile(&latencies, 0.99),
        max: latencies.last().copied().unwrap_or_default(),
        path_ok,
        workload_ok,
        time_budget_exhausted,
        benchmark_ok,
    }
}

fn consume_snapshots(
    snapshots: &[StationSnapshot],
    snapshot_count: &mut usize,
    entity_count: &mut usize,
    checksum: &mut u64,
) {
    *snapshot_count = snapshot_count.saturating_add(snapshots.len());
    for snapshot in snapshots {
        *entity_count = entity_count.saturating_add(snapshot.entities.len());
        for entity in &snapshot.entities {
            *checksum = checksum.saturating_add(entity.id.get());
        }
    }
}

fn parse_config() -> Config {
    let mut rooms = DEFAULT_ROOMS;
    let mut entities_per_room = DEFAULT_ENTITIES_PER_ROOM;
    let mut exports = DEFAULT_EXPORTS;
    let mut reuse = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--rooms=") {
            rooms = value.parse().unwrap_or(rooms);
        } else if let Some(value) = arg.strip_prefix("--entities-per-room=") {
            entities_per_room = value.parse().unwrap_or(entities_per_room);
        } else if let Some(value) = arg.strip_prefix("--exports=") {
            exports = value.parse().unwrap_or(exports);
        } else if arg == "--fresh-storage" {
            reuse = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    rooms = rooms.max(1);
    entities_per_room = entities_per_room.max(1);
    exports = exports.max(1);
    let requested = (rooms, entities_per_room, exports);
    if !allow_heavy {
        rooms = rooms.min(GUARD_MAX_ROOMS);
        entities_per_room = entities_per_room.min(GUARD_MAX_ENTITIES_PER_ROOM);
        exports = exports.min(GUARD_MAX_EXPORTS);
        let passes = rooms.saturating_mul(exports).max(1);
        if passes.saturating_mul(entities_per_room) > GUARD_MAX_ENTITY_COPIES {
            entities_per_room = (GUARD_MAX_ENTITY_COPIES / passes).max(1);
        }
    }
    Config {
        rooms,
        entities_per_room,
        exports,
        reuse,
        allow_heavy,
        guard_applied: requested != (rooms, entities_per_room, exports),
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
    println!("SectorSync barrier snapshot storage benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_entities_per_room={GUARD_MAX_ENTITIES_PER_ROOM}");
    println!("guard_max_exports={GUARD_MAX_EXPORTS}");
    println!("guard_max_entity_copies={GUARD_MAX_ENTITY_COPIES}");
    println!("rooms={}", config.rooms);
    println!("entities_per_room={}", config.entities_per_room);
    println!("exports={}", config.exports);
    println!("exports_completed={}", result.exports_completed);
    println!("reusable_storage={}", config.reuse);
    println!("snapshots_exported={}", result.snapshots_exported);
    println!("entities_snapshotted={}", result.entities_snapshotted);
    println!("snapshot_checksum={}", result.snapshot_checksum);
    println!("fresh_snapshot_batches={}", result.fresh_snapshot_batches);
    println!("retained_snapshot_slots={}", result.retained_snapshot_slots);
    println!(
        "retained_entity_capacity={}",
        result.retained_entity_capacity
    );
    println!(
        "barrier_snapshots_exported={}",
        result.barrier_snapshots_exported
    );
    println!("tick_ms_p50={:.3}", result.p50);
    println!("tick_ms_p95={:.3}", result.p95);
    println!("tick_ms_p99={:.3}", result.p99);
    println!("tick_ms_max={:.3}", result.max);
    println!("threshold_storage_path_ok={}", result.path_ok);
    println!("threshold_workload_completed_ok={}", result.workload_ok);
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", result.time_budget_exhausted);
    println!("benchmark_ok={}", result.benchmark_ok);
}
