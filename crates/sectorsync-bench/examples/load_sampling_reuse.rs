//! Guarded A/B benchmark for reusable periodic station load sampling.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, EntityId, GridSpec, InstanceId, NodeId, PolicyId, Position3, Station,
    StationConfig, StationId,
};
use sectorsync_runtime::{
    EventRouter, StationIndexSet, StationLoadSampler, StationLoadSamplerScratch, StationSet,
};

const DEFAULT_ROOMS: usize = 256;
const DEFAULT_ENTITIES_PER_ROOM: usize = 16;
const DEFAULT_CALLS_PER_TICK: usize = 20;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_ROOMS: usize = 1_000;
const GUARD_MAX_ENTITIES_PER_ROOM: usize = 64;
const GUARD_MAX_CALLS_PER_TICK: usize = 50;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OutputMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    rooms: usize,
    entities_per_room: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: OutputMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rooms: DEFAULT_ROOMS,
            entities_per_room: DEFAULT_ENTITIES_PER_ROOM,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: OutputMode::Reuse,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--fresh-output") {
                OutputMode::Fresh
            } else {
                OutputMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--rooms=") {
                config.rooms = value.parse().unwrap_or(config.rooms);
            } else if let Some(value) = arg.strip_prefix("--entities-per-room=") {
                config.entities_per_room = value.parse().unwrap_or(config.entities_per_room);
            } else if let Some(value) = arg.strip_prefix("--calls-per-tick=") {
                config.calls_per_tick = value.parse().unwrap_or(config.calls_per_tick);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.rooms = self.rooms.max(1);
        self.entities_per_room = self.entities_per_room.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.rooms = self.rooms.min(GUARD_MAX_ROOMS);
            self.entities_per_room = self.entities_per_room.min(GUARD_MAX_ENTITIES_PER_ROOM);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.rooms != requested.rooms
            || self.entities_per_room != requested.entities_per_room
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    samples: usize,
    entity_checksum: usize,
    cell_checksum: usize,
    subscriber_checksum: usize,
    fresh_outputs: usize,
    retained_subscriber_capacity: usize,
    retained_occupancy_capacity: usize,
    retained_sample_slots: usize,
    retained_cell_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let workload = create_workload(config);
    let stats = run(&workload, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let expected_samples = expected_calls.saturating_mul(config.rooms);
    let output_path_ok = match config.mode {
        OutputMode::Reuse => {
            stats.fresh_outputs == 0
                && stats.retained_subscriber_capacity >= config.rooms
                && stats.retained_occupancy_capacity >= config.entities_per_room
                && stats.retained_sample_slots >= config.rooms
                && stats.retained_cell_capacity
                    >= config.rooms.saturating_mul(config.entities_per_room)
        }
        OutputMode::Fresh => stats.fresh_outputs == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.samples == expected_samples
        && stats.entity_checksum > 0
        && stats.cell_checksum > 0
        && stats.subscriber_checksum > 0
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && output_path_ok;

    println!("SectorSync load sampling reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_entities_per_room={GUARD_MAX_ENTITIES_PER_ROOM}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("rooms={}", config.rooms);
    println!("entities_per_room={}", config.entities_per_room);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("calls={}", stats.calls);
    println!("samples={}", stats.samples);
    println!("entity_checksum={}", stats.entity_checksum);
    println!("cell_checksum={}", stats.cell_checksum);
    println!("subscriber_checksum={}", stats.subscriber_checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!(
        "retained_subscriber_capacity={}",
        stats.retained_subscriber_capacity
    );
    println!(
        "retained_occupancy_capacity={}",
        stats.retained_occupancy_capacity
    );
    println!("retained_sample_slots={}", stats.retained_sample_slots);
    println!("retained_cell_capacity={}", stats.retained_cell_capacity);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={output_path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.calls == expected_calls && stats.samples == expected_samples
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

struct Workload {
    stations: StationSet,
    indexes: StationIndexSet,
    router: EventRouter,
    subscriber_counts: Vec<(StationId, usize)>,
}

fn create_workload(config: Config) -> Workload {
    let mut stations = StationSet::with_capacity(config.rooms);
    let mut indexes = StationIndexSet::with_capacity(config.rooms);
    let mut router = EventRouter::default();
    let mut subscriber_counts = Vec::with_capacity(config.rooms.saturating_mul(2));
    let grid = GridSpec::new(10.0).expect("grid should build");
    for room in 0..config.rooms {
        let raw_station = u32::try_from(room.saturating_add(1)).expect("guarded room fits u32");
        let station_id = StationId::new(raw_station);
        let mut station = Station::new(StationConfig {
            station_id,
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(u64::from(raw_station)),
            tick_rate_hz: 30,
        });
        let mut index =
            CellIndex::with_capacity(grid, config.entities_per_room, config.entities_per_room);
        for entity in 0..config.entities_per_room {
            let entity_id = u64::from(raw_station)
                .saturating_mul(100_000)
                .saturating_add(u64::try_from(entity).expect("guarded entity fits u64"));
            let entity_offset = u16::try_from(entity).expect("guarded entity fits u16");
            let position = Position3::new(f32::from(entity_offset) * 11.0, 0.0, 0.0);
            let handle = station
                .spawn_owned(
                    EntityId::new(entity_id),
                    position,
                    Bounds::Point,
                    PolicyId::new(1),
                )
                .expect("unique entity should spawn");
            index.upsert(handle, position, Bounds::Point);
        }
        stations.push(station);
        indexes.insert(station_id, index);
        router.register_station(station_id);
        subscriber_counts.push((station_id, room % 10 + 1));
        subscriber_counts.push((station_id, room % 7 + 1));
    }
    Workload {
        stations,
        indexes,
        router,
        subscriber_counts,
    }
}

fn run(workload: &Workload, config: Config) -> RunStats {
    let load_sampler = StationLoadSampler::default();
    let mut scratch = StationLoadSamplerScratch::new();
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.calls_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            match config.mode {
                OutputMode::Reuse => {
                    let output = load_sampler.sample_all_into(
                        &workload.stations,
                        &workload.indexes,
                        &workload.router,
                        &workload.subscriber_counts,
                        &mut scratch,
                    );
                    add_samples(&mut stats, output);
                }
                OutputMode::Fresh => {
                    let mut fresh_scratch = StationLoadSamplerScratch::new();
                    let output = load_sampler.sample_all_into(
                        &workload.stations,
                        &workload.indexes,
                        &workload.router,
                        &workload.subscriber_counts,
                        &mut fresh_scratch,
                    );
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    add_samples(&mut stats, output);
                }
            }
            stats.calls = stats.calls.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_subscriber_capacity = scratch.retained_subscriber_capacity();
    stats.retained_occupancy_capacity = scratch.retained_occupancy_capacity();
    stats.retained_sample_slots = scratch.retained_sample_slots();
    stats.retained_cell_capacity = scratch.retained_cell_capacity();
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn add_samples(stats: &mut RunStats, samples: &[sectorsync_core::prelude::StationLoadSample]) {
    stats.samples = stats.samples.saturating_add(samples.len());
    for sample in samples {
        stats.entity_checksum = stats
            .entity_checksum
            .saturating_add(sample.total_entities());
        stats.cell_checksum = stats.cell_checksum.saturating_add(sample.cells.len());
        stats.subscriber_checksum = stats.subscriber_checksum.saturating_add(sample.subscribers);
    }
    black_box(samples);
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile_ms(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_clamps_all_sampling_dimensions() {
        let config = Config::from_args(
            [
                "--rooms=99999",
                "--entities-per-room=99999",
                "--calls-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.rooms, GUARD_MAX_ROOMS);
        assert_eq!(config.entities_per_room, GUARD_MAX_ENTITIES_PER_ROOM);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn output_modes_produce_identical_sample_checksums() {
        let config = Config {
            rooms: 8,
            entities_per_room: 6,
            calls_per_tick: 2,
            ticks: 2,
            ..Config::default()
        };
        let workload = create_workload(config);
        let reused = run(&workload, config);
        let fresh = run(
            &workload,
            Config {
                mode: OutputMode::Fresh,
                ..config
            },
        );
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.samples, fresh.samples);
        assert_eq!(reused.entity_checksum, fresh.entity_checksum);
        assert_eq!(reused.cell_checksum, fresh.cell_checksum);
        assert_eq!(reused.subscriber_checksum, fresh.subscriber_checksum);
        assert_eq!(reused.fresh_outputs, 0);
        assert_eq!(fresh.fresh_outputs, fresh.calls);
        assert!(reused.retained_sample_slots >= config.rooms);
        assert!(reused.retained_cell_capacity >= config.rooms * config.entities_per_room);
    }
}
