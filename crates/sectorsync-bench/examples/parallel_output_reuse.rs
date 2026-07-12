//! Guarded A/B benchmark for retained multi-Station parallel planning output.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, ReplicationBudget, Station, StationConfig, StationId,
    ViewerQuery,
};
use sectorsync_runtime::{
    ParallelReplicationScratch, ReplicationThreadPool, ReplicationThreadPoolConfig,
    StationReplicationBatch,
};

const DEFAULT_ROOMS: usize = 12;
const DEFAULT_PLAYERS: usize = 6;
const DEFAULT_ENTITIES: usize = 128;
const DEFAULT_CALLS_PER_TICK: usize = 20;
const DEFAULT_TICKS: usize = 20;
const GUARD_MAX_ROOMS: usize = 64;
const GUARD_MAX_PLAYERS: usize = 10;
const GUARD_MAX_ENTITIES: usize = 512;
const GUARD_MAX_CALLS_PER_TICK: usize = 100;
const GUARD_MAX_TICKS: usize = 30;
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
    players: usize,
    entities: usize,
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
            players: DEFAULT_PLAYERS,
            entities: DEFAULT_ENTITIES,
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
            } else if let Some(value) = arg.strip_prefix("--players=") {
                config.players = value.parse().unwrap_or(config.players);
            } else if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities);
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
        self.players = self.players.max(1);
        self.entities = self.entities.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.rooms = self.rooms.min(GUARD_MAX_ROOMS);
            self.players = self.players.min(GUARD_MAX_PLAYERS);
            self.entities = self.entities.min(GUARD_MAX_ENTITIES);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.rooms != requested.rooms
            || self.players != requested.players
            || self.entities != requested.entities
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

struct World {
    stations: Vec<Station>,
    indexes: Vec<CellIndex>,
    viewers: Vec<Vec<ViewerQuery>>,
    policies: PolicyTable,
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    selected_checksum: usize,
    fresh_outputs: usize,
    retained_batch_slots: usize,
    retained_entity_capacity: usize,
    pool_threads: usize,
    active_scratch_lanes: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let world = create_world(config);
    let stats = run(&world, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let output_path_ok = match config.mode {
        OutputMode::Reuse => {
            stats.fresh_outputs == 0
                && stats.retained_batch_slots >= config.rooms
                && stats.retained_entity_capacity > 0
        }
        OutputMode::Fresh => stats.fresh_outputs == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.selected_checksum > 0
        && stats.active_scratch_lanes == expected_active_lanes(stats.pool_threads, config.rooms)
        && output_path_ok;

    println!("SectorSync parallel output reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_players={GUARD_MAX_PLAYERS}");
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("rooms={}", config.rooms);
    println!("players_per_room={}", config.players);
    println!("entities_per_room={}", config.entities);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("calls={}", stats.calls);
    println!("selected_checksum={}", stats.selected_checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!("pool_threads={}", stats.pool_threads);
    println!("active_scratch_lanes={}", stats.active_scratch_lanes);
    println!("retained_batch_slots={}", stats.retained_batch_slots);
    println!(
        "retained_entity_capacity={}",
        stats.retained_entity_capacity
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={output_path_ok}");
    println!(
        "threshold_lane_utilization_ok={}",
        stats.active_scratch_lanes == expected_active_lanes(stats.pool_threads, config.rooms)
    );
    println!(
        "threshold_workload_completed_ok={}",
        stats.calls == expected_calls
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_world(config: Config) -> World {
    let mut stations = Vec::with_capacity(config.rooms);
    let mut indexes = Vec::with_capacity(config.rooms);
    let mut viewers = Vec::with_capacity(config.rooms);
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 30, 256.0));
    for room in 0..config.rooms {
        let mut station = Station::with_capacity(
            StationConfig {
                station_id: StationId::new(u32::try_from(room).expect("guarded room fits u32")),
                node_id: NodeId::new(1),
                instance_id: InstanceId::new(u64::try_from(room).expect("room fits u64")),
                tick_rate_hz: 30,
            },
            config.entities,
        );
        let grid = GridSpec::new(16.0).expect("fixed grid is valid");
        let mut index = CellIndex::with_capacity(grid, config.entities, config.entities);
        for entity in 0..config.entities {
            let x = u16::try_from(entity % 32).expect("x lane fits u16");
            let z = u16::try_from((entity / 32) % 32).expect("z lane fits u16");
            let position = Position3::new(f32::from(x) * 4.0, 0.0, f32::from(z) * 4.0);
            let handle = station
                .spawn_owned(
                    EntityId::new(
                        u64::try_from(room * config.entities + entity).expect("entity id fits u64"),
                    ),
                    position,
                    Bounds::Point,
                    PolicyId::new(1),
                )
                .expect("entity ids are unique");
            index.upsert(handle, position, Bounds::Point);
        }
        let room_viewers = (0..config.players)
            .map(|player| {
                let player_offset = u16::try_from(player).expect("guarded player count fits u16");
                ViewerQuery {
                    client_id: ClientId::new(
                        u64::try_from(room * config.players + player).expect("client id fits u64"),
                    ),
                    position: Position3::new(48.0 + f32::from(player_offset), 0.0, 48.0),
                    radius: 128.0,
                    max_entities: 96,
                }
            })
            .collect();
        stations.push(station);
        indexes.push(index);
        viewers.push(room_viewers);
    }
    World {
        stations,
        indexes,
        viewers,
        policies,
    }
}

fn run(world: &World, config: Config) -> RunStats {
    let batches = world
        .stations
        .iter()
        .zip(&world.indexes)
        .zip(&world.viewers)
        .map(|((station, index), viewers)| StationReplicationBatch::new(station, index, viewers))
        .collect::<Vec<_>>();
    let pool = ReplicationThreadPool::new(ReplicationThreadPoolConfig::new(2, 2))
        .expect("bounded pool builds");
    let mut scratch = ParallelReplicationScratch::new();
    let budget = ReplicationBudget::default();
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    for _ in 0..config.ticks {
        if started.elapsed() >= time_budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        for _ in 0..config.calls_per_tick {
            let selected = match config.mode {
                OutputMode::Reuse => {
                    pool.plan_station_range_batches_into(
                        &batches,
                        &world.policies,
                        budget,
                        &mut scratch,
                    )
                    .stats
                    .selected
                }
                OutputMode::Fresh => {
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    let view = pool.plan_station_range_batches_into(
                        &batches,
                        &world.policies,
                        budget,
                        &mut scratch,
                    );
                    let selected = view.stats.selected;
                    let owned = view
                        .batches
                        .iter()
                        .map(|batch| batch.view().plans.to_vec())
                        .collect::<Vec<_>>();
                    black_box(owned);
                    selected
                }
            };
            stats.selected_checksum = stats.selected_checksum.saturating_add(selected);
            stats.calls = stats.calls.saturating_add(1);
            black_box(selected);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_batch_slots = scratch.retained_batch_slots();
    stats.retained_entity_capacity = scratch.retained_entity_capacity();
    stats.pool_threads = pool.threads();
    stats.active_scratch_lanes = scratch.active_lanes();
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn expected_active_lanes(pool_threads: usize, batches: usize) -> usize {
    let lanes = pool_threads.min(batches);
    if lanes == 0 {
        0
    } else {
        batches.div_ceil(batches.div_ceil(lanes))
    }
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
    fn guard_clamps_all_parallel_dimensions() {
        let config = Config::from_args(
            [
                "--rooms=9999",
                "--players=9999",
                "--entities=9999",
                "--calls-per-tick=9999",
                "--ticks=9999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.rooms, GUARD_MAX_ROOMS);
        assert_eq!(config.players, GUARD_MAX_PLAYERS);
        assert_eq!(config.entities, GUARD_MAX_ENTITIES);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn output_modes_have_identical_selection_checksum() {
        let config = Config {
            rooms: 3,
            players: 2,
            entities: 16,
            calls_per_tick: 2,
            ticks: 2,
            ..Config::default()
        };
        let world = create_world(config);
        let reused = run(&world, config);
        let fresh = run(
            &world,
            Config {
                mode: OutputMode::Fresh,
                ..config
            },
        );
        assert_eq!(reused.selected_checksum, fresh.selected_checksum);
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.fresh_outputs, 0);
        assert_eq!(fresh.fresh_outputs, fresh.calls);
        assert_eq!(reused.retained_batch_slots, config.rooms);
        assert!(reused.retained_entity_capacity > 0);
    }
}
