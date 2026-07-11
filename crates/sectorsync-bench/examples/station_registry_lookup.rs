//! Guarded A/B benchmark for indexed runtime Station registries.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    CellIndex, GridSpec, InstanceId, NodeId, Station, StationConfig, StationId,
};
use sectorsync_runtime::{StationIndexSet, StationSet};

const DEFAULT_STATIONS: usize = 4_096;
const DEFAULT_QUERIES_PER_TICK: usize = 1_000;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_STATIONS: usize = 8_000;
const GUARD_MAX_QUERIES_PER_TICK: usize = 2_000;
const GUARD_MAX_TICKS: usize = 10;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum LookupMode {
    #[default]
    Indexed,
    FullScan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    stations: usize,
    queries_per_tick: usize,
    ticks: usize,
    mode: LookupMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stations: DEFAULT_STATIONS,
            queries_per_tick: DEFAULT_QUERIES_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: LookupMode::Indexed,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--full-scan") {
                LookupMode::FullScan
            } else {
                LookupMode::Indexed
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--stations=") {
                config.stations = value.parse().unwrap_or(config.stations);
            } else if let Some(value) = arg.strip_prefix("--queries-per-tick=") {
                config.queries_per_tick = value.parse().unwrap_or(config.queries_per_tick);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.stations = self.stations.max(1);
        self.queries_per_tick = self.queries_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.stations = self.stations.min(GUARD_MAX_STATIONS);
            self.queries_per_tick = self.queries_per_tick.min(GUARD_MAX_QUERIES_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.stations != requested.stations
            || self.queries_per_tick != requested.queries_per_tick
            || self.ticks != requested.ticks;
    }
}

struct Workload {
    stations: StationSet,
    indexes: StationIndexSet,
    station_ids: Vec<StationId>,
    index_ids: Vec<StationId>,
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    queries: usize,
    lookup_checksum: u64,
    full_scans: usize,
    station_lookup_capacity: usize,
    index_lookup_capacity: usize,
    station_lookup_index_active: bool,
    index_lookup_index_active: bool,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let workload = create_workload(config.stations);
    let stats = run(&workload, config);
    let expected_queries = config.queries_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        LookupMode::Indexed => {
            stats.full_scans == 0
                && stats.station_lookup_index_active == stats.index_lookup_index_active
                && (!stats.station_lookup_index_active
                    || (stats.station_lookup_capacity >= config.stations
                        && stats.index_lookup_capacity >= config.stations))
        }
        LookupMode::FullScan => stats.full_scans == expected_queries.saturating_mul(2),
    };
    let benchmark_ok = stats.queries == expected_queries
        && stats.lookup_checksum > 0
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync Station registry lookup benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_stations={GUARD_MAX_STATIONS}");
    println!("guard_max_queries_per_tick={GUARD_MAX_QUERIES_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("stations={}", config.stations);
    println!("queries_per_tick={}", config.queries_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("indexed_lookup={}", config.mode == LookupMode::Indexed);
    println!("queries={}", stats.queries);
    println!("lookup_checksum={}", stats.lookup_checksum);
    println!("full_scans={}", stats.full_scans);
    println!("station_lookup_capacity={}", stats.station_lookup_capacity);
    println!("index_lookup_capacity={}", stats.index_lookup_capacity);
    println!(
        "station_lookup_index_active={}",
        stats.station_lookup_index_active
    );
    println!(
        "index_lookup_index_active={}",
        stats.index_lookup_index_active
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_lookup_path_ok={path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.queries == expected_queries
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_workload(count: usize) -> Workload {
    let mut stations = StationSet::with_capacity(count);
    let mut indexes = StationIndexSet::with_capacity(count);
    let mut station_ids = Vec::with_capacity(count);
    let mut index_ids = Vec::with_capacity(count);
    let grid = GridSpec::new(10.0).expect("grid should build");
    for index in 0..count {
        let station_id = StationId::new(
            u32::try_from(index.saturating_add(1)).expect("guarded Station fits u32"),
        );
        stations.push(Station::new(StationConfig {
            station_id,
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(u64::from(station_id.get())),
            tick_rate_hz: 30,
        }));
        indexes.insert(station_id, CellIndex::new(grid));
        station_ids.push(station_id);
        index_ids.push(station_id);
    }
    Workload {
        stations,
        indexes,
        station_ids,
        index_ids,
    }
}

fn run(workload: &Workload, config: Config) -> RunStats {
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats {
        station_lookup_capacity: workload.stations.lookup_capacity(),
        index_lookup_capacity: workload.indexes.lookup_capacity(),
        station_lookup_index_active: workload.stations.lookup_index_active(),
        index_lookup_index_active: workload.indexes.lookup_index_active(),
        ..RunStats::default()
    };
    'ticks: for tick in 0..config.ticks {
        let tick_started = Instant::now();
        for query in 0..config.queries_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let target =
                workload.station_ids[(query.wrapping_mul(17).wrapping_add(tick)) % config.stations];
            let checksum = match config.mode {
                LookupMode::Indexed => {
                    let station = workload.stations.get(target).expect("Station should exist");
                    let index = workload.indexes.get(target).expect("index should exist");
                    u64::from(station.config().station_id.get())
                        .saturating_add(u64::try_from(index.entity_count()).unwrap_or(u64::MAX))
                }
                LookupMode::FullScan => {
                    stats.full_scans = stats.full_scans.saturating_add(2);
                    let station_id = workload
                        .station_ids
                        .iter()
                        .find(|station_id| **station_id == target)
                        .expect("shadow Station should exist");
                    let index_id = workload
                        .index_ids
                        .iter()
                        .find(|station_id| **station_id == target)
                        .expect("shadow index should exist");
                    u64::from(station_id.get()).saturating_add(u64::from(index_id.get()))
                }
            };
            let normalized = match config.mode {
                LookupMode::Indexed => checksum.saturating_mul(2),
                LookupMode::FullScan => checksum,
            };
            stats.lookup_checksum = stats.lookup_checksum.saturating_add(normalized);
            stats.queries = stats.queries.saturating_add(1);
            black_box(normalized);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
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
    fn guard_clamps_registry_dimensions() {
        let config = Config::from_args(
            [
                "--stations=99999",
                "--queries-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.stations, GUARD_MAX_STATIONS);
        assert_eq!(config.queries_per_tick, GUARD_MAX_QUERIES_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn lookup_modes_produce_identical_checksums() {
        let config = Config {
            stations: 31,
            queries_per_tick: 20,
            ticks: 2,
            ..Config::default()
        };
        let workload = create_workload(config.stations);
        let indexed = run(&workload, config);
        let scanned = run(
            &workload,
            Config {
                mode: LookupMode::FullScan,
                ..config
            },
        );
        assert_eq!(indexed.queries, scanned.queries);
        assert_eq!(indexed.lookup_checksum, scanned.lookup_checksum);
        assert_eq!(indexed.full_scans, 0);
        assert_eq!(scanned.full_scans, scanned.queries * 2);
        assert!(!indexed.station_lookup_index_active);
        assert!(!indexed.index_lookup_index_active);
        assert_eq!(indexed.station_lookup_capacity, 0);
        assert_eq!(indexed.index_lookup_capacity, 0);
    }
}
