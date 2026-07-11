//! Guarded A/B benchmark for reusable budgeted Station scheduling.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    InstanceId, NodeId, Station, StationConfig, StationId, StationLoadSample,
};
use sectorsync_runtime::{
    StationScheduleConfig, StationScheduleScratch, StationScheduler, StationSet,
};

const DEFAULT_STATIONS: usize = 512;
const DEFAULT_LIMIT: usize = 16;
const DEFAULT_CALLS_PER_TICK: usize = 100;
const DEFAULT_TICKS: usize = 20;
const GUARD_MAX_STATIONS: usize = 2_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 200;
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
    stations: usize,
    limit: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: OutputMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stations: DEFAULT_STATIONS,
            limit: DEFAULT_LIMIT,
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
            if let Some(value) = arg.strip_prefix("--stations=") {
                config.stations = value.parse().unwrap_or(config.stations);
            } else if let Some(value) = arg.strip_prefix("--limit=") {
                config.limit = value.parse().unwrap_or(config.limit);
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
        self.stations = self.stations.max(1);
        self.limit = self.limit.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.stations = self.stations.min(GUARD_MAX_STATIONS);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.limit = self.limit.min(self.stations);
        self.guard_applied = self.stations != requested.stations
            || self.limit != requested.limit
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    selected_checksum: u64,
    fresh_outputs: usize,
    retained_score_capacity: usize,
    retained_candidate_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let (stations, samples) = create_workload(config.stations);
    let stats = run(&stations, &samples, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let output_path_ok = match config.mode {
        OutputMode::Reuse => {
            stats.fresh_outputs == 0
                && stats.retained_score_capacity >= config.stations
                && stats.retained_candidate_capacity >= config.stations
        }
        OutputMode::Fresh => stats.fresh_outputs == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.ticks_completed == config.ticks
        && stats.selected_checksum > 0
        && !stats.time_budget_exhausted
        && output_path_ok;

    println!("SectorSync Station schedule reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_stations={GUARD_MAX_STATIONS}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("stations={}", config.stations);
    println!("selection_limit={}", config.limit);
    println!(
        "top_k_partition_applied={}",
        config.limit.saturating_mul(2) < config.stations
    );
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("calls={}", stats.calls);
    println!("selected_checksum={}", stats.selected_checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!("retained_score_capacity={}", stats.retained_score_capacity);
    println!(
        "retained_candidate_capacity={}",
        stats.retained_candidate_capacity
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={output_path_ok}");
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

fn create_workload(count: usize) -> (StationSet, Vec<StationLoadSample>) {
    let mut stations = StationSet::default();
    let mut samples = Vec::with_capacity(count);
    for index in 0..count {
        let station_id = StationId::new(u32::try_from(index).expect("guarded Station fits u32"));
        let mut station = Station::new(StationConfig {
            station_id,
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 30,
        });
        for _ in 0..index % 7 {
            station.advance_tick();
        }
        stations.push(station);
        samples.push(StationLoadSample {
            station_id,
            owned_entities: index.wrapping_mul(37) % 2_000,
            subscribers: index.wrapping_mul(19) % 200,
            queued_events: index.wrapping_mul(13) % 500,
            tick_cost_units: u64::try_from(index.wrapping_mul(97) % 20_000)
                .expect("bounded tick cost fits u64"),
            ..StationLoadSample::default()
        });
    }
    (stations, samples)
}

fn run(stations: &StationSet, samples: &[StationLoadSample], config: Config) -> RunStats {
    let scheduler = StationScheduler::default();
    let mut scratch = StationScheduleScratch::new();
    let schedule_config = StationScheduleConfig {
        max_station_advances_per_step: config.limit,
    };
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
            let checksum = match config.mode {
                OutputMode::Reuse => scheduler
                    .plan_loaded_into(stations, samples, schedule_config, &mut scratch)
                    .selected
                    .iter()
                    .map(|candidate| u64::from(candidate.station_id.get()))
                    .sum::<u64>(),
                OutputMode::Fresh => {
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    scheduler
                        .plan_loaded(stations, samples, schedule_config)
                        .selected
                        .iter()
                        .map(|candidate| u64::from(candidate.station_id.get()))
                        .sum::<u64>()
                }
            };
            stats.selected_checksum = stats.selected_checksum.saturating_add(checksum);
            stats.calls = stats.calls.saturating_add(1);
            black_box(checksum);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_score_capacity = scratch.score_capacity();
    stats.retained_candidate_capacity = scratch.candidate_capacity();
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
    fn guard_clamps_scheduler_dimensions_and_limit() {
        let config = Config::from_args(
            [
                "--stations=99999",
                "--limit=99999",
                "--calls-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.stations, GUARD_MAX_STATIONS);
        assert_eq!(config.limit, GUARD_MAX_STATIONS);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn output_modes_select_identical_station_checksum() {
        let config = Config {
            stations: 33,
            limit: 5,
            calls_per_tick: 3,
            ticks: 2,
            ..Config::default()
        };
        let (stations, samples) = create_workload(config.stations);
        let reused = run(&stations, &samples, config);
        let fresh = run(
            &stations,
            &samples,
            Config {
                mode: OutputMode::Fresh,
                ..config
            },
        );
        assert_eq!(reused.selected_checksum, fresh.selected_checksum);
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.fresh_outputs, 0);
        assert_eq!(fresh.fresh_outputs, fresh.calls);
        assert!(reused.retained_score_capacity >= config.stations);
        assert!(reused.retained_candidate_capacity >= config.stations);
    }
}
