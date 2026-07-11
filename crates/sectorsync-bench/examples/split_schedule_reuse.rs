//! Guarded A/B benchmark for fully reusable split schedule planning output.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    CellCoord3, CellLoadSample, HotspotDecision, HotspotSeverity, HotspotThresholds, StationId,
    StationLoadSample,
};
use sectorsync_runtime::{
    SplitAction, SplitSchedule, SplitScheduleView, SplitScheduler, SplitSchedulerConfig,
    SplitSchedulerScratch,
};

const DEFAULT_STATIONS: usize = 64;
const DEFAULT_CELLS_PER_HOT_SOURCE: usize = 128;
const DEFAULT_ACTIONS: usize = 4;
const DEFAULT_CALLS_PER_TICK: usize = 100;
const DEFAULT_TICKS: usize = 20;
const GUARD_MAX_STATIONS: usize = 256;
const GUARD_MAX_CELLS_PER_HOT_SOURCE: usize = 512;
const GUARD_MAX_ACTIONS: usize = 16;
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
    cells_per_hot_source: usize,
    actions: usize,
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
            cells_per_hot_source: DEFAULT_CELLS_PER_HOT_SOURCE,
            actions: DEFAULT_ACTIONS,
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
            } else if let Some(value) = arg.strip_prefix("--cells-per-hot-source=") {
                config.cells_per_hot_source = value.parse().unwrap_or(config.cells_per_hot_source);
            } else if let Some(value) = arg.strip_prefix("--actions=") {
                config.actions = value.parse().unwrap_or(config.actions);
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
        self.stations = self.stations.max(2);
        self.cells_per_hot_source = self.cells_per_hot_source.max(1);
        self.actions = self.actions.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.stations = self.stations.min(GUARD_MAX_STATIONS);
            self.cells_per_hot_source = self
                .cells_per_hot_source
                .min(GUARD_MAX_CELLS_PER_HOT_SOURCE);
            self.actions = self.actions.min(GUARD_MAX_ACTIONS);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.actions = self.actions.min(self.stations - 1);
        self.guard_applied = self.stations != requested.stations
            || self.cells_per_hot_source != requested.cells_per_hot_source
            || self.actions != requested.actions
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    schedule_checksum: u64,
    fresh_outputs: usize,
    retained_decision_slots: usize,
    retained_action_slots: usize,
    retained_reason_capacity: usize,
    retained_action_cell_capacity: usize,
    retained_candidate_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let samples = create_samples(config);
    let scheduler = create_scheduler(config);
    let stats = run(&scheduler, &samples, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let output_path_ok = match config.mode {
        OutputMode::Reuse => {
            stats.fresh_outputs == 0
                && stats.retained_decision_slots >= config.stations
                && stats.retained_action_slots >= config.actions
                && stats.retained_reason_capacity > 0
                && stats.retained_action_cell_capacity >= config.actions
                && stats.retained_candidate_capacity >= config.cells_per_hot_source
        }
        OutputMode::Fresh => stats.fresh_outputs == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.ticks_completed == config.ticks
        && stats.schedule_checksum > 0
        && !stats.time_budget_exhausted
        && output_path_ok;

    println!("SectorSync split schedule reuse benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_stations={GUARD_MAX_STATIONS}");
    println!("guard_max_cells_per_hot_source={GUARD_MAX_CELLS_PER_HOT_SOURCE}");
    println!("guard_max_actions={GUARD_MAX_ACTIONS}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("stations={}", config.stations);
    println!("cells_per_hot_source={}", config.cells_per_hot_source);
    println!("max_actions_per_pass={}", config.actions);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("calls={}", stats.calls);
    println!("schedule_checksum={}", stats.schedule_checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!("retained_decision_slots={}", stats.retained_decision_slots);
    println!("retained_action_slots={}", stats.retained_action_slots);
    println!(
        "retained_reason_capacity={}",
        stats.retained_reason_capacity
    );
    println!(
        "retained_action_cell_capacity={}",
        stats.retained_action_cell_capacity
    );
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

fn create_scheduler(config: Config) -> SplitScheduler {
    SplitScheduler::new(SplitSchedulerConfig {
        thresholds: HotspotThresholds {
            max_station_entities: 100,
            max_station_subscribers: 100,
            max_cell_pressure: 500,
            ..HotspotThresholds::default()
        },
        max_actions_per_pass: config.actions,
        max_cells_per_action: 1,
        ..SplitSchedulerConfig::default()
    })
}

fn create_samples(config: Config) -> Vec<StationLoadSample> {
    (0..config.stations)
        .map(|station_index| {
            let is_hot = station_index < config.actions;
            let cells = if is_hot {
                (0..config.cells_per_hot_source)
                    .map(|cell_index| CellLoadSample {
                        cell: CellCoord3::new(
                            i32::try_from(cell_index).expect("guarded cell fits i32"),
                            i32::try_from(station_index).expect("guarded Station fits i32"),
                            0,
                        ),
                        owned_entities: 100 + cell_index % 200,
                        subscribers: 150 + cell_index % 50,
                        estimated_updates: cell_index.wrapping_mul(37) % 2_000,
                        event_pressure: cell_index.wrapping_mul(19) % 200,
                        ..CellLoadSample::default()
                    })
                    .collect()
            } else {
                Vec::new()
            };
            StationLoadSample {
                station_id: StationId::new(
                    u32::try_from(station_index).expect("guarded Station fits u32"),
                ),
                owned_entities: if is_hot { 1_000 } else { station_index % 10 },
                subscribers: if is_hot { 500 } else { station_index % 5 },
                tick_cost_units: if is_hot { 5_000 } else { 0 },
                cells,
                ..StationLoadSample::default()
            }
        })
        .collect()
}

fn run(scheduler: &SplitScheduler, samples: &[StationLoadSample], config: Config) -> RunStats {
    let mut scratch = SplitSchedulerScratch::new();
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
                OutputMode::Reuse => checksum_view(scheduler.plan_into(samples, &mut scratch)),
                OutputMode::Fresh => {
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    checksum_owned(&scheduler.plan(samples))
                }
            };
            stats.schedule_checksum = stats.schedule_checksum.saturating_add(checksum);
            stats.calls = stats.calls.saturating_add(1);
            black_box(checksum);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.retained_decision_slots = scratch.retained_decision_slots();
    stats.retained_action_slots = scratch.retained_action_slots();
    stats.retained_reason_capacity = scratch.retained_reason_capacity();
    stats.retained_action_cell_capacity = scratch.retained_action_cell_capacity();
    stats.retained_candidate_capacity = scratch.retained_candidate_capacity();
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn checksum_view(schedule: SplitScheduleView<'_>) -> u64 {
    checksum_parts(
        schedule.decisions,
        schedule.actions,
        [
            schedule.skipped_no_target,
            schedule.skipped_no_cells,
            schedule.skipped_cooldown,
            schedule.skipped_target_severity,
            schedule.skipped_target_capacity,
            schedule.skipped_insufficient_improvement,
        ],
    )
}

fn checksum_owned(schedule: &SplitSchedule) -> u64 {
    checksum_parts(
        &schedule.decisions,
        &schedule.actions,
        [
            schedule.skipped_no_target,
            schedule.skipped_no_cells,
            schedule.skipped_cooldown,
            schedule.skipped_target_severity,
            schedule.skipped_target_capacity,
            schedule.skipped_insufficient_improvement,
        ],
    )
}

fn checksum_parts(
    decisions: &[HotspotDecision],
    actions: &[SplitAction],
    skips: [usize; 6],
) -> u64 {
    let decision_checksum = decisions.iter().fold(0_u64, |checksum, decision| {
        checksum
            .saturating_add(u64::from(decision.station_id.get()))
            .saturating_add(decision.score)
            .saturating_add(u64::try_from(decision.reasons.len()).expect("length fits u64"))
            .saturating_add(match decision.severity {
                HotspotSeverity::Normal => 1,
                HotspotSeverity::Warm => 2,
                HotspotSeverity::Hot => 3,
            })
    });
    let action_checksum = actions.iter().fold(0_u64, |checksum, action| {
        checksum
            .saturating_add(u64::from(action.source_station.get()))
            .saturating_add(u64::from(action.target_station.get()))
            .saturating_add(action.proposal.moved_pressure_score)
            .saturating_add(action.source_score)
            .saturating_add(action.target_score)
            .saturating_add(action.estimated_target_score_after_move)
    });
    skips.iter().fold(
        decision_checksum.saturating_add(action_checksum),
        |checksum, value| {
            checksum.saturating_add(u64::try_from(*value).expect("skip count fits u64"))
        },
    )
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
    fn guard_clamps_all_schedule_dimensions() {
        let config = Config::from_args(
            [
                "--stations=99999",
                "--cells-per-hot-source=99999",
                "--actions=99999",
                "--calls-per-tick=99999",
                "--ticks=99999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.stations, GUARD_MAX_STATIONS);
        assert_eq!(config.cells_per_hot_source, GUARD_MAX_CELLS_PER_HOT_SOURCE);
        assert_eq!(config.actions, GUARD_MAX_ACTIONS);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn output_modes_produce_identical_schedule_checksum() {
        let config = Config {
            stations: 8,
            cells_per_hot_source: 12,
            actions: 2,
            calls_per_tick: 3,
            ticks: 2,
            ..Config::default()
        };
        let samples = create_samples(config);
        let scheduler = create_scheduler(config);
        let reused = run(&scheduler, &samples, config);
        let fresh = run(
            &scheduler,
            &samples,
            Config {
                mode: OutputMode::Fresh,
                ..config
            },
        );
        assert_eq!(reused.schedule_checksum, fresh.schedule_checksum);
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.fresh_outputs, 0);
        assert_eq!(fresh.fresh_outputs, fresh.calls);
        assert_eq!(reused.retained_decision_slots, config.stations);
        assert_eq!(reused.retained_action_slots, config.actions);
        assert!(reused.retained_reason_capacity > 0);
        assert!(reused.retained_action_cell_capacity >= config.actions);
        assert!(reused.retained_candidate_capacity >= config.cells_per_hot_source);
    }
}
