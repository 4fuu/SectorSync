//! Guarded benchmark for reusable single-viewer replication plan output.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, EntityId, GridSpec, InstanceId, NodeId,
    PolicyId, PolicyTable, Position3, RangeOnlyVisibility, ReplicationBudget, ReplicationPlan,
    ReplicationPlanner, ReplicationScratch, Station, StationConfig, StationId, ViewerQuery,
};

const DEFAULT_ENTITIES: usize = 2_000;
const DEFAULT_CALLS_PER_TICK: usize = 200;
const DEFAULT_TICKS: usize = 20;
const DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
const DEFAULT_TICK_P99_BUDGET_MS: f64 = 1_000.0;
const GUARD_MAX_ENTITIES: usize = 4_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 500;
const GUARD_MAX_TICKS: usize = 30;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OutputMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Config {
    entities: usize,
    calls_per_tick: usize,
    ticks: usize,
    output_mode: OutputMode,
    time_budget_ms: u64,
    tick_p99_budget_ms: f64,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            entities: DEFAULT_ENTITIES,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            output_mode: OutputMode::Reuse,
            time_budget_ms: DEFAULT_TIME_BUDGET_MS,
            tick_p99_budget_ms: DEFAULT_TICK_P99_BUDGET_MS,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            output_mode: if args.iter().any(|arg| arg == "--fresh-plan-output") {
                OutputMode::Fresh
            } else {
                OutputMode::Reuse
            },
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities);
            } else if let Some(value) = arg.strip_prefix("--calls-per-tick=") {
                config.calls_per_tick = value.parse().unwrap_or(config.calls_per_tick);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            } else if let Some(value) = arg.strip_prefix("--time-budget-ms=") {
                config.time_budget_ms = value.parse().unwrap_or(config.time_budget_ms);
            } else if let Some(value) = arg.strip_prefix("--tick-p99-budget-ms=") {
                config.tick_p99_budget_ms = value.parse().unwrap_or(config.tick_p99_budget_ms);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let before = *self;
        self.entities = self.entities.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        self.time_budget_ms = self.time_budget_ms.max(1);
        self.tick_p99_budget_ms = self.tick_p99_budget_ms.max(0.001);
        if !self.allow_heavy {
            self.entities = self.entities.min(GUARD_MAX_ENTITIES);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.entities != before.entities
            || self.calls_per_tick != before.calls_per_tick
            || self.ticks != before.ticks;
    }
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    calls: usize,
    selected_entities: usize,
    fresh_plan_outputs: usize,
    retained_plan_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let (station, index, policies, viewer) = create_world(config.entities);
    let stats = run(&station, &index, &policies, &viewer, config);
    let tick_p99 = percentile_ms(&stats.tick_ms, 0.99);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let output_path_ok = match config.output_mode {
        OutputMode::Reuse => stats.fresh_plan_outputs == 0 && stats.retained_plan_capacity > 0,
        OutputMode::Fresh => stats.fresh_plan_outputs == expected_calls,
    };
    let benchmark_ok = stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.calls == expected_calls
        && stats.selected_entities > 0
        && output_path_ok
        && tick_p99 <= config.tick_p99_budget_ms;

    println!("SectorSync single-viewer planning output benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("entities={}", config.entities);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!(
        "reusable_plan_output={}",
        config.output_mode == OutputMode::Reuse
    );
    println!("calls={}", stats.calls);
    println!("selected_entities={}", stats.selected_entities);
    println!("fresh_plan_outputs={}", stats.fresh_plan_outputs);
    println!("retained_plan_capacity={}", stats.retained_plan_capacity);
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={tick_p99:.3}");
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_tick_ms_p99={:.3}", config.tick_p99_budget_ms);
    println!(
        "threshold_tick_ok={}",
        tick_p99 <= config.tick_p99_budget_ms
    );
    println!(
        "threshold_workload_completed_ok={}",
        stats.calls == expected_calls
    );
    println!("threshold_output_path_ok={output_path_ok}");
    println!("time_budget_ms={}", config.time_budget_ms);
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_world(entity_count: usize) -> (Station, CellIndex, PolicyTable, ViewerQuery) {
    let mut station = Station::with_capacity(
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 30,
        },
        entity_count,
    );
    let grid = GridSpec::new(16.0).expect("fixed grid is valid");
    let mut index = CellIndex::with_capacity(grid, entity_count, 256);
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 30, 256.0));
    for entity_index in 0..entity_count {
        let x = u16::try_from(entity_index % 40).expect("x lane fits u16");
        let z = u16::try_from((entity_index / 40) % 40).expect("z lane fits u16");
        let position = Position3::new(f32::from(x) * 4.0, 0.0, f32::from(z) * 4.0);
        let handle = station
            .spawn_owned(
                EntityId::new(u64::try_from(entity_index).expect("entity id fits u64")),
                position,
                Bounds::Point,
                PolicyId::new(1),
            )
            .expect("entity ids are unique");
        index.upsert(handle, position, Bounds::Point);
    }
    let viewer = ViewerQuery {
        client_id: ClientId::new(1),
        position: Position3::new(80.0, 0.0, 80.0),
        radius: 256.0,
        max_entities: 300,
    };
    (station, index, policies, viewer)
}

fn run(
    station: &Station,
    index: &CellIndex,
    policies: &PolicyTable,
    viewer: &ViewerQuery,
    config: Config,
) -> Stats {
    let budget = ReplicationBudget::default();
    let mut scratch = ReplicationScratch::default();
    let mut reusable = ReplicationPlan::default();
    let started = Instant::now();
    let time_budget = Duration::from_millis(config.time_budget_ms);
    let mut stats = Stats::default();
    for _ in 0..config.ticks {
        if started.elapsed() >= time_budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        for _ in 0..config.calls_per_tick {
            match config.output_mode {
                OutputMode::Reuse => {
                    ReplicationPlanner::plan_for_viewer_into(
                        station,
                        index,
                        policies,
                        viewer,
                        &RangeOnlyVisibility,
                        budget,
                        &mut scratch,
                        &mut reusable,
                    );
                    stats.selected_entities = stats
                        .selected_entities
                        .saturating_add(reusable.stats.selected);
                    black_box(&reusable.entities);
                }
                OutputMode::Fresh => {
                    let mut plan = ReplicationPlan::default();
                    ReplicationPlanner::plan_for_viewer_into(
                        station,
                        index,
                        policies,
                        viewer,
                        &RangeOnlyVisibility,
                        budget,
                        &mut scratch,
                        &mut plan,
                    );
                    stats.selected_entities =
                        stats.selected_entities.saturating_add(plan.stats.selected);
                    stats.fresh_plan_outputs = stats.fresh_plan_outputs.saturating_add(1);
                    black_box(plan.entities);
                }
            }
            stats.calls = stats.calls.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.retained_plan_capacity = reusable.entities.capacity();
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
        stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    }
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
    fn guard_clamps_manual_workload() {
        let config = Config::from_args(
            [
                "--entities=999999",
                "--calls-per-tick=999999",
                "--ticks=999999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.entities, GUARD_MAX_ENTITIES);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn small_workload_matches_across_output_modes() {
        let (station, index, policies, viewer) = create_world(32);
        let base = Config {
            entities: 32,
            calls_per_tick: 4,
            ticks: 2,
            tick_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let reused = run(&station, &index, &policies, &viewer, base);
        let fresh = run(
            &station,
            &index,
            &policies,
            &viewer,
            Config {
                output_mode: OutputMode::Fresh,
                ..base
            },
        );

        assert_eq!(reused.calls, 8);
        assert_eq!(reused.selected_entities, fresh.selected_entities);
        assert_eq!(reused.fresh_plan_outputs, 0);
        assert_eq!(fresh.fresh_plan_outputs, 8);
        assert!(reused.retained_plan_capacity >= 32);
    }
}
