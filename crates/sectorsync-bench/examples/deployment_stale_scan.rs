//! Guarded A/B benchmark for allocation-free deployment stale-node marking.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{NodeId, Tick};
use sectorsync_runtime::{DeploymentConfig, DeploymentNodeState, DeploymentRouteTable};

const DEFAULT_NODES: usize = 5_000;
const DEFAULT_CALLS_PER_TICK: usize = 50;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_NODES: usize = 20_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 100;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;
const NOW: u64 = 100;
const STALE_AFTER: u64 = 20;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MarkMode {
    #[default]
    Direct,
    CollectMark,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    nodes: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: MarkMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            nodes: DEFAULT_NODES,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: MarkMode::Direct,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--collect-mark") {
                MarkMode::CollectMark
            } else {
                MarkMode::Direct
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--nodes=") {
                config.nodes = value.parse().unwrap_or(config.nodes);
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
        self.nodes = self.nodes.max(1);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.nodes = self.nodes.min(GUARD_MAX_NODES);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.nodes != requested.nodes
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    operation_ms: Vec<f64>,
    calls: usize,
    nodes_marked: usize,
    route_checksum: u64,
    temporary_id_collections: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let source = create_table(config.nodes);
    let stats = run(&source, config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let path_ok = match config.mode {
        MarkMode::Direct => stats.temporary_id_collections == 0,
        MarkMode::CollectMark => stats.temporary_id_collections == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.ticks_completed == config.ticks
        && stats.nodes_marked > 0
        && stats.route_checksum > 0
        && !stats.time_budget_exhausted
        && path_ok;

    println!("SectorSync deployment stale-node scan benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_nodes={GUARD_MAX_NODES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("nodes={}", config.nodes);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("direct_scan={}", config.mode == MarkMode::Direct);
    println!("calls={}", stats.calls);
    println!("nodes_marked={}", stats.nodes_marked);
    println!("route_checksum={}", stats.route_checksum);
    println!(
        "temporary_id_collections={}",
        stats.temporary_id_collections
    );
    println!(
        "operation_ms_p50={:.3}",
        percentile_ms(&stats.operation_ms, 0.50)
    );
    println!(
        "operation_ms_p95={:.3}",
        percentile_ms(&stats.operation_ms, 0.95)
    );
    println!(
        "operation_ms_p99={:.3}",
        percentile_ms(&stats.operation_ms, 0.99)
    );
    println!(
        "operation_ms_max={:.3}",
        percentile_ms(&stats.operation_ms, 1.00)
    );
    println!("threshold_path_ok={path_ok}");
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

fn create_table(count: usize) -> DeploymentRouteTable {
    let mut table = DeploymentRouteTable::new(DeploymentConfig {
        max_nodes: count,
        max_stations_per_node: 1,
        stale_after_ticks: STALE_AFTER,
    });
    for index in 0..count {
        let node_id = NodeId::new(u32::try_from(index).expect("guarded node fits u32"));
        table
            .register_node(node_id, 1, Tick::new(0))
            .expect("node should register inside configured capacity");
        match index % 4 {
            0 => {
                table
                    .heartbeat(node_id, Tick::new(NOW))
                    .expect("fresh heartbeat succeeds");
            }
            1 => {
                table
                    .heartbeat(node_id, Tick::new(NOW - STALE_AFTER))
                    .expect("boundary heartbeat succeeds");
            }
            3 => {
                table
                    .mark_offline(node_id)
                    .expect("explicit offline succeeds");
            }
            _ => {}
        }
    }
    table
}

fn run(source: &DeploymentRouteTable, config: Config) -> RunStats {
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();
    'ticks: for _ in 0..config.ticks {
        for _ in 0..config.calls_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            let mut table = source.clone();
            let operation_started = Instant::now();
            let marked = match config.mode {
                MarkMode::Direct => table.mark_stale_offline(Tick::new(NOW)),
                MarkMode::CollectMark => {
                    stats.temporary_id_collections =
                        stats.temporary_id_collections.saturating_add(1);
                    collect_mark_stale(&mut table)
                }
            };
            stats
                .operation_ms
                .push(operation_started.elapsed().as_secs_f64() * 1_000.0);
            stats.nodes_marked = stats.nodes_marked.saturating_add(marked);
            stats.route_checksum = stats
                .route_checksum
                .saturating_add(route_checksum(&table, config.nodes));
            stats.calls = stats.calls.saturating_add(1);
            black_box(table);
        }
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn collect_mark_stale(table: &mut DeploymentRouteTable) -> usize {
    let stale = table.stale_nodes(Tick::new(NOW));
    for node_id in &stale {
        table
            .mark_offline(*node_id)
            .expect("stale id came from the same route table");
    }
    stale.len()
}

fn route_checksum(table: &DeploymentRouteTable, nodes: usize) -> u64 {
    (0..nodes).fold(0_u64, |checksum, index| {
        let route = table
            .node_route(NodeId::new(
                u32::try_from(index).expect("guarded node fits u32"),
            ))
            .expect("registered node route exists");
        checksum
            .saturating_add(route.route_epoch)
            .saturating_add(match route.state {
                DeploymentNodeState::Online => 1,
                DeploymentNodeState::Draining => 2,
                DeploymentNodeState::Offline => 3,
            })
    })
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
    fn guard_clamps_deployment_dimensions() {
        let config = Config::from_args(
            [
                "--nodes=999999",
                "--calls-per-tick=999999",
                "--ticks=999999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.nodes, GUARD_MAX_NODES);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn direct_and_collect_mark_routes_are_identical() {
        let source = create_table(37);
        let mut direct = source.clone();
        let mut collected = source;
        let direct_count = direct.mark_stale_offline(Tick::new(NOW));
        let collected_count = collect_mark_stale(&mut collected);

        assert_eq!(direct_count, collected_count);
        assert_eq!(route_checksum(&direct, 37), route_checksum(&collected, 37));
        for index in 0..37 {
            let node_id = NodeId::new(index);
            assert_eq!(direct.node_route(node_id), collected.node_route(node_id));
        }
    }
}
