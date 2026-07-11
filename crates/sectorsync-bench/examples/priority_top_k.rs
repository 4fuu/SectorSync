//! Guarded benchmark for budgeted deterministic priority selection.

use std::cmp::Ordering;
use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::EntityHandle;

const DEFAULT_CANDIDATES: usize = 2_000;
const DEFAULT_LIMIT: usize = 32;
const DEFAULT_CALLS_PER_TICK: usize = 100;
const DEFAULT_TICKS: usize = 20;
const DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
const DEFAULT_TICK_P99_BUDGET_MS: f64 = 1_000.0;
const GUARD_MAX_CANDIDATES: usize = 10_000;
const GUARD_MAX_CALLS_PER_TICK: usize = 200;
const GUARD_MAX_TICKS: usize = 30;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum SelectionMode {
    #[default]
    TopK,
    FullSort,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Config {
    candidates: usize,
    limit: usize,
    calls_per_tick: usize,
    ticks: usize,
    selection_mode: SelectionMode,
    time_budget_ms: u64,
    tick_p99_budget_ms: f64,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            candidates: DEFAULT_CANDIDATES,
            limit: DEFAULT_LIMIT,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            selection_mode: SelectionMode::TopK,
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
            selection_mode: if args.iter().any(|arg| arg == "--full-sort") {
                SelectionMode::FullSort
            } else {
                SelectionMode::TopK
            },
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--candidates=") {
                config.candidates = value.parse().unwrap_or(config.candidates);
            } else if let Some(value) = arg.strip_prefix("--limit=") {
                config.limit = value.parse().unwrap_or(config.limit);
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
        self.candidates = self.candidates.max(1);
        self.limit = self.limit.max(1).min(self.candidates);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        self.time_budget_ms = self.time_budget_ms.max(1);
        self.tick_p99_budget_ms = self.tick_p99_budget_ms.max(0.001);
        if !self.allow_heavy {
            self.candidates = self.candidates.min(GUARD_MAX_CANDIDATES);
            self.limit = self.limit.min(self.candidates);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.candidates != before.candidates
            || self.limit != before.limit
            || self.calls_per_tick != before.calls_per_tick
            || self.ticks != before.ticks;
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Candidate {
    handle: EntityHandle,
    score: u64,
    distance_squared: f32,
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    calls: usize,
    selected: usize,
    checksum: u64,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let source = create_candidates(config.candidates);
    let stats = run(&source, config);
    let tick_p99 = percentile_ms(&stats.tick_ms, 0.99);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let expected_selected = expected_calls.saturating_mul(config.limit);
    let benchmark_ok = stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.calls == expected_calls
        && stats.selected == expected_selected
        && stats.checksum > 0
        && tick_p99 <= config.tick_p99_budget_ms;

    println!("SectorSync budgeted priority selection benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_candidates={GUARD_MAX_CANDIDATES}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("candidates={}", config.candidates);
    println!("limit={}", config.limit);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!(
        "top_k_selection={}",
        config.selection_mode == SelectionMode::TopK
    );
    println!(
        "partition_applied={}",
        config.selection_mode == SelectionMode::TopK
            && config.limit.saturating_mul(2) < config.candidates
    );
    println!("calls={}", stats.calls);
    println!("selected={}", stats.selected);
    println!("selection_checksum={}", stats.checksum);
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
    println!("time_budget_ms={}", config.time_budget_ms);
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_candidates(count: usize) -> Vec<Candidate> {
    (0..count)
        .map(|index| {
            let index = u32::try_from(index).expect("guarded candidate index fits u32");
            Candidate {
                handle: EntityHandle::new(index, index % 5),
                score: u64::from(index.wrapping_mul(2_654_435_761) % 10_007),
                distance_squared: f32::from(
                    u16::try_from(index.wrapping_mul(4051) % 8191).expect("distance fits u16"),
                ),
            }
        })
        .collect()
}

fn compare_candidates(left: &Candidate, right: &Candidate) -> Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.distance_squared.total_cmp(&right.distance_squared))
        .then_with(|| left.handle.cmp(&right.handle))
}

fn select(candidates: &mut [Candidate], limit: usize, mode: SelectionMode) -> usize {
    let selected = candidates.len().min(limit);
    match mode {
        SelectionMode::TopK => {
            if selected == 0 {
                return 0;
            }
            if selected.saturating_mul(2) < candidates.len() {
                candidates.select_nth_unstable_by(selected, compare_candidates);
                candidates[..selected].sort_by(compare_candidates);
            } else {
                candidates.sort_by(compare_candidates);
            }
        }
        SelectionMode::FullSort => candidates.sort_by(compare_candidates),
    }
    selected
}

fn run(source: &[Candidate], config: Config) -> Stats {
    let mut work = source.to_vec();
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
            work.clone_from_slice(source);
            let selected = select(&mut work, config.limit, config.selection_mode);
            stats.selected = stats.selected.saturating_add(selected);
            stats.checksum = stats.checksum.wrapping_add(
                work[..selected]
                    .iter()
                    .fold(0_u64, |sum, candidate| sum.wrapping_add(candidate.score)),
            );
            black_box(&work[..selected]);
            stats.calls = stats.calls.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
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
                "--candidates=999999",
                "--limit=999999",
                "--calls-per-tick=999999",
                "--ticks=999999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.candidates, GUARD_MAX_CANDIDATES);
        assert_eq!(config.limit, GUARD_MAX_CANDIDATES);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn top_k_matches_full_sort_for_budget_edges() {
        let source = create_candidates(257);
        for limit in [0, 1, 7, 64, 256, 257, 300] {
            let mut top_k = source.clone();
            let top_k_len = select(&mut top_k, limit, SelectionMode::TopK);
            let mut full = source.clone();
            let full_len = select(&mut full, limit, SelectionMode::FullSort);

            assert_eq!(top_k_len, full_len);
            assert_eq!(&top_k[..top_k_len], &full[..full_len]);
        }
    }
}
