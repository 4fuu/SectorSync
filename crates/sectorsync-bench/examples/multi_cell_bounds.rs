//! Guarded benchmark for repeated multi-cell bounds updates.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, CellIndexUpdate, EntityHandle, GridSpec, Position3,
};

const CELL_SIZE: f32 = 32.0;
const ENTITY_RADIUS: f32 = 20.0;
const DEFAULT_ENTITIES: usize = 20_000;
const DEFAULT_TICKS: usize = 10;
const DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
const DEFAULT_UPDATE_P99_BUDGET_MS: f64 = 100.0;
const GUARD_MAX_ENTITIES: usize = 50_000;
const GUARD_MAX_TICKS: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Config {
    entities: usize,
    ticks: usize,
    materialize_cell_list: bool,
    time_budget_ms: u64,
    update_p99_budget_ms: f64,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            entities: DEFAULT_ENTITIES,
            ticks: DEFAULT_TICKS,
            materialize_cell_list: false,
            time_budget_ms: DEFAULT_TIME_BUDGET_MS,
            update_p99_budget_ms: DEFAULT_UPDATE_P99_BUDGET_MS,
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
            materialize_cell_list: args.iter().any(|arg| arg == "--materialize-cell-list"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--entities=") {
                config.entities = value.parse().unwrap_or(config.entities);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = value.parse().unwrap_or(config.ticks);
            } else if let Some(value) = arg.strip_prefix("--time-budget-ms=") {
                config.time_budget_ms = value.parse().unwrap_or(config.time_budget_ms);
            } else if let Some(value) = arg.strip_prefix("--update-p99-budget-ms=") {
                config.update_p99_budget_ms = value.parse().unwrap_or(config.update_p99_budget_ms);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let before = *self;
        self.entities = self.entities.max(1);
        self.ticks = self.ticks.max(1);
        self.time_budget_ms = self.time_budget_ms.max(1);
        self.update_p99_budget_ms = self.update_p99_budget_ms.max(0.001);
        if !self.allow_heavy {
            self.entities = self.entities.min(GUARD_MAX_ENTITIES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.entities != before.entities || self.ticks != before.ticks;
    }
}

#[derive(Debug, Default)]
struct Stats {
    update_ms: Vec<f64>,
    updates: usize,
    unchanged: usize,
    relocated: usize,
    inserted: usize,
    materialized_cells: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let grid = GridSpec::new(CELL_SIZE).expect("fixed grid is valid");
    let bounds = Bounds::Sphere {
        radius: ENTITY_RADIUS,
    };
    let mut index = CellIndex::with_capacity(grid, config.entities, 256);
    let entities = create_entities(&mut index, config.entities, bounds);
    let stats = run(&mut index, &entities, bounds, config);
    let update_p99 = percentile_ms(&stats.update_ms, 0.99);
    let expected_updates = config.entities.saturating_mul(config.ticks);
    let path_ok = if config.materialize_cell_list {
        stats.materialized_cells > 0
    } else {
        stats.materialized_cells == 0
    };
    let benchmark_ok = stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.updates == expected_updates
        && stats.unchanged == expected_updates
        && stats.relocated == 0
        && stats.inserted == 0
        && path_ok
        && update_p99 <= config.update_p99_budget_ms;

    println!("SectorSync multi-cell bounds update benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_entities={GUARD_MAX_ENTITIES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("entities={}", config.entities);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("cell_size={CELL_SIZE:.3}");
    println!("entity_radius={ENTITY_RADIUS:.3}");
    println!("cells_per_entity=27");
    println!(
        "allocation_free_membership_check={}",
        !config.materialize_cell_list
    );
    println!("updates={}", stats.updates);
    println!("updates_unchanged={}", stats.unchanged);
    println!("updates_relocated={}", stats.relocated);
    println!("updates_inserted={}", stats.inserted);
    println!("materialized_cells={}", stats.materialized_cells);
    println!("update_ms_p50={:.3}", percentile_ms(&stats.update_ms, 0.50));
    println!("update_ms_p95={:.3}", percentile_ms(&stats.update_ms, 0.95));
    println!("update_ms_p99={update_p99:.3}");
    println!("update_ms_max={:.3}", percentile_ms(&stats.update_ms, 1.00));
    println!("threshold_update_ms_p99={:.3}", config.update_p99_budget_ms);
    println!(
        "threshold_update_ok={}",
        update_p99 <= config.update_p99_budget_ms
    );
    println!(
        "threshold_workload_completed_ok={}",
        stats.updates == expected_updates
    );
    println!(
        "threshold_membership_unchanged_ok={}",
        stats.unchanged == expected_updates
    );
    println!("threshold_path_ok={path_ok}");
    println!("time_budget_ms={}", config.time_budget_ms);
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn create_entities(
    index: &mut CellIndex,
    count: usize,
    bounds: Bounds,
) -> Vec<(EntityHandle, Position3)> {
    (0..count)
        .map(|entity_index| {
            let handle = EntityHandle::new(
                u32::try_from(entity_index).expect("guarded entity index fits u32"),
                0,
            );
            let lane = u16::try_from(entity_index % 64).expect("lane fits u16");
            let position = Position3::new(f32::from(lane) * 64.0 + 16.0, 16.0, 16.0);
            assert_eq!(
                index.upsert_tracked(handle, position, bounds),
                CellIndexUpdate::Inserted
            );
            (handle, position)
        })
        .collect()
}

fn run(
    index: &mut CellIndex,
    entities: &[(EntityHandle, Position3)],
    bounds: Bounds,
    config: Config,
) -> Stats {
    let started = Instant::now();
    let time_budget = Duration::from_millis(config.time_budget_ms);
    let mut stats = Stats::default();
    for tick in 0..config.ticks {
        if started.elapsed() >= time_budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let update_started = Instant::now();
        let offset = if tick.is_multiple_of(2) { 0.25 } else { 0.0 };
        for &(handle, base) in entities {
            let position = Position3::new(base.x + offset, base.y, base.z);
            if config.materialize_cell_list {
                let cells = index.grid().cells_for_bounds(position, bounds);
                stats.materialized_cells = stats.materialized_cells.saturating_add(cells.len());
                black_box(cells);
            }
            match index.upsert_tracked(handle, position, bounds) {
                CellIndexUpdate::Unchanged => stats.unchanged = stats.unchanged.saturating_add(1),
                CellIndexUpdate::Relocated => stats.relocated = stats.relocated.saturating_add(1),
                CellIndexUpdate::Inserted => stats.inserted = stats.inserted.saturating_add(1),
            }
            stats.updates = stats.updates.saturating_add(1);
        }
        stats
            .update_ms
            .push(update_started.elapsed().as_secs_f64() * 1_000.0);
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
    fn guard_clamps_manual_workload_without_heavy_opt_in() {
        let config = Config::from_args(
            ["--entities=999999", "--ticks=999"]
                .into_iter()
                .map(str::to_owned),
        );
        assert_eq!(config.entities, GUARD_MAX_ENTITIES);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn small_workload_keeps_membership_unchanged_in_both_modes() {
        for materialize_cell_list in [false, true] {
            let config = Config {
                entities: 8,
                ticks: 2,
                materialize_cell_list,
                update_p99_budget_ms: f64::MAX,
                ..Config::default()
            };
            let grid = GridSpec::new(CELL_SIZE).expect("fixed grid is valid");
            let bounds = Bounds::Sphere {
                radius: ENTITY_RADIUS,
            };
            let mut index = CellIndex::with_capacity(grid, config.entities, 256);
            let entities = create_entities(&mut index, config.entities, bounds);
            let stats = run(&mut index, &entities, bounds, config);

            assert_eq!(stats.updates, 16);
            assert_eq!(stats.unchanged, 16);
            assert_eq!(stats.relocated, 0);
            assert_eq!(stats.materialized_cells > 0, materialize_cell_list);
        }
    }
}
