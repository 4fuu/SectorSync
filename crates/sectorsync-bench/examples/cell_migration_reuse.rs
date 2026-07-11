//! Guarded A/B benchmark for reusable cell-migration storage.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellCoord3, CellIndex, EntityId, GridSpec, InstanceId, NodeId, PolicyId, Position3,
    Station, StationConfig, StationId,
};
use sectorsync_runtime::{
    CellMigrationExecutor, CellMigrationReport, CellMigrationScratch, StationSet,
};

const DEFAULT_ENTITIES_PER_CELL: usize = 500;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_ENTITIES_PER_CELL: usize = 2_000;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_ENTITIES: usize = 20_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    entities_per_cell: usize,
    ticks: usize,
    reuse: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

fn main() {
    let config = parse_config();
    let result = run(config);
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    ticks_completed: usize,
    entities_migrated: usize,
    migration_checksum: u64,
    fresh_storage_passes: usize,
    retained_handle_capacity: usize,
    retained_entity_capacity: usize,
    retained_candidate_capacity: usize,
    retained_cell_capacity: usize,
    retained_migration_capacity: usize,
    target_index_entities: usize,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    path_ok: bool,
    workload_ok: bool,
    time_budget_exhausted: bool,
    benchmark_ok: bool,
}

#[allow(clippy::too_many_lines)]
fn run(config: Config) -> ResultSummary {
    let total_entities = config.entities_per_cell.saturating_mul(config.ticks);
    let grid = GridSpec::new(16.0).expect("valid benchmark grid");
    let source_id = StationId::new(1);
    let target_id = StationId::new(2);
    let mut source = station(source_id);
    let mut target = station(target_id);
    source.reserve_entities(total_entities);
    target.reserve_entities(total_entities);
    let mut source_index = CellIndex::new(grid);
    let mut target_index = CellIndex::new(grid);
    source_index.reserve(total_entities, config.ticks);
    target_index.reserve(total_entities, config.ticks);

    let mut cells = Vec::with_capacity(config.ticks);
    let mut next_entity_id = 1_u64;
    for tick in 0..config.ticks {
        let cell_offset = u16::try_from(tick).expect("guarded tick count fits u16");
        let cell_x = i32::from(cell_offset);
        let cell = CellCoord3::new(cell_x, 0, 0);
        cells.push(cell);
        let x = f32::from(cell_offset).mul_add(16.0, 1.0);
        for _ in 0..config.entities_per_cell {
            let entity_id = EntityId::new(next_entity_id);
            next_entity_id = next_entity_id.saturating_add(1);
            let position = Position3::new(x, 1.0, 1.0);
            let handle = source
                .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(0))
                .expect("guarded entity should spawn");
            source_index.upsert(handle, position, Bounds::Point);
        }
    }

    let mut stations = StationSet::with_capacity(2);
    stations.push(source);
    stations.push(target);
    let mut scratch = CellMigrationScratch::new();
    let mut reusable_report = CellMigrationReport::default();
    if config.reuse {
        scratch.reserve(config.entities_per_cell, config.entities_per_cell);
        reusable_report.scanned_cells.reserve(1);
        reusable_report
            .entity_migrations
            .reserve(config.entities_per_cell);
    }

    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.ticks);
    let mut entities_migrated = 0_usize;
    let mut migration_checksum = 0_u64;
    let mut fresh_storage_passes = 0_usize;
    let mut ticks_completed = 0_usize;
    let mut time_budget_exhausted = false;

    for cell in cells {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        if config.reuse {
            CellMigrationExecutor::migrate_cells_into(
                &mut stations,
                &mut source_index,
                &mut target_index,
                source_id,
                target_id,
                core::slice::from_ref(&cell),
                4,
                &mut scratch,
                &mut reusable_report,
            )
            .expect("reusable migration should complete");
            consume_report(
                &reusable_report,
                &mut entities_migrated,
                &mut migration_checksum,
            );
        } else {
            let report = CellMigrationExecutor::migrate_cells(
                &mut stations,
                &mut source_index,
                &mut target_index,
                source_id,
                target_id,
                core::slice::from_ref(&cell),
                4,
            )
            .expect("owned migration should complete");
            fresh_storage_passes = fresh_storage_passes.saturating_add(1);
            consume_report(&report, &mut entities_migrated, &mut migration_checksum);
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        ticks_completed = ticks_completed.saturating_add(1);
    }

    black_box(migration_checksum);
    latencies.sort_by(f64::total_cmp);
    let target_index_entities = target_index.entity_count();
    let path_ok = if config.reuse {
        fresh_storage_passes == 0
            && scratch.handle_capacity() >= config.entities_per_cell
            && scratch.entity_capacity() >= config.entities_per_cell
            && scratch.candidate_capacity() >= config.entities_per_cell
    } else {
        fresh_storage_passes == ticks_completed
    };
    let workload_ok = ticks_completed == config.ticks
        && entities_migrated == total_entities
        && target_index_entities == total_entities;
    let benchmark_ok = path_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        ticks_completed,
        entities_migrated,
        migration_checksum,
        fresh_storage_passes,
        retained_handle_capacity: scratch.handle_capacity(),
        retained_entity_capacity: scratch.entity_capacity(),
        retained_candidate_capacity: scratch.candidate_capacity(),
        retained_cell_capacity: reusable_report.scanned_cells.capacity(),
        retained_migration_capacity: reusable_report.entity_migrations.capacity(),
        target_index_entities,
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

fn station(station_id: StationId) -> Station {
    Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}

fn consume_report(report: &CellMigrationReport, count: &mut usize, checksum: &mut u64) {
    *count = count.saturating_add(report.entity_migrations.len());
    for migration in &report.entity_migrations {
        *checksum = checksum.saturating_add(migration.transfer.entity_id.get());
    }
}

fn parse_config() -> Config {
    let mut entities_per_cell = DEFAULT_ENTITIES_PER_CELL;
    let mut ticks = DEFAULT_TICKS;
    let mut reuse = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--entities-per-cell=") {
            entities_per_cell = value.parse().unwrap_or(entities_per_cell);
        } else if let Some(value) = arg.strip_prefix("--ticks=") {
            ticks = value.parse().unwrap_or(ticks);
        } else if arg == "--fresh-storage" {
            reuse = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    entities_per_cell = entities_per_cell.max(1);
    ticks = ticks.max(1);
    let requested = (entities_per_cell, ticks);
    if !allow_heavy {
        entities_per_cell = entities_per_cell.min(GUARD_MAX_ENTITIES_PER_CELL);
        ticks = ticks.min(GUARD_MAX_TICKS);
        if entities_per_cell.saturating_mul(ticks) > GUARD_MAX_TOTAL_ENTITIES {
            entities_per_cell = (GUARD_MAX_TOTAL_ENTITIES / ticks).max(1);
        }
    }
    Config {
        entities_per_cell,
        ticks,
        reuse,
        allow_heavy,
        guard_applied: requested != (entities_per_cell, ticks),
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

fn print_result(config: Config, result: &ResultSummary) {
    println!("SectorSync cell migration storage benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_entities_per_cell={GUARD_MAX_ENTITIES_PER_CELL}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_entities={GUARD_MAX_TOTAL_ENTITIES}");
    println!("entities_per_cell={}", config.entities_per_cell);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", result.ticks_completed);
    println!("reusable_storage={}", config.reuse);
    println!("entities_migrated={}", result.entities_migrated);
    println!("migration_checksum={}", result.migration_checksum);
    println!("fresh_storage_passes={}", result.fresh_storage_passes);
    println!(
        "retained_handle_capacity={}",
        result.retained_handle_capacity
    );
    println!(
        "retained_entity_capacity={}",
        result.retained_entity_capacity
    );
    println!(
        "retained_candidate_capacity={}",
        result.retained_candidate_capacity
    );
    println!("retained_cell_capacity={}", result.retained_cell_capacity);
    println!(
        "retained_migration_capacity={}",
        result.retained_migration_capacity
    );
    println!("target_index_entities={}", result.target_index_entities);
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
