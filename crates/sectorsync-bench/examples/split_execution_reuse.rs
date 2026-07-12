//! Guarded A/B benchmark for reusable multi-room split execution storage.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellCoord3, CellIndex, EntityId, GridSpec, InstanceId, NodeId, PolicyId, Position3,
    SplitProposal, Station, StationConfig, StationId,
};
use sectorsync_runtime::{
    CellOwnershipTable, SplitAction, SplitSchedule, SplitScheduleExecutionScratch, SplitScheduler,
    SplitSchedulerConfig, StationIndexSet, StationSet,
};

const DEFAULT_ROOMS: usize = 10;
const DEFAULT_ACTIONS_PER_ROOM: usize = 4;
const DEFAULT_ENTITIES_PER_ACTION: usize = 128;
const GUARD_MAX_ROOMS: usize = 20;
const GUARD_MAX_ACTIONS_PER_ROOM: usize = 8;
const GUARD_MAX_ENTITIES_PER_ACTION: usize = 512;
const GUARD_MAX_TOTAL_ENTITIES: usize = 20_000;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug)]
struct Config {
    rooms: usize,
    actions_per_room: usize,
    entities_per_action: usize,
    reuse: bool,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug)]
struct ResultSummary {
    rooms_completed: usize,
    actions_executed: usize,
    ownership_updates: usize,
    entities_migrated: usize,
    migration_checksum: u64,
    fresh_execution_reports: usize,
    retained_ownership_slots: usize,
    retained_migration_slots: usize,
    retained_update_cell_capacity: usize,
    retained_entity_migration_capacity: usize,
    retained_candidate_capacity: usize,
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

fn main() {
    let config = parse_config();
    let result = run(config);
    print_result(config, &result);
    if !result.benchmark_ok {
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_lines)]
fn run(config: Config) -> ResultSummary {
    let total_actions = config.rooms.saturating_mul(config.actions_per_room);
    let total_entities = total_actions.saturating_mul(config.entities_per_action);
    let grid = GridSpec::new(16.0).expect("valid benchmark grid");
    let mut stations = StationSet::with_capacity(config.rooms.saturating_mul(2));
    let mut indexes = StationIndexSet::with_capacity(config.rooms.saturating_mul(2));
    let mut ownership = CellOwnershipTable::default();
    let mut room_schedules = Vec::with_capacity(config.rooms);
    let mut next_entity_id = 1_u64;

    for room in 0..config.rooms {
        let source_id = station_id(room.saturating_mul(2).saturating_add(1));
        let target_id = station_id(room.saturating_mul(2).saturating_add(2));
        let room_entities = config
            .actions_per_room
            .saturating_mul(config.entities_per_action);
        let mut source = station(source_id, room);
        let mut target = station(target_id, room);
        source.reserve_entities(room_entities);
        target.reserve_entities(room_entities);
        let mut source_index = CellIndex::new(grid);
        let mut target_index = CellIndex::new(grid);
        source_index.reserve(room_entities, config.actions_per_room);
        target_index.reserve(room_entities, config.actions_per_room);
        let mut schedule = SplitSchedule::default();
        schedule.actions.reserve(config.actions_per_room);

        for action_index in 0..config.actions_per_room {
            let global_cell = room
                .saturating_mul(config.actions_per_room)
                .saturating_add(action_index);
            let cell_offset = u16::try_from(global_cell).expect("guarded cell count fits u16");
            let cell = CellCoord3::new(i32::from(cell_offset), 0, 0);
            let x = f32::from(cell_offset).mul_add(16.0, 1.0);
            for _ in 0..config.entities_per_action {
                let entity_id = EntityId::new(next_entity_id);
                next_entity_id = next_entity_id.saturating_add(1);
                let position = Position3::new(x, 1.0, 1.0);
                let handle = source
                    .spawn_owned(entity_id, position, Bounds::Point, PolicyId::new(0))
                    .expect("guarded entity should spawn");
                source_index.upsert(handle, position, Bounds::Point);
            }
            ownership.assign(cell, source_id);
            schedule.actions.push(SplitAction {
                source_station: source_id,
                target_station: target_id,
                proposal: SplitProposal {
                    source_station: source_id,
                    cells_to_move: vec![cell],
                    moved_pressure_score: u64::try_from(config.entities_per_action)
                        .expect("guarded entity count fits u64"),
                },
                ..SplitAction::default()
            });
        }

        stations.push(source);
        stations.push(target);
        indexes.insert(source_id, source_index);
        indexes.insert(target_id, target_index);
        room_schedules.push(schedule);
    }

    let scheduler = SplitScheduler::new(SplitSchedulerConfig {
        max_actions_per_pass: config.actions_per_room,
        max_cells_per_action: 1,
        ghost_ttl_ticks: 4,
        ..SplitSchedulerConfig::default()
    });
    let mut scratch = SplitScheduleExecutionScratch::new();
    if config.reuse {
        scratch.reserve(config.actions_per_room, 1, config.entities_per_action);
    }
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let benchmark_started = Instant::now();
    let mut latencies = Vec::with_capacity(config.rooms);
    let mut rooms_completed = 0_usize;
    let mut actions_executed = 0_usize;
    let mut ownership_updates = 0_usize;
    let mut entities_migrated = 0_usize;
    let mut migration_checksum = 0_u64;
    let mut fresh_execution_reports = 0_usize;
    let mut time_budget_exhausted = false;

    for schedule in &room_schedules {
        if benchmark_started.elapsed() >= budget {
            time_budget_exhausted = true;
            break;
        }
        let started = Instant::now();
        if config.reuse {
            let report = scheduler
                .execute_into(
                    schedule.view(),
                    &mut stations,
                    &mut indexes,
                    &mut ownership,
                    &mut scratch,
                )
                .expect("reusable split execution should complete");
            consume_report(
                report.ownership_updates,
                report.cell_migrations,
                &mut ownership_updates,
                &mut entities_migrated,
                &mut migration_checksum,
            );
        } else {
            let mut fresh_scratch = SplitScheduleExecutionScratch::new();
            let report = scheduler
                .execute_into(
                    schedule.view(),
                    &mut stations,
                    &mut indexes,
                    &mut ownership,
                    &mut fresh_scratch,
                )
                .expect("owned split execution should complete");
            fresh_execution_reports = fresh_execution_reports.saturating_add(1);
            consume_report(
                report.ownership_updates,
                report.cell_migrations,
                &mut ownership_updates,
                &mut entities_migrated,
                &mut migration_checksum,
            );
        }
        latencies.push(started.elapsed().as_secs_f64() * 1_000.0);
        rooms_completed = rooms_completed.saturating_add(1);
        actions_executed = actions_executed.saturating_add(schedule.actions.len());
    }

    black_box(migration_checksum);
    latencies.sort_by(f64::total_cmp);
    let target_index_entities = indexes
        .iter()
        .filter(|(station_id, _)| station_id.get() % 2 == 0)
        .map(|(_, index)| index.entity_count())
        .sum::<usize>();
    let path_ok = if config.reuse {
        fresh_execution_reports == 0
            && scratch.retained_ownership_slots() >= config.actions_per_room
            && scratch.retained_migration_slots() >= config.actions_per_room
            && scratch.retained_candidate_capacity() >= config.entities_per_action
    } else {
        fresh_execution_reports == rooms_completed
    };
    let workload_ok = rooms_completed == config.rooms
        && actions_executed == total_actions
        && ownership_updates == total_actions
        && entities_migrated == total_entities
        && target_index_entities == total_entities;
    let benchmark_ok = path_ok && workload_ok && !time_budget_exhausted;

    ResultSummary {
        rooms_completed,
        actions_executed,
        ownership_updates,
        entities_migrated,
        migration_checksum,
        fresh_execution_reports,
        retained_ownership_slots: scratch.retained_ownership_slots(),
        retained_migration_slots: scratch.retained_migration_slots(),
        retained_update_cell_capacity: scratch.retained_update_cell_capacity(),
        retained_entity_migration_capacity: scratch.retained_entity_migration_capacity(),
        retained_candidate_capacity: scratch.retained_candidate_capacity(),
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

fn station_id(value: usize) -> StationId {
    StationId::new(u32::try_from(value).expect("guarded station id fits u32"))
}

fn station(station_id: StationId, room: usize) -> Station {
    Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(1),
        instance_id: InstanceId::new(
            u64::try_from(room.saturating_add(1)).expect("guarded room id fits u64"),
        ),
        tick_rate_hz: 20,
    })
}

fn consume_report(
    updates: &[sectorsync_runtime::CellOwnershipUpdate],
    migrations: &[sectorsync_runtime::CellMigrationReport],
    ownership_updates: &mut usize,
    entities_migrated: &mut usize,
    checksum: &mut u64,
) {
    *ownership_updates = ownership_updates.saturating_add(updates.len());
    for migration in migrations {
        *entities_migrated = entities_migrated.saturating_add(migration.entity_migrations.len());
        for entity in &migration.entity_migrations {
            *checksum = checksum.saturating_add(entity.transfer.entity_id.get());
        }
    }
}

fn parse_config() -> Config {
    let mut rooms = DEFAULT_ROOMS;
    let mut actions_per_room = DEFAULT_ACTIONS_PER_ROOM;
    let mut entities_per_action = DEFAULT_ENTITIES_PER_ACTION;
    let mut reuse = true;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--rooms=") {
            rooms = value.parse().unwrap_or(rooms);
        } else if let Some(value) = arg.strip_prefix("--actions-per-room=") {
            actions_per_room = value.parse().unwrap_or(actions_per_room);
        } else if let Some(value) = arg.strip_prefix("--entities-per-action=") {
            entities_per_action = value.parse().unwrap_or(entities_per_action);
        } else if arg == "--fresh-storage" {
            reuse = false;
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    rooms = rooms.max(1);
    actions_per_room = actions_per_room.max(1);
    entities_per_action = entities_per_action.max(1);
    let requested = (rooms, actions_per_room, entities_per_action);
    if !allow_heavy {
        rooms = rooms.min(GUARD_MAX_ROOMS);
        actions_per_room = actions_per_room.min(GUARD_MAX_ACTIONS_PER_ROOM);
        entities_per_action = entities_per_action.min(GUARD_MAX_ENTITIES_PER_ACTION);
        let passes = rooms.saturating_mul(actions_per_room).max(1);
        if passes.saturating_mul(entities_per_action) > GUARD_MAX_TOTAL_ENTITIES {
            entities_per_action = (GUARD_MAX_TOTAL_ENTITIES / passes).max(1);
        }
    }
    Config {
        rooms,
        actions_per_room,
        entities_per_action,
        reuse,
        allow_heavy,
        guard_applied: requested != (rooms, actions_per_room, entities_per_action),
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

#[allow(clippy::too_many_lines)]
fn print_result(config: Config, result: &ResultSummary) {
    println!("SectorSync split execution storage benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_actions_per_room={GUARD_MAX_ACTIONS_PER_ROOM}");
    println!("guard_max_entities_per_action={GUARD_MAX_ENTITIES_PER_ACTION}");
    println!("guard_max_total_entities={GUARD_MAX_TOTAL_ENTITIES}");
    println!("rooms={}", config.rooms);
    println!("actions_per_room={}", config.actions_per_room);
    println!("entities_per_action={}", config.entities_per_action);
    println!("rooms_completed={}", result.rooms_completed);
    println!("reusable_storage={}", config.reuse);
    println!("actions_executed={}", result.actions_executed);
    println!("ownership_updates={}", result.ownership_updates);
    println!("entities_migrated={}", result.entities_migrated);
    println!("migration_checksum={}", result.migration_checksum);
    println!("fresh_execution_reports={}", result.fresh_execution_reports);
    println!(
        "retained_ownership_slots={}",
        result.retained_ownership_slots
    );
    println!(
        "retained_migration_slots={}",
        result.retained_migration_slots
    );
    println!(
        "retained_update_cell_capacity={}",
        result.retained_update_cell_capacity
    );
    println!(
        "retained_entity_migration_capacity={}",
        result.retained_entity_migration_capacity
    );
    println!(
        "retained_candidate_capacity={}",
        result.retained_candidate_capacity
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
