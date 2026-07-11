//! Guarded single-thread benchmark for many small room instances.

use std::env;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, CellIndexUpdate, ClientId, CompiledSyncPolicy, ComponentDescriptor,
    ComponentId, ComponentMigrationMode, ComponentStore, ComponentSyncMode, EntityHandle, EntityId,
    GridSpec, InstanceId, NodeId, PolicyId, PolicyTable, Position3, ReplicationBatchScratch,
    ReplicationBudget, ReplicationPlan, ReplicationPlanner, ReplicationScratch, Station,
    StationConfig, StationId, ViewerQuery,
};
use sectorsync_wire::{ComponentSelection, ReplicationFrameBuilder, ReplicationFrameLimits};

const DEFAULT_ROOMS: usize = 500;
const DEFAULT_MIN_PLAYERS: usize = 4;
const DEFAULT_MAX_PLAYERS: usize = 24;
const DEFAULT_PLAYERS_PER_STATION: usize = 12;
const DEFAULT_ENTITIES_PER_PLAYER: usize = 8;
const DEFAULT_COMPONENT_BYTES: usize = 4;
const DEFAULT_DIRTY_PERCENT: usize = 100;
const DEFAULT_TICKS: usize = 8;
const DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
const DEFAULT_SWEEP_P99_BUDGET_MS: f64 = 50.0;

const GUARD_MAX_ROOMS: usize = 500;
const GUARD_MAX_PLAYERS: usize = 32;
const GUARD_MIN_PLAYERS_PER_STATION: usize = 4;
const GUARD_MAX_STATIONS_PER_ROOM: usize = 8;
const GUARD_MAX_ENTITIES_PER_PLAYER: usize = 16;
const GUARD_MAX_ENTITIES_PER_ROOM: usize = 256;
const GUARD_MAX_COMPONENT_BYTES: usize = 256;
const GUARD_MAX_TICKS: usize = 10;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum IndexUpdateMode {
    #[default]
    SameCellFastPath,
    ForceReinsert,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MovementPattern {
    #[default]
    SameCell,
    CrossCell,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ComponentUpdateMode {
    #[default]
    InPlace,
    ForceReplace,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FrameCapacityMode {
    #[default]
    Hint,
    Growth,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Config {
    rooms: usize,
    min_players: usize,
    max_players: usize,
    players_per_station: usize,
    max_stations_per_room: usize,
    entities_per_player: usize,
    entities_per_room: usize,
    component_bytes: usize,
    dirty_percent: usize,
    component_update_percent: usize,
    component_update_mode: ComponentUpdateMode,
    frame_capacity_mode: FrameCapacityMode,
    moving_percent: usize,
    movement_pattern: MovementPattern,
    index_update_mode: IndexUpdateMode,
    preallocate: bool,
    ticks: usize,
    time_budget_ms: u64,
    sweep_p99_budget_ms: f64,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rooms: DEFAULT_ROOMS,
            min_players: DEFAULT_MIN_PLAYERS,
            max_players: DEFAULT_MAX_PLAYERS,
            players_per_station: DEFAULT_PLAYERS_PER_STATION,
            max_stations_per_room: GUARD_MAX_STATIONS_PER_ROOM,
            entities_per_player: DEFAULT_ENTITIES_PER_PLAYER,
            entities_per_room: 0,
            component_bytes: DEFAULT_COMPONENT_BYTES,
            dirty_percent: DEFAULT_DIRTY_PERCENT,
            component_update_percent: 0,
            component_update_mode: ComponentUpdateMode::InPlace,
            frame_capacity_mode: FrameCapacityMode::Hint,
            moving_percent: 0,
            movement_pattern: MovementPattern::SameCell,
            index_update_mode: IndexUpdateMode::SameCellFastPath,
            preallocate: true,
            ticks: DEFAULT_TICKS,
            time_budget_ms: DEFAULT_TIME_BUDGET_MS,
            sweep_p99_budget_ms: DEFAULT_SWEEP_P99_BUDGET_MS,
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
            preallocate: !args.iter().any(|arg| arg == "--no-preallocate"),
            index_update_mode: if args.iter().any(|arg| arg == "--force-index-reinsert") {
                IndexUpdateMode::ForceReinsert
            } else {
                IndexUpdateMode::SameCellFastPath
            },
            movement_pattern: if args.iter().any(|arg| arg == "--cross-cell-movement") {
                MovementPattern::CrossCell
            } else {
                MovementPattern::SameCell
            },
            component_update_mode: if args.iter().any(|arg| arg == "--force-component-replace") {
                ComponentUpdateMode::ForceReplace
            } else {
                ComponentUpdateMode::InPlace
            },
            frame_capacity_mode: if args.iter().any(|arg| arg == "--no-frame-capacity-hint") {
                FrameCapacityMode::Growth
            } else {
                FrameCapacityMode::Hint
            },
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--rooms=") {
                config.rooms = parse_usize(value, config.rooms);
            } else if let Some(value) = arg.strip_prefix("--min-players=") {
                config.min_players = parse_usize(value, config.min_players);
            } else if let Some(value) = arg.strip_prefix("--max-players=") {
                config.max_players = parse_usize(value, config.max_players);
            } else if let Some(value) = arg.strip_prefix("--players-per-station=") {
                config.players_per_station = parse_usize(value, config.players_per_station);
            } else if let Some(value) = arg.strip_prefix("--max-stations-per-room=") {
                config.max_stations_per_room = parse_usize(value, config.max_stations_per_room);
            } else if let Some(value) = arg.strip_prefix("--entities-per-player=") {
                config.entities_per_player = parse_usize(value, config.entities_per_player);
            } else if let Some(value) = arg.strip_prefix("--entities-per-room=") {
                config.entities_per_room = parse_usize(value, config.entities_per_room);
            } else if let Some(value) = arg.strip_prefix("--component-bytes=") {
                config.component_bytes = parse_usize(value, config.component_bytes);
            } else if let Some(value) = arg.strip_prefix("--dirty-percent=") {
                config.dirty_percent = parse_usize(value, config.dirty_percent);
            } else if let Some(value) = arg.strip_prefix("--component-update-percent=") {
                config.component_update_percent =
                    parse_usize(value, config.component_update_percent);
            } else if let Some(value) = arg.strip_prefix("--moving-percent=") {
                config.moving_percent = parse_usize(value, config.moving_percent);
            } else if let Some(value) = arg.strip_prefix("--ticks=") {
                config.ticks = parse_usize(value, config.ticks);
            } else if let Some(value) = arg.strip_prefix("--time-budget-ms=") {
                config.time_budget_ms = value.parse().unwrap_or(config.time_budget_ms);
            } else if let Some(value) = arg.strip_prefix("--sweep-p99-budget-ms=") {
                config.sweep_p99_budget_ms = value.parse().unwrap_or(config.sweep_p99_budget_ms);
            }
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let before = *self;
        self.rooms = self.rooms.max(1);
        self.min_players = self.min_players.max(1);
        self.max_players = self.max_players.max(self.min_players);
        self.players_per_station = self.players_per_station.max(1);
        self.max_stations_per_room = self.max_stations_per_room.max(1);
        self.entities_per_player = self.entities_per_player.max(1);
        self.component_bytes = self.component_bytes.max(1);
        self.dirty_percent = self.dirty_percent.min(100);
        self.component_update_percent = self.component_update_percent.min(100);
        self.moving_percent = self.moving_percent.min(100);
        self.ticks = self.ticks.max(1);
        self.time_budget_ms = self.time_budget_ms.max(1);
        self.sweep_p99_budget_ms = self.sweep_p99_budget_ms.max(0.001);
        if !self.allow_heavy {
            self.rooms = self.rooms.min(GUARD_MAX_ROOMS);
            self.max_players = self.max_players.min(GUARD_MAX_PLAYERS);
            self.min_players = self.min_players.min(self.max_players);
            self.players_per_station = self.players_per_station.max(GUARD_MIN_PLAYERS_PER_STATION);
            self.max_stations_per_room =
                self.max_stations_per_room.min(GUARD_MAX_STATIONS_PER_ROOM);
            self.entities_per_player = self.entities_per_player.min(GUARD_MAX_ENTITIES_PER_PLAYER);
            self.entities_per_room = self.entities_per_room.min(GUARD_MAX_ENTITIES_PER_ROOM);
            self.component_bytes = self.component_bytes.min(GUARD_MAX_COMPONENT_BYTES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = before.rooms != self.rooms
            || before.min_players != self.min_players
            || before.max_players != self.max_players
            || before.players_per_station != self.players_per_station
            || before.max_stations_per_room != self.max_stations_per_room
            || before.entities_per_player != self.entities_per_player
            || before.entities_per_room != self.entities_per_room
            || before.component_bytes != self.component_bytes
            || before.dirty_percent != self.dirty_percent
            || before.component_update_percent != self.component_update_percent
            || before.moving_percent != self.moving_percent
            || before.ticks != self.ticks;
    }

    fn players_for_room(self, room_index: usize) -> usize {
        let span = self
            .max_players
            .saturating_sub(self.min_players)
            .saturating_add(1);
        self.min_players + (room_index % span)
    }

    fn stations_for_players(self, players: usize) -> usize {
        players
            .div_ceil(self.players_per_station)
            .clamp(1, self.max_stations_per_room)
    }

    fn entities_for_players(self, players: usize) -> usize {
        if self.entities_per_room == 0 {
            players.saturating_mul(self.entities_per_player)
        } else {
            self.entities_per_room
        }
    }
}

fn parse_usize(value: &str, fallback: usize) -> usize {
    value.parse().unwrap_or(fallback)
}

#[derive(Debug)]
struct StationWork {
    station: Station,
    index: CellIndex,
    components: ComponentStore,
    viewers: Vec<ViewerQuery>,
    scratch: ReplicationScratch,
    batch_scratch: ReplicationBatchScratch,
    moving_entities: Vec<(EntityHandle, Position3)>,
    updating_components: Vec<EntityHandle>,
    component_payload: Vec<u8>,
}

#[derive(Debug)]
struct Room {
    stations: Vec<StationWork>,
    players: usize,
    entities: usize,
}

#[derive(Debug, Default)]
struct Stats {
    sweep_ms: Vec<f64>,
    planning_ms: Vec<f64>,
    encoding_ms: Vec<f64>,
    movement_ms: Vec<f64>,
    component_update_ms: Vec<f64>,
    room_updates: usize,
    viewer_queries: usize,
    selected_entities: usize,
    encoded_frames: usize,
    encoded_entities: usize,
    encoded_components: usize,
    encoded_bytes: usize,
    frames_skipped_empty: usize,
    batch_plan_slots_max: usize,
    batch_entity_capacity_max: usize,
    index_updates: usize,
    index_updates_inserted: usize,
    index_updates_unchanged: usize,
    index_updates_relocated: usize,
    component_updates: usize,
    component_updates_in_place: usize,
    component_updates_replaced: usize,
    frame_capacity_hint_bytes: usize,
    frame_capacity_bytes: usize,
    frame_capacity_slack_bytes: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Check {
    Pass,
    Fail,
}

impl Check {
    const fn from_bool(value: bool) -> Self {
        if value { Self::Pass } else { Self::Fail }
    }

    const fn passed(self) -> bool {
        matches!(self, Self::Pass)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Verdicts {
    workload: Check,
    retained_capacity: Check,
    movement: Check,
    component_updates: Check,
    frame_capacity: Check,
}

impl Verdicts {
    const fn all(self) -> bool {
        self.workload.passed()
            && self.retained_capacity.passed()
            && self.movement.passed()
            && self.component_updates.passed()
            && self.frame_capacity.passed()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Inventory {
    instances: usize,
    stations: usize,
    players: usize,
    entities: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RetainedCapacity {
    station_entities: usize,
    station_ids: usize,
    station_free_handles: usize,
    index_entities: usize,
    index_cells: usize,
    occupied_cells: usize,
    component_entities: usize,
    component_column_slots: usize,
}

struct ReplicationWorkload {
    selection: ComponentSelection,
    builder: ReplicationFrameBuilder,
    budget: ReplicationBudget,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let setup_start = Instant::now();
    let mut rooms = create_rooms(config);
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1_000.0;
    let inventory = inventory(&rooms);
    let retained = retained_capacity(&rooms);
    let run_start = Instant::now();
    let stats = run(&mut rooms, config);
    let elapsed = run_start.elapsed();
    let sweep_p99 = percentile_ms(&stats.sweep_ms, 0.99);
    let verdicts = Verdicts {
        workload: Check::from_bool(workload_completed(config, inventory, &stats)),
        retained_capacity: Check::from_bool(retained_capacity_sufficient(inventory, retained)),
        movement: Check::from_bool(movement_updates_succeeded(config, &stats)),
        component_updates: Check::from_bool(component_updates_succeeded(config, &stats)),
        frame_capacity: Check::from_bool(frame_capacity_succeeded(config, &stats)),
    };
    let benchmark_ok = verdicts.all() && sweep_p99 <= config.sweep_p99_budget_ms;

    println!("SectorSync many-room single-thread benchmark");
    println!("single_thread=true");
    println!("direct_binary_encoding=true");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_players={GUARD_MAX_PLAYERS}");
    println!("guard_max_stations_per_room={GUARD_MAX_STATIONS_PER_ROOM}");
    println!("guard_max_entities_per_player={GUARD_MAX_ENTITIES_PER_PLAYER}");
    println!("guard_max_entities_per_room={GUARD_MAX_ENTITIES_PER_ROOM}");
    println!("guard_max_component_bytes={GUARD_MAX_COMPONENT_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("rooms={}", inventory.instances);
    println!("instances={}", inventory.instances);
    println!("stations={}", inventory.stations);
    println!("players={}", inventory.players);
    println!("entities={}", inventory.entities);
    print_retained_capacity(retained);
    println!("min_players_per_room={}", config.min_players);
    println!("max_players_per_room={}", config.max_players);
    println!("players_per_station={}", config.players_per_station);
    println!("entities_per_player={}", config.entities_per_player);
    println!("entities_per_room={}", config.entities_per_room);
    println!(
        "entity_count_mode={}",
        if config.entities_per_room == 0 {
            "per-player"
        } else {
            "per-room"
        }
    );
    println!("component_bytes={}", config.component_bytes);
    println!("dirty_percent={}", config.dirty_percent);
    println!("dirty_distribution=per-room-scaled");
    print_component_update_config(config);
    print_frame_capacity_config(config);
    print_movement_config(config);
    println!("preallocate={}", config.preallocate);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("room_updates={}", stats.room_updates);
    println!("viewer_queries={}", stats.viewer_queries);
    println!("selected_entities={}", stats.selected_entities);
    println!("encoded_frames={}", stats.encoded_frames);
    println!("frames_skipped_empty={}", stats.frames_skipped_empty);
    println!("encoded_entities={}", stats.encoded_entities);
    println!("encoded_components={}", stats.encoded_components);
    println!("encoded_bytes={}", stats.encoded_bytes);
    println!("batch_plan_slots_max={}", stats.batch_plan_slots_max);
    println!(
        "batch_entity_capacity_max={}",
        stats.batch_entity_capacity_max
    );
    print_movement_stats(&stats);
    print_component_update_stats(&stats);
    print_frame_capacity_stats(&stats);
    println!("setup_ms={setup_ms:.3}");
    print_latency_stats(&stats, sweep_p99);
    println!("threshold_sweep_ms_p99={:.3}", config.sweep_p99_budget_ms);
    println!(
        "threshold_sweep_ok={}",
        sweep_p99 <= config.sweep_p99_budget_ms
    );
    print_verdicts(verdicts);
    println!("time_budget_ms={}", config.time_budget_ms);
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1_000.0);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn print_latency_stats(stats: &Stats, sweep_p99: f64) {
    println!("sweep_ms_p50={:.3}", percentile_ms(&stats.sweep_ms, 0.50));
    println!("sweep_ms_p95={:.3}", percentile_ms(&stats.sweep_ms, 0.95));
    println!("sweep_ms_p99={sweep_p99:.3}");
    println!("sweep_ms_max={:.3}", percentile_ms(&stats.sweep_ms, 1.00));
    println!(
        "planning_ms_p99={:.3}",
        percentile_ms(&stats.planning_ms, 0.99)
    );
    println!(
        "encoding_ms_p99={:.3}",
        percentile_ms(&stats.encoding_ms, 0.99)
    );
    println!(
        "movement_ms_p99={:.3}",
        percentile_ms(&stats.movement_ms, 0.99)
    );
    println!(
        "component_update_ms_p99={:.3}",
        percentile_ms(&stats.component_update_ms, 0.99)
    );
}

fn print_component_update_config(config: Config) {
    println!(
        "component_update_percent={}",
        config.component_update_percent
    );
    println!(
        "component_update_in_place={}",
        config.component_update_mode == ComponentUpdateMode::InPlace
    );
}

fn print_frame_capacity_config(config: Config) {
    println!(
        "frame_capacity_hint_enabled={}",
        config.frame_capacity_mode == FrameCapacityMode::Hint
    );
}

fn print_movement_config(config: Config) {
    println!("moving_percent={}", config.moving_percent);
    println!(
        "movement_pattern={}",
        match config.movement_pattern {
            MovementPattern::SameCell => "same-cell",
            MovementPattern::CrossCell => "cross-cell",
        }
    );
    print_index_update_mode(config.index_update_mode);
}

fn print_index_update_mode(mode: IndexUpdateMode) {
    let optimized = mode == IndexUpdateMode::SameCellFastPath;
    println!("same_cell_fast_path={optimized}");
    println!("point_relocation_in_place={optimized}");
}

fn retained_capacity_sufficient(inventory: Inventory, retained: RetainedCapacity) -> bool {
    retained.station_entities >= inventory.entities
        && retained.station_ids >= inventory.entities
        && retained.index_entities >= inventory.entities
        && retained.index_cells >= retained.occupied_cells
        && retained.component_entities >= inventory.entities
}

fn movement_updates_succeeded(config: Config, stats: &Stats) -> bool {
    if config.moving_percent == 0 {
        stats.index_updates == 0
    } else {
        let expected_updates = match (config.index_update_mode, config.movement_pattern) {
            (IndexUpdateMode::ForceReinsert, _) => stats.index_updates_inserted,
            (IndexUpdateMode::SameCellFastPath, MovementPattern::SameCell) => {
                stats.index_updates_unchanged
            }
            (IndexUpdateMode::SameCellFastPath, MovementPattern::CrossCell) => {
                stats.index_updates_relocated
            }
        };
        stats.index_updates > 0
            && expected_updates == stats.index_updates
            && stats.index_updates_inserted
                + stats.index_updates_unchanged
                + stats.index_updates_relocated
                == stats.index_updates
    }
}

fn component_updates_succeeded(config: Config, stats: &Stats) -> bool {
    if config.component_update_percent == 0 {
        stats.component_updates == 0
    } else {
        let expected_updates = match config.component_update_mode {
            ComponentUpdateMode::InPlace => stats.component_updates_in_place,
            ComponentUpdateMode::ForceReplace => stats.component_updates_replaced,
        };
        stats.component_updates > 0
            && expected_updates == stats.component_updates
            && stats.component_updates_in_place + stats.component_updates_replaced
                == stats.component_updates
    }
}

fn frame_capacity_succeeded(config: Config, stats: &Stats) -> bool {
    if stats.viewer_queries == 0 {
        return stats.frame_capacity_hint_bytes == 0 && stats.frame_capacity_bytes == 0;
    }
    match config.frame_capacity_mode {
        FrameCapacityMode::Hint => stats.frame_capacity_hint_bytes <= stats.frame_capacity_bytes,
        FrameCapacityMode::Growth => stats.frame_capacity_hint_bytes == 0,
    }
}

fn print_movement_stats(stats: &Stats) {
    println!("index_updates={}", stats.index_updates);
    println!("index_updates_inserted={}", stats.index_updates_inserted);
    println!("index_updates_unchanged={}", stats.index_updates_unchanged);
    println!("index_updates_relocated={}", stats.index_updates_relocated);
}

fn print_component_update_stats(stats: &Stats) {
    println!("component_updates={}", stats.component_updates);
    println!(
        "component_updates_in_place={}",
        stats.component_updates_in_place
    );
    println!(
        "component_updates_replaced={}",
        stats.component_updates_replaced
    );
}

fn print_frame_capacity_stats(stats: &Stats) {
    println!(
        "frame_capacity_hint_bytes={}",
        stats.frame_capacity_hint_bytes
    );
    println!("frame_capacity_bytes={}", stats.frame_capacity_bytes);
    println!(
        "frame_capacity_slack_bytes={}",
        stats.frame_capacity_slack_bytes
    );
}

fn print_verdicts(verdicts: Verdicts) {
    println!(
        "threshold_workload_completed_ok={}",
        verdicts.workload.passed()
    );
    println!(
        "threshold_retained_capacity_ok={}",
        verdicts.retained_capacity.passed()
    );
    println!(
        "threshold_movement_updates_ok={}",
        verdicts.movement.passed()
    );
    println!(
        "threshold_same_cell_movement_ok={}",
        verdicts.movement.passed()
    );
    println!(
        "threshold_component_updates_ok={}",
        verdicts.component_updates.passed()
    );
    println!(
        "threshold_frame_capacity_ok={}",
        verdicts.frame_capacity.passed()
    );
}

fn print_retained_capacity(retained: RetainedCapacity) {
    println!("station_entity_capacity={}", retained.station_entities);
    println!("station_id_capacity={}", retained.station_ids);
    println!(
        "station_free_handle_capacity={}",
        retained.station_free_handles
    );
    println!("index_entity_capacity={}", retained.index_entities);
    println!("index_cell_capacity={}", retained.index_cells);
    println!("occupied_cells={}", retained.occupied_cells);
    println!("component_entity_capacity={}", retained.component_entities);
    println!(
        "component_column_slots_capacity={}",
        retained.component_column_slots
    );
}

fn workload_completed(config: Config, inventory: Inventory, stats: &Stats) -> bool {
    let expected_room_updates = inventory
        .instances
        .checked_mul(config.ticks)
        .expect("guarded room update count fits usize");
    let expected_viewer_queries = inventory
        .players
        .checked_mul(config.ticks)
        .expect("guarded viewer query count fits usize");
    stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && stats.room_updates == expected_room_updates
        && stats.viewer_queries == expected_viewer_queries
        && stats
            .encoded_frames
            .saturating_add(stats.frames_skipped_empty)
            == expected_viewer_queries
        && stats.selected_entities > 0
        && stats.encoded_entities <= stats.selected_entities
        && stats.encoded_components == stats.encoded_entities
        && (config.dirty_percent < 100 || stats.encoded_entities == stats.selected_entities)
        && (config.dirty_percent > 0 || stats.encoded_entities == 0)
}

fn create_rooms(config: Config) -> Vec<Room> {
    let mut next_station_id = 1_u32;
    let mut next_client_id = 1_u64;
    let mut next_entity_id = 1_u64;
    let component_descriptor = benchmark_component_descriptor(config.component_bytes);
    (0..config.rooms)
        .map(|room_index| {
            let instance_id = InstanceId::new(
                u64::try_from(room_index)
                    .expect("guarded room index fits u64")
                    .saturating_add(1),
            );
            let players = config.players_for_room(room_index);
            let station_count = config.stations_for_players(players);
            let entities = config.entities_for_players(players);
            let mut stations = (0..station_count)
                .map(|station_index| {
                    let station_id = StationId::new(next_station_id);
                    next_station_id = next_station_id.saturating_add(1);
                    let entity_capacity = entities / station_count
                        + usize::from(station_index < entities % station_count);
                    create_station_work(
                        station_id,
                        instance_id,
                        entity_capacity,
                        config.preallocate,
                        config.moving_percent,
                        config.component_update_percent,
                        config.component_bytes,
                    )
                })
                .collect::<Vec<_>>();

            for player_index in 0..players {
                let station_index = player_index % station_count;
                stations[station_index].viewers.push(ViewerQuery {
                    client_id: ClientId::new(next_client_id),
                    position: benchmark_position(player_index),
                    radius: 96.0,
                    max_entities: 128,
                });
                next_client_id = next_client_id.saturating_add(1);
            }
            for entity_index in 0..entities {
                let station_index = entity_index % station_count;
                let position = benchmark_position(entity_index);
                let handle = stations[station_index]
                    .station
                    .spawn_owned(
                        EntityId::new(next_entity_id),
                        position,
                        Bounds::Point,
                        PolicyId::new(1),
                    )
                    .expect("benchmark entity ids are unique");
                stations[station_index]
                    .index
                    .upsert(handle, position, Bounds::Point);
                let movement_bucket = entity_index.saturating_mul(100) / entities.max(1);
                if movement_bucket < config.moving_percent {
                    stations[station_index]
                        .moving_entities
                        .push((handle, position));
                }
                let component_update_bucket = entity_index.saturating_mul(100) / entities.max(1);
                if component_update_bucket < config.component_update_percent {
                    stations[station_index].updating_components.push(handle);
                }
                stations[station_index]
                    .components
                    .set_blob(
                        &component_descriptor,
                        handle,
                        1,
                        vec![
                            u8::try_from(entity_index % 251).expect("value fits u8");
                            config.component_bytes
                        ],
                    )
                    .expect("guarded benchmark component should fit");
                let dirty_bucket = entity_index.saturating_mul(100) / entities.max(1);
                if dirty_bucket >= config.dirty_percent {
                    stations[station_index]
                        .components
                        .clear_dirty_for_entity(handle);
                }
                next_entity_id = next_entity_id.saturating_add(1);
            }

            Room {
                stations,
                players,
                entities,
            }
        })
        .collect()
}

fn create_station_work(
    station_id: StationId,
    instance_id: InstanceId,
    entity_capacity: usize,
    preallocate: bool,
    moving_percent: usize,
    component_update_percent: usize,
    component_bytes: usize,
) -> StationWork {
    let grid = GridSpec::new(32.0).expect("valid benchmark grid");
    let mut components = ComponentStore::default();
    if preallocate {
        components.reserve_component(ComponentId::new(1), entity_capacity);
    }
    let station_config = StationConfig {
        station_id,
        node_id: NodeId::new(1),
        instance_id,
        tick_rate_hz: 30,
    };
    StationWork {
        station: if preallocate {
            Station::with_capacity(station_config, entity_capacity)
        } else {
            Station::new(station_config)
        },
        index: if preallocate {
            CellIndex::with_capacity(grid, entity_capacity, entity_capacity.min(36))
        } else {
            CellIndex::new(grid)
        },
        components,
        viewers: Vec::new(),
        scratch: ReplicationScratch::default(),
        batch_scratch: ReplicationBatchScratch::new(),
        moving_entities: Vec::with_capacity(
            entity_capacity.saturating_mul(moving_percent).div_ceil(100),
        ),
        updating_components: Vec::with_capacity(
            entity_capacity
                .saturating_mul(component_update_percent)
                .div_ceil(100),
        ),
        component_payload: if component_update_percent == 0 {
            Vec::new()
        } else {
            vec![0; component_bytes]
        },
    }
}

fn benchmark_position(index: usize) -> Position3 {
    let x = u16::try_from(index.wrapping_mul(17) % 192).expect("coordinate fits u16");
    let z = u16::try_from(index.wrapping_mul(29) % 192).expect("coordinate fits u16");
    Position3::new(f32::from(x), 0.0, f32::from(z))
}

fn create_policies() -> PolicyTable {
    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 1, 30, 128.0));
    policies
}

fn benchmark_component_descriptor(component_bytes: usize) -> ComponentDescriptor {
    ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "benchmark",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        component_bytes,
    )
}

fn inventory(rooms: &[Room]) -> Inventory {
    Inventory {
        instances: rooms.len(),
        stations: rooms.iter().map(|room| room.stations.len()).sum(),
        players: rooms.iter().map(|room| room.players).sum(),
        entities: rooms.iter().map(|room| room.entities).sum(),
    }
}

fn retained_capacity(rooms: &[Room]) -> RetainedCapacity {
    let mut retained = RetainedCapacity::default();
    for work in rooms.iter().flat_map(|room| &room.stations) {
        retained.station_entities = retained
            .station_entities
            .saturating_add(work.station.entity_capacity());
        retained.station_ids = retained
            .station_ids
            .saturating_add(work.station.id_index_capacity());
        retained.station_free_handles = retained
            .station_free_handles
            .saturating_add(work.station.free_list_capacity());
        retained.index_entities = retained
            .index_entities
            .saturating_add(work.index.entity_capacity());
        retained.index_cells = retained
            .index_cells
            .saturating_add(work.index.occupied_cell_capacity());
        retained.occupied_cells = retained
            .occupied_cells
            .saturating_add(work.index.occupied_cell_count());
        retained.component_entities = retained
            .component_entities
            .saturating_add(work.components.component_capacity(ComponentId::new(1)));
        retained.component_column_slots = retained
            .component_column_slots
            .saturating_add(work.components.column_slots_capacity());
    }
    retained
}

fn run(rooms: &mut [Room], config: Config) -> Stats {
    let policies = create_policies();
    let component_descriptor = benchmark_component_descriptor(config.component_bytes);
    let replication = create_replication_workload(config.component_bytes);
    let started = Instant::now();
    let time_budget = Duration::from_millis(config.time_budget_ms);
    let mut stats = Stats::default();

    for _ in 0..config.ticks {
        if started.elapsed() >= time_budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let sweep_start = Instant::now();
        let mut movement_elapsed = Duration::ZERO;
        let mut component_update_elapsed = Duration::ZERO;
        let mut planning_elapsed = Duration::ZERO;
        let mut encoding_elapsed = Duration::ZERO;
        for room in &mut *rooms {
            for work in &mut room.stations {
                work.station.advance_tick();
                movement_elapsed += move_indexed_entities(work, &mut stats, config);
                if config.component_update_percent > 0 {
                    component_update_elapsed += update_components(
                        work,
                        &component_descriptor,
                        &mut stats,
                        config.component_update_mode,
                    );
                }
                let planning_start = Instant::now();
                let batch = ReplicationPlanner::plan_for_viewers_range_into(
                    &work.station,
                    &work.index,
                    &policies,
                    &work.viewers,
                    replication.budget,
                    &mut work.scratch,
                    &mut work.batch_scratch,
                );
                planning_elapsed = planning_elapsed.saturating_add(planning_start.elapsed());
                stats.viewer_queries = stats.viewer_queries.saturating_add(batch.stats.viewers);
                stats.selected_entities =
                    stats.selected_entities.saturating_add(batch.stats.selected);
                let encoding_start = Instant::now();
                for (viewer, plan) in work.viewers.iter().zip(batch.plans) {
                    encode_viewer(
                        &replication,
                        viewer,
                        plan,
                        &work.station,
                        &work.components,
                        config.frame_capacity_mode,
                        &mut stats,
                    );
                }
                encoding_elapsed = encoding_elapsed.saturating_add(encoding_start.elapsed());
                stats.batch_plan_slots_max = stats
                    .batch_plan_slots_max
                    .max(work.batch_scratch.retained_plan_slots());
                stats.batch_entity_capacity_max = stats
                    .batch_entity_capacity_max
                    .max(work.batch_scratch.retained_entity_capacity());
            }
            stats.room_updates = stats.room_updates.saturating_add(1);
        }
        push_tick_latencies(
            &mut stats,
            sweep_start.elapsed(),
            planning_elapsed,
            encoding_elapsed,
            movement_elapsed,
            component_update_elapsed,
        );
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
        stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    }
    stats
}

fn encode_viewer(
    replication: &ReplicationWorkload,
    viewer: &ViewerQuery,
    plan: &ReplicationPlan,
    station: &Station,
    components: &ComponentStore,
    capacity_mode: FrameCapacityMode,
    stats: &mut Stats,
) {
    let capacity_hint = match capacity_mode {
        FrameCapacityMode::Hint => replication.builder.sampled_binary_capacity_hint(
            station,
            plan,
            components,
            &replication.selection,
        ),
        FrameCapacityMode::Growth => 0,
    };
    let mut bytes = Vec::with_capacity(capacity_hint);
    stats.frame_capacity_hint_bytes = stats
        .frame_capacity_hint_bytes
        .saturating_add(capacity_hint);
    let build_stats = replication
        .builder
        .encode_binary_into(
            viewer.client_id,
            station.tick(),
            station,
            plan,
            components,
            &replication.selection,
            &mut bytes,
        )
        .expect("guarded benchmark frame should encode");
    stats.frame_capacity_bytes = stats.frame_capacity_bytes.saturating_add(bytes.capacity());
    stats.frame_capacity_slack_bytes = stats
        .frame_capacity_slack_bytes
        .saturating_add(bytes.capacity().saturating_sub(bytes.len()));
    stats.encoded_entities = stats
        .encoded_entities
        .saturating_add(build_stats.encoded_entities);
    stats.encoded_components = stats
        .encoded_components
        .saturating_add(build_stats.encoded_components);
    if build_stats.encoded_entities == 0 {
        stats.frames_skipped_empty = stats.frames_skipped_empty.saturating_add(1);
    } else {
        stats.encoded_frames = stats.encoded_frames.saturating_add(1);
        stats.encoded_bytes = stats.encoded_bytes.saturating_add(bytes.len());
    }
}

fn create_replication_workload(component_bytes: usize) -> ReplicationWorkload {
    ReplicationWorkload {
        selection: ComponentSelection {
            component_ids: vec![ComponentId::new(1)],
        },
        builder: ReplicationFrameBuilder::new(ReplicationFrameLimits {
            max_entity_deltas: 128,
            max_components_per_entity: 1,
            max_component_bytes: component_bytes,
        }),
        budget: ReplicationBudget {
            max_entities: 128,
            max_bytes: 128 * 32,
            estimated_entity_bytes: 32,
        },
    }
}

fn push_tick_latencies(
    stats: &mut Stats,
    sweep: Duration,
    planning: Duration,
    encoding: Duration,
    movement: Duration,
    component_update: Duration,
) {
    const MILLIS_PER_SECOND: f64 = 1_000.0;
    stats.sweep_ms.push(sweep.as_secs_f64() * MILLIS_PER_SECOND);
    stats
        .planning_ms
        .push(planning.as_secs_f64() * MILLIS_PER_SECOND);
    stats
        .encoding_ms
        .push(encoding.as_secs_f64() * MILLIS_PER_SECOND);
    stats
        .movement_ms
        .push(movement.as_secs_f64() * MILLIS_PER_SECOND);
    stats
        .component_update_ms
        .push(component_update.as_secs_f64() * MILLIS_PER_SECOND);
}

fn update_components(
    work: &mut StationWork,
    descriptor: &ComponentDescriptor,
    stats: &mut Stats,
    mode: ComponentUpdateMode,
) -> Duration {
    let started = Instant::now();
    if let Some(first) = work.component_payload.first_mut() {
        *first = work.station.tick().get().to_le_bytes()[0];
    }
    for &handle in &work.updating_components {
        let version = work.station.tick().get();
        match mode {
            ComponentUpdateMode::InPlace => work
                .components
                .set_blob_from_slice(descriptor, handle, version, &work.component_payload)
                .expect("guarded in-place component update should fit"),
            ComponentUpdateMode::ForceReplace => work
                .components
                .set_blob(descriptor, handle, version, work.component_payload.clone())
                .expect("guarded replacement component update should fit"),
        }
        stats.component_updates = stats.component_updates.saturating_add(1);
        match mode {
            ComponentUpdateMode::InPlace => {
                stats.component_updates_in_place =
                    stats.component_updates_in_place.saturating_add(1);
            }
            ComponentUpdateMode::ForceReplace => {
                stats.component_updates_replaced =
                    stats.component_updates_replaced.saturating_add(1);
            }
        }
    }
    started.elapsed()
}

fn move_indexed_entities(work: &mut StationWork, stats: &mut Stats, config: Config) -> Duration {
    let started = Instant::now();
    let movement_distance = match config.movement_pattern {
        MovementPattern::SameCell => 0.25,
        MovementPattern::CrossCell => work.index.grid().cell_size() + 0.25,
    };
    let offset = if work.station.tick().get().is_multiple_of(2) {
        0.0
    } else {
        movement_distance
    };
    for &(handle, base_position) in &work.moving_entities {
        let position = Position3::new(base_position.x + offset, base_position.y, base_position.z);
        work.station
            .move_owned(handle, position)
            .expect("benchmark moving entity should remain owned");
        stats.index_updates = stats.index_updates.saturating_add(1);
        let update = if config.index_update_mode == IndexUpdateMode::SameCellFastPath {
            work.index.upsert_tracked(handle, position, Bounds::Point)
        } else {
            assert!(work.index.remove(handle));
            work.index.upsert_tracked(handle, position, Bounds::Point)
        };
        match update {
            CellIndexUpdate::Unchanged => {
                stats.index_updates_unchanged = stats.index_updates_unchanged.saturating_add(1);
            }
            CellIndexUpdate::Relocated => {
                stats.index_updates_relocated = stats.index_updates_relocated.saturating_add(1);
            }
            CellIndexUpdate::Inserted => {
                stats.index_updates_inserted = stats.index_updates_inserted.saturating_add(1);
            }
        }
    }
    started.elapsed()
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
    fn station_count_scales_with_players_and_is_bounded() {
        let config = Config {
            players_per_station: 10,
            max_stations_per_room: 4,
            ..Config::default()
        };

        assert_eq!(config.stations_for_players(1), 1);
        assert_eq!(config.stations_for_players(10), 1);
        assert_eq!(config.stations_for_players(11), 2);
        assert_eq!(config.stations_for_players(100), 4);
    }

    #[test]
    fn preallocation_is_enabled_by_default_and_can_be_disabled_for_comparison() {
        assert!(Config::default().preallocate);
        let config = Config::from_args(["--no-preallocate".to_owned()].into_iter());
        assert!(!config.preallocate);
    }

    #[test]
    fn same_cell_fast_path_is_enabled_by_default_and_can_be_bypassed_for_comparison() {
        assert_eq!(
            Config::default().index_update_mode,
            IndexUpdateMode::SameCellFastPath
        );
        let config = Config::from_args(["--force-index-reinsert".to_owned()].into_iter());
        assert_eq!(config.index_update_mode, IndexUpdateMode::ForceReinsert);
    }

    #[test]
    fn cross_cell_movement_is_explicit() {
        assert_eq!(
            Config::default().movement_pattern,
            MovementPattern::SameCell
        );
        let config = Config::from_args(["--cross-cell-movement".to_owned()].into_iter());
        assert_eq!(config.movement_pattern, MovementPattern::CrossCell);
    }

    #[test]
    fn component_updates_are_in_place_by_default_and_replace_only_for_comparison() {
        assert_eq!(
            Config::default().component_update_mode,
            ComponentUpdateMode::InPlace
        );
        let config = Config::from_args(["--force-component-replace".to_owned()].into_iter());
        assert_eq!(
            config.component_update_mode,
            ComponentUpdateMode::ForceReplace
        );
    }

    #[test]
    fn frame_capacity_hint_is_enabled_by_default_and_can_be_disabled_for_comparison() {
        assert_eq!(
            Config::default().frame_capacity_mode,
            FrameCapacityMode::Hint
        );
        let config = Config::from_args(["--no-frame-capacity-hint".to_owned()].into_iter());
        assert_eq!(config.frame_capacity_mode, FrameCapacityMode::Growth);
    }

    #[test]
    fn frame_capacity_comparison_records_growth_without_hints() {
        let config = Config {
            rooms: 1,
            min_players: 1,
            max_players: 1,
            players_per_station: 1,
            entities_per_player: 2,
            frame_capacity_mode: FrameCapacityMode::Growth,
            ticks: 1,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let stats = run(&mut rooms, config);

        assert_eq!(stats.frame_capacity_hint_bytes, 0);
        assert!(stats.frame_capacity_bytes > 0);
        assert!(frame_capacity_succeeded(config, &stats));
    }

    #[test]
    fn component_update_workload_records_in_place_updates() {
        let config = Config {
            rooms: 1,
            min_players: 1,
            max_players: 1,
            players_per_station: 1,
            entities_per_player: 2,
            component_update_percent: 100,
            ticks: 2,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let stats = run(&mut rooms, config);

        assert_eq!(stats.component_updates, 4);
        assert_eq!(stats.component_updates_in_place, 4);
        assert_eq!(stats.component_updates_replaced, 0);
        assert!(component_updates_succeeded(config, &stats));
    }

    #[test]
    fn component_update_comparison_records_replacements() {
        let config = Config {
            rooms: 1,
            min_players: 1,
            max_players: 1,
            players_per_station: 1,
            entities_per_player: 2,
            component_update_percent: 100,
            component_update_mode: ComponentUpdateMode::ForceReplace,
            ticks: 1,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let stats = run(&mut rooms, config);

        assert_eq!(stats.component_updates, 2);
        assert_eq!(stats.component_updates_in_place, 0);
        assert_eq!(stats.component_updates_replaced, 2);
        assert!(component_updates_succeeded(config, &stats));
    }

    #[test]
    fn forced_reinsert_comparison_records_inserted_updates() {
        let config = Config {
            rooms: 1,
            min_players: 1,
            max_players: 1,
            players_per_station: 1,
            entities_per_player: 2,
            moving_percent: 100,
            index_update_mode: IndexUpdateMode::ForceReinsert,
            ticks: 1,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let stats = run(&mut rooms, config);

        assert_eq!(stats.index_updates, 2);
        assert_eq!(stats.index_updates_inserted, 2);
        assert_eq!(stats.index_updates_unchanged, 0);
        assert_eq!(stats.index_updates_relocated, 0);
        assert!(stats.frame_capacity_hint_bytes > 0);
        assert!(frame_capacity_succeeded(config, &stats));
        assert!(movement_updates_succeeded(config, &stats));
    }

    #[test]
    fn cross_cell_movement_records_relocated_updates() {
        let config = Config {
            rooms: 1,
            min_players: 1,
            max_players: 1,
            players_per_station: 1,
            entities_per_player: 2,
            moving_percent: 100,
            movement_pattern: MovementPattern::CrossCell,
            ticks: 2,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let stats = run(&mut rooms, config);

        assert_eq!(stats.index_updates, 4);
        assert_eq!(stats.index_updates_inserted, 0);
        assert_eq!(stats.index_updates_unchanged, 0);
        assert_eq!(stats.index_updates_relocated, 4);
        assert!(movement_updates_succeeded(config, &stats));
    }

    #[test]
    fn default_guard_clamps_oversized_manual_workload() {
        let config = Config::from_args(
            [
                "--rooms=100000",
                "--max-players=1000",
                "--players-per-station=1",
                "--entities-per-player=1000",
                "--entities-per-room=1000",
                "--component-bytes=1000",
                "--dirty-percent=1000",
                "--component-update-percent=1000",
                "--moving-percent=1000",
                "--ticks=100",
            ]
            .into_iter()
            .map(str::to_owned),
        );

        assert_eq!(config.rooms, GUARD_MAX_ROOMS);
        assert_eq!(config.max_players, GUARD_MAX_PLAYERS);
        assert_eq!(config.players_per_station, GUARD_MIN_PLAYERS_PER_STATION);
        assert_eq!(config.entities_per_player, GUARD_MAX_ENTITIES_PER_PLAYER);
        assert_eq!(config.entities_per_room, GUARD_MAX_ENTITIES_PER_ROOM);
        assert_eq!(config.component_bytes, GUARD_MAX_COMPONENT_BYTES);
        assert_eq!(config.dirty_percent, 100);
        assert_eq!(config.component_update_percent, 100);
        assert_eq!(config.moving_percent, 100);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn per_room_entity_count_and_dirty_ratio_are_independent_from_players() {
        let config = Config {
            rooms: 2,
            min_players: 4,
            max_players: 4,
            players_per_station: 4,
            entities_per_room: 10,
            dirty_percent: 30,
            ticks: 1,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);
        let inventory = inventory(&rooms);
        let stats = run(&mut rooms, config);

        assert_eq!(inventory.players, 8);
        assert_eq!(inventory.entities, 20);
        assert!(stats.encoded_entities > 0);
        assert!(stats.encoded_entities < stats.selected_entities);
        assert_eq!(stats.encoded_components, stats.encoded_entities);
        assert!(workload_completed(config, inventory, &stats));
    }

    #[test]
    fn small_single_thread_run_keeps_rooms_isolated_and_encodes_all_selected_entities() {
        let config = Config {
            rooms: 3,
            min_players: 4,
            max_players: 6,
            players_per_station: 4,
            max_stations_per_room: 2,
            entities_per_player: 2,
            moving_percent: 100,
            ticks: 2,
            sweep_p99_budget_ms: f64::MAX,
            ..Config::default()
        };
        let mut rooms = create_rooms(config);

        assert_eq!(
            rooms[0].stations[0].station.config().instance_id,
            InstanceId::new(1)
        );
        assert_eq!(
            rooms[1].stations[0].station.config().instance_id,
            InstanceId::new(2)
        );
        assert_eq!(
            rooms[2].stations[0].station.config().instance_id,
            InstanceId::new(3)
        );
        assert!(rooms.iter().enumerate().all(|(room_index, room)| {
            let instance_id = InstanceId::new(
                u64::try_from(room_index)
                    .expect("test room index fits u64")
                    .saturating_add(1),
            );
            room.stations
                .iter()
                .all(|work| work.station.config().instance_id == instance_id)
        }));

        let stats = run(&mut rooms, config);
        let retained = retained_capacity(&rooms);

        assert_eq!(stats.ticks_completed, 2);
        assert_eq!(stats.room_updates, 6);
        assert_eq!(stats.viewer_queries, 30);
        assert!(stats.selected_entities > 0);
        assert_eq!(stats.encoded_frames, stats.viewer_queries);
        assert_eq!(stats.encoded_entities, stats.selected_entities);
        assert_eq!(stats.index_updates, 60);
        assert_eq!(stats.index_updates_inserted, 0);
        assert_eq!(stats.index_updates_unchanged, 60);
        assert_eq!(stats.index_updates_relocated, 0);
        assert!(retained.station_entities >= inventory(&rooms).entities);
        assert!(retained.index_entities >= inventory(&rooms).entities);
        assert!(retained.component_entities >= inventory(&rooms).entities);
        assert!(retained.index_cells >= retained.occupied_cells);
        assert!(!stats.time_budget_exhausted);
        assert!(workload_completed(config, inventory(&rooms), &stats));
    }

    #[test]
    fn incomplete_or_empty_workload_cannot_pass() {
        let config = Config {
            rooms: 2,
            min_players: 2,
            max_players: 2,
            ticks: 1,
            ..Config::default()
        };
        let inventory = Inventory {
            instances: 2,
            stations: 2,
            players: 4,
            entities: 8,
        };
        let mut stats = Stats {
            room_updates: 2,
            viewer_queries: 4,
            encoded_frames: 4,
            ticks_completed: 1,
            ..Stats::default()
        };

        assert!(!workload_completed(config, inventory, &stats));
        stats.selected_entities = 4;
        stats.encoded_entities = 4;
        stats.time_budget_exhausted = true;
        assert!(!workload_completed(config, inventory, &stats));
    }
}
