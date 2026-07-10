//! Guarded single-thread benchmark for many small room instances.

use std::env;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    Bounds, CellIndex, ClientId, CompiledSyncPolicy, ComponentId, EntityId, GridSpec, InstanceId,
    NodeId, PolicyId, PolicyTable, Position3, ReplicationBudget, ReplicationPlanner,
    ReplicationScratch, Station, StationConfig, StationId, ViewerQuery,
};
use sectorsync_wire::{
    BinaryFrameEncoder, ComponentDelta, EntityDelta, FrameEncoder, ReplicationFrame,
};

const DEFAULT_ROOMS: usize = 500;
const DEFAULT_MIN_PLAYERS: usize = 4;
const DEFAULT_MAX_PLAYERS: usize = 24;
const DEFAULT_PLAYERS_PER_STATION: usize = 12;
const DEFAULT_ENTITIES_PER_PLAYER: usize = 8;
const DEFAULT_TICKS: usize = 8;
const DEFAULT_TIME_BUDGET_MS: u64 = 10_000;
const DEFAULT_SWEEP_P99_BUDGET_MS: f64 = 50.0;

const GUARD_MAX_ROOMS: usize = 500;
const GUARD_MAX_PLAYERS: usize = 32;
const GUARD_MIN_PLAYERS_PER_STATION: usize = 4;
const GUARD_MAX_STATIONS_PER_ROOM: usize = 8;
const GUARD_MAX_ENTITIES_PER_PLAYER: usize = 16;
const GUARD_MAX_TICKS: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Config {
    rooms: usize,
    min_players: usize,
    max_players: usize,
    players_per_station: usize,
    max_stations_per_room: usize,
    entities_per_player: usize,
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
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = before.rooms != self.rooms
            || before.min_players != self.min_players
            || before.max_players != self.max_players
            || before.players_per_station != self.players_per_station
            || before.max_stations_per_room != self.max_stations_per_room
            || before.entities_per_player != self.entities_per_player
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
}

fn parse_usize(value: &str, fallback: usize) -> usize {
    value.parse().unwrap_or(fallback)
}

#[derive(Debug)]
struct StationWork {
    station: Station,
    index: CellIndex,
    viewers: Vec<ViewerQuery>,
    scratch: ReplicationScratch,
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
    room_updates: usize,
    viewer_queries: usize,
    selected_entities: usize,
    encoded_frames: usize,
    encoded_entities: usize,
    encoded_bytes: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Inventory {
    instances: usize,
    stations: usize,
    players: usize,
    entities: usize,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let setup_start = Instant::now();
    let mut rooms = create_rooms(config);
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1_000.0;
    let inventory = inventory(&rooms);
    let run_start = Instant::now();
    let stats = run(&mut rooms, config);
    let elapsed = run_start.elapsed();
    let sweep_p99 = percentile_ms(&stats.sweep_ms, 0.99);
    let workload_ok = workload_completed(config, inventory, &stats);
    let benchmark_ok = workload_ok && sweep_p99 <= config.sweep_p99_budget_ms;

    println!("SectorSync many-room single-thread benchmark");
    println!("single_thread=true");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_players={GUARD_MAX_PLAYERS}");
    println!("guard_max_stations_per_room={GUARD_MAX_STATIONS_PER_ROOM}");
    println!("guard_max_entities_per_player={GUARD_MAX_ENTITIES_PER_PLAYER}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("rooms={}", inventory.instances);
    println!("instances={}", inventory.instances);
    println!("stations={}", inventory.stations);
    println!("players={}", inventory.players);
    println!("entities={}", inventory.entities);
    println!("min_players_per_room={}", config.min_players);
    println!("max_players_per_room={}", config.max_players);
    println!("players_per_station={}", config.players_per_station);
    println!("entities_per_player={}", config.entities_per_player);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("room_updates={}", stats.room_updates);
    println!("viewer_queries={}", stats.viewer_queries);
    println!("selected_entities={}", stats.selected_entities);
    println!("encoded_frames={}", stats.encoded_frames);
    println!("encoded_entities={}", stats.encoded_entities);
    println!("encoded_bytes={}", stats.encoded_bytes);
    println!("setup_ms={setup_ms:.3}");
    println!("sweep_ms_p50={:.3}", percentile_ms(&stats.sweep_ms, 0.50));
    println!("sweep_ms_p95={:.3}", percentile_ms(&stats.sweep_ms, 0.95));
    println!("sweep_ms_p99={sweep_p99:.3}");
    println!("sweep_ms_max={:.3}", percentile_ms(&stats.sweep_ms, 1.00));
    println!("threshold_sweep_ms_p99={:.3}", config.sweep_p99_budget_ms);
    println!(
        "threshold_sweep_ok={}",
        sweep_p99 <= config.sweep_p99_budget_ms
    );
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("time_budget_ms={}", config.time_budget_ms);
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1_000.0);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
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
        && stats.encoded_frames == expected_viewer_queries
        && stats.selected_entities > 0
        && stats.encoded_entities == stats.selected_entities
}

fn create_rooms(config: Config) -> Vec<Room> {
    let mut next_station_id = 1_u32;
    let mut next_client_id = 1_u64;
    let mut next_entity_id = 1_u64;
    (0..config.rooms)
        .map(|room_index| {
            let instance_id = InstanceId::new(
                u64::try_from(room_index)
                    .expect("guarded room index fits u64")
                    .saturating_add(1),
            );
            let players = config.players_for_room(room_index);
            let station_count = config.stations_for_players(players);
            let entities = players.saturating_mul(config.entities_per_player);
            let mut stations = (0..station_count)
                .map(|_| {
                    let station_id = StationId::new(next_station_id);
                    next_station_id = next_station_id.saturating_add(1);
                    StationWork {
                        station: Station::new(StationConfig {
                            station_id,
                            node_id: NodeId::new(1),
                            instance_id,
                            tick_rate_hz: 30,
                        }),
                        index: CellIndex::new(GridSpec::new(32.0).expect("valid benchmark grid")),
                        viewers: Vec::new(),
                        scratch: ReplicationScratch::default(),
                    }
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

fn inventory(rooms: &[Room]) -> Inventory {
    Inventory {
        instances: rooms.len(),
        stations: rooms.iter().map(|room| room.stations.len()).sum(),
        players: rooms.iter().map(|room| room.players).sum(),
        entities: rooms.iter().map(|room| room.entities).sum(),
    }
}

fn run(rooms: &mut [Room], config: Config) -> Stats {
    let policies = create_policies();
    let budget = ReplicationBudget {
        max_entities: 128,
        max_bytes: 128 * 32,
        estimated_entity_bytes: 32,
    };
    let started = Instant::now();
    let time_budget = Duration::from_millis(config.time_budget_ms);
    let mut stats = Stats::default();

    for _ in 0..config.ticks {
        if started.elapsed() >= time_budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let sweep_start = Instant::now();
        for room in &mut *rooms {
            for work in &mut room.stations {
                work.station.advance_tick();
                let batch = ReplicationPlanner::plan_for_viewers_range_with_scratch(
                    &work.station,
                    &work.index,
                    &policies,
                    &work.viewers,
                    budget,
                    &mut work.scratch,
                );
                stats.viewer_queries = stats.viewer_queries.saturating_add(batch.stats.viewers);
                stats.selected_entities =
                    stats.selected_entities.saturating_add(batch.stats.selected);
                for (viewer, plan) in work.viewers.iter().zip(&batch.plans) {
                    let (bytes, entities) = encode_plan(&work.station, viewer.client_id, plan);
                    stats.encoded_frames = stats.encoded_frames.saturating_add(1);
                    stats.encoded_entities = stats.encoded_entities.saturating_add(entities);
                    stats.encoded_bytes = stats.encoded_bytes.saturating_add(bytes);
                }
            }
            stats.room_updates = stats.room_updates.saturating_add(1);
        }
        stats
            .sweep_ms
            .push(sweep_start.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
        stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    }
    stats
}

fn encode_plan(
    station: &Station,
    client_id: ClientId,
    plan: &sectorsync_core::prelude::ReplicationPlan,
) -> (usize, usize) {
    let tick = station.tick();
    let tick_bytes = u32::try_from(tick.get()).unwrap_or(u32::MAX).to_le_bytes();
    let entities = plan
        .entities
        .iter()
        .filter_map(|handle| station.get(*handle))
        .map(|entity| EntityDelta {
            entity_id: entity.id,
            owner_epoch: entity.role.owner_epoch(),
            components: vec![ComponentDelta {
                component_id: ComponentId::new(1),
                version: tick.get(),
                flags: 0,
                bytes: tick_bytes.to_vec(),
            }],
        })
        .collect::<Vec<_>>();
    let entity_count = entities.len();
    let frame = ReplicationFrame {
        client_id,
        server_tick: tick,
        entity_count: u32::try_from(entity_count).unwrap_or(u32::MAX),
        estimated_payload_bytes: u32::try_from(entity_count.saturating_mul(32)).unwrap_or(u32::MAX),
        entities,
    };
    let mut bytes = Vec::new();
    BinaryFrameEncoder
        .encode_replication(&frame, &mut bytes)
        .expect("binary encoder is infallible");
    (bytes.len(), entity_count)
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
    fn default_guard_clamps_oversized_manual_workload() {
        let config = Config::from_args(
            [
                "--rooms=100000",
                "--max-players=1000",
                "--players-per-station=1",
                "--entities-per-player=1000",
                "--ticks=100",
            ]
            .into_iter()
            .map(str::to_owned),
        );

        assert_eq!(config.rooms, GUARD_MAX_ROOMS);
        assert_eq!(config.max_players, GUARD_MAX_PLAYERS);
        assert_eq!(config.players_per_station, GUARD_MIN_PLAYERS_PER_STATION);
        assert_eq!(config.entities_per_player, GUARD_MAX_ENTITIES_PER_PLAYER);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
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

        assert_eq!(stats.ticks_completed, 2);
        assert_eq!(stats.room_updates, 6);
        assert_eq!(stats.viewer_queries, 30);
        assert!(stats.selected_entities > 0);
        assert_eq!(stats.encoded_frames, stats.viewer_queries);
        assert_eq!(stats.encoded_entities, stats.selected_entities);
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
