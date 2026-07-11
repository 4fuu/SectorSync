//! Guarded multi-room capacity benchmark for core command and event queues.

use std::env;
use std::time::Instant;

use sectorsync_core::prelude::{
    ClientId, CommandEnvelope, CommandId, CommandIngress, CommandPriority, CommandQueueLimits,
    CommandQueues, EntityId, EventId, EventKind, EventPriority, EventQueueLimits, EventQueues,
    StationEvent, StationId, Tick,
};

const DEFAULT_ROOMS: usize = 100;
const DEFAULT_STATIONS_PER_ROOM: usize = 1;
const DEFAULT_BURST: usize = 8;
const GUARD_MAX_ROOMS: usize = 1_000;
const GUARD_MAX_STATIONS_PER_ROOM: usize = 4;
const GUARD_MAX_BURST: usize = 64;
const GUARD_MAX_ITEMS: usize = 500_000;
const TIME_BUDGET_MS: f64 = 10_000.0;

#[derive(Clone, Copy, Debug)]
struct Config {
    rooms: usize,
    stations_per_room: usize,
    burst: usize,
    allow_heavy: bool,
    guard_applied: bool,
}

fn main() {
    let config = parse_config();
    let command_limits = CommandQueueLimits::default();
    let event_limits = EventQueueLimits::default();
    let station_count = config.rooms.saturating_mul(config.stations_per_room);
    let expected_items = station_count.saturating_mul(config.burst).saturating_mul(2);
    let eager_per_station = command_limits
        .high
        .saturating_add(command_limits.normal)
        .saturating_add(command_limits.low)
        .saturating_add(event_limits.critical)
        .saturating_add(event_limits.important)
        .saturating_add(event_limits.best_effort);
    let eager_capacity = station_count.saturating_mul(eager_per_station);
    let started = Instant::now();
    let mut room_latencies = Vec::with_capacity(config.rooms);
    let mut retained_command_capacity = 0_usize;
    let mut retained_event_capacity = 0_usize;
    let mut queued_commands = 0_usize;
    let mut queued_events = 0_usize;
    let mut next_id = 0_u64;

    for _ in 0..config.rooms {
        let room_started = Instant::now();
        for _ in 0..config.stations_per_room {
            let mut commands = CommandQueues::new(command_limits);
            let mut events = EventQueues::new(event_limits);
            for _ in 0..config.burst {
                commands
                    .push(command(next_id), CommandIngress::RUNNING)
                    .expect("guarded command should queue");
                events
                    .push(event(next_id))
                    .expect("guarded event should queue");
                next_id = next_id.saturating_add(1);
            }
            retained_command_capacity =
                retained_command_capacity.saturating_add(commands.total_ready_retained_capacity());
            retained_event_capacity =
                retained_event_capacity.saturating_add(events.total_retained_capacity());
            queued_commands = queued_commands.saturating_add(commands.ready_len());
            queued_events = queued_events.saturating_add(events.len());
        }
        room_latencies.push(room_started.elapsed().as_secs_f64() * 1_000.0);
    }

    room_latencies.sort_by(f64::total_cmp);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let retained_capacity = retained_command_capacity.saturating_add(retained_event_capacity);
    let avoided_capacity = eager_capacity.saturating_sub(retained_capacity);
    let workload_ok = queued_commands.saturating_add(queued_events) == expected_items;
    let capacity_bounded_ok = retained_capacity <= eager_capacity;
    let lazy_allocation_ok = config.burst != 0 || retained_capacity == 0;
    let time_budget_ok = elapsed_ms <= TIME_BUDGET_MS;
    let benchmark_ok = workload_ok && capacity_bounded_ok && lazy_allocation_ok && time_budget_ok;

    println!("SectorSync core queue capacity benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_stations_per_room={GUARD_MAX_STATIONS_PER_ROOM}");
    println!("guard_max_burst={GUARD_MAX_BURST}");
    println!("guard_max_items={GUARD_MAX_ITEMS}");
    println!("rooms={}", config.rooms);
    println!("stations_per_room={}", config.stations_per_room);
    println!("station_count={station_count}");
    println!("burst={}", config.burst);
    println!("queued_commands={queued_commands}");
    println!("queued_events={queued_events}");
    println!("expected_items={expected_items}");
    println!("eager_capacity_per_station={eager_per_station}");
    println!("eager_queue_capacity={eager_capacity}");
    println!("retained_command_capacity={retained_command_capacity}");
    println!("retained_event_capacity={retained_event_capacity}");
    println!("retained_queue_capacity={retained_capacity}");
    println!("avoided_queue_capacity={avoided_capacity}");
    println!(
        "avoided_queue_capacity_percent={:.3}",
        percent(avoided_capacity, eager_capacity)
    );
    println!("room_ms_p50={:.3}", percentile(&room_latencies, 0.50));
    println!("room_ms_p95={:.3}", percentile(&room_latencies, 0.95));
    println!("room_ms_p99={:.3}", percentile(&room_latencies, 0.99));
    println!(
        "room_ms_max={:.3}",
        room_latencies.last().copied().unwrap_or_default()
    );
    println!("elapsed_ms={elapsed_ms:.3}");
    println!("time_budget_ms={TIME_BUDGET_MS:.3}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("threshold_capacity_bounded_ok={capacity_bounded_ok}");
    println!("threshold_lazy_allocation_ok={lazy_allocation_ok}");
    println!("threshold_time_budget_ok={time_budget_ok}");
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn command(id: u64) -> CommandEnvelope {
    CommandEnvelope {
        id: CommandId::new(id),
        client_id: ClientId::new(id % 10),
        entity_id: EntityId::new(id),
        sequence: id,
        received_at: Tick::new(0),
        kind: 1,
        priority: CommandPriority::Normal,
        payload: Vec::new(),
    }
}

fn event(id: u64) -> StationEvent {
    StationEvent {
        id: EventId::new(id),
        source: StationId::new(1),
        target: StationId::new(2),
        source_tick: Tick::new(0),
        target_tick: Tick::new(0),
        priority: EventPriority::Important,
        kind: EventKind::Custom(u32::try_from(id % 1_024).expect("bounded kind fits u32")),
    }
}

fn parse_config() -> Config {
    let mut rooms = DEFAULT_ROOMS;
    let mut stations_per_room = DEFAULT_STATIONS_PER_ROOM;
    let mut burst = DEFAULT_BURST;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--rooms=") {
            rooms = value.parse().unwrap_or(rooms);
        } else if let Some(value) = arg.strip_prefix("--stations-per-room=") {
            stations_per_room = value.parse().unwrap_or(stations_per_room);
        } else if let Some(value) = arg.strip_prefix("--burst=") {
            burst = value.parse().unwrap_or(burst);
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (rooms, stations_per_room, burst);
    rooms = rooms.max(1);
    stations_per_room = stations_per_room.max(1);
    if !allow_heavy {
        rooms = rooms.min(GUARD_MAX_ROOMS);
        stations_per_room = stations_per_room.min(GUARD_MAX_STATIONS_PER_ROOM);
        burst = burst.min(GUARD_MAX_BURST);
        let queues = rooms.saturating_mul(stations_per_room).saturating_mul(2);
        if queues.saturating_mul(burst) > GUARD_MAX_ITEMS {
            burst = GUARD_MAX_ITEMS / queues;
        }
    }
    Config {
        rooms,
        stations_per_room,
        burst,
        allow_heavy,
        guard_applied: requested != (rooms, stations_per_room, burst),
    }
}

#[allow(clippy::cast_precision_loss)]
fn percent(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 * 100.0 / total as f64
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
