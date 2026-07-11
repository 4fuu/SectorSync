//! Guarded capacity benchmark for multi-room in-memory transport queues.

use std::env;
use std::net::{Ipv6Addr, SocketAddr};
use std::time::Instant;

use sectorsync_core::prelude::{ClientId, StationId};
use sectorsync_transport::{
    ClientTransportLimits, InMemoryStationTransport, InMemoryTransportHub, OutboundPacket,
    StationOutboundPacket, StationTransportLimits, StationTransportSink, TransportSink,
};

const DEFAULT_ROOMS: usize = 100;
const DEFAULT_CLIENTS_PER_ROOM: usize = 10;
const DEFAULT_STATIONS_PER_ROOM: usize = 1;
const DEFAULT_QUEUE_LIMIT: usize = 4_096;
const DEFAULT_BURST: usize = 8;
const GUARD_MAX_ROOMS: usize = 1_000;
const GUARD_MAX_CLIENTS_PER_ROOM: usize = 10;
const GUARD_MAX_STATIONS_PER_ROOM: usize = 4;
const GUARD_MAX_QUEUE_LIMIT: usize = 16_384;
const GUARD_MAX_BURST: usize = 64;
const TIME_BUDGET_MS: f64 = 10_000.0;

#[derive(Clone, Copy, Debug)]
struct Config {
    rooms: usize,
    clients_per_room: usize,
    stations_per_room: usize,
    queue_limit: usize,
    burst: usize,
    allow_heavy: bool,
    guard_applied: bool,
}

#[allow(clippy::too_many_lines)]
fn main() {
    let config = parse_config();
    let started = Instant::now();
    let client_count = config.rooms.saturating_mul(config.clients_per_room);
    let station_count = config.rooms.saturating_mul(config.stations_per_room);
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: config.queue_limit,
        max_packet_bytes: 1,
    });
    hub.register_client(ClientId::new(0), address(0))
        .expect("source client should register");
    let mut source = hub.endpoint_for_registered(ClientId::new(0));
    let mut room_latencies = Vec::with_capacity(config.rooms);
    let mut next_client = 1_usize;
    for _ in 0..config.rooms {
        let room_started = Instant::now();
        for _ in 0..config.clients_per_room {
            let client_id = ClientId::new(u64::try_from(next_client).expect("guarded id fits u64"));
            hub.register_client(client_id, address(next_client))
                .expect("target client should register");
            for _ in 0..config.burst {
                source
                    .send(OutboundPacket {
                        client_id,
                        bytes: vec![1],
                    })
                    .expect("guarded client burst should send");
            }
            next_client = next_client.saturating_add(1);
        }
        room_latencies.push(room_started.elapsed().as_secs_f64() * 1_000.0);
    }

    let mut station_transport = InMemoryStationTransport::new(StationTransportLimits {
        max_queued_packets_per_station: config.queue_limit,
        max_packet_bytes: 1,
    });
    for station in 1..=station_count {
        let target = StationId::new(u32::try_from(station).expect("guarded station id fits u32"));
        station_transport.register_station(target);
        for _ in 0..config.burst {
            station_transport
                .send_station(StationOutboundPacket {
                    source_station: StationId::new(0),
                    target_station: target,
                    bytes: vec![1],
                })
                .expect("guarded station burst should send");
        }
    }

    room_latencies.sort_by(f64::total_cmp);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
    let client_capacity = hub
        .retained_queue_capacity()
        .expect("client capacity should read");
    let station_capacity = station_transport.retained_queue_capacity();
    let retained_capacity = client_capacity.saturating_add(station_capacity);
    let registered_queues = client_count.saturating_add(1).saturating_add(station_count);
    let eager_capacity = registered_queues.saturating_mul(config.queue_limit);
    let avoided_capacity = eager_capacity.saturating_sub(retained_capacity);
    let queued_packets = client_count
        .saturating_add(station_count)
        .saturating_mul(config.burst);
    let capacity_bounded_ok = retained_capacity <= eager_capacity;
    let workload_ok = hub
        .stats()
        .expect("client stats should read")
        .packets_sent
        .saturating_add(station_transport.stats().packets_sent)
        == queued_packets;
    let lazy_allocation_ok = config.burst != 0 || retained_capacity == 0;
    let time_budget_ok = elapsed_ms <= TIME_BUDGET_MS;
    let benchmark_ok = capacity_bounded_ok && workload_ok && lazy_allocation_ok && time_budget_ok;

    println!("SectorSync in-memory queue capacity benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_rooms={GUARD_MAX_ROOMS}");
    println!("guard_max_clients_per_room={GUARD_MAX_CLIENTS_PER_ROOM}");
    println!("guard_max_stations_per_room={GUARD_MAX_STATIONS_PER_ROOM}");
    println!("guard_max_queue_limit={GUARD_MAX_QUEUE_LIMIT}");
    println!("guard_max_burst={GUARD_MAX_BURST}");
    println!("rooms={}", config.rooms);
    println!("clients_per_room={}", config.clients_per_room);
    println!("stations_per_room={}", config.stations_per_room);
    println!("client_count={client_count}");
    println!("station_count={station_count}");
    println!("queue_limit={}", config.queue_limit);
    println!("burst={}", config.burst);
    println!("queued_packets={queued_packets}");
    println!("registered_queues={registered_queues}");
    println!("eager_queue_capacity={eager_capacity}");
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
    println!("threshold_capacity_bounded_ok={capacity_bounded_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("threshold_lazy_allocation_ok={lazy_allocation_ok}");
    println!("threshold_time_budget_ok={time_budget_ok}");
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn parse_config() -> Config {
    let mut rooms = DEFAULT_ROOMS;
    let mut clients_per_room = DEFAULT_CLIENTS_PER_ROOM;
    let mut stations_per_room = DEFAULT_STATIONS_PER_ROOM;
    let mut queue_limit = DEFAULT_QUEUE_LIMIT;
    let mut burst = DEFAULT_BURST;
    let mut allow_heavy = false;
    for arg in env::args().skip(1) {
        if let Some(value) = arg.strip_prefix("--rooms=") {
            rooms = value.parse().unwrap_or(rooms);
        } else if let Some(value) = arg.strip_prefix("--clients-per-room=") {
            clients_per_room = value.parse().unwrap_or(clients_per_room);
        } else if let Some(value) = arg.strip_prefix("--stations-per-room=") {
            stations_per_room = value.parse().unwrap_or(stations_per_room);
        } else if let Some(value) = arg.strip_prefix("--queue-limit=") {
            queue_limit = value.parse().unwrap_or(queue_limit);
        } else if let Some(value) = arg.strip_prefix("--burst=") {
            burst = value.parse().unwrap_or(burst);
        } else if arg == "--allow-heavy" {
            allow_heavy = true;
        }
    }
    let requested = (
        rooms,
        clients_per_room,
        stations_per_room,
        queue_limit,
        burst,
    );
    rooms = rooms.max(1);
    clients_per_room = clients_per_room.max(1);
    stations_per_room = stations_per_room.max(1);
    queue_limit = queue_limit.max(1);
    if !allow_heavy {
        rooms = rooms.min(GUARD_MAX_ROOMS);
        clients_per_room = clients_per_room.min(GUARD_MAX_CLIENTS_PER_ROOM);
        stations_per_room = stations_per_room.min(GUARD_MAX_STATIONS_PER_ROOM);
        queue_limit = queue_limit.min(GUARD_MAX_QUEUE_LIMIT);
        burst = burst.min(GUARD_MAX_BURST);
    }
    burst = burst.min(queue_limit);
    Config {
        rooms,
        clients_per_room,
        stations_per_room,
        queue_limit,
        burst,
        allow_heavy,
        guard_applied: requested
            != (
                rooms,
                clients_per_room,
                stations_per_room,
                queue_limit,
                burst,
            ),
    }
}

fn address(index: usize) -> SocketAddr {
    SocketAddr::from((Ipv6Addr::from(index as u128), 10_000))
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
