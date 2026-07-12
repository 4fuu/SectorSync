//! Guarded A/B benchmark for allocation-light multi-Station event draining.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    EventId, EventKind, EventPriority, InstanceId, NodeId, Station, StationConfig, StationEvent,
    StationId, Tick,
};
use sectorsync_runtime::{EventRouter, StationScheduler, StationSet};

const DEFAULT_STATIONS: usize = 12;
const DEFAULT_EVENTS_PER_STATION: usize = 16;
const DEFAULT_CALLS_PER_TICK: usize = 20;
const DEFAULT_TICKS: usize = 20;
const GUARD_MAX_STATIONS: usize = 32;
const GUARD_MAX_EVENTS_PER_STATION: usize = 64;
const GUARD_MAX_CALLS_PER_TICK: usize = 50;
const GUARD_MAX_TICKS: usize = 20;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum OutputMode {
    #[default]
    Reuse,
    Fresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Config {
    stations: usize,
    events_per_station: usize,
    calls_per_tick: usize,
    ticks: usize,
    mode: OutputMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            stations: DEFAULT_STATIONS,
            events_per_station: DEFAULT_EVENTS_PER_STATION,
            calls_per_tick: DEFAULT_CALLS_PER_TICK,
            ticks: DEFAULT_TICKS,
            mode: OutputMode::Reuse,
            allow_heavy: false,
            guard_applied: false,
        }
    }
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            mode: if args.iter().any(|arg| arg == "--fresh-output") {
                OutputMode::Fresh
            } else {
                OutputMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            ..Self::default()
        };
        for arg in args {
            if let Some(value) = arg.strip_prefix("--stations=") {
                config.stations = value.parse().unwrap_or(config.stations);
            } else if let Some(value) = arg.strip_prefix("--events-per-station=") {
                config.events_per_station = value.parse().unwrap_or(config.events_per_station);
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
        self.stations = self.stations.max(1);
        self.events_per_station = self.events_per_station.max(2);
        self.calls_per_tick = self.calls_per_tick.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.stations = self.stations.min(GUARD_MAX_STATIONS);
            self.events_per_station = self.events_per_station.min(GUARD_MAX_EVENTS_PER_STATION);
            self.calls_per_tick = self.calls_per_tick.min(GUARD_MAX_CALLS_PER_TICK);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
        }
        self.guard_applied = self.stations != requested.stations
            || self.events_per_station != requested.events_per_station
            || self.calls_per_tick != requested.calls_per_tick
            || self.ticks != requested.ticks;
    }
}

#[derive(Debug, Default)]
struct RunStats {
    tick_ms: Vec<f64>,
    calls: usize,
    routed_events: usize,
    drained_events: usize,
    fresh_outputs: usize,
    retained_output_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_calls = config.calls_per_tick.saturating_mul(config.ticks);
    let expected_routed = expected_calls
        .saturating_mul(config.stations)
        .saturating_mul(config.events_per_station);
    let delayed_tail = config
        .stations
        .saturating_mul(config.events_per_station - config.events_per_station / 2);
    let expected_drained = expected_routed.saturating_sub(delayed_tail);
    let output_path_ok = match config.mode {
        OutputMode::Reuse => stats.fresh_outputs == 0 && stats.retained_output_capacity > 0,
        OutputMode::Fresh => stats.fresh_outputs == expected_calls,
    };
    let benchmark_ok = stats.calls == expected_calls
        && stats.routed_events == expected_routed
        && stats.drained_events == expected_drained
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted
        && output_path_ok;

    println!("SectorSync event drain output benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_stations={GUARD_MAX_STATIONS}");
    println!("guard_max_events_per_station={GUARD_MAX_EVENTS_PER_STATION}");
    println!("guard_max_calls_per_tick={GUARD_MAX_CALLS_PER_TICK}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("stations={}", config.stations);
    println!("events_per_station={}", config.events_per_station);
    println!("calls_per_tick={}", config.calls_per_tick);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == OutputMode::Reuse);
    println!("calls={}", stats.calls);
    println!("routed_events={}", stats.routed_events);
    println!("drained_events={}", stats.drained_events);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!(
        "retained_output_capacity={}",
        stats.retained_output_capacity
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={output_path_ok}");
    println!(
        "threshold_workload_completed_ok={}",
        stats.routed_events == expected_routed && stats.drained_events == expected_drained
    );
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> RunStats {
    let mut stations = StationSet::default();
    for index in 0..config.stations {
        stations.push(Station::new(StationConfig {
            station_id: StationId::new(u32::try_from(index).expect("guarded station fits u32")),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 30,
        }));
    }
    let mut router = EventRouter::default();
    router.register_stations(&stations);
    let mut scheduler = StationScheduler::default();
    let mut reusable = Vec::new();
    let mut next_event_id = 0_u64;
    let started = Instant::now();
    let time_budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = RunStats::default();

    'ticks: for _ in 0..config.ticks {
        let tick_started = Instant::now();
        for _ in 0..config.calls_per_tick {
            if started.elapsed() >= time_budget {
                stats.time_budget_exhausted = true;
                break 'ticks;
            }
            route_workload(
                &stations,
                &mut router,
                config.events_per_station,
                &mut next_event_id,
            );
            let drained = match config.mode {
                OutputMode::Reuse => {
                    scheduler
                        .drain_ready_events_into(&stations, &mut router, &mut reusable)
                        .expect("registered Station drain succeeds");
                    reusable.len()
                }
                OutputMode::Fresh => {
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    let mut fresh = Vec::new();
                    scheduler
                        .drain_ready_events_into(&stations, &mut router, &mut fresh)
                        .expect("registered Station drain succeeds");
                    fresh.len()
                }
            };
            stats.drained_events = stats.drained_events.saturating_add(drained);
            stats.calls = stats.calls.saturating_add(1);
            scheduler.advance_all(&mut stations);
            black_box(drained);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    stats.routed_events = router.stats().routed_events;
    stats.retained_output_capacity = reusable.capacity();
    stats.time_budget_exhausted |= started.elapsed() >= time_budget;
    stats
}

fn route_workload(
    stations: &StationSet,
    router: &mut EventRouter,
    events_per_station: usize,
    next_event_id: &mut u64,
) {
    for station in stations.iter() {
        let station_id = station.config().station_id;
        for event_index in 0..events_per_station {
            let id = *next_event_id;
            *next_event_id = next_event_id.saturating_add(1);
            let delayed = event_index >= events_per_station / 2;
            router
                .route(StationEvent {
                    id: EventId::new(id),
                    source: station_id,
                    target: station_id,
                    source_tick: station.tick(),
                    target_tick: Tick::new(station.tick().get().saturating_add(u64::from(delayed))),
                    priority: match event_index % 3 {
                        0 => EventPriority::Critical,
                        1 => EventPriority::Important,
                        _ => EventPriority::BestEffort,
                    },
                    kind: EventKind::Custom(
                        u32::try_from(id % u64::from(u32::MAX)).expect("reduced id fits u32"),
                    ),
                })
                .expect("guarded queue workload remains bounded");
        }
    }
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
    fn guard_clamps_all_event_dimensions() {
        let config = Config::from_args(
            [
                "--stations=9999",
                "--events-per-station=9999",
                "--calls-per-tick=9999",
                "--ticks=9999",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        assert_eq!(config.stations, GUARD_MAX_STATIONS);
        assert_eq!(config.events_per_station, GUARD_MAX_EVENTS_PER_STATION);
        assert_eq!(config.calls_per_tick, GUARD_MAX_CALLS_PER_TICK);
        assert_eq!(config.ticks, GUARD_MAX_TICKS);
        assert!(config.guard_applied);
    }

    #[test]
    fn output_modes_drain_identical_event_counts() {
        let config = Config {
            stations: 3,
            events_per_station: 5,
            calls_per_tick: 2,
            ticks: 2,
            ..Config::default()
        };
        let reused = run(config);
        let fresh = run(Config {
            mode: OutputMode::Fresh,
            ..config
        });
        assert_eq!(reused.routed_events, fresh.routed_events);
        assert_eq!(reused.drained_events, fresh.drained_events);
        assert_eq!(reused.calls, fresh.calls);
        assert_eq!(reused.fresh_outputs, 0);
        assert_eq!(fresh.fresh_outputs, fresh.calls);
        assert!(reused.retained_output_capacity > 0);
        let routed = config.stations * config.events_per_station * reused.calls;
        let delayed_tail =
            config.stations * (config.events_per_station - config.events_per_station / 2);
        assert_eq!(reused.routed_events, routed);
        assert_eq!(reused.drained_events, routed - delayed_tail);
    }
}
