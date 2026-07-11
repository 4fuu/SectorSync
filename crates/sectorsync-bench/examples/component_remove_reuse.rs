//! Guarded A/B benchmark for reusable component entity-removal output.

use std::env;
use std::hint::black_box;
use std::time::{Duration, Instant};

use sectorsync_core::prelude::{
    ComponentBlob, ComponentDescriptor, ComponentId, ComponentMigrationMode, ComponentStore,
    ComponentSyncMode, EntityHandle,
};

const DEFAULT_ENTITIES_PER_TICK: usize = 1_000;
const DEFAULT_COMPONENTS: usize = 8;
const DEFAULT_PAYLOAD_BYTES: usize = 32;
const DEFAULT_TICKS: usize = 10;
const GUARD_MAX_ENTITIES_PER_TICK: usize = 2_000;
const GUARD_MAX_COMPONENTS: usize = 32;
const GUARD_MAX_PAYLOAD_BYTES: usize = 4 * 1_024;
const GUARD_MAX_TICKS: usize = 20;
const GUARD_MAX_TOTAL_PAYLOAD_BYTES: usize = 64 * 1_024 * 1_024;
const TIME_BUDGET_MS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RemoveMode {
    #[default]
    Reuse,
    Fresh,
    Discard,
}

#[derive(Clone, Copy, Debug)]
struct Config {
    entities_per_tick: usize,
    components: usize,
    payload_bytes: usize,
    ticks: usize,
    mode: RemoveMode,
    allow_heavy: bool,
    guard_applied: bool,
}

impl Config {
    fn from_args(args: impl Iterator<Item = String>) -> Self {
        let args = args.collect::<Vec<_>>();
        let mut config = Self {
            entities_per_tick: DEFAULT_ENTITIES_PER_TICK,
            components: DEFAULT_COMPONENTS,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            ticks: DEFAULT_TICKS,
            mode: if args.iter().any(|arg| arg == "--fresh-output") {
                RemoveMode::Fresh
            } else if args.iter().any(|arg| arg == "--discard") {
                RemoveMode::Discard
            } else {
                RemoveMode::Reuse
            },
            allow_heavy: args.iter().any(|arg| arg == "--allow-heavy"),
            guard_applied: false,
        };
        for arg in args {
            parse_usize(&arg, "--entities-per-tick=", &mut config.entities_per_tick);
            parse_usize(&arg, "--components=", &mut config.components);
            parse_usize(&arg, "--payload-bytes=", &mut config.payload_bytes);
            parse_usize(&arg, "--ticks=", &mut config.ticks);
        }
        config.normalize();
        config
    }

    fn normalize(&mut self) {
        let requested = *self;
        self.entities_per_tick = self.entities_per_tick.max(1);
        self.components = self.components.max(1);
        self.payload_bytes = self.payload_bytes.max(1);
        self.ticks = self.ticks.max(1);
        if !self.allow_heavy {
            self.entities_per_tick = self.entities_per_tick.min(GUARD_MAX_ENTITIES_PER_TICK);
            self.components = self.components.min(GUARD_MAX_COMPONENTS);
            self.payload_bytes = self.payload_bytes.min(GUARD_MAX_PAYLOAD_BYTES);
            self.ticks = self.ticks.min(GUARD_MAX_TICKS);
            let payload_per_entity = self.components.saturating_mul(self.payload_bytes);
            let max_entities = GUARD_MAX_TOTAL_PAYLOAD_BYTES
                .checked_div(payload_per_entity)
                .unwrap_or(0)
                .max(1);
            self.entities_per_tick = self
                .entities_per_tick
                .min(max_entities.checked_div(self.ticks).unwrap_or(0).max(1));
        }
        self.guard_applied = self.entities_per_tick != requested.entities_per_tick
            || self.components != requested.components
            || self.payload_bytes != requested.payload_bytes
            || self.ticks != requested.ticks;
    }
}

fn parse_usize(arg: &str, prefix: &str, target: &mut usize) {
    if let Some(value) = arg.strip_prefix(prefix) {
        *target = value.parse().unwrap_or(*target);
    }
}

#[derive(Debug, Default)]
struct Stats {
    tick_ms: Vec<f64>,
    entities_removed: usize,
    blobs_removed: usize,
    payload_bytes: usize,
    checksum: u64,
    fresh_outputs: usize,
    retained_output_capacity: usize,
    ticks_completed: usize,
    time_budget_exhausted: bool,
}

fn main() {
    let config = Config::from_args(env::args().skip(1));
    let stats = run(config);
    let expected_entities = config.entities_per_tick.saturating_mul(config.ticks);
    let expected_blobs = expected_entities.saturating_mul(config.components);
    let expected_payload_bytes = expected_blobs.saturating_mul(config.payload_bytes);
    let path_ok = match config.mode {
        RemoveMode::Reuse => {
            stats.fresh_outputs == 0 && stats.retained_output_capacity >= config.components
        }
        RemoveMode::Fresh => stats.fresh_outputs == expected_entities,
        RemoveMode::Discard => {
            stats.fresh_outputs == 0
                && stats.retained_output_capacity == 0
                && stats.payload_bytes == 0
        }
    };
    let payload_ok = config.mode == RemoveMode::Discard
        || (stats.payload_bytes == expected_payload_bytes && stats.checksum > 0);
    let workload_ok = stats.entities_removed == expected_entities
        && stats.blobs_removed == expected_blobs
        && stats.ticks_completed == config.ticks
        && !stats.time_budget_exhausted;
    let benchmark_ok = path_ok && payload_ok && workload_ok;

    println!("SectorSync component entity removal benchmark");
    println!("allow_heavy={}", config.allow_heavy);
    println!("resource_guard_applied={}", config.guard_applied);
    println!("guard_max_entities_per_tick={GUARD_MAX_ENTITIES_PER_TICK}");
    println!("guard_max_components={GUARD_MAX_COMPONENTS}");
    println!("guard_max_payload_bytes={GUARD_MAX_PAYLOAD_BYTES}");
    println!("guard_max_ticks={GUARD_MAX_TICKS}");
    println!("guard_max_total_payload_bytes={GUARD_MAX_TOTAL_PAYLOAD_BYTES}");
    println!("entities_per_tick={}", config.entities_per_tick);
    println!("components_per_entity={}", config.components);
    println!("payload_bytes_per_component={}", config.payload_bytes);
    println!("ticks={}", config.ticks);
    println!("ticks_completed={}", stats.ticks_completed);
    println!("reusable_output={}", config.mode == RemoveMode::Reuse);
    println!("discard_output={}", config.mode == RemoveMode::Discard);
    println!("entities_removed={}", stats.entities_removed);
    println!("blobs_removed={}", stats.blobs_removed);
    println!("removed_payload_bytes={}", stats.payload_bytes);
    println!("removal_checksum={}", stats.checksum);
    println!("fresh_outputs={}", stats.fresh_outputs);
    println!(
        "retained_output_capacity={}",
        stats.retained_output_capacity
    );
    println!("tick_ms_p50={:.3}", percentile_ms(&stats.tick_ms, 0.50));
    println!("tick_ms_p95={:.3}", percentile_ms(&stats.tick_ms, 0.95));
    println!("tick_ms_p99={:.3}", percentile_ms(&stats.tick_ms, 0.99));
    println!("tick_ms_max={:.3}", percentile_ms(&stats.tick_ms, 1.00));
    println!("threshold_output_path_ok={path_ok}");
    println!("threshold_payload_consumed_ok={payload_ok}");
    println!("threshold_workload_completed_ok={workload_ok}");
    println!("time_budget_ms={TIME_BUDGET_MS}");
    println!("time_budget_exhausted={}", stats.time_budget_exhausted);
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}

fn run(config: Config) -> Stats {
    let total_entities = config.entities_per_tick.saturating_mul(config.ticks);
    let descriptors = descriptors(config);
    let mut store = ComponentStore::default();
    for descriptor in &descriptors {
        store.reserve_component(descriptor.id, total_entities);
    }
    let payload = vec![0x5a; config.payload_bytes];
    for entity_index in 0..total_entities {
        let entity = entity_handle(entity_index);
        for descriptor in &descriptors {
            store
                .set_blob(descriptor, entity, 1, payload.clone())
                .expect("guarded component blob should write");
        }
    }

    let mut removed = Vec::<(ComponentId, ComponentBlob)>::with_capacity(config.components);
    let started = Instant::now();
    let budget = Duration::from_millis(TIME_BUDGET_MS);
    let mut stats = Stats::default();
    for tick in 0..config.ticks {
        if started.elapsed() >= budget {
            stats.time_budget_exhausted = true;
            break;
        }
        let tick_started = Instant::now();
        let start = tick.saturating_mul(config.entities_per_tick);
        let end = start.saturating_add(config.entities_per_tick);
        for entity_index in start..end {
            let entity = entity_handle(entity_index);
            match config.mode {
                RemoveMode::Reuse => {
                    store.remove_entity_into(entity, &mut removed);
                    consume_removed(&removed, &mut stats);
                }
                RemoveMode::Fresh => {
                    let fresh = store.remove_entity(entity);
                    stats.fresh_outputs = stats.fresh_outputs.saturating_add(1);
                    consume_removed(&fresh, &mut stats);
                    black_box(fresh);
                }
                RemoveMode::Discard => {
                    stats.blobs_removed = stats
                        .blobs_removed
                        .saturating_add(store.clear_entity(entity));
                }
            }
            stats.entities_removed = stats.entities_removed.saturating_add(1);
        }
        stats
            .tick_ms
            .push(tick_started.elapsed().as_secs_f64() * 1_000.0);
        stats.ticks_completed = stats.ticks_completed.saturating_add(1);
    }
    if config.mode == RemoveMode::Reuse {
        stats.retained_output_capacity = removed.capacity();
    }
    stats.time_budget_exhausted |= started.elapsed() >= budget;
    assert_eq!(
        store.blob_count(),
        0,
        "completed removal should empty store"
    );
    stats
}

fn consume_removed(removed: &[(ComponentId, ComponentBlob)], stats: &mut Stats) {
    stats.blobs_removed = stats.blobs_removed.saturating_add(removed.len());
    for (component_id, blob) in removed {
        stats.payload_bytes = stats.payload_bytes.saturating_add(blob.bytes.len());
        stats.checksum = stats
            .checksum
            .wrapping_add(u64::from(component_id.get()))
            .wrapping_add(blob.version)
            .wrapping_add(u64::from(blob.bytes.first().copied().unwrap_or(0)))
            .wrapping_add(u64::from(blob.bytes.last().copied().unwrap_or(0)));
    }
}

fn descriptors(config: Config) -> Vec<ComponentDescriptor> {
    (0..config.components)
        .map(|index| {
            ComponentDescriptor::sparse_blob(
                ComponentId::new(u16::try_from(index).expect("guarded component id")),
                "benchmark-component",
                ComponentSyncMode::Delta,
                ComponentMigrationMode::Copy,
                config.payload_bytes,
            )
        })
        .collect()
}

fn entity_handle(index: usize) -> EntityHandle {
    EntityHandle::new(u32::try_from(index).expect("guarded entity index"), 0)
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn percentile_ms(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() - 1) as f64 * percentile).ceil() as usize;
    sorted[index]
}
