//! Minimal caller-driven maintenance flow using retained product-path storage.

use sectorsync::low_level::runtime::{EventRouter, StationIndexSet, StationSet};
use sectorsync::prelude::{LoadSampler, SplitExecutor, Tick};

fn main() {
    let stations = StationSet::default();
    let indexes = StationIndexSet::default();
    let router = EventRouter::default();

    let mut sampler = LoadSampler::default();
    let samples = sampler.sample(&stations, &indexes, &router, &[]);

    let mut splits = SplitExecutor::default();
    let schedule = splits.plan(samples, Tick::new(1));

    println!("samples={}", samples.len());
    println!("split_actions={}", schedule.actions.len());
}
