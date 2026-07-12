//! Minimal caller-driven maintenance flow using retained product-path storage.

use sectorsync::low_level::runtime::{EventRouter, StationIndexSet, StationSet};
use sectorsync::prelude::{LoadSampler, SplitExecutor, StationExecutor, Tick};

fn main() {
    let mut stations = StationSet::default();
    let indexes = StationIndexSet::default();
    let mut router = EventRouter::default();

    let mut sampler = LoadSampler::default();
    let load = sampler.sample(&stations, &indexes, &router, &[]);

    let mut splits = SplitExecutor::default();
    let schedule = splits.plan(load, Tick::new(1));
    let mut station_execution = StationExecutor::default();
    let advances = station_execution
        .advance_loaded(&mut stations, load)
        .stations_selected;
    let ready_events = station_execution
        .drain_ready_events(&stations, &mut router)
        .expect("empty event drain should work")
        .len();

    println!("samples={}", load.len());
    println!("split_actions={}", schedule.actions.len());
    println!("station_advances={advances}");
    println!("ready_events={ready_events}");
}
