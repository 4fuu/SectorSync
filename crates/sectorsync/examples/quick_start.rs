//! Minimal coherent Station-local product path.

use sectorsync::prelude::{
    Bounds, EntityId, GridSpec, InstanceId, NodeId, PolicyId, Position3, SpawnEntity,
    StationConfig, StationId, StationRuntime, StationRuntimeConfig,
};

fn main() {
    let config = StationRuntimeConfig::new(
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        },
        GridSpec::new(32.0).expect("grid must be valid"),
    )
    .with_capacity(128, 64);
    let mut station = StationRuntime::new(config);
    let position = Position3::new(64.0, 0.0, 64.0);

    let handle = station
        .spawn_owned(SpawnEntity::new(
            EntityId::new(42),
            position,
            Bounds::Point,
            PolicyId::new(1),
        ))
        .expect("entity should spawn");

    assert_eq!(station.index().query_sphere(position, 128.0), vec![handle]);
    println!("station_runtime_ok=true");
    println!("entity_handle={}:{}", handle.index(), handle.generation());
}
