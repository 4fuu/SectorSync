//! Runtime barrier upgrade hook SDK example.

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, Bounds, CommandQueueMode, EntityId, InstanceId, NodeId,
    PolicyId, Position3, RuntimeUpgradeHook, SnapshotMeta, SnapshotVersion, Station, StationConfig,
    StationId, StationSnapshot, Tick,
};
use sectorsync_runtime::{BarrierController, BarrierUpgradeExecutor, StationSet};

fn main() {
    let mut first = station(1, 10);
    first
        .spawn_owned(
            EntityId::new(100),
            Position3::new(1.0, 2.0, 3.0),
            Bounds::Point,
            PolicyId::new(0),
        )
        .expect("spawn should work");
    let mut stations = StationSet::default();
    stations.push(first);
    stations.push(station(2, 10));
    for station in stations.iter_mut() {
        station.advance_tick();
        station.advance_tick();
    }

    let mut controller = BarrierController::default();
    controller
        .request(
            &stations,
            BarrierId::new(8),
            BarrierScope::Instance(InstanceId::new(10)),
            Tick::new(2),
            CommandQueueMode::Buffer,
        )
        .expect("barrier should request");
    assert_eq!(
        controller
            .poll(&stations)
            .expect("barrier should freeze")
            .state,
        BarrierState::Frozen
    );

    let mut hook = ShiftPositions { migrated: 0 };
    let report = BarrierUpgradeExecutor::migrate_frozen(
        &mut controller,
        &mut stations,
        SnapshotVersion {
            runtime_version: 2,
            ..SnapshotVersion::default()
        },
        &mut hook,
    )
    .expect("upgrade should migrate snapshots");
    let moved = stations
        .get(StationId::new(1))
        .expect("station should exist")
        .get_by_id(EntityId::new(100))
        .expect("entity should restore");
    assert_eq!(moved.position, Position3::new(11.0, 2.0, 3.0));

    let metrics = controller.resume().expect("barrier should resume");
    println!(
        "barrier_upgrade snapshots={} stations={} entities={} hook_migrations={} resumed_stations={}",
        report.snapshots_migrated,
        report.stations_restored,
        report.entities_restored,
        hook.migrated,
        metrics.station_count
    );
}

struct ShiftPositions {
    migrated: usize,
}

impl RuntimeUpgradeHook for ShiftPositions {
    fn pre_upgrade(&mut self, meta: &SnapshotMeta) {
        assert_eq!(meta.version.runtime_version, 2);
    }

    fn migrate_state(&mut self, mut snapshot: StationSnapshot) -> StationSnapshot {
        self.migrated += 1;
        for entity in &mut snapshot.entities {
            entity.position.x += 10.0;
        }
        snapshot
    }

    fn post_upgrade(&mut self, meta: &SnapshotMeta) {
        assert_eq!(meta.version.runtime_version, 2);
    }
}

fn station(station_id: u32, instance_id: u64) -> Station {
    Station::new(StationConfig {
        station_id: StationId::new(station_id),
        node_id: NodeId::new(0),
        instance_id: InstanceId::new(instance_id),
        tick_rate_hz: 20,
    })
}
