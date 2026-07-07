//! Snapshot metadata and station snapshot containers.

use crate::entity::EntityRecord;
use crate::ids::{InstanceId, OwnerEpoch, StationId, Tick};

/// Version metadata associated with a snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotVersion {
    /// Runtime version selected by the embedding application.
    pub runtime_version: u32,
    /// Entity/schema version selected by the embedding application.
    pub schema_version: u32,
    /// Ruleset version selected by the embedding application.
    pub ruleset_version: u32,
    /// Module version selected by the embedding application.
    pub module_version: u32,
}

impl Default for SnapshotVersion {
    fn default() -> Self {
        Self {
            runtime_version: 1,
            schema_version: 1,
            ruleset_version: 1,
            module_version: 1,
        }
    }
}

/// Snapshot metadata for a single station.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// World instance id.
    pub instance_id: InstanceId,
    /// Station id.
    pub station_id: StationId,
    /// Tick captured by the snapshot.
    pub tick: Tick,
    /// Entity count captured by the snapshot.
    pub entity_count: usize,
    /// Current station owner epoch.
    pub owner_epoch: OwnerEpoch,
    /// Version metadata.
    pub version: SnapshotVersion,
}

/// In-memory station snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct StationSnapshot {
    /// Snapshot metadata.
    pub meta: SnapshotMeta,
    /// Entity records captured by the snapshot.
    pub entities: Vec<EntityRecord>,
}

/// Hook interface for runtime version upgrades around a full barrier.
pub trait RuntimeUpgradeHook {
    /// Called before state migration while the runtime is frozen.
    fn pre_upgrade(&mut self, _meta: &SnapshotMeta) {}

    /// Called to migrate a station snapshot while frozen.
    fn migrate_state(&mut self, snapshot: StationSnapshot) -> StationSnapshot {
        snapshot
    }

    /// Called after migration and before resume.
    fn post_upgrade(&mut self, _meta: &SnapshotMeta) {}
}
