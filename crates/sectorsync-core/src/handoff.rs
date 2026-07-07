//! Two-phase entity owner handoff primitives.

use crate::entity::EntityRecord;
use crate::ids::{EntityHandle, EntityId, OwnerEpoch, StationId, Tick};

/// In-memory transfer payload for a two-phase owner handoff.
#[derive(Clone, Debug, PartialEq)]
pub struct HandoffTransfer {
    /// Entity being transferred.
    pub entity_id: EntityId,
    /// Source owner station.
    pub source_station: StationId,
    /// Target owner station.
    pub target_station: StationId,
    /// Owner epoch observed at the source before transfer.
    pub source_owner_epoch: OwnerEpoch,
    /// Owner epoch that target will use after commit.
    pub target_owner_epoch: OwnerEpoch,
    /// Tick at which source prepared the transfer.
    pub prepared_at: Tick,
    /// Tick after which old source ghost can be discarded.
    pub source_ghost_expires_at: Tick,
    /// Authoritative entity state captured for transfer.
    pub entity: EntityRecord,
}

/// Handles produced by committing a two-phase handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandoffCommitHandles {
    /// Source station handle after it has been downgraded to a ghost.
    pub source_ghost: EntityHandle,
    /// Target station handle after it has become authoritative owner.
    pub target_owner: EntityHandle,
}
