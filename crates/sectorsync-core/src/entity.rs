//! Entity metadata, authority roles, tags, and dirty state.

use crate::ids::{EntityHandle, EntityId, OwnerEpoch, PolicyId, StationId, Tick};
use crate::spatial::{Bounds, Position3};

/// Bitset of entity tags. Higher-level code can assign tag meanings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EntityTags(u64);

impl EntityTags {
    /// Empty tag set.
    pub const EMPTY: Self = Self(0);

    /// Creates tags from raw bits.
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns raw tag bits.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns whether all bits in `mask` are present.
    pub const fn contains(self, mask: Self) -> bool {
        (self.0 & mask.0) == mask.0
    }

    /// Returns whether any bit in `mask` is present.
    pub const fn intersects(self, mask: Self) -> bool {
        (self.0 & mask.0) != 0
    }

    /// Adds all tags in `mask`.
    pub fn insert(&mut self, mask: Self) {
        self.0 |= mask.0;
    }

    /// Removes all tags in `mask`.
    pub fn remove(&mut self, mask: Self) {
        self.0 &= !mask.0;
    }
}

/// Component-level dirty bitset used by replication planning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DirtyMask(u64);

impl DirtyMask {
    /// No dirty components.
    pub const NONE: Self = Self(0);
    /// Transform or bounds changed.
    pub const TRANSFORM: Self = Self(1 << 0);
    /// Replication policy changed.
    pub const POLICY: Self = Self(1 << 1);
    /// Entity tags changed.
    pub const TAGS: Self = Self(1 << 2);
    /// Custom component changed.
    pub const CUSTOM: Self = Self(1 << 63);

    /// Returns raw dirty bits.
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns whether all bits in `mask` are present.
    pub const fn contains(self, mask: Self) -> bool {
        (self.0 & mask.0) == mask.0
    }

    /// Marks components dirty.
    pub fn insert(&mut self, mask: Self) {
        self.0 |= mask.0;
    }

    /// Clears dirty bits present in `mask`.
    pub fn remove(&mut self, mask: Self) {
        self.0 &= !mask.0;
    }

    /// Clears all dirty bits.
    pub fn clear(&mut self) {
        self.0 = 0;
    }
}

/// Authority role for an entity copy stored in a station.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityRole {
    /// This station is the authoritative owner.
    Owned {
        /// Owner epoch for stale message detection.
        owner_epoch: OwnerEpoch,
    },
    /// This station has a read-only ghost copy from an owner station.
    Ghost {
        /// Authoritative owner station.
        owner_station: StationId,
        /// Owner epoch for stale message detection.
        owner_epoch: OwnerEpoch,
        /// Tick after which this ghost can be discarded.
        expires_at: Tick,
    },
}

impl EntityRole {
    /// Returns whether this copy is authoritative.
    pub const fn is_owned(self) -> bool {
        matches!(self, Self::Owned { .. })
    }

    /// Returns the owner epoch for ordering handoffs and stale messages.
    pub const fn owner_epoch(self) -> OwnerEpoch {
        match self {
            Self::Owned { owner_epoch }
            | Self::Ghost {
                owner_epoch,
                owner_station: _,
                expires_at: _,
            } => owner_epoch,
        }
    }
}

/// Station-local entity record.
#[derive(Clone, Debug, PartialEq)]
pub struct EntityRecord {
    /// Stable entity identifier.
    pub id: EntityId,
    /// Station-local dense handle.
    pub handle: EntityHandle,
    /// Current position.
    pub position: Position3,
    /// Entity bounds.
    pub bounds: Bounds,
    /// Compiled sync policy id.
    pub policy_id: PolicyId,
    /// User-defined tags.
    pub tags: EntityTags,
    /// Owner or ghost role.
    pub role: EntityRole,
    /// Dirty component mask.
    pub dirty: DirtyMask,
}

impl EntityRecord {
    /// Creates an authoritative entity record.
    pub fn owned(
        id: EntityId,
        handle: EntityHandle,
        position: Position3,
        bounds: Bounds,
        policy_id: PolicyId,
        owner_epoch: OwnerEpoch,
    ) -> Self {
        Self {
            id,
            handle,
            position,
            bounds,
            policy_id,
            tags: EntityTags::EMPTY,
            role: EntityRole::Owned { owner_epoch },
            dirty: DirtyMask::TRANSFORM,
        }
    }

    /// Creates a read-only ghost entity record.
    pub fn ghost(
        id: EntityId,
        handle: EntityHandle,
        position: Position3,
        bounds: Bounds,
        policy_id: PolicyId,
        owner_station: StationId,
        owner_epoch: OwnerEpoch,
        expires_at: Tick,
    ) -> Self {
        Self {
            id,
            handle,
            position,
            bounds,
            policy_id,
            tags: EntityTags::EMPTY,
            role: EntityRole::Ghost {
                owner_station,
                owner_epoch,
                expires_at,
            },
            dirty: DirtyMask::TRANSFORM,
        }
    }

    /// Returns whether this record is authoritative in its station.
    pub const fn is_owned(&self) -> bool {
        self.role.is_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_tags_support_contains_intersects_and_remove() {
        let mut tags = EntityTags::from_bits(0b1011);

        assert!(tags.contains(EntityTags::from_bits(0b0011)));
        assert!(tags.intersects(EntityTags::from_bits(0b1000)));
        assert!(!tags.intersects(EntityTags::from_bits(0b0100_0000)));

        tags.remove(EntityTags::from_bits(0b0010));
        assert_eq!(tags.bits(), 0b1001);
    }
}
