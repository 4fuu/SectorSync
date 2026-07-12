//! Common product-path imports for `SectorSync` embedders.

pub use sectorsync_core::{
    command::{CommandQueueLimits, CommandQueues},
    component::{ComponentDescriptor, ComponentStoreError},
    entity::{EntityRecord, EntityTags},
    handoff::HandoffTransfer,
    ids::{
        ClientId, ComponentId, EntityHandle, EntityId, InstanceId, NodeId, OwnerEpoch, PolicyId,
        StationId, Tick,
    },
    spatial::{Bounds, GridSpec, Position3},
    spatial_index::CellIndexUpdate,
    station::{StationConfig, StationError},
};

pub use crate::station::{
    DespawnReport, GhostEntity, SpawnEntity, StationEntityUpdateReport, StationMoveReport,
    StationRuntime, StationRuntimeCapacities, StationRuntimeConfig, StationRuntimeError,
    StationRuntimeReclaimReport,
};
