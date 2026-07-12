//! Common product-path imports for `SectorSync` embedders.

pub use sectorsync_core::{
    component::{ComponentDescriptor, ComponentStoreError},
    entity::{EntityRecord, EntityTags},
    ids::{
        ClientId, ComponentId, EntityHandle, EntityId, InstanceId, NodeId, PolicyId, StationId,
        Tick,
    },
    spatial::{Bounds, GridSpec, Position3},
    spatial_index::CellIndexUpdate,
    station::{StationConfig, StationError},
};

pub use crate::station::{
    DespawnReport, SpawnEntity, StationMoveReport, StationRuntime, StationRuntimeConfig,
};
