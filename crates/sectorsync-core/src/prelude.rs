//! Common imports for SectorSync embedders.

pub use crate::barrier::{BarrierScope, BarrierState, CommandQueueMode, RuntimeBarrier};
pub use crate::command::{
    CommandDecision, CommandEnvelope, CommandIngress, CommandPriority, CommandPushOutcome,
    CommandQueueError, CommandQueueLimits, CommandQueues, CommandRejectReason,
};
pub use crate::component::{
    ComponentBlob, ComponentCodec, ComponentCodecError, ComponentDescriptor,
    ComponentMigrationMode, ComponentRegistry, ComponentRegistryError, ComponentSchema,
    ComponentStorageKind, ComponentStore, ComponentStoreError, ComponentSyncMode, F32LeCodec,
    U32LeCodec, Vec3LeCodec,
};
pub use crate::entity::{DirtyMask, EntityRecord, EntityRole, EntityTags};
pub use crate::event::{
    EventKind, EventPriority, EventQueueError, EventQueueLimits, EventQueues, PushOutcome,
    StationEvent,
};
pub use crate::handoff::{HandoffCommitHandles, HandoffTransfer};
pub use crate::hotspot::{
    CellLoadSample, HotspotDecision, HotspotPlanner, HotspotSeverity, HotspotThresholds,
    SplitProposal, StationLoadSample,
};
pub use crate::ids::{
    BarrierId, ClientId, CommandId, ComponentId, EntityHandle, EntityId, EventId, InstanceId,
    NodeId, OwnerEpoch, PolicyId, StationId, Tick,
};
pub use crate::interest::{RangeOnlyVisibility, ViewerQuery, VisibilityFilter};
pub use crate::policy::{CompiledSyncPolicy, PolicyTable};
pub use crate::replication::{
    ReplicationBudget, ReplicationPlan, ReplicationPlanner, ReplicationStats,
};
pub use crate::snapshot::{RuntimeUpgradeHook, SnapshotMeta, SnapshotVersion, StationSnapshot};
pub use crate::spatial::{Aabb3, Bounds, CellCoord3, GridSpec, Position3, Vec3};
pub use crate::spatial_index::{CellIndex, CellOccupancy};
pub use crate::station::{Station, StationConfig, StationError};
