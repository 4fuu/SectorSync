//! Common imports for `SectorSync` embedders.

pub use crate::barrier::{BarrierScope, BarrierState, CommandQueueMode, RuntimeBarrier};
pub use crate::command::{
    CommandDecision, CommandEnvelope, CommandIngress, CommandPriority, CommandPushOutcome,
    CommandQueueError, CommandQueueLimits, CommandQueues, CommandRejectReason,
};
pub use crate::component::{
    ComponentBlob, ComponentCodec, ComponentCodecError, ComponentDescriptor,
    ComponentEncodeScratch, ComponentFieldDescriptor, ComponentFieldType, ComponentMigrationMode,
    ComponentRegistry, ComponentRegistryError, ComponentSchema, ComponentSchemaError,
    ComponentStorageKind, ComponentStore, ComponentStoreError, ComponentSyncMode, F32LeCodec,
    GeneratedComponentSchema, GeneratedSchemaRegistrationError, U32LeCodec, Vec3LeCodec,
};
pub use crate::entity::{DirtyMask, EntityRecord, EntityRole, EntityTags};
pub use crate::event::{
    EventKind, EventPriority, EventQueueError, EventQueueLimits, EventQueues, PushOutcome,
    StationEvent,
};
pub use crate::gateway::{
    GatewayCommandAdmission, GatewayConfig, GatewayConnectOutcome, GatewayConnectReport,
    GatewayError, GatewayRoute, GatewaySession, GatewaySessionState, GatewaySessionTable,
    GatewayStats,
};
pub use crate::handoff::{HandoffCommitHandles, HandoffTransfer};
pub use crate::hotspot::{
    CellLoadSample, HotspotDecision, HotspotPlanner, HotspotSeverity, HotspotSplitScratch,
    HotspotThresholds, SplitProposal, StationLoadSample,
};
pub use crate::ids::{
    BarrierId, ClientId, CommandId, ComponentId, EntityHandle, EntityId, EventId, InstanceId,
    NodeId, OwnerEpoch, PolicyId, StationId, Tick,
};
pub use crate::interest::{
    AndVisibility, FrustumVisibility, RangeOnlyVisibility, TagVisibility, ViewerQuery,
    VisibilityFilter,
};
pub use crate::policy::{CompiledSyncPolicy, PolicyTable};
pub use crate::replication::{
    ReplicationBatchResult, ReplicationBatchScratch, ReplicationBatchStats, ReplicationBatchView,
    ReplicationBudget, ReplicationCadence, ReplicationPlan, ReplicationPlanner,
    ReplicationPriority, ReplicationScratch, ReplicationStats, ReplicationTrackKey,
    ReplicationTrackRecord, ReplicationTracker, ReplicationTrackerConfig, ReplicationTrackerError,
    ReplicationTrackerStats,
};
pub use crate::snapshot::{RuntimeUpgradeHook, SnapshotMeta, SnapshotVersion, StationSnapshot};
pub use crate::spatial::{Aabb3, Bounds, CellCoord3, Frustum3, GridSpec, Plane3, Position3, Vec3};
pub use crate::spatial_index::{
    CellIndex, CellIndexUpdate, CellIndexUpdateReport, CellIndexUpdateScratch, CellOccupancy,
    CellQueryScratch, CellQueryStats, CellQueryStrategy,
};
pub use crate::station::{Station, StationConfig, StationError, StationRestoreStats};
