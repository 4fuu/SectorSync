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
pub use crate::{
    client::{ReceiveExecutor, ReceiveExecutorConfig},
    maintenance::{LoadSampler, SplitExecutor},
    replication::{
        AllEligible, CandidateEligibility, LastSentLookup, NoLastSent, ReplicationBatchReport,
        ReplicationBatchRequest, ReplicationExecutionError, ReplicationExecutionFailure,
        ReplicationExecutor, ReplicationExecutorConfig, ReplicationExecutorStats,
        ReplicationReport, ReplicationRequest,
    },
};
pub use sectorsync_core::{
    interest::{RangeOnlyVisibility, ViewerQuery, VisibilityFilter},
    policy::{CompiledSyncPolicy, PolicyTable},
    replication::{ReplicationBudget, ReplicationSelectionMode},
};
pub use sectorsync_runtime::{SplitSchedulerConfig, StationLoadSamplerConfig};
pub use sectorsync_wire::{ComponentSelection, ReplicationFrameLimits};

#[cfg(feature = "parallel")]
pub use crate::replication::{
    ParallelReplicationExecutor, ParallelReplicationRequest, ParallelStationBatch,
};
#[cfg(feature = "parallel")]
pub use sectorsync_runtime::{ReplicationThreadPoolBuildError, ReplicationThreadPoolConfig};
