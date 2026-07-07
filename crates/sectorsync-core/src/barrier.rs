//! Full runtime barrier state for pause, snapshot, upgrade, and resume.

use crate::ids::{BarrierId, InstanceId, StationId, Tick};

/// Runtime barrier scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierScope {
    /// Barrier applies to a full world instance.
    Instance(InstanceId),
    /// Barrier applies to a single station.
    Station(StationId),
}

/// Strategy for commands received while a barrier is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandQueueMode {
    /// Buffer commands until resume.
    Buffer,
    /// Reject commands until resume.
    Reject,
    /// Drain already queued commands and reject new commands.
    Drain,
}

/// Barrier lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierState {
    /// Normal running state.
    Running,
    /// Barrier was requested but not aligned.
    Requested,
    /// Waiting for a tick boundary.
    WaitingTickBoundary,
    /// Runtime state is frozen.
    Frozen,
    /// Runtime is resuming.
    Resuming,
}

/// Full runtime barrier descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeBarrier {
    /// Barrier id.
    pub id: BarrierId,
    /// Barrier scope.
    pub scope: BarrierScope,
    /// Tick observed when requested.
    pub requested_at: Tick,
    /// Tick boundary selected for freezing.
    pub target_tick: Tick,
    /// Command behavior during the barrier.
    pub command_mode: CommandQueueMode,
    /// Current barrier state.
    pub state: BarrierState,
}

impl RuntimeBarrier {
    /// Creates a requested barrier.
    pub const fn requested(
        id: BarrierId,
        scope: BarrierScope,
        requested_at: Tick,
        target_tick: Tick,
        command_mode: CommandQueueMode,
    ) -> Self {
        Self {
            id,
            scope,
            requested_at,
            target_tick,
            command_mode,
            state: BarrierState::Requested,
        }
    }

    /// Marks this barrier as waiting for the target tick boundary.
    pub fn wait_for_tick_boundary(&mut self) {
        self.state = BarrierState::WaitingTickBoundary;
    }

    /// Marks this barrier as frozen.
    pub fn freeze(&mut self) {
        self.state = BarrierState::Frozen;
    }

    /// Marks this barrier as resuming.
    pub fn resume(&mut self) {
        self.state = BarrierState::Resuming;
    }

    /// Clears this barrier back to running state.
    pub fn finish(&mut self) {
        self.state = BarrierState::Running;
    }
}
