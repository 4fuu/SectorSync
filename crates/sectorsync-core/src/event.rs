//! Cross-station event envelopes and bounded priority queues.

use std::collections::VecDeque;

use crate::ids::{EntityId, EventId, OwnerEpoch, StationId, Tick};

/// Event priority class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventPriority {
    /// Must be delivered or cause backpressure.
    Critical,
    /// Should be delivered with bounded retry/ack policy.
    Important,
    /// Can be dropped, merged, or downgraded under pressure.
    BestEffort,
}

/// Core event kind understood by the runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// User-defined event kind id.
    Custom(u32),
    /// Prepare a two-phase entity handoff.
    HandoffPrepare {
        /// Entity being handed off.
        entity_id: EntityId,
    },
    /// Commit a two-phase entity handoff.
    HandoffCommit {
        /// Entity being handed off.
        entity_id: EntityId,
        /// New owner epoch.
        owner_epoch: OwnerEpoch,
    },
}

/// Event envelope routed between stations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StationEvent {
    /// Idempotency key.
    pub id: EventId,
    /// Source station.
    pub source: StationId,
    /// Target station.
    pub target: StationId,
    /// Tick observed at source.
    pub source_tick: Tick,
    /// Tick at which target should apply the event.
    pub target_tick: Tick,
    /// Priority class.
    pub priority: EventPriority,
    /// Event payload kind.
    pub kind: EventKind,
}

/// Bounded queue limits by priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventQueueLimits {
    /// Critical queue capacity.
    pub critical: usize,
    /// Important queue capacity.
    pub important: usize,
    /// Best-effort queue capacity.
    pub best_effort: usize,
}

impl Default for EventQueueLimits {
    fn default() -> Self {
        Self {
            critical: 1024,
            important: 4096,
            best_effort: 8192,
        }
    }
}

/// Outcome of a queue push.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// Event was accepted without dropping another event.
    Accepted,
    /// A best-effort event was dropped to admit the new one.
    DroppedOldestBestEffort,
}

/// Event queue error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventQueueError {
    /// Reliable queue is full and caller must apply backpressure.
    QueueFull(EventPriority),
}

impl core::fmt::Display for EventQueueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::QueueFull(priority) => write!(f, "{priority:?} event queue is full"),
        }
    }
}

impl std::error::Error for EventQueueError {}

/// Bounded priority queues for station events.
#[derive(Clone, Debug)]
pub struct EventQueues {
    limits: EventQueueLimits,
    critical: VecDeque<StationEvent>,
    important: VecDeque<StationEvent>,
    best_effort: VecDeque<StationEvent>,
}

impl EventQueues {
    /// Creates empty event queues.
    pub fn new(limits: EventQueueLimits) -> Self {
        Self {
            limits,
            critical: VecDeque::with_capacity(limits.critical),
            important: VecDeque::with_capacity(limits.important),
            best_effort: VecDeque::with_capacity(limits.best_effort),
        }
    }

    /// Pushes an event according to priority and queue semantics.
    pub fn push(&mut self, event: StationEvent) -> Result<PushOutcome, EventQueueError> {
        match event.priority {
            EventPriority::Critical => {
                if self.critical.len() >= self.limits.critical {
                    Err(EventQueueError::QueueFull(EventPriority::Critical))
                } else {
                    self.critical.push_back(event);
                    Ok(PushOutcome::Accepted)
                }
            }
            EventPriority::Important => {
                if self.important.len() >= self.limits.important {
                    Err(EventQueueError::QueueFull(EventPriority::Important))
                } else {
                    self.important.push_back(event);
                    Ok(PushOutcome::Accepted)
                }
            }
            EventPriority::BestEffort => {
                let outcome = if self.best_effort.len() >= self.limits.best_effort {
                    self.best_effort.pop_front();
                    PushOutcome::DroppedOldestBestEffort
                } else {
                    PushOutcome::Accepted
                };
                self.best_effort.push_back(event);
                Ok(outcome)
            }
        }
    }

    /// Pops the next event, preferring higher priority.
    pub fn pop_next(&mut self) -> Option<StationEvent> {
        self.critical
            .pop_front()
            .or_else(|| self.important.pop_front())
            .or_else(|| self.best_effort.pop_front())
    }

    /// Returns total queued events.
    pub fn len(&self) -> usize {
        self.critical.len() + self.important.len() + self.best_effort.len()
    }

    /// Returns whether all queues are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
