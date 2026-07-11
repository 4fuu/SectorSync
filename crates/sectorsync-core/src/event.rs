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
            critical: VecDeque::new(),
            important: VecDeque::new(),
            best_effort: VecDeque::new(),
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

    /// Appends events ready at `current_tick` while retaining delayed events in place.
    ///
    /// Priority and FIFO order match repeated [`Self::pop_next`] calls. The
    /// caller owns `out` and may retain its capacity across ticks.
    pub fn drain_ready_into(&mut self, current_tick: Tick, out: &mut Vec<StationEvent>) -> usize {
        let before = out.len();
        drain_priority_ready(&mut self.critical, current_tick, out);
        drain_priority_ready(&mut self.important, current_tick, out);
        drain_priority_ready(&mut self.best_effort, current_tick, out);
        out.len() - before
    }

    /// Returns total queued events.
    pub fn len(&self) -> usize {
        self.critical.len() + self.important.len() + self.best_effort.len()
    }

    /// Returns whether all queues are empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns slots retained by one event priority queue.
    pub fn retained_capacity(&self, priority: EventPriority) -> usize {
        match priority {
            EventPriority::Critical => self.critical.capacity(),
            EventPriority::Important => self.important.capacity(),
            EventPriority::BestEffort => self.best_effort.capacity(),
        }
    }

    /// Returns slots retained across all event priority queues.
    pub fn total_retained_capacity(&self) -> usize {
        self.critical
            .capacity()
            .saturating_add(self.important.capacity())
            .saturating_add(self.best_effort.capacity())
    }
}

fn drain_priority_ready(
    queue: &mut VecDeque<StationEvent>,
    current_tick: Tick,
    out: &mut Vec<StationEvent>,
) {
    let queued = queue.len();
    for _ in 0..queued {
        let event = queue
            .pop_front()
            .expect("initial queue length bounds the drain loop");
        if event.target_tick <= current_tick {
            out.push(event);
        } else {
            queue.push_back(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::EventId;

    fn event(id: u64, priority: EventPriority, target_tick: u64) -> StationEvent {
        StationEvent {
            id: EventId::new(id),
            source: StationId::new(1),
            target: StationId::new(2),
            source_tick: Tick::new(0),
            target_tick: Tick::new(target_tick),
            priority,
            kind: EventKind::Custom(u32::try_from(id).expect("test id fits u32")),
        }
    }

    #[test]
    fn ready_drain_preserves_priority_fifo_and_delayed_order() {
        let mut queues = EventQueues::new(EventQueueLimits {
            critical: 8,
            important: 8,
            best_effort: 8,
        });
        for value in [
            event(1, EventPriority::Critical, 3),
            event(2, EventPriority::Critical, 1),
            event(3, EventPriority::Important, 1),
            event(4, EventPriority::Important, 4),
            event(5, EventPriority::BestEffort, 1),
            event(6, EventPriority::BestEffort, 5),
        ] {
            queues.push(value).expect("test queue has capacity");
        }
        let mut ready = Vec::with_capacity(8);

        assert_eq!(queues.drain_ready_into(Tick::new(1), &mut ready), 3);
        assert_eq!(
            ready.iter().map(|event| event.id).collect::<Vec<_>>(),
            [EventId::new(2), EventId::new(3), EventId::new(5)]
        );
        assert_eq!(queues.len(), 3);

        ready.clear();
        assert_eq!(queues.drain_ready_into(Tick::new(5), &mut ready), 3);
        assert_eq!(
            ready.iter().map(|event| event.id).collect::<Vec<_>>(),
            [EventId::new(1), EventId::new(4), EventId::new(6)]
        );
        assert!(queues.is_empty());
    }

    #[test]
    fn event_queues_allocate_lazily_and_retain_peak_capacity() {
        let mut queues = EventQueues::new(EventQueueLimits::default());
        assert_eq!(queues.total_retained_capacity(), 0);

        for (offset, priority) in [
            EventPriority::Critical,
            EventPriority::Important,
            EventPriority::BestEffort,
        ]
        .into_iter()
        .enumerate()
        {
            for index in 0..8 {
                queues
                    .push(event(
                        u64::try_from(offset * 8 + index).expect("test id fits u64"),
                        priority,
                        0,
                    ))
                    .expect("event burst should queue");
            }
            assert!(queues.retained_capacity(priority) >= 8);
        }
        let peak = queues.total_retained_capacity();
        while queues.pop_next().is_some() {}
        assert_eq!(queues.total_retained_capacity(), peak);
    }
}
