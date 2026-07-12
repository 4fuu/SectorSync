//! Client command envelopes and validation decisions.

use std::collections::VecDeque;

use crate::barrier::{BarrierState, CommandQueueMode};
use crate::ids::{ClientId, CommandId, EntityId, Tick};

/// Client command priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandPriority {
    /// Normal gameplay command.
    Normal,
    /// Latency-sensitive command.
    High,
    /// Low-priority command that may be delayed under pressure.
    Low,
}

/// Command envelope accepted by the generic command pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    /// Command id used for replay and audit.
    pub id: CommandId,
    /// Client that submitted the command.
    pub client_id: ClientId,
    /// Entity the command intends to control.
    pub entity_id: EntityId,
    /// Client-side sequence number.
    pub sequence: u64,
    /// Server tick observed when the command entered the runtime.
    pub received_at: Tick,
    /// Game-defined command kind.
    pub kind: u32,
    /// Command priority.
    pub priority: CommandPriority,
    /// Opaque payload owned by the embedding game.
    pub payload: Vec<u8>,
}

/// Result of command validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandDecision {
    /// Command can be applied.
    Accept,
    /// Command is invalid and should not be applied.
    Reject {
        /// Machine-readable reject reason.
        reason: CommandRejectReason,
    },
    /// Command should be treated as suspicious for audit purposes.
    FlagSuspicious {
        /// Suspicion score chosen by the embedding application.
        score: u32,
        /// Machine-readable reject or audit reason.
        reason: CommandRejectReason,
    },
}

/// Generic command reject reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRejectReason {
    /// Command failed schema or size validation.
    InvalidSchema,
    /// Command was submitted too frequently.
    RateLimited,
    /// Command was stale or replayed.
    ReplayOrStale,
    /// Command targeted an entity not owned by this station.
    NotOwner,
    /// Game-specific validator rejected the command.
    GameRule,
}

/// Bounded queue limits by command priority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandQueueLimits {
    /// High-priority queue capacity.
    pub high: usize,
    /// Normal-priority queue capacity.
    pub normal: usize,
    /// Low-priority queue capacity.
    pub low: usize,
}

impl Default for CommandQueueLimits {
    fn default() -> Self {
        Self {
            high: 1024,
            normal: 8192,
            low: 4096,
        }
    }
}

/// Barrier-aware command ingress policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandIngress {
    /// Current barrier state.
    pub barrier_state: BarrierState,
    /// Command behavior configured for the active barrier.
    pub command_mode: CommandQueueMode,
}

impl CommandIngress {
    /// Normal running ingress.
    pub const RUNNING: Self = Self {
        barrier_state: BarrierState::Running,
        command_mode: CommandQueueMode::Buffer,
    };
}

/// Outcome of enqueueing a command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandPushOutcome {
    /// Command was queued for normal application.
    Queued,
    /// Command was buffered while a barrier is active.
    BufferedByBarrier,
}

/// Command queue error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandQueueError {
    /// Queue is full and caller must apply backpressure.
    QueueFull(CommandPriority),
    /// Barrier mode rejects this command.
    RejectedByBarrier(CommandQueueMode),
}

impl core::fmt::Display for CommandQueueError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::QueueFull(priority) => write!(f, "{priority:?} command queue is full"),
            Self::RejectedByBarrier(mode) => {
                write!(f, "command rejected by active barrier mode {mode:?}")
            }
        }
    }
}

impl std::error::Error for CommandQueueError {}

/// Bounded, priority-aware command queues for a station or gateway shard.
#[derive(Clone, Debug)]
pub struct CommandQueues {
    limits: CommandQueueLimits,
    high: VecDeque<CommandEnvelope>,
    normal: VecDeque<CommandEnvelope>,
    low: VecDeque<CommandEnvelope>,
    barrier_buffer: VecDeque<CommandEnvelope>,
}

impl CommandQueues {
    /// Creates empty command queues.
    pub fn new(limits: CommandQueueLimits) -> Self {
        Self {
            limits,
            high: VecDeque::new(),
            normal: VecDeque::new(),
            low: VecDeque::new(),
            barrier_buffer: VecDeque::new(),
        }
    }

    /// Pushes a command through the barrier-aware ingress policy.
    pub fn push(
        &mut self,
        command: CommandEnvelope,
        ingress: CommandIngress,
    ) -> Result<CommandPushOutcome, CommandQueueError> {
        match ingress.barrier_state {
            BarrierState::Running | BarrierState::Resuming => {
                self.push_ready(command)?;
                Ok(CommandPushOutcome::Queued)
            }
            BarrierState::Requested | BarrierState::WaitingTickBoundary | BarrierState::Frozen => {
                match ingress.command_mode {
                    CommandQueueMode::Buffer => {
                        if self.barrier_buffer.len() >= self.barrier_buffer_capacity() {
                            Err(CommandQueueError::QueueFull(command.priority))
                        } else {
                            self.barrier_buffer.push_back(command);
                            Ok(CommandPushOutcome::BufferedByBarrier)
                        }
                    }
                    CommandQueueMode::Reject | CommandQueueMode::Drain => {
                        Err(CommandQueueError::RejectedByBarrier(ingress.command_mode))
                    }
                }
            }
        }
    }

    /// Moves commands buffered by a barrier back into priority queues.
    ///
    /// If a target priority queue is full, the first blocked command and all
    /// commands after it remain in the barrier buffer for a later retry.
    pub fn release_barrier_buffer(&mut self) -> Result<usize, CommandQueueError> {
        let mut released = 0;
        while let Some(priority) = self.barrier_buffer.front().map(|command| command.priority) {
            if !self.has_ready_capacity(priority) {
                return Err(CommandQueueError::QueueFull(priority));
            }
            let command = self
                .barrier_buffer
                .pop_front()
                .expect("front command was checked above");
            match priority {
                CommandPriority::High => self.high.push_back(command),
                CommandPriority::Normal => self.normal.push_back(command),
                CommandPriority::Low => self.low.push_back(command),
            }
            released += 1;
        }
        Ok(released)
    }

    /// Drops commands buffered by a barrier.
    pub fn clear_barrier_buffer(&mut self) -> usize {
        let dropped = self.barrier_buffer.len();
        self.barrier_buffer.clear();
        dropped
    }

    /// Pops the next command, preferring high priority.
    pub fn pop_next(&mut self) -> Option<CommandEnvelope> {
        self.high
            .pop_front()
            .or_else(|| self.normal.pop_front())
            .or_else(|| self.low.pop_front())
    }

    /// Returns queued command count excluding the barrier buffer.
    pub fn ready_len(&self) -> usize {
        self.high.len() + self.normal.len() + self.low.len()
    }

    /// Returns command count buffered by an active barrier.
    pub fn barrier_buffer_len(&self) -> usize {
        self.barrier_buffer.len()
    }

    /// Returns slots retained by one ready priority queue.
    pub fn ready_retained_capacity(&self, priority: CommandPriority) -> usize {
        match priority {
            CommandPriority::High => self.high.capacity(),
            CommandPriority::Normal => self.normal.capacity(),
            CommandPriority::Low => self.low.capacity(),
        }
    }

    /// Returns slots retained across all ready priority queues.
    pub fn total_ready_retained_capacity(&self) -> usize {
        self.high
            .capacity()
            .saturating_add(self.normal.capacity())
            .saturating_add(self.low.capacity())
    }

    /// Returns slots retained by the barrier buffer.
    pub fn barrier_buffer_retained_capacity(&self) -> usize {
        self.barrier_buffer.capacity()
    }

    /// Releases unused retained queue storage without changing queued commands.
    pub fn reclaim_retained_capacity(&mut self) {
        self.high.shrink_to_fit();
        self.normal.shrink_to_fit();
        self.low.shrink_to_fit();
        self.barrier_buffer.shrink_to_fit();
    }

    /// Maximum commands retained while a barrier buffers ingress.
    ///
    /// The buffer uses the saturating sum of the three ready-queue limits so
    /// integrations get a bounded default without a separate hidden capacity.
    pub fn barrier_buffer_capacity(&self) -> usize {
        self.limits
            .high
            .saturating_add(self.limits.normal)
            .saturating_add(self.limits.low)
    }

    /// Returns total command count including barrier buffer.
    pub fn total_len(&self) -> usize {
        self.ready_len() + self.barrier_buffer.len()
    }

    /// Returns whether no commands are queued.
    pub fn is_empty(&self) -> bool {
        self.total_len() == 0
    }

    fn push_ready(&mut self, command: CommandEnvelope) -> Result<(), CommandQueueError> {
        if !self.has_ready_capacity(command.priority) {
            return Err(CommandQueueError::QueueFull(command.priority));
        }
        match command.priority {
            CommandPriority::High => {
                self.high.push_back(command);
            }
            CommandPriority::Normal => {
                self.normal.push_back(command);
            }
            CommandPriority::Low => {
                self.low.push_back(command);
            }
        }
        Ok(())
    }

    fn has_ready_capacity(&self, priority: CommandPriority) -> bool {
        match priority {
            CommandPriority::High => self.high.len() < self.limits.high,
            CommandPriority::Normal => self.normal.len() < self.limits.normal,
            CommandPriority::Low => self.low.len() < self.limits.low,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(id: u64, priority: CommandPriority) -> CommandEnvelope {
        CommandEnvelope {
            id: CommandId::new(id),
            client_id: ClientId::new(1),
            entity_id: EntityId::new(10),
            sequence: id,
            received_at: Tick::new(0),
            kind: 1,
            priority,
            payload: Vec::new(),
        }
    }

    #[test]
    fn command_queues_pop_by_priority() {
        let mut queues = CommandQueues::new(CommandQueueLimits {
            high: 2,
            normal: 2,
            low: 2,
        });

        queues
            .push(command(1, CommandPriority::Low), CommandIngress::RUNNING)
            .expect("low should queue");
        queues
            .push(command(2, CommandPriority::High), CommandIngress::RUNNING)
            .expect("high should queue");
        queues
            .push(command(3, CommandPriority::Normal), CommandIngress::RUNNING)
            .expect("normal should queue");

        assert_eq!(queues.pop_next().expect("high").id, CommandId::new(2));
        assert_eq!(queues.pop_next().expect("normal").id, CommandId::new(3));
        assert_eq!(queues.pop_next().expect("low").id, CommandId::new(1));
    }

    #[test]
    fn command_queues_allocate_lazily_and_retain_peak_capacity() {
        let mut queues = CommandQueues::new(CommandQueueLimits::default());
        assert_eq!(queues.total_ready_retained_capacity(), 0);
        assert_eq!(queues.barrier_buffer_retained_capacity(), 0);

        for (offset, priority) in [
            CommandPriority::High,
            CommandPriority::Normal,
            CommandPriority::Low,
        ]
        .into_iter()
        .enumerate()
        {
            for index in 0..8 {
                queues
                    .push(
                        command(
                            u64::try_from(offset * 8 + index).expect("test id fits u64"),
                            priority,
                        ),
                        CommandIngress::RUNNING,
                    )
                    .expect("ready burst should queue");
            }
            assert!(queues.ready_retained_capacity(priority) >= 8);
        }
        let ready_peak = queues.total_ready_retained_capacity();
        while queues.pop_next().is_some() {}
        assert_eq!(queues.total_ready_retained_capacity(), ready_peak);

        let frozen = CommandIngress {
            barrier_state: BarrierState::Frozen,
            command_mode: CommandQueueMode::Buffer,
        };
        for index in 0..8 {
            queues
                .push(command(100 + index, CommandPriority::Normal), frozen)
                .expect("barrier burst should queue");
        }
        let barrier_peak = queues.barrier_buffer_retained_capacity();
        assert!(barrier_peak >= 8);
        assert_eq!(queues.clear_barrier_buffer(), 8);
        assert_eq!(queues.barrier_buffer_retained_capacity(), barrier_peak);
    }

    #[test]
    fn reclaim_retained_capacity_preserves_queued_order() {
        let mut queues = CommandQueues::new(CommandQueueLimits::default());
        queues
            .push(command(1, CommandPriority::Low), CommandIngress::RUNNING)
            .expect("low");
        queues
            .push(command(2, CommandPriority::High), CommandIngress::RUNNING)
            .expect("high");

        queues.reclaim_retained_capacity();

        assert_eq!(queues.pop_next().expect("high first").id, CommandId::new(2));
        assert_eq!(queues.pop_next().expect("low second").id, CommandId::new(1));
    }

    #[test]
    fn barrier_buffer_mode_holds_and_releases_commands() {
        let mut queues = CommandQueues::new(CommandQueueLimits::default());
        let ingress = CommandIngress {
            barrier_state: BarrierState::Frozen,
            command_mode: CommandQueueMode::Buffer,
        };

        let outcome = queues
            .push(command(1, CommandPriority::Normal), ingress)
            .expect("buffer should work");
        assert_eq!(outcome, CommandPushOutcome::BufferedByBarrier);
        assert_eq!(queues.ready_len(), 0);
        assert_eq!(queues.barrier_buffer_len(), 1);

        assert_eq!(
            queues
                .release_barrier_buffer()
                .expect("release should work"),
            1
        );
        assert_eq!(queues.ready_len(), 1);
    }

    #[test]
    fn barrier_reject_mode_rejects_commands() {
        let mut queues = CommandQueues::new(CommandQueueLimits::default());
        let ingress = CommandIngress {
            barrier_state: BarrierState::Frozen,
            command_mode: CommandQueueMode::Reject,
        };

        let error = queues
            .push(command(1, CommandPriority::Normal), ingress)
            .expect_err("reject mode should reject");
        assert_eq!(
            error,
            CommandQueueError::RejectedByBarrier(CommandQueueMode::Reject)
        );
    }

    #[test]
    fn barrier_buffer_is_bounded_by_ready_queue_limits() {
        let mut queues = CommandQueues::new(CommandQueueLimits {
            high: 1,
            normal: 0,
            low: 0,
        });
        let ingress = CommandIngress {
            barrier_state: BarrierState::Frozen,
            command_mode: CommandQueueMode::Buffer,
        };

        assert_eq!(queues.barrier_buffer_capacity(), 1);
        queues
            .push(command(1, CommandPriority::High), ingress)
            .expect("first command should buffer");
        let error = queues
            .push(command(2, CommandPriority::High), ingress)
            .expect_err("bounded barrier buffer should reject overflow");

        assert_eq!(error, CommandQueueError::QueueFull(CommandPriority::High));
        assert_eq!(queues.barrier_buffer_len(), 1);
    }

    #[test]
    fn failed_barrier_release_keeps_blocked_command_buffered() {
        let mut queues = CommandQueues::new(CommandQueueLimits {
            high: 1,
            normal: 0,
            low: 0,
        });
        queues
            .push(command(1, CommandPriority::High), CommandIngress::RUNNING)
            .expect("ready queue should accept first command");
        queues
            .push(
                command(2, CommandPriority::High),
                CommandIngress {
                    barrier_state: BarrierState::Frozen,
                    command_mode: CommandQueueMode::Buffer,
                },
            )
            .expect("barrier buffer should accept second command");

        let error = queues
            .release_barrier_buffer()
            .expect_err("full ready queue should block release");
        assert_eq!(error, CommandQueueError::QueueFull(CommandPriority::High));
        assert_eq!(queues.barrier_buffer_len(), 1);
        assert_eq!(
            queues.pop_next().expect("first command should remain").id,
            CommandId::new(1)
        );
        assert_eq!(queues.release_barrier_buffer(), Ok(1));
        assert_eq!(
            queues.pop_next().expect("second command should release").id,
            CommandId::new(2)
        );
    }
}
