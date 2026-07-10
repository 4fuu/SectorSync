//! Low-level gateway session and client routing primitives.

use std::collections::BTreeMap;

use crate::command::{CommandEnvelope, CommandRejectReason};
use crate::ids::{ClientId, StationId, Tick};

/// Gateway/session table configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayConfig {
    /// Maximum tracked client sessions.
    pub max_sessions: usize,
    /// Number of ticks a disconnected session may reconnect with its current
    /// generation.
    pub reconnect_grace_ticks: u64,
    /// Maximum admitted commands per client per tick.
    pub max_commands_per_tick: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_sessions: 65_536,
            reconnect_grace_ticks: 20 * 60,
            max_commands_per_tick: 64,
        }
    }
}

/// Gateway route snapshot for a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayRoute {
    /// Routed client.
    pub client_id: ClientId,
    /// Current target station for client commands.
    pub station_id: StationId,
    /// Session generation used by reconnect handshakes.
    pub generation: u64,
    /// Route epoch incremented every time a client changes station route.
    pub route_epoch: u64,
}

/// Session connection state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewaySessionState {
    /// Client is connected and may submit commands.
    Connected,
    /// Client disconnected and may reconnect until the grace window expires.
    Disconnected {
        /// Tick at which the disconnect was observed.
        since: Tick,
    },
}

/// Tracked client gateway session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySession {
    /// Client id.
    pub client_id: ClientId,
    /// Current target station.
    pub station_id: StationId,
    /// Tick at which this generation was first connected.
    pub connected_at: Tick,
    /// Last tick at which the client was observed.
    pub last_seen: Tick,
    /// Reconnect generation. External gateways may expose this as an opaque
    /// token after adding their own authentication.
    pub generation: u64,
    /// Route epoch incremented on station route changes.
    pub route_epoch: u64,
    /// Connection state.
    pub state: GatewaySessionState,
    last_sequence: Option<u64>,
    command_tick: Tick,
    commands_this_tick: usize,
}

impl GatewaySession {
    /// Returns a route snapshot.
    pub const fn route(&self) -> GatewayRoute {
        GatewayRoute {
            client_id: self.client_id,
            station_id: self.station_id,
            generation: self.generation,
            route_epoch: self.route_epoch,
        }
    }

    /// Returns the latest accepted command sequence.
    pub const fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// Returns command count admitted in the currently tracked tick.
    pub const fn commands_this_tick(&self) -> usize {
        self.commands_this_tick
    }

    /// Returns whether the session is connected.
    pub const fn is_connected(&self) -> bool {
        matches!(self.state, GatewaySessionState::Connected)
    }
}

/// Result of connecting or reconnecting a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayConnectOutcome {
    /// A new session entry was created.
    Created,
    /// An already connected session was refreshed.
    AlreadyConnected,
    /// A disconnected session reconnected within its grace window.
    Reconnected {
        /// Ticks between disconnect and reconnect.
        disconnected_for: u64,
    },
    /// A stale disconnected session was replaced with a new generation.
    ReplacedExpired {
        /// Ticks between disconnect and replacement.
        disconnected_for: u64,
    },
}

/// Connection report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayConnectReport {
    /// Outcome.
    pub outcome: GatewayConnectOutcome,
    /// Current route.
    pub route: GatewayRoute,
}

/// Result of admitting a command through gateway metadata checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayCommandAdmission {
    /// Route that should receive the command.
    pub route: GatewayRoute,
    /// Accepted client-side command sequence.
    pub sequence: u64,
    /// Commands accepted for this client in the current tick.
    pub commands_this_tick: usize,
}

/// Gateway session table statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GatewayStats {
    /// Sessions created.
    pub sessions_created: usize,
    /// Successful reconnects.
    pub sessions_reconnected: usize,
    /// Disconnected sessions expired or replaced after grace.
    pub sessions_expired: usize,
    /// Route changes.
    pub routes_changed: usize,
    /// Commands admitted.
    pub commands_admitted: usize,
    /// Commands rejected as replay/stale.
    pub commands_rejected_replay: usize,
    /// Commands rejected by per-client rate limit.
    pub commands_rejected_rate_limit: usize,
}

/// Gateway/session error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayError {
    /// Session table is full.
    CapacityFull {
        /// Configured maximum session count.
        capacity: usize,
    },
    /// Session does not exist.
    MissingSession(ClientId),
    /// Session is disconnected.
    SessionDisconnected {
        /// Client id.
        client_id: ClientId,
        /// Disconnect tick.
        since: Tick,
    },
    /// Session reconnect generation does not match.
    BadGeneration {
        /// Expected generation.
        expected: u64,
        /// Provided generation.
        actual: u64,
    },
    /// Command sequence was stale or replayed.
    ReplayOrStale {
        /// Latest accepted sequence, if any.
        last_sequence: Option<u64>,
        /// Submitted sequence.
        sequence: u64,
    },
    /// Client exceeded the per-tick command admission limit.
    RateLimited {
        /// Configured limit.
        limit: usize,
        /// Attempted count in the tick.
        attempted: usize,
    },
}

impl GatewayError {
    /// Maps gateway metadata errors into generic command reject reasons.
    pub const fn command_reject_reason(&self) -> Option<CommandRejectReason> {
        match self {
            Self::ReplayOrStale { .. } => Some(CommandRejectReason::ReplayOrStale),
            Self::RateLimited { .. } => Some(CommandRejectReason::RateLimited),
            Self::MissingSession(_)
            | Self::SessionDisconnected { .. }
            | Self::BadGeneration { .. }
            | Self::CapacityFull { .. } => None,
        }
    }
}

impl core::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::CapacityFull { capacity } => {
                write!(f, "gateway session table is full at capacity {capacity}")
            }
            Self::MissingSession(client_id) => {
                write!(
                    f,
                    "gateway session for client {} is missing",
                    client_id.get()
                )
            }
            Self::SessionDisconnected { client_id, since } => write!(
                f,
                "gateway session for client {} disconnected at tick {}",
                client_id.get(),
                since.get()
            ),
            Self::BadGeneration { expected, actual } => write!(
                f,
                "gateway reconnect generation mismatch: expected {expected}, actual {actual}"
            ),
            Self::ReplayOrStale {
                last_sequence,
                sequence,
            } => write!(
                f,
                "gateway command sequence {sequence} is not newer than {last_sequence:?}"
            ),
            Self::RateLimited { limit, attempted } => write!(
                f,
                "gateway command rate limited: limit {limit}, attempted {attempted}"
            ),
        }
    }
}

impl std::error::Error for GatewayError {}

/// Bounded in-memory gateway session and route table.
#[derive(Clone, Debug)]
pub struct GatewaySessionTable {
    config: GatewayConfig,
    sessions: BTreeMap<ClientId, GatewaySession>,
    stats: GatewayStats,
}

impl GatewaySessionTable {
    /// Creates an empty gateway session table.
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            sessions: BTreeMap::new(),
            stats: GatewayStats::default(),
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> GatewayConfig {
        self.config
    }

    /// Returns statistics.
    pub const fn stats(&self) -> GatewayStats {
        self.stats
    }

    /// Returns tracked session count.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Returns whether no sessions are tracked.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// Returns a session by client id.
    pub fn session(&self, client_id: ClientId) -> Option<&GatewaySession> {
        self.sessions.get(&client_id)
    }

    /// Returns a connected route by client id.
    pub fn route(&self, client_id: ClientId) -> Result<GatewayRoute, GatewayError> {
        let session = self
            .sessions
            .get(&client_id)
            .ok_or(GatewayError::MissingSession(client_id))?;
        match session.state {
            GatewaySessionState::Connected => Ok(session.route()),
            GatewaySessionState::Disconnected { since } => {
                Err(GatewayError::SessionDisconnected { client_id, since })
            }
        }
    }

    /// Connects a client, reconnecting within grace or replacing stale sessions.
    pub fn connect(
        &mut self,
        client_id: ClientId,
        station_id: StationId,
        now: Tick,
    ) -> Result<GatewayConnectReport, GatewayError> {
        if self.sessions.contains_key(&client_id) {
            return Ok(self.connect_existing(client_id, station_id, now));
        }

        if self.sessions.len() >= self.config.max_sessions {
            return Err(GatewayError::CapacityFull {
                capacity: self.config.max_sessions,
            });
        }

        let session = GatewaySession {
            client_id,
            station_id,
            connected_at: now,
            last_seen: now,
            generation: 1,
            route_epoch: 1,
            state: GatewaySessionState::Connected,
            last_sequence: None,
            command_tick: now,
            commands_this_tick: 0,
        };
        let route = session.route();
        self.sessions.insert(client_id, session);
        self.stats.sessions_created = self.stats.sessions_created.saturating_add(1);
        Ok(GatewayConnectReport {
            outcome: GatewayConnectOutcome::Created,
            route,
        })
    }

    /// Reconnects a disconnected client by generation.
    pub fn reconnect(
        &mut self,
        client_id: ClientId,
        generation: u64,
        now: Tick,
    ) -> Result<GatewayConnectReport, GatewayError> {
        let session = self
            .sessions
            .get_mut(&client_id)
            .ok_or(GatewayError::MissingSession(client_id))?;
        if session.generation != generation {
            return Err(GatewayError::BadGeneration {
                expected: session.generation,
                actual: generation,
            });
        }

        match session.state {
            GatewaySessionState::Connected => {
                session.last_seen = now;
                Ok(GatewayConnectReport {
                    outcome: GatewayConnectOutcome::AlreadyConnected,
                    route: session.route(),
                })
            }
            GatewaySessionState::Disconnected { since } => {
                let disconnected_for = now.get().saturating_sub(since.get());
                if disconnected_for > self.config.reconnect_grace_ticks {
                    session.connected_at = now;
                    session.last_seen = now;
                    session.generation = session.generation.saturating_add(1);
                    session.route_epoch = session.route_epoch.saturating_add(1);
                    session.state = GatewaySessionState::Connected;
                    session.last_sequence = None;
                    session.command_tick = now;
                    session.commands_this_tick = 0;
                    self.stats.sessions_expired = self.stats.sessions_expired.saturating_add(1);
                    Ok(GatewayConnectReport {
                        outcome: GatewayConnectOutcome::ReplacedExpired { disconnected_for },
                        route: session.route(),
                    })
                } else {
                    session.last_seen = now;
                    session.state = GatewaySessionState::Connected;
                    self.stats.sessions_reconnected =
                        self.stats.sessions_reconnected.saturating_add(1);
                    Ok(GatewayConnectReport {
                        outcome: GatewayConnectOutcome::Reconnected { disconnected_for },
                        route: session.route(),
                    })
                }
            }
        }
    }

    /// Marks a connected session disconnected.
    pub fn disconnect(&mut self, client_id: ClientId, now: Tick) -> Result<(), GatewayError> {
        let session = self
            .sessions
            .get_mut(&client_id)
            .ok_or(GatewayError::MissingSession(client_id))?;
        session.last_seen = now;
        session.state = GatewaySessionState::Disconnected { since: now };
        Ok(())
    }

    /// Removes disconnected sessions whose grace window has expired.
    pub fn expire_disconnected(&mut self, now: Tick) -> usize {
        let grace = self.config.reconnect_grace_ticks;
        let expired = self
            .sessions
            .iter()
            .filter_map(|(client_id, session)| match session.state {
                GatewaySessionState::Connected => None,
                GatewaySessionState::Disconnected { since } => {
                    (now.get().saturating_sub(since.get()) > grace).then_some(*client_id)
                }
            })
            .collect::<Vec<_>>();

        for client_id in &expired {
            self.sessions.remove(client_id);
        }
        self.stats.sessions_expired = self.stats.sessions_expired.saturating_add(expired.len());
        expired.len()
    }

    /// Changes the target station for a connected client.
    pub fn reroute(
        &mut self,
        client_id: ClientId,
        station_id: StationId,
        now: Tick,
    ) -> Result<GatewayRoute, GatewayError> {
        let session = self
            .sessions
            .get_mut(&client_id)
            .ok_or(GatewayError::MissingSession(client_id))?;
        match session.state {
            GatewaySessionState::Connected => {
                session.last_seen = now;
                if session.station_id != station_id {
                    session.station_id = station_id;
                    session.route_epoch = session.route_epoch.saturating_add(1);
                    self.stats.routes_changed = self.stats.routes_changed.saturating_add(1);
                }
                Ok(session.route())
            }
            GatewaySessionState::Disconnected { since } => {
                Err(GatewayError::SessionDisconnected { client_id, since })
            }
        }
    }

    /// Applies session metadata checks to a command and returns the route that
    /// should receive it.
    pub fn admit_command(
        &mut self,
        command: &CommandEnvelope,
    ) -> Result<GatewayCommandAdmission, GatewayError> {
        self.admit_sequence(command.client_id, command.sequence, command.received_at)
    }

    /// Applies session metadata checks to a client command sequence.
    pub fn admit_sequence(
        &mut self,
        client_id: ClientId,
        sequence: u64,
        now: Tick,
    ) -> Result<GatewayCommandAdmission, GatewayError> {
        let session = self
            .sessions
            .get_mut(&client_id)
            .ok_or(GatewayError::MissingSession(client_id))?;
        match session.state {
            GatewaySessionState::Connected => {}
            GatewaySessionState::Disconnected { since } => {
                return Err(GatewayError::SessionDisconnected { client_id, since });
            }
        }

        if session
            .last_sequence
            .is_some_and(|last_sequence| sequence <= last_sequence)
        {
            self.stats.commands_rejected_replay =
                self.stats.commands_rejected_replay.saturating_add(1);
            return Err(GatewayError::ReplayOrStale {
                last_sequence: session.last_sequence,
                sequence,
            });
        }

        if session.command_tick != now {
            session.command_tick = now;
            session.commands_this_tick = 0;
        }
        let attempted = session.commands_this_tick.saturating_add(1);
        if attempted > self.config.max_commands_per_tick {
            self.stats.commands_rejected_rate_limit =
                self.stats.commands_rejected_rate_limit.saturating_add(1);
            return Err(GatewayError::RateLimited {
                limit: self.config.max_commands_per_tick,
                attempted,
            });
        }

        session.commands_this_tick = attempted;
        session.last_sequence = Some(sequence);
        session.last_seen = now;
        self.stats.commands_admitted = self.stats.commands_admitted.saturating_add(1);
        Ok(GatewayCommandAdmission {
            route: session.route(),
            sequence,
            commands_this_tick: attempted,
        })
    }

    fn connect_existing(
        &mut self,
        client_id: ClientId,
        station_id: StationId,
        now: Tick,
    ) -> GatewayConnectReport {
        let session = self
            .sessions
            .get_mut(&client_id)
            .expect("session existence was checked");
        match session.state {
            GatewaySessionState::Connected => {
                session.last_seen = now;
                if session.station_id != station_id {
                    session.station_id = station_id;
                    session.route_epoch = session.route_epoch.saturating_add(1);
                    self.stats.routes_changed = self.stats.routes_changed.saturating_add(1);
                }
                GatewayConnectReport {
                    outcome: GatewayConnectOutcome::AlreadyConnected,
                    route: session.route(),
                }
            }
            GatewaySessionState::Disconnected { since } => {
                let disconnected_for = now.get().saturating_sub(since.get());
                if disconnected_for > self.config.reconnect_grace_ticks {
                    session.station_id = station_id;
                    session.connected_at = now;
                    session.last_seen = now;
                    session.generation = session.generation.saturating_add(1);
                    session.route_epoch = session.route_epoch.saturating_add(1);
                    session.state = GatewaySessionState::Connected;
                    session.last_sequence = None;
                    session.command_tick = now;
                    session.commands_this_tick = 0;
                    self.stats.sessions_expired = self.stats.sessions_expired.saturating_add(1);
                    GatewayConnectReport {
                        outcome: GatewayConnectOutcome::ReplacedExpired { disconnected_for },
                        route: session.route(),
                    }
                } else {
                    session.station_id = station_id;
                    session.last_seen = now;
                    session.state = GatewaySessionState::Connected;
                    self.stats.sessions_reconnected =
                        self.stats.sessions_reconnected.saturating_add(1);
                    GatewayConnectReport {
                        outcome: GatewayConnectOutcome::Reconnected { disconnected_for },
                        route: session.route(),
                    }
                }
            }
        }
    }
}

impl Default for GatewaySessionTable {
    fn default() -> Self {
        Self::new(GatewayConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CommandEnvelope, CommandPriority};
    use crate::ids::{CommandId, EntityId};

    fn command(client_id: ClientId, sequence: u64, tick: u64) -> CommandEnvelope {
        CommandEnvelope {
            id: CommandId::new(sequence),
            client_id,
            entity_id: EntityId::new(10),
            sequence,
            received_at: Tick::new(tick),
            kind: 1,
            priority: CommandPriority::Normal,
            payload: Vec::new(),
        }
    }

    #[test]
    fn connects_routes_and_reroutes_sessions() {
        let client_id = ClientId::new(7);
        let mut table = GatewaySessionTable::new(GatewayConfig {
            max_sessions: 4,
            reconnect_grace_ticks: 3,
            max_commands_per_tick: 4,
        });

        let connected = table
            .connect(client_id, StationId::new(1), Tick::new(10))
            .expect("connect should work");
        assert_eq!(connected.outcome, GatewayConnectOutcome::Created);
        assert_eq!(connected.route.station_id, StationId::new(1));
        assert_eq!(connected.route.generation, 1);
        assert_eq!(connected.route.route_epoch, 1);

        let route = table
            .reroute(client_id, StationId::new(2), Tick::new(11))
            .expect("reroute should work");
        assert_eq!(route.station_id, StationId::new(2));
        assert_eq!(route.route_epoch, 2);
        assert_eq!(table.stats().routes_changed, 1);
        assert_eq!(
            table
                .route(client_id)
                .expect("route should exist")
                .station_id,
            StationId::new(2)
        );
    }

    #[test]
    fn reconnects_with_generation_and_expires_disconnected_sessions() {
        let client_id = ClientId::new(7);
        let mut table = GatewaySessionTable::new(GatewayConfig {
            max_sessions: 4,
            reconnect_grace_ticks: 3,
            max_commands_per_tick: 4,
        });
        let connected = table
            .connect(client_id, StationId::new(1), Tick::new(10))
            .expect("connect should work");
        table
            .disconnect(client_id, Tick::new(12))
            .expect("disconnect should work");
        assert!(matches!(
            table.route(client_id),
            Err(GatewayError::SessionDisconnected { .. })
        ));

        let bad = table
            .reconnect(client_id, connected.route.generation + 1, Tick::new(13))
            .expect_err("bad generation should fail");
        assert_eq!(
            bad,
            GatewayError::BadGeneration {
                expected: 1,
                actual: 2
            }
        );

        let reconnected = table
            .reconnect(client_id, connected.route.generation, Tick::new(14))
            .expect("reconnect should work");
        assert_eq!(
            reconnected.outcome,
            GatewayConnectOutcome::Reconnected {
                disconnected_for: 2
            }
        );
        assert_eq!(reconnected.route.generation, 1);

        table
            .disconnect(client_id, Tick::new(15))
            .expect("disconnect should work");
        assert_eq!(table.expire_disconnected(Tick::new(19)), 1);
        assert_eq!(table.len(), 0);
        assert_eq!(table.stats().sessions_expired, 1);
    }

    #[test]
    fn connect_replaces_stale_disconnected_session() {
        let client_id = ClientId::new(7);
        let mut table = GatewaySessionTable::new(GatewayConfig {
            max_sessions: 4,
            reconnect_grace_ticks: 3,
            max_commands_per_tick: 4,
        });
        let connected = table
            .connect(client_id, StationId::new(1), Tick::new(10))
            .expect("connect should work");
        table
            .admit_sequence(client_id, 10, Tick::new(11))
            .expect("first command should admit");
        table
            .disconnect(client_id, Tick::new(12))
            .expect("disconnect should work");

        let replaced = table
            .connect(client_id, StationId::new(2), Tick::new(20))
            .expect("stale reconnect should replace generation");
        assert_eq!(
            replaced.outcome,
            GatewayConnectOutcome::ReplacedExpired {
                disconnected_for: 8
            }
        );
        assert_eq!(replaced.route.generation, connected.route.generation + 1);
        assert_eq!(replaced.route.station_id, StationId::new(2));
        assert_eq!(
            table
                .admit_sequence(client_id, 1, Tick::new(21))
                .expect("new generation should reset sequence")
                .sequence,
            1
        );
    }

    #[test]
    fn command_admission_rejects_replay_and_rate_limit() {
        let client_id = ClientId::new(7);
        let mut table = GatewaySessionTable::new(GatewayConfig {
            max_sessions: 4,
            reconnect_grace_ticks: 3,
            max_commands_per_tick: 2,
        });
        table
            .connect(client_id, StationId::new(1), Tick::new(10))
            .expect("connect should work");

        let first = table
            .admit_command(&command(client_id, 1, 10))
            .expect("first command should admit");
        assert_eq!(first.commands_this_tick, 1);
        assert_eq!(first.route.station_id, StationId::new(1));

        let replay = table
            .admit_command(&command(client_id, 1, 10))
            .expect_err("same sequence should reject");
        assert_eq!(
            replay,
            GatewayError::ReplayOrStale {
                last_sequence: Some(1),
                sequence: 1
            }
        );
        assert_eq!(
            replay.command_reject_reason(),
            Some(CommandRejectReason::ReplayOrStale)
        );

        table
            .admit_command(&command(client_id, 2, 10))
            .expect("second command should admit");
        let limited = table
            .admit_command(&command(client_id, 3, 10))
            .expect_err("third same-tick command should rate limit");
        assert_eq!(
            limited,
            GatewayError::RateLimited {
                limit: 2,
                attempted: 3
            }
        );
        assert_eq!(
            limited.command_reject_reason(),
            Some(CommandRejectReason::RateLimited)
        );

        let next_tick = table
            .admit_command(&command(client_id, 3, 11))
            .expect("next tick should reset rate count");
        assert_eq!(next_tick.commands_this_tick, 1);
        assert_eq!(table.stats().commands_admitted, 3);
        assert_eq!(table.stats().commands_rejected_replay, 1);
        assert_eq!(table.stats().commands_rejected_rate_limit, 1);
    }

    #[test]
    fn capacity_limit_is_enforced() {
        let mut table = GatewaySessionTable::new(GatewayConfig {
            max_sessions: 1,
            reconnect_grace_ticks: 3,
            max_commands_per_tick: 4,
        });
        table
            .connect(ClientId::new(1), StationId::new(1), Tick::new(0))
            .expect("first session should fit");
        let error = table
            .connect(ClientId::new(2), StationId::new(1), Tick::new(0))
            .expect_err("second session should exceed capacity");
        assert_eq!(error, GatewayError::CapacityFull { capacity: 1 });
    }
}
