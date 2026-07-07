//! Low-level deployment routing and station placement primitives.

use std::collections::{BTreeMap, BTreeSet};

use sectorsync_core::prelude::{NodeId, StationId, Tick};

/// Deployment route table configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeploymentConfig {
    /// Maximum registered nodes.
    pub max_nodes: usize,
    /// Maximum stations accepted by one node unless the node advertises a
    /// lower capacity.
    pub max_stations_per_node: usize,
    /// Ticks without heartbeat before a node is considered stale.
    pub stale_after_ticks: u64,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_stations_per_node: 1024,
            stale_after_ticks: 20 * 10,
        }
    }
}

/// Node availability state for placement decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentNodeState {
    /// Node may receive new station placements.
    Online,
    /// Node keeps existing station placements but should not receive new ones.
    Draining,
    /// Node is unavailable for station routes.
    Offline,
}

/// Route metadata for one node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeploymentNodeRoute {
    /// Node id.
    pub node_id: NodeId,
    /// Node availability state.
    pub state: DeploymentNodeState,
    /// Last heartbeat tick.
    pub last_heartbeat: Tick,
    /// Station capacity advertised for this node.
    pub station_capacity: usize,
    /// Current assigned station count.
    pub assigned_stations: usize,
    /// Route epoch incremented when node state/capacity changes.
    pub route_epoch: u64,
}

/// Route metadata for one station.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeploymentStationRoute {
    /// Station id.
    pub station_id: StationId,
    /// Node currently hosting the station.
    pub node_id: NodeId,
    /// Route epoch incremented on station placement changes.
    pub route_epoch: u64,
    /// Tick at which this placement was last assigned.
    pub assigned_at: Tick,
}

/// Result of moving a station route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeploymentStationMove {
    /// Previous station route.
    pub previous: DeploymentStationRoute,
    /// New station route.
    pub current: DeploymentStationRoute,
}

/// Deployment route table statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeploymentStats {
    /// Nodes registered.
    pub nodes_registered: usize,
    /// Nodes marked draining.
    pub nodes_draining: usize,
    /// Nodes marked offline.
    pub nodes_offline: usize,
    /// Station assignments created.
    pub stations_assigned: usize,
    /// Station routes moved.
    pub stations_moved: usize,
    /// Station routes removed.
    pub stations_unassigned: usize,
    /// Placement attempts rejected by capacity.
    pub placements_rejected_capacity: usize,
    /// Stale nodes detected.
    pub stale_nodes_detected: usize,
}

/// Deployment route error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeploymentError {
    /// Node table is full.
    NodeCapacityFull {
        /// Configured node table capacity.
        capacity: usize,
    },
    /// Node does not exist.
    MissingNode(NodeId),
    /// Station route does not exist.
    MissingStation(StationId),
    /// Node cannot accept new placements in its current state.
    NodeUnavailable {
        /// Node id.
        node_id: NodeId,
        /// Current state.
        state: DeploymentNodeState,
    },
    /// Node station capacity would be exceeded.
    NodeStationCapacity {
        /// Node id.
        node_id: NodeId,
        /// Configured/adverised station capacity.
        capacity: usize,
        /// Attempted station count.
        attempted: usize,
    },
}

impl core::fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NodeCapacityFull { capacity } => {
                write!(f, "deployment node table is full at capacity {capacity}")
            }
            Self::MissingNode(node_id) => {
                write!(f, "deployment node {} is missing", node_id.get())
            }
            Self::MissingStation(station_id) => {
                write!(f, "deployment station {} is missing", station_id.get())
            }
            Self::NodeUnavailable { node_id, state } => write!(
                f,
                "deployment node {} cannot accept placements in state {state:?}",
                node_id.get()
            ),
            Self::NodeStationCapacity {
                node_id,
                capacity,
                attempted,
            } => write!(
                f,
                "deployment node {} station capacity exceeded: capacity {capacity}, attempted {attempted}",
                node_id.get()
            ),
        }
    }
}

impl std::error::Error for DeploymentError {}

#[derive(Clone, Debug)]
struct DeploymentNodeRecord {
    route: DeploymentNodeRoute,
    stations: BTreeSet<StationId>,
}

impl DeploymentNodeRecord {
    fn new(node_id: NodeId, station_capacity: usize, now: Tick) -> Self {
        Self {
            route: DeploymentNodeRoute {
                node_id,
                state: DeploymentNodeState::Online,
                last_heartbeat: now,
                station_capacity,
                assigned_stations: 0,
                route_epoch: 1,
            },
            stations: BTreeSet::new(),
        }
    }

    fn refresh_assigned_count(&mut self) {
        self.route.assigned_stations = self.stations.len();
    }
}

/// Bounded station-to-node deployment route table.
#[derive(Clone, Debug)]
pub struct DeploymentRouteTable {
    config: DeploymentConfig,
    nodes: BTreeMap<NodeId, DeploymentNodeRecord>,
    stations: BTreeMap<StationId, DeploymentStationRoute>,
    stats: DeploymentStats,
}

impl DeploymentRouteTable {
    /// Creates an empty deployment route table.
    pub fn new(config: DeploymentConfig) -> Self {
        Self {
            config,
            nodes: BTreeMap::new(),
            stations: BTreeMap::new(),
            stats: DeploymentStats::default(),
        }
    }

    /// Returns configuration.
    pub const fn config(&self) -> DeploymentConfig {
        self.config
    }

    /// Returns statistics.
    pub const fn stats(&self) -> DeploymentStats {
        self.stats
    }

    /// Returns registered node count.
    pub fn node_len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns station route count.
    pub fn station_len(&self) -> usize {
        self.stations.len()
    }

    /// Registers or refreshes a node.
    pub fn register_node(
        &mut self,
        node_id: NodeId,
        station_capacity: usize,
        now: Tick,
    ) -> Result<DeploymentNodeRoute, DeploymentError> {
        let station_capacity = station_capacity.min(self.config.max_stations_per_node);
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.route.last_heartbeat = now;
            node.route.state = DeploymentNodeState::Online;
            if node.route.station_capacity != station_capacity {
                node.route.station_capacity = station_capacity;
                node.route.route_epoch = node.route.route_epoch.saturating_add(1);
            }
            node.refresh_assigned_count();
            return Ok(node.route);
        }

        if self.nodes.len() >= self.config.max_nodes {
            return Err(DeploymentError::NodeCapacityFull {
                capacity: self.config.max_nodes,
            });
        }

        let node = DeploymentNodeRecord::new(node_id, station_capacity, now);
        let route = node.route;
        self.nodes.insert(node_id, node);
        self.stats.nodes_registered = self.stats.nodes_registered.saturating_add(1);
        Ok(route)
    }

    /// Updates a node heartbeat.
    pub fn heartbeat(
        &mut self,
        node_id: NodeId,
        now: Tick,
    ) -> Result<DeploymentNodeRoute, DeploymentError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        node.route.last_heartbeat = now;
        Ok(node.route)
    }

    /// Marks a node draining. Existing station routes remain valid.
    pub fn mark_draining(
        &mut self,
        node_id: NodeId,
    ) -> Result<DeploymentNodeRoute, DeploymentError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        if node.route.state != DeploymentNodeState::Draining {
            node.route.state = DeploymentNodeState::Draining;
            node.route.route_epoch = node.route.route_epoch.saturating_add(1);
            self.stats.nodes_draining = self.stats.nodes_draining.saturating_add(1);
        }
        Ok(node.route)
    }

    /// Marks a node offline. Station routes are retained for explicit
    /// remediation by the embedder.
    pub fn mark_offline(
        &mut self,
        node_id: NodeId,
    ) -> Result<DeploymentNodeRoute, DeploymentError> {
        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        if node.route.state != DeploymentNodeState::Offline {
            node.route.state = DeploymentNodeState::Offline;
            node.route.route_epoch = node.route.route_epoch.saturating_add(1);
            self.stats.nodes_offline = self.stats.nodes_offline.saturating_add(1);
        }
        Ok(node.route)
    }

    /// Assigns a station to an online node.
    pub fn assign_station(
        &mut self,
        station_id: StationId,
        node_id: NodeId,
        now: Tick,
    ) -> Result<DeploymentStationRoute, DeploymentError> {
        self.ensure_node_can_accept(node_id)?;

        if let Some(existing) = self.stations.get(&station_id).copied() {
            if existing.node_id == node_id {
                return Ok(existing);
            }
            return self
                .move_station(station_id, node_id, now)
                .map(|move_report| move_report.current);
        }

        let node = self
            .nodes
            .get_mut(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        let attempted = node.stations.len().saturating_add(1);
        if attempted > node.route.station_capacity {
            self.stats.placements_rejected_capacity =
                self.stats.placements_rejected_capacity.saturating_add(1);
            return Err(DeploymentError::NodeStationCapacity {
                node_id,
                capacity: node.route.station_capacity,
                attempted,
            });
        }

        node.stations.insert(station_id);
        node.refresh_assigned_count();
        let route = DeploymentStationRoute {
            station_id,
            node_id,
            route_epoch: 1,
            assigned_at: now,
        };
        self.stations.insert(station_id, route);
        self.stats.stations_assigned = self.stats.stations_assigned.saturating_add(1);
        Ok(route)
    }

    /// Moves an existing station route to another online node.
    pub fn move_station(
        &mut self,
        station_id: StationId,
        target_node: NodeId,
        now: Tick,
    ) -> Result<DeploymentStationMove, DeploymentError> {
        self.ensure_node_can_accept(target_node)?;
        let previous = self
            .stations
            .get(&station_id)
            .copied()
            .ok_or(DeploymentError::MissingStation(station_id))?;
        if previous.node_id == target_node {
            return Ok(DeploymentStationMove {
                previous,
                current: previous,
            });
        }

        {
            let target = self
                .nodes
                .get(&target_node)
                .ok_or(DeploymentError::MissingNode(target_node))?;
            let attempted = target.stations.len().saturating_add(1);
            if attempted > target.route.station_capacity {
                self.stats.placements_rejected_capacity =
                    self.stats.placements_rejected_capacity.saturating_add(1);
                return Err(DeploymentError::NodeStationCapacity {
                    node_id: target_node,
                    capacity: target.route.station_capacity,
                    attempted,
                });
            }
        }

        if let Some(source) = self.nodes.get_mut(&previous.node_id) {
            source.stations.remove(&station_id);
            source.refresh_assigned_count();
        }
        let target = self
            .nodes
            .get_mut(&target_node)
            .ok_or(DeploymentError::MissingNode(target_node))?;
        target.stations.insert(station_id);
        target.refresh_assigned_count();

        let current = DeploymentStationRoute {
            station_id,
            node_id: target_node,
            route_epoch: previous.route_epoch.saturating_add(1),
            assigned_at: now,
        };
        self.stations.insert(station_id, current);
        self.stats.stations_moved = self.stats.stations_moved.saturating_add(1);
        Ok(DeploymentStationMove { previous, current })
    }

    /// Removes one station route.
    pub fn unassign_station(
        &mut self,
        station_id: StationId,
    ) -> Result<DeploymentStationRoute, DeploymentError> {
        let route = self
            .stations
            .remove(&station_id)
            .ok_or(DeploymentError::MissingStation(station_id))?;
        if let Some(node) = self.nodes.get_mut(&route.node_id) {
            node.stations.remove(&station_id);
            node.refresh_assigned_count();
        }
        self.stats.stations_unassigned = self.stats.stations_unassigned.saturating_add(1);
        Ok(route)
    }

    /// Returns a node route.
    pub fn node_route(&self, node_id: NodeId) -> Result<DeploymentNodeRoute, DeploymentError> {
        self.nodes
            .get(&node_id)
            .map(|node| node.route)
            .ok_or(DeploymentError::MissingNode(node_id))
    }

    /// Returns a station route.
    pub fn station_route(
        &self,
        station_id: StationId,
    ) -> Result<DeploymentStationRoute, DeploymentError> {
        self.stations
            .get(&station_id)
            .copied()
            .ok_or(DeploymentError::MissingStation(station_id))
    }

    /// Returns station ids assigned to a node.
    pub fn stations_on_node(&self, node_id: NodeId) -> Result<Vec<StationId>, DeploymentError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        Ok(node.stations.iter().copied().collect())
    }

    /// Returns stale node ids without mutating node state.
    pub fn stale_nodes(&mut self, now: Tick) -> Vec<NodeId> {
        let stale = self
            .nodes
            .iter()
            .filter_map(|(node_id, node)| {
                (node.route.state != DeploymentNodeState::Offline
                    && now.get().saturating_sub(node.route.last_heartbeat.get())
                        > self.config.stale_after_ticks)
                    .then_some(*node_id)
            })
            .collect::<Vec<_>>();
        self.stats.stale_nodes_detected =
            self.stats.stale_nodes_detected.saturating_add(stale.len());
        stale
    }

    /// Marks stale nodes offline.
    pub fn mark_stale_offline(&mut self, now: Tick) -> usize {
        let stale = self.stale_nodes(now);
        for node_id in &stale {
            let _ = self.mark_offline(*node_id);
        }
        stale.len()
    }

    fn ensure_node_can_accept(&self, node_id: NodeId) -> Result<(), DeploymentError> {
        let node = self
            .nodes
            .get(&node_id)
            .ok_or(DeploymentError::MissingNode(node_id))?;
        match node.route.state {
            DeploymentNodeState::Online => Ok(()),
            DeploymentNodeState::Draining | DeploymentNodeState::Offline => {
                Err(DeploymentError::NodeUnavailable {
                    node_id,
                    state: node.route.state,
                })
            }
        }
    }
}

impl Default for DeploymentRouteTable {
    fn default() -> Self {
        Self::new(DeploymentConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DeploymentConfig {
        DeploymentConfig {
            max_nodes: 2,
            max_stations_per_node: 2,
            stale_after_ticks: 3,
        }
    }

    #[test]
    fn registers_nodes_and_assigns_station_routes() {
        let mut table = DeploymentRouteTable::new(config());
        let node = table
            .register_node(NodeId::new(1), 2, Tick::new(10))
            .expect("node should register");
        assert_eq!(node.route_epoch, 1);
        assert_eq!(node.state, DeploymentNodeState::Online);

        let route = table
            .assign_station(StationId::new(11), NodeId::new(1), Tick::new(10))
            .expect("station should assign");
        assert_eq!(route.node_id, NodeId::new(1));
        assert_eq!(route.route_epoch, 1);
        assert_eq!(
            table.stations_on_node(NodeId::new(1)).expect("node exists"),
            vec![StationId::new(11)]
        );
        assert_eq!(
            table
                .node_route(NodeId::new(1))
                .expect("node route should exist")
                .assigned_stations,
            1
        );
    }

    #[test]
    fn enforces_node_and_station_capacity() {
        let mut table = DeploymentRouteTable::new(DeploymentConfig {
            max_nodes: 1,
            max_stations_per_node: 1,
            stale_after_ticks: 3,
        });
        table
            .register_node(NodeId::new(1), 2, Tick::new(0))
            .expect("first node should register");
        assert_eq!(
            table
                .register_node(NodeId::new(2), 1, Tick::new(0))
                .expect_err("second node should exceed capacity"),
            DeploymentError::NodeCapacityFull { capacity: 1 }
        );
        table
            .assign_station(StationId::new(10), NodeId::new(1), Tick::new(0))
            .expect("first station should fit");
        assert_eq!(
            table
                .assign_station(StationId::new(11), NodeId::new(1), Tick::new(0))
                .expect_err("second station should exceed node capacity"),
            DeploymentError::NodeStationCapacity {
                node_id: NodeId::new(1),
                capacity: 1,
                attempted: 2
            }
        );
        assert_eq!(table.stats().placements_rejected_capacity, 1);
    }

    #[test]
    fn draining_nodes_keep_routes_but_reject_new_placements() {
        let mut table = DeploymentRouteTable::new(config());
        table
            .register_node(NodeId::new(1), 2, Tick::new(0))
            .expect("node should register");
        table
            .assign_station(StationId::new(10), NodeId::new(1), Tick::new(0))
            .expect("station should assign");
        let draining = table
            .mark_draining(NodeId::new(1))
            .expect("node should drain");
        assert_eq!(draining.state, DeploymentNodeState::Draining);
        assert_eq!(
            table
                .station_route(StationId::new(10))
                .expect("existing route remains")
                .node_id,
            NodeId::new(1)
        );
        assert_eq!(
            table
                .assign_station(StationId::new(11), NodeId::new(1), Tick::new(1))
                .expect_err("draining node should reject new placement"),
            DeploymentError::NodeUnavailable {
                node_id: NodeId::new(1),
                state: DeploymentNodeState::Draining
            }
        );
    }

    #[test]
    fn moves_station_routes_between_nodes() {
        let mut table = DeploymentRouteTable::new(config());
        table
            .register_node(NodeId::new(1), 2, Tick::new(0))
            .expect("source node should register");
        table
            .register_node(NodeId::new(2), 2, Tick::new(0))
            .expect("target node should register");
        table
            .assign_station(StationId::new(10), NodeId::new(1), Tick::new(0))
            .expect("station should assign");

        let moved = table
            .move_station(StationId::new(10), NodeId::new(2), Tick::new(5))
            .expect("station should move");
        assert_eq!(moved.previous.node_id, NodeId::new(1));
        assert_eq!(moved.current.node_id, NodeId::new(2));
        assert_eq!(moved.current.route_epoch, 2);
        assert!(
            table
                .stations_on_node(NodeId::new(1))
                .expect("source exists")
                .is_empty()
        );
        assert_eq!(
            table
                .stations_on_node(NodeId::new(2))
                .expect("target exists"),
            vec![StationId::new(10)]
        );
    }

    #[test]
    fn detects_and_marks_stale_nodes_offline() {
        let mut table = DeploymentRouteTable::new(config());
        table
            .register_node(NodeId::new(1), 2, Tick::new(10))
            .expect("node should register");
        assert!(table.stale_nodes(Tick::new(13)).is_empty());
        assert_eq!(table.stale_nodes(Tick::new(14)), vec![NodeId::new(1)]);
        assert_eq!(table.mark_stale_offline(Tick::new(14)), 1);
        assert_eq!(
            table
                .node_route(NodeId::new(1))
                .expect("node route should exist")
                .state,
            DeploymentNodeState::Offline
        );
    }
}
