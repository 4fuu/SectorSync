//! Borrowed visitor-first replication receive path.

use sectorsync_runtime::{
    ReplicationReceiveBridge, ReplicationReceiveConfig, ReplicationReceiveError,
    ReplicationReceivePump, ReplicationReceiveStats, ReplicationReceiveVisitError,
    ReplicationReceiveVisitReport,
};
use sectorsync_transport::TransportReceiver;
use sectorsync_wire::ReplicationFrameRef;

/// Replication receiver that validates source and target metadata.
#[derive(Clone, Debug)]
pub struct ReceiveExecutor {
    bridge: ReplicationReceiveBridge,
}

impl ReceiveExecutor {
    /// Creates a borrowed visitor-first receiver.
    pub const fn new(config: ReplicationReceiveConfig) -> Self {
        Self {
            bridge: ReplicationReceiveBridge::new(config),
        }
    }

    /// Returns accumulated receive statistics.
    pub const fn stats(&self) -> ReplicationReceiveStats {
        self.bridge.stats()
    }

    /// Receives and visits borrowed replication frames without payload copies.
    pub fn pump<T, F, V>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
        visitor: F,
    ) -> Result<ReplicationReceiveVisitReport, ReplicationReceiveVisitError<T::Error, V>>
    where
        T: TransportReceiver,
        F: for<'frame> FnMut(ReplicationFrameRef<'frame>) -> Result<(), V>,
    {
        self.bridge.pump(transport, max_packets, visitor)
    }

    /// Receives replication frames into owned nested storage for retention.
    pub fn pump_owned<T>(
        &mut self,
        transport: &mut T,
        max_packets: usize,
    ) -> Result<ReplicationReceivePump, ReplicationReceiveError<T::Error>>
    where
        T: TransportReceiver,
    {
        self.bridge.pump_owned(transport, max_packets)
    }
}

/// Product receive configuration.
pub use sectorsync_runtime::ReplicationReceiveConfig as ReceiveExecutorConfig;
