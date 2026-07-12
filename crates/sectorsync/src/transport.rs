//! Product transport boundaries and bounded in-memory adapters.

pub use sectorsync_transport::{
    ClientTransportLimits, InMemoryTransportEndpoint, InMemoryTransportError, InMemoryTransportHub,
    InboundPacket, OutboundPacket, TransportReceiver, TransportSink,
};
