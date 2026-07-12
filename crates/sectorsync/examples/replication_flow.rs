//! Default direct-encode and borrowed-receive replication flow.

use std::{
    collections::VecDeque,
    convert::Infallible,
    net::{Ipv4Addr, SocketAddr},
};

use sectorsync::low_level::core::component::{ComponentMigrationMode, ComponentSyncMode};
use sectorsync::low_level::wire::ReplicationFrameBuilder;
use sectorsync::{
    low_level::transport::{InboundPacket, OutboundPacket, TransportReceiver, TransportSink},
    prelude::{
        Bounds, ClientId, CompiledSyncPolicy, ComponentDescriptor, ComponentId, ComponentSelection,
        EntityId, GridSpec, InstanceId, NodeId, PolicyId, PolicyTable, Position3,
        RangeOnlyVisibility, ReceiveExecutor, ReceiveExecutorConfig, ReplicationBudget,
        ReplicationExecutor, ReplicationExecutorConfig, ReplicationRequest, SpawnEntity,
        StationConfig, StationId, StationRuntime, StationRuntimeConfig, ViewerQuery,
    },
};

#[derive(Debug)]
struct Loopback {
    source: ClientId,
    packets: VecDeque<InboundPacket>,
}

impl TransportSink for Loopback {
    type Error = Infallible;

    fn send(&mut self, packet: OutboundPacket) -> Result<(), Self::Error> {
        self.packets.push_back(InboundPacket {
            client_id: Some(self.source),
            remote_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 30000)),
            bytes: packet.bytes,
        });
        Ok(())
    }
}

impl TransportReceiver for Loopback {
    type Error = Infallible;

    fn try_recv(&mut self) -> Result<Option<InboundPacket>, Self::Error> {
        Ok(self.packets.pop_front())
    }
}

fn main() {
    let mut station = StationRuntime::new(StationRuntimeConfig::new(
        StationConfig {
            station_id: StationId::new(1),
            node_id: NodeId::new(1),
            instance_id: InstanceId::new(1),
            tick_rate_hz: 20,
        },
        GridSpec::new(16.0).expect("grid"),
    ));
    let handle = station
        .spawn_owned(SpawnEntity::new(
            EntityId::new(1),
            Position3::new(1.0, 0.0, 0.0),
            Bounds::Point,
            PolicyId::new(1),
        ))
        .expect("spawn");
    let descriptor = ComponentDescriptor::sparse_blob(
        ComponentId::new(1),
        "health",
        ComponentSyncMode::Delta,
        ComponentMigrationMode::Copy,
        4,
    );
    station
        .set_component_blob(&descriptor, handle, 1, &[1, 2, 3, 4])
        .expect("component");

    let mut policies = PolicyTable::default();
    policies.set(CompiledSyncPolicy::new(PolicyId::new(1), 20, 20, 128.0));
    let selection = ComponentSelection {
        component_ids: vec![ComponentId::new(1)],
    };
    let client = ClientId::new(7);
    let server = ClientId::new(99);
    let viewer = ViewerQuery {
        client_id: client,
        position: Position3::new(0.0, 0.0, 0.0),
        radius: 128.0,
        max_entities: 16,
    };
    let mut transport = Loopback {
        source: server,
        packets: VecDeque::new(),
    };
    let mut replication = ReplicationExecutor::new(
        ReplicationExecutorConfig::throughput(ReplicationBudget {
            max_entities: 16,
            max_bytes: 4096,
            estimated_entity_bytes: 32,
        }),
        ReplicationFrameBuilder::default(),
    );
    let sent = replication
        .replicate(
            ReplicationRequest::new(
                &station,
                &policies,
                &selection,
                &viewer,
                &RangeOnlyVisibility,
            ),
            &mut transport,
        )
        .expect("send");

    let mut entities = 0;
    let mut receive =
        ReceiveExecutor::new(ReceiveExecutorConfig::new(client).with_expected_source(server));
    receive
        .pump(&mut transport, 1, |frame| {
            entities += frame.encoded_entity_count();
            Ok::<_, Infallible>(())
        })
        .expect("receive");

    println!("selected_entities={}", sent.selected_entities);
    println!("received_entities={entities}");
    println!("replication_flow_ok={}", entities == 1);
}
