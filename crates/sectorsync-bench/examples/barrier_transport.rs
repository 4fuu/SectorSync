//! Runtime barrier notification transport SDK example.

use sectorsync_core::prelude::{
    BarrierId, BarrierScope, BarrierState, ClientId, CommandQueueMode, InstanceId, NodeId, Station,
    StationConfig, Tick,
};
use sectorsync_runtime::{
    BarrierController, BarrierTransportBridge, ClientTransportBridge, ClientTransportConfig,
    StationSet,
};
use sectorsync_transport::{ClientTransportLimits, InMemoryTransportHub};

fn main() {
    let server_id = ClientId::new(0);
    let clients = [ClientId::new(7), ClientId::new(8)];
    let hub = InMemoryTransportHub::new(ClientTransportLimits {
        max_queued_packets_per_client: 4,
        max_packet_bytes: 512,
    });
    let mut server_transport = hub
        .endpoint(server_id, "127.0.0.1:26000".parse().expect("server addr"))
        .expect("server endpoint should register");
    let mut client_transports = clients
        .iter()
        .copied()
        .enumerate()
        .map(|(index, client_id)| {
            hub.endpoint(
                client_id,
                format!("127.0.0.1:{}", 26007 + index)
                    .parse()
                    .expect("client addr"),
            )
            .expect("client endpoint should register")
        })
        .collect::<Vec<_>>();
    let mut client_bridges = clients
        .iter()
        .copied()
        .map(|client_id| {
            ClientTransportBridge::new(
                ClientTransportConfig::new(client_id, server_id).with_expected_source(server_id),
            )
        })
        .collect::<Vec<_>>();

    let mut stations = StationSet::default();
    stations.push(station(1, 10));
    stations.push(station(2, 10));
    for station in stations.iter_mut() {
        station.advance_tick();
        station.advance_tick();
    }

    let mut controller = BarrierController::default();
    controller
        .request(
            &stations,
            BarrierId::new(5),
            BarrierScope::Instance(InstanceId::new(10)),
            Tick::new(2),
            CommandQueueMode::Buffer,
        )
        .expect("barrier should request");
    let frozen = controller.poll(&stations).expect("barrier should freeze");
    assert_eq!(frozen.state, BarrierState::Frozen);

    let mut barrier_transport = BarrierTransportBridge::default();
    let barrier = controller.active().expect("barrier should be active");
    let frozen_report = barrier_transport
        .broadcast_barrier(&mut server_transport, clients, barrier)
        .expect("frozen barrier should notify clients");

    let mut frozen_notices = 0;
    for (bridge, transport) in client_bridges.iter_mut().zip(&mut client_transports) {
        let pump = bridge
            .pump(transport, 2)
            .expect("client should receive frozen barrier");
        assert_eq!(pump.barriers[0].state, BarrierState::Frozen);
        frozen_notices += pump.barrier_frames_received();
    }

    let snapshots = controller
        .export_snapshots(
            &stations,
            sectorsync_core::prelude::SnapshotVersion::default(),
        )
        .expect("snapshots should export while frozen");
    let metrics = controller.resume().expect("barrier should resume");

    let running_report = barrier_transport
        .broadcast_state(
            &mut server_transport,
            clients,
            BarrierId::new(5),
            Tick::new(2),
            BarrierState::Running,
        )
        .expect("running barrier should notify clients");
    let mut running_notices = 0;
    for (bridge, transport) in client_bridges.iter_mut().zip(&mut client_transports) {
        let pump = bridge
            .pump(transport, 2)
            .expect("client should receive running barrier");
        assert_eq!(pump.barriers[0].state, BarrierState::Running);
        running_notices += pump.barrier_frames_received();
    }

    println!(
        "barrier_transport frozen_clients={} running_clients={} snapshots={} resumed_stations={} bytes={}",
        frozen_notices,
        running_notices,
        snapshots.len(),
        metrics.station_count,
        frozen_report.bytes_sent + running_report.bytes_sent
    );
}

fn station(station_id: u32, instance_id: u64) -> Station {
    Station::new(StationConfig {
        station_id: sectorsync_core::prelude::StationId::new(station_id),
        node_id: NodeId::new(0),
        instance_id: InstanceId::new(instance_id),
        tick_rate_hz: 20,
    })
}
