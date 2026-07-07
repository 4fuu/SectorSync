//! UDP station event SDK example.

use std::thread;
use std::time::Duration;

use sectorsync_core::prelude::{
    EventId, EventKind, EventPriority, InstanceId, NodeId, Station, StationConfig, StationEvent,
    StationId, Tick,
};
use sectorsync_runtime::{
    EventRouter, StationEventPumpReport, StationEventTransportBridge, StationScheduler, StationSet,
};
use sectorsync_transport::UdpStationTransport;

fn main() {
    let source_station = StationId::new(1);
    let target_station = StationId::new(2);
    let mut source_transport = UdpStationTransport::bind(source_station, "127.0.0.1:0")
        .expect("source station transport should bind");
    let mut target_transport = UdpStationTransport::bind(target_station, "127.0.0.1:0")
        .expect("target station transport should bind");
    let source_addr = source_transport
        .local_addr()
        .expect("source address should exist");
    let target_addr = target_transport
        .local_addr()
        .expect("target address should exist");

    source_transport.register_station(target_station, target_addr);
    target_transport.register_station(source_station, source_addr);

    let mut stations = StationSet::default();
    stations.push(station(source_station));
    stations.push(station(target_station));

    let mut router = EventRouter::default();
    router.register_stations(&stations);
    let mut bridge = StationEventTransportBridge::default();
    let event = StationEvent {
        id: EventId::new(200),
        source: source_station,
        target: target_station,
        source_tick: Tick::new(0),
        target_tick: Tick::new(2),
        priority: EventPriority::Critical,
        kind: EventKind::HandoffPrepare {
            entity_id: sectorsync_core::prelude::EntityId::new(900),
        },
    };

    bridge
        .send_event(&mut source_transport, &event)
        .expect("source station should send event through UDP");
    let pump = pump_with_retry(
        &mut bridge,
        &mut target_transport,
        &mut router,
        target_station,
    );
    assert_eq!(pump.events_routed, 1);

    let mut scheduler = StationScheduler::default();
    scheduler.advance_all(&mut stations);
    assert!(
        scheduler
            .drain_ready_events(&stations, &mut router)
            .expect("early drain should work")
            .is_empty()
    );
    scheduler.advance_all(&mut stations);
    let drained = scheduler
        .drain_ready_events(&stations, &mut router)
        .expect("target tick drain should work");
    assert_eq!(drained, vec![event]);

    println!(
        "udp_station_event sent_events={} source_packets={} target_packets={} routed={} drained={}",
        bridge.stats().events_sent,
        source_transport.stats().packets_sent,
        target_transport.stats().packets_received,
        router.stats().routed_events,
        router.stats().drained_events
    );
}

fn pump_with_retry(
    bridge: &mut StationEventTransportBridge,
    transport: &mut UdpStationTransport,
    router: &mut EventRouter,
    target_station: StationId,
) -> StationEventPumpReport {
    for _ in 0..50 {
        let report = bridge
            .pump_target(transport, router, target_station, 4)
            .expect("target station should pump UDP packet");
        if report.events_routed > 0 {
            return report;
        }
        thread::sleep(Duration::from_millis(2));
    }
    panic!("udp station event was not pumped");
}

fn station(station_id: StationId) -> Station {
    Station::new(StationConfig {
        station_id,
        node_id: NodeId::new(0),
        instance_id: InstanceId::new(1),
        tick_rate_hz: 20,
    })
}
