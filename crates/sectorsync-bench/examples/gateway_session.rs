//! Gateway session/routing SDK example.

use std::collections::BTreeMap;

use sectorsync_core::prelude::{
    ClientId, CommandEnvelope, CommandId, CommandPriority, CommandQueueLimits, CommandQueues,
    EntityId, GatewayConfig, GatewayConnectOutcome, GatewayError, GatewaySessionTable, StationId,
    Tick,
};

fn main() {
    let client_id = ClientId::new(7);
    let station_one = StationId::new(1);
    let station_two = StationId::new(2);
    let mut gateway = GatewaySessionTable::new(GatewayConfig {
        max_sessions: 8,
        reconnect_grace_ticks: 3,
        max_commands_per_tick: 2,
    });
    let mut station_queues = station_command_queues([station_one, station_two]);

    let connected = gateway
        .connect(client_id, station_one, Tick::new(10))
        .expect("client should connect");
    assert_eq!(connected.outcome, GatewayConnectOutcome::Created);
    assert_eq!(connected.route.station_id, station_one);

    let first = command(client_id, 1, Tick::new(10));
    route_command(&mut gateway, &mut station_queues, first);
    assert_eq!(station_queues[&station_one].ready_len(), 1);

    let route = gateway
        .reroute(client_id, station_two, Tick::new(10))
        .expect("client should reroute");
    assert_eq!(route.station_id, station_two);

    route_command(
        &mut gateway,
        &mut station_queues,
        command(client_id, 2, Tick::new(10)),
    );
    let limited = gateway
        .admit_command(&command(client_id, 3, Tick::new(10)))
        .expect_err("third command in the same tick should rate limit");
    assert!(matches!(limited, GatewayError::RateLimited { .. }));
    route_command(
        &mut gateway,
        &mut station_queues,
        command(client_id, 3, Tick::new(11)),
    );
    assert_eq!(station_queues[&station_two].ready_len(), 2);

    gateway
        .disconnect(client_id, Tick::new(12))
        .expect("disconnect should work");
    assert!(matches!(
        gateway.route(client_id),
        Err(GatewayError::SessionDisconnected { .. })
    ));
    let reconnected = gateway
        .reconnect(client_id, connected.route.generation, Tick::new(14))
        .expect("client should reconnect inside grace");
    assert_eq!(
        reconnected.outcome,
        GatewayConnectOutcome::Reconnected {
            disconnected_for: 2
        }
    );
    route_command(
        &mut gateway,
        &mut station_queues,
        command(client_id, 4, Tick::new(14)),
    );

    let station_one_applied = drain_all(
        station_queues
            .get_mut(&station_one)
            .expect("station one queue should exist"),
    );
    let station_two_applied = drain_all(
        station_queues
            .get_mut(&station_two)
            .expect("station two queue should exist"),
    );
    assert_eq!(station_one_applied, vec![CommandId::new(1)]);
    assert_eq!(
        station_two_applied,
        vec![CommandId::new(2), CommandId::new(3), CommandId::new(4)]
    );
    let route_epoch = gateway
        .route(client_id)
        .expect("route should exist")
        .route_epoch;
    gateway
        .disconnect(client_id, Tick::new(15))
        .expect("client should disconnect");
    assert_eq!(gateway.expire_disconnected(Tick::new(19)), 1);

    println!(
        "gateway_session sessions={} expired={} generation={} route_epoch={} admitted={} rate_limited={} station_one_applied={} station_two_applied={}",
        gateway.len(),
        gateway.stats().sessions_expired,
        reconnected.route.generation,
        route_epoch,
        gateway.stats().commands_admitted,
        gateway.stats().commands_rejected_rate_limit,
        station_one_applied.len(),
        station_two_applied.len()
    );
}

fn station_command_queues<const N: usize>(
    station_ids: [StationId; N],
) -> BTreeMap<StationId, CommandQueues> {
    station_ids
        .into_iter()
        .map(|station_id| {
            (
                station_id,
                CommandQueues::new(CommandQueueLimits {
                    high: 4,
                    normal: 4,
                    low: 4,
                }),
            )
        })
        .collect()
}

fn command(client_id: ClientId, sequence: u64, received_at: Tick) -> CommandEnvelope {
    CommandEnvelope {
        id: CommandId::new(sequence),
        client_id,
        entity_id: EntityId::new(100),
        sequence,
        received_at,
        kind: 1,
        priority: CommandPriority::High,
        payload: b"move:north".to_vec(),
    }
}

fn route_command(
    gateway: &mut GatewaySessionTable,
    station_queues: &mut BTreeMap<StationId, CommandQueues>,
    command: CommandEnvelope,
) {
    let admission = gateway
        .admit_command(&command)
        .expect("gateway metadata should admit command");
    station_queues
        .get_mut(&admission.route.station_id)
        .expect("target station queue should exist")
        .push(command, sectorsync_core::prelude::CommandIngress::RUNNING)
        .expect("station command queue should accept command");
}

fn drain_all(queue: &mut CommandQueues) -> Vec<CommandId> {
    let mut applied = Vec::new();
    while let Some(command) = queue.pop_next() {
        applied.push(command.id);
    }
    applied
}
