//! Focused deadline-indexed reliable retry benchmark.

use std::time::Instant;

use sectorsync_core::prelude::ClientId;
use sectorsync_transport::{
    FakeTransport, OutboundPacket, ReliableClientConfig, ReliableClientRetryScratch,
    ReliableClientSender,
};

const IN_FLIGHT: usize = 1_024;
const NON_DUE_POLLS: u64 = 32;
const RETRY_AFTER_TICKS: u64 = 64;

fn main() {
    let mut sender = ReliableClientSender::new(ReliableClientConfig {
        max_in_flight_per_peer: IN_FLIGHT,
        retry_after_ticks: RETRY_AFTER_TICKS,
        max_attempts: 2,
        max_payload_bytes: 32,
        max_delivered_history: 0,
    });
    let mut transport = FakeTransport::default();
    let peer = ClientId::new(7);
    for value in 0..IN_FLIGHT {
        sender
            .send(
                &mut transport,
                OutboundPacket {
                    client_id: peer,
                    bytes: u64::try_from(value)
                        .expect("guarded value fits u64")
                        .to_le_bytes()
                        .to_vec(),
                },
                0,
            )
            .expect("guarded reliable window should admit");
    }

    let mut scratch = ReliableClientRetryScratch::new();
    let started = Instant::now();
    let mut non_due_examined = 0_usize;
    for tick in 1..=NON_DUE_POLLS {
        let report = sender
            .retry_due_with_scratch(&mut transport, tick, &mut scratch)
            .expect("non-due retry poll should succeed");
        non_due_examined = non_due_examined.saturating_add(report.examined);
    }
    let non_due_elapsed = started.elapsed();

    let due_started = Instant::now();
    let due = sender
        .retry_due_with_scratch(&mut transport, RETRY_AFTER_TICKS, &mut scratch)
        .expect("due retry poll should succeed");
    let due_elapsed = due_started.elapsed();
    let benchmark_ok = non_due_examined == 0
        && due.examined == IN_FLIGHT
        && due.retried == IN_FLIGHT
        && due.timed_out == 0
        && sender.in_flight_len() == IN_FLIGHT;

    println!("SectorSync reliable retry benchmark");
    println!("in_flight={IN_FLIGHT}");
    println!("non_due_polls={NON_DUE_POLLS}");
    println!("non_due_examined={non_due_examined}");
    println!("due_examined={}", due.examined);
    println!("due_retried={}", due.retried);
    println!("due_timed_out={}", due.timed_out);
    println!("retained_due_capacity={}", scratch.retained_key_capacity());
    println!("non_due_elapsed_us={}", non_due_elapsed.as_micros());
    println!("due_elapsed_us={}", due_elapsed.as_micros());
    println!("benchmark_ok={benchmark_ok}");
    if !benchmark_ok {
        std::process::exit(1);
    }
}
