# Production Adapter Boundaries

SectorSync exposes low-level hooks for embedding into a production stack, but it
does not become that stack. This guide maps each hook to the external system
that must supply policy, credentials, endpoints, storage, or execution.

The examples use deterministic in-memory or localhost adapters. They prove the
contract and rejection behavior; they are not production implementations.

## Adapter Map

| SectorSync surface | External adapter supplies | SectorSync retains |
| --- | --- | --- |
| `TransportSink`, `TransportReceiver` | Socket/event-loop integration, endpoint lifecycle, IO errors | Packet/batch boundary and non-blocking contract |
| Station transport traits | Node endpoint mapping and actual network transport | Station source/target metadata and bounded packet flow |
| Reliable client/station helpers | When to poll/retry and how to react to exhaustion | Bounded windows, ACK/retry frames, timeout and duplicate accounting |
| `PacketAuthenticator` | Real MAC/signature algorithm and secret lookup | Envelope framing, tag budget, replay ordering |
| `PacketCipher` | Real authenticated encryption/confidentiality algorithm and secret lookup | In-place cipher hook ordering and payload budget |
| `PacketKeyRing` | Secret storage, key generation, distribution, certificate/KMS rotation | Bounded key-id lifecycle metadata and deterministic accept/send policy |
| `GatewaySessionTable` | Authenticated account/client identity and reconnect authorization | Bounded session, generation, replay, rate, and station-route metadata |
| `DeploymentRouteTable` | Service discovery snapshot, health policy, placement decisions | Bounded node/station metadata, route epochs, capacity, stale checks |
| Snapshot/barrier hooks | Durable write, schema migration policy, rollout decision | In-memory freeze/export/restore/resume invariants |
| Components/events | Business ECS, rule execution, persistence and GPU batch results | Bounded opaque storage/events and owner checks |

## Identity And Command Admission

Authenticate accounts, authorize reconnects, run anti-cheat, validate game
rules, and translate payloads before calling SectorSync. Only then associate an
external identity with a `ClientId` and call `GatewaySessionTable::connect`.

SectorSync can reject missing/disconnected sessions, stale generations, replayed
sequences, rate limits, missing station queues, and missing deployment routes.
It cannot decide whether a credential, account, movement, purchase, or combat
action is legitimate.

Do not place raw access tokens, account records, or anti-cheat state inside
gateway session metadata.

## Packet Authentication And Encryption

`PacketSecurityBox` owns bounded framing, nonces, tag/payload limits, and replay
history. The embedding stack supplies both `PacketAuthenticator` and
`PacketCipher` implementations. Those implementations may call a vetted crypto
library, HSM, KMS-backed key cache, or another integration-owned provider.

High-rate send paths should retain `PacketSecurityScratch` and use the `*_into`
sealing APIs. SectorSync then reuses ciphertext and tag buffers while leaving
the final transport packet caller-owned. Scratch contains transient packet
material and must follow the embedding application's memory-clearing policy;
SectorSync does not claim secure erasure or own secret-memory management.
High-rate receive paths may similarly retain `PacketSecurityOpenScratch` and
use the scratch opening APIs. Its plaintext remains allocated after the
borrowed view expires, so the same application-owned clearing and retention
policy applies. Use owned opening when the payload must cross the scratch
lifetime or ownership boundary.

`PacketKeyRing` contains key ids and lifecycle metadata only. It never contains
secret bytes. Keep these operations external:

- Generate/import secret material.
- Distribute keys to peers.
- Rotate certificates or KMS versions.
- Choose the production algorithm and nonce/key derivation policy.
- Persist or revoke credentials across process restarts.

`secure_command_ingress` injects an external authenticator and a deliberately
insecure reversible test cipher to prove both hooks. `secure_key_rotation`
demonstrates active/retiring/revoked key-id metadata. Neither example is a
cryptographic recommendation.

Client adapters that consume replication packets synchronously may use
`BinaryFrameDecoder::decode_replication_ref` to avoid nested frame allocations.
Borrowed component bytes are valid only while the immutable input packet is
alive; queueing, cross-thread ownership transfer, or deferred application
requires an owned frame or application-owned copy. The specialized decoder
validates the complete frame before exposing iterators but does not interpret
game component schemas or update the client world.

`ReplicationReceiveBridge::pump_visit` is the higher-level immediate-apply
path when adapters still use `TransportReceiver`. It preserves bridge
source/target checks and reports caller application failures separately.
Visitor code must finish consuming borrowed bytes before returning and must not
retain references in client state. Use owned pumping at queues, replay buffers,
cross-thread handoff, and deferred-application boundaries.

The explicit `PlaintextPacketCipher` is only appropriate when another trusted
layer already provides confidentiality or for tests that need authentication
framing alone.

## Transport Integration

Production client and station transports should implement the sink/receiver
traits at packet or batch granularity. A receiver must return `Ok(None)` when no
packet is ready; it must not block a station tick. Enforce bounded packet bytes,
queue depth, and work per pump call in the adapter.

The standard UDP adapters offer borrowed `try_recv_ref` variants for immediate
synchronous consumption. Their byte slices alias the configured internal
receive buffer and expire on the next mutable adapter operation. This removes
per-datagram owned payload materialization but does not remove socket syscall,
kernel buffering, or application decode costs. Use owned receiver traits at
queue and cross-thread ownership boundaries.

Keep these concerns outside the standard UDP and in-memory adapters:

- Connection establishment and reconnect loops.
- NAT traversal and relay selection.
- TLS/DTLS/QUIC session ownership.
- DNS/service discovery and endpoint watches.
- Hidden retry queues or durable replay logs.

SectorSync reliable helpers are optional framing primitives, not a production
connection manager. When their in-flight window or retry budget is exhausted,
surface the error to the external connection policy.

## Route Discovery And Placement

Treat `DeploymentRouteTable` as a bounded in-memory projection of external
control-plane decisions:

1. External discovery/placement selects nodes and station assignments.
2. The adapter calls `register_node`, `heartbeat`, `assign_station`,
   `move_station`, `mark_draining`, or `mark_offline`.
3. Gateway admission resolves the current station/node route and stamps route
   epochs into delivery metadata.
4. External transport maps the resolved `NodeId` to a concrete endpoint.
5. Missing/stale routes return errors; SectorSync does not discover or fail over
   nodes automatically.

`deployment_routing` demonstrates injected node/station metadata, draining,
route movement, heartbeat staleness, and offline state.
`gateway_deployment_dispatch` demonstrates route resolution followed by bounded
station packet delivery while preserving the gateway-stamped command tick.

## Persistence, Upgrade, And GPU Work

SectorSync snapshots are in-memory values. An external adapter may serialize or
persist them, but durable storage, crash recovery, backups, and failover remain
outside the runtime. Barrier hooks preserve freeze/restore ordering; they do not
load scripts or decide rollout policy.

GPU or batch business systems may read caller-owned inputs, compute externally,
and feed validated component/state/event results back through authoritative
APIs. SectorSync does not schedule accelerators, own GPU memory, or run kernels.

## Failure Policy

| Failure | External response |
| --- | --- |
| Authentication or game-rule failure | Reject before SectorSync admission. |
| Packet auth/cipher failure | Drop, count, and apply external security/audit policy. |
| Replay or revoked/expired key id | Reject; refresh external key/control state if appropriate. |
| Transport queue or byte limit | Apply backpressure or reduce bounded batch size. |
| Unknown packet source/target | Drop and count; do not silently rewrite metadata. |
| Missing station/node route | Refresh external discovery snapshot; do not invent a route. |
| Draining/offline node | External placement chooses a target, then updates route metadata. |
| Durable write/restore failure | Keep barrier/error state explicit; do not claim resume succeeded. |

Never solve an adapter failure by adding an unbounded queue, hidden blocking
wait, implicit retry loop, or business fallback inside SectorSync.

## Verification

Run the bounded adapter examples:

```bash
cargo run -p sectorsync-bench --example secure_command_ingress
cargo run -p sectorsync-bench --example secure_key_rotation
cargo run -p sectorsync-bench --example reliable_command_ingress
cargo run -p sectorsync-bench --example reliable_station_event
cargo run -p sectorsync-bench --example deployment_routing
cargo run -p sectorsync-bench --example gateway_deployment_dispatch
```

All examples are local and bounded. They require no external network service,
credential provider, cloud API, or durable database.
