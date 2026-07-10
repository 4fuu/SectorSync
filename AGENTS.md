# Agent Instructions for SectorSync

## Verification

Run the release-quality gate for completed changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo run -q -p sectorsync-bench -- --profile=smoke
git diff --check
```

- Keep routine checks smoke-safe. Medium, large, and oversized manual profiles
  require an explicit reason and `--allow-heavy`.
- Keep manual `--entities`, `--clients`, `--stations`, and `--ticks` values
  clamped to smoke-safe limits unless `--allow-heavy` is present. Print applied
  guard metadata.
- When benchmark fields, thresholds, or baseline logic change, run the
  `sectorsync`, `full`, `room`, and `naive-grid` smoke baselines.
- Public SDK changes require a focused test and the relevant executable example.

Use the closest examples for focused verification:

| Area | Examples |
| --- | --- |
| End-to-end SDK | `sdk_flow` |
| Replication and client flow | `replication_bridge`, `replication_bridge_priority`, `client_bridge` |
| Gateway and deployment | `gateway_session`, `gateway_command_pipeline`, `gateway_deployment_dispatch`, `deployment_routing` |
| Commands and security hooks | `command_ingress`, `reliable_command_ingress`, `secure_command_ingress`, `secure_key_rotation` |
| Station events and UDP | `station_event_transport`, `udp_station_event`, `reliable_station_event`, `udp_loopback` |
| Visibility and policy | `frustum_visibility`, `tag_visibility`, `adaptive_cadence`, `priority_budget` |
| Allocation and tracking | `scratch_planning`, `replication_tracker` |
| Load and migration | `load_sampling`, `load_scheduler`, `split_tuning`, `split_migration` |
| Barriers and upgrades | `barrier_transport`, `barrier_upgrade` |
| Component schemas | `generated_schema` |

Benchmark output must retain machine-readable latency percentiles, command and
router pressure/drop counters, replication selection/byte/scratch fields,
split/scheduler decisions, threshold verdicts, and `benchmark_ok`.

## Project Boundary

SectorSync is a high-performance embedded Rust middleware for spatial real-time
entity replication. It is not a game engine, ECS framework, full game-server
framework, or production cluster platform.

SectorSync owns:

- 3D cell topology, spatial indexing, AOI, and visibility hooks.
- Entity authority, read-only ghosts, handoff, and cell migration primitives.
- Compiled sync policy, cadence, priority, replication planning, and bounded
  tracking.
- Bounded command/event queues and frame/packet-level transport adapters.
- Runtime barriers, in-memory snapshots, load sampling, hotspot planning, and
  deterministic station scheduling.
- Low-level gateway, deployment-route, security-policy, and integration hooks.

The embedding application owns:

- Game rules, combat, inventory, quests, economy, and gameplay ECS state.
- Account authentication, anti-cheat, matchmaking, and production gateways.
- Durable persistence, crash recovery, backup, and failover.
- Service discovery, process supervision, cloud APIs, and cluster scheduling.
- Production cryptographic algorithms, key services, and certificate stores.
- GPU kernels, accelerator scheduling, and mandatory GPU runtimes.

## Architecture Invariants

### Authority and State

- Every entity has exactly one authoritative owner station. Ghost entities are
  read-only and cannot make final state changes.
- Two-phase handoff prewarms target ghosts before owner commit and leaves the
  previous owner as a short-lived ghost afterward.
- Cell migration updates ownership metadata and both spatial indexes while
  preserving source ghost visibility during handoff.
- Station-local APIs must preserve owner, dirty, replication-budget, barrier,
  and event-ordering checks.
- Component APIs remain low-level descriptors, codecs, and stores; they must not
  turn SectorSync into a mandatory ECS or runtime-reflection framework.

### Replication and Scheduling

- Hot paths should remain allocation-light and dependency-light. Compiled policy
  data must not introduce scripts, per-entity dynamic dispatch, or avoidable
  hash-map work.
- Caller-owned scratch may retain candidate, deduplication, matched-cell, and
  sorting capacity. Do not add thread-local scratch, global caches, or implicit
  cross-client state.
- Adaptive cell queries must choose deterministically from query volume and
  occupancy, and occupied-cell scans must restore grid order before collection.
- Cadence and priority planning stays stateless. Per-client last-sent state,
  business priority, and client-world state remain caller-owned.
- Replication tracking stays explicit and bounded and must not invent wire ACK
  semantics or clear global dirty state implicitly.
- Split and station schedulers remain deterministic and conservative: bounded
  actions, bounded moved cells, cooldown/capacity/improvement guards, and
  preference for lower-pressure targets.
- Load sampling may read station/index/router state plus caller subscriber
  counts. It must not infer gameplay semantics, collect OS metrics, place
  processes, execute GPU work, or add hidden threads.

### Commands and Runtime

- Command and event queues are bounded and expose backpressure. Barrier-buffer
  overflow must fail explicitly, and failed release retains blocked commands.
- External validation, schema translation, anti-cheat, and game-rule checks run
  before the SectorSync command pipeline.
- Cross-station events are tick-boundary ordered and use station identifiers and
  bounded station packet queues. Do not add distributed transactions.
- Runtime barriers preserve request, tick alignment, freeze, snapshot/migrate,
  and resume ordering.
- Upgrade hooks operate only on frozen in-memory snapshots. They do not load
  scripts, own plugin systems, persist snapshots, or bypass barrier checks.
- SDK flows surface queue, route, barrier, and transport failures instead of
  hiding retries or side buffers.

### Transport and Security

- Wire and transport abstractions operate at frame, packet, or batch boundaries,
  not through per-entity network calls.
- Receivers validate source/target metadata before routing commands, events, or
  replication frames.
- Transport adapters are non-blocking or externally bounded. Do not add hidden
  waits, reconnect loops, unbounded buffers, or implicit service discovery.
- Reliable helpers preserve payload and in-flight limits, retry attempts,
  timeout accounting, source identity, and bounded duplicate suppression.
- UDP station adapters represent one local station and reject endpoint
  mismatches. UDP tests stay localhost-only with bounded retries.
- Security helpers provide envelopes, key metadata, nonces, replay windows, and
  authenticator/cipher traits. Secret storage, algorithms, rotation services,
  certificates, and account auth stay external.
- Examples may use illustrative security providers only when clearly labeled as
  non-production.

### Gateway and Deployment

- Gateway/session code remains bounded metadata and admission logic: sessions,
  generations, route epochs, replay checks, expiry, and per-client limits.
- Gateway command pipelines may decode frames, admit metadata, enqueue bounded
  commands, produce ACKs, and resolve deployment delivery metadata.
- Deployment routes remain caller-managed metadata. Do not add process control,
  cloud integration, durable cluster state, automatic failover, or RPC systems.
- Client bridges do not own authentication, reconnect, NAT traversal, blocking
  IO, client world state, or game payload interpretation.

## Documentation and Release

- Keep `README.md` aligned with project scope, crate layout, supported workflows,
  MSRV, and public SDK behavior.
- Keep focused design and integration detail under `docs/`; avoid duplicating
  large command inventories or stale implementation summaries.
- Update this file when architectural invariants or release gates change.
- Validate workflow changes with
  `go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12`.
- The automatic release workflow runs daily at 08:00 Asia/Hong_Kong and may
  also be dispatched manually. It releases only when `main` is not fully
  represented by the current GitHub Release and crates.io versions.
- Automatic releases use the unpadded Asia/Hong_Kong calendar version
  `YYYY.M.D` and publish at most once per local day. Additional same-day commits
  wait for the next day. Keep all internal crate requirements exact and equal,
  and update README installation versions in the generated release commit.
- Keep crate publication ordered as core, wire, transport, then runtime, and
  keep retries bounded and safe to resume after a partially completed
  publication.
- Scheduled crates.io publishing uses only OIDC Trusted Publishing with a
  short-lived token. Do not add a long-lived crates.io token to repository
  secrets; bootstrap new crate names manually before configuring publishers.
- Published crate archives must include README and MIT LICENSE files.

## Git Rules

- Use focused commits for meaningful milestones and inspect `git status --short`
  before committing.
- Do not rewrite, discard, or overwrite changes that are not part of the task.
- Before a public push, audit tracked/untracked and ignored files, large/history
  blobs, credentials, private keys, personal paths, generated artifacts, and
  placeholder URLs.
