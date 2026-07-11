# SectorSync Delivery Status

SectorSync's basic embedded SDK is complete. This page records the delivered
boundary and prevents future work from expanding the middleware into a game
engine or production platform.

## Delivered

- Authority, ghosts, handoff, 3D spatial indexing, AOI, visibility, compiled
  sync policy, replication planning, and bounded tracking.
- Bounded command/event queues, binary frames, in-memory/UDP/reliable transport
  primitives, gateway admission, and deployment-route metadata.
- Runtime barriers, in-memory snapshots, load sampling, conservative hotspot
  planning, cell migration, and deterministic station scheduling.
- A cohesive integration flow, production-adapter contracts, guarded
  performance acceptance, executable examples, and public API documentation.
- CI coverage for formatting, strict Clippy, all-feature tests, rustdoc, smoke
  benchmarks, MSRV, packaging, and ordered crate publication.

The detailed contracts live in the [SDK integration guide](sdk-integration.md),
[production adapter boundaries](production-adapters.md), and
[performance acceptance matrix](performance-acceptance.md).

## Explicit Non-Goals

SectorSync does not own:

- Game rules, gameplay ECS state, combat, inventory, quests, or economy.
- Account authentication, anti-cheat, matchmaking, or reconnect policy.
- Durable persistence, crash recovery, backups, or failover orchestration.
- Process supervision, service discovery, cloud APIs, or cluster scheduling.
- Production cryptographic algorithms, key services, or certificate storage.
- GPU kernels, accelerator scheduling, or mandatory GPU runtimes.
- Dynamic script, WASM, or plugin loading inside the core runtime.

These concerns may integrate through explicit external adapters; they are not
missing SDK features.

## Future Work

Accept future work when it does at least one of the following:

- Improves a general-purpose hot path with reproducible, guarded evidence.
- Tightens boundedness, determinism, error visibility, or integration safety.
- Adds a low-level adapter contract required by multiple applications.
- Clarifies public SDK behavior without duplicating rustdoc or release history.

Reject or keep external work that introduces business semantics, hidden
threads, unbounded state, durable infrastructure, or automatic cluster policy.
