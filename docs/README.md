# SectorSync Documentation

This directory contains integration contracts, performance evidence, and the
boundary between SectorSync and the embedding application. API-level details
belong in rustdoc; release history belongs in `CHANGELOG.md`.

## Choose a Guide

| Goal | Document |
| --- | --- |
| Embed SectorSync in a game or simulation server | [SDK integration](sdk-integration.md) |
| Configure and interpret benchmarks | [Performance acceptance](performance-acceptance.md) |
| Connect security, transport, routing, storage, or infrastructure | [Production adapters](production-adapters.md) |
| Check delivered scope and explicit non-goals | [Delivery status](gaps.md) |

## Recommended Reading Order

1. Read the root [README](../README.md) for scope, installation, and a minimal
   spatial-index example.
2. Follow [SDK integration](sdk-integration.md) to establish ownership,
   capacities, per-tick ordering, and failure handling.
3. Use [Production adapters](production-adapters.md) when connecting external
   services or security providers.
4. Run the smoke profile and consult [Performance acceptance](performance-acceptance.md)
   before tuning thresholds or attempting heavier workloads.

## Documentation Rules

- Keep API signatures and type-level behavior in rustdoc rather than duplicating
  them here.
- Keep executable command inventories beside the workflow they verify.
- Treat local timings as regression evidence, not cross-machine capacity claims.
- Preserve the middleware boundary: game rules and production infrastructure
  remain external.
- Update or remove stale evidence when benchmark workloads or output contracts
  change.
