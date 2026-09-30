# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas (~N× read
throughput), **writes route** (single-writer by design, multi-writer by
deployment — see issue #1).

Status: Fase 4 (see issue #1): promotion epochs + audit log
(`promote` returns the epoch, idempotent re-promote; fence stays the
read-only flags). Lease granting / auto-failover deliberately NOT here:
needs balancer HA first (see hakobalancer#2). Non-unix builds stay green:
peering compiles out, so N > 1 fails closed and N = 1 works as a
degenerate single-node cluster.
Engine prerequisites (shipped in hakodb): `sync_core`, `socket_sync`,
configurable `group_commit_interval_ms` (1..=30_000, default 5).

```toml
[dependencies]
hakocluster = "0.1"
```
