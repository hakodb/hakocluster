# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas (~N× read
throughput), **writes route** (single-writer by design, multi-writer by
deployment — see issue #1).

Status: Fase 3 (see issue #1): read-only enforcement (engine flag,
replicas fail closed, ingest unaffected) + manual failover
(`promote`) + Option C (`ManualRotation` + caller-driven `tick_flush`;
assessed: Manual flush of 2000 docs is 2ms Linux / 48ms Windows —
rotation at any sane cadence keeps up). Non-unix builds stay green:
peering compiles out, so N > 1 fails closed and N = 1 works as a
degenerate single-node cluster.
Engine prerequisites (shipped in hakodb): `sync_core`, `socket_sync`,
configurable `group_commit_interval_ms` (1..=30_000, default 5).

```toml
[dependencies]
hakocluster = "0.1"
```
