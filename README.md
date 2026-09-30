# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas (~N× read
throughput), **writes route** (single-writer by design, multi-writer by
deployment — see issue #1).

Status: Fase 2 (see issue #1): stagger policy (same interval +
spaced opens, or per-instance intervals) + lag guard (eject past
`max_replica_lag_versions`, re-admit at half). Non-unix builds stay green:
peering compiles out, so N > 1 fails closed and N = 1 works as a
degenerate single-node cluster.
Engine prerequisites (shipped in hakodb): `sync_core`, `socket_sync`,
configurable `group_commit_interval_ms` (1..=30_000, default 5).

```toml
[dependencies]
hakocluster = "0.1"
```
