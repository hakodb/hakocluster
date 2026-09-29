# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas (~N× read
throughput), **writes route** (single-writer by design, multi-writer by
deployment — see issue #1).

Status: skeleton. The design lives in [issue #1](../../issues/1).
Engine prerequisites (shipped in hakodb): `sync_core`, `socket_sync`,
configurable `group_commit_interval_ms` (1..=30_000, default 5).

```toml
[dependencies]
hakocluster = "0.1"
```
