# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas,
**writes route** (single-writer by design — see issue #1).

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
hakocluster = "0.3"
```

## Multidatabase: named clusters in one process

`Databases` holds N named clusters behind exact-name lookup — no
default database (a silent default would route writes somewhere the
operator did not choose). Each database meshes under
`sock_root/{name}` (the mesh boundary); bad/duplicate/empty
declarations, or any per-db open failure, refuse the whole registry.

```rust
let dbs = Databases::open(vec![
    DbSpec { name: "billing".into(), paths: vec!["/data/b1".into()], config: ClusterConfig::default() },
    DbSpec { name: "analytics".into(), paths: vec!["/data/a1".into(), "/data/a2".into()], config: ClusterConfig::default() },
], "/run/hc".into())?;
let billing = dbs.get("billing").expect("unknown name");
```

C ABI: `hk_databases_open(config_json)` (`{"sock_root":"…","databases":[{"name","paths","config"}]}`),
`hk_db_get` (fresh `HK_Cluster` handle over the same cluster),
`hk_databases_names`, `hk_databases_close`. Members are Hako data
dirs only — other engines (sqlite, …) are backend drivers, not
cluster members.

## Non-Rust consumers (go, pascal, ...)

The C ABI (`src/ffi.rs`, header `target/<profile>/hakocluster.h`,
cbindgen-generated every build) speaks JSON across the
boundary — docs and queries cross as UTF-8 strings, never as structs.
Binaries per tag live on the [releases page](../../releases): windows
`.dll`, linux `.so` per glibc family, macos `.dylib`, each bundled with
the header and `tests/ffi_smoke.c` (the consumer contract — build and
run it against your download first).

```go
// cgo sketch (see ffi_smoke.c for the full contract)
/*
#cgo LDFLAGS: -lhakocluster
#include "hakocluster.h"
*/
import "C"

// h := C.hk_cluster_open(C.CString(`["/data/a","/data/b"]`), C.CString("/run/hc"))
// js := C.hk_cluster_get(h, cc("b"), cc("k1")); defer C.hk_cluster_string_free(js)
```

```pascal
{ fpc sketch }
function hk_cluster_open(paths_json, sock_dir: PChar): Pointer; cdecl; external 'hakocluster';
procedure hk_cluster_string_free(p: PChar); cdecl; external 'hakocluster';
// h := hk_cluster_open('["/data/a","/data/b"]', '/run/hc');
```

N > 1 needs a unix host (socket peering); elsewhere open refuses N > 1
and N = 1 works as a single-node cluster. `stagger:"c"`
(ManualRotation) additionally requires `"durability":"manual"`.
