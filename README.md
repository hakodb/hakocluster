# hakocluster

In-process dispatcher over N `hakodb` instances sharing identical data
(kept so by `socket_sync`): **reads fan out** across replicas,
**writes route** to a single designated writer. Manual failover moves
the writer (`promote` returns an epoch; re-promote is idempotent, and
the fence stays the read-only flags). There is no automatic failover:
promotion is always operator-driven.

Scope: cluster, mesh, and replica only. Multidb routing and non-Hako
engines are out of scope — named databases hold Hako clusters, and
other engines are reached through their own drivers, never as members.

Non-unix builds stay green: peering compiles out, so N > 1 fails closed
and N = 1 works as a degenerate single-node cluster.

```toml
[dependencies]
hakocluster = "0.5"
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
dirs only.

## Sync tailer: one box to other boxes

A cluster can derive one extra (+1) member whose only extra job is
bridging outward, so serving members never touch cross-box latency.
The tailer meshes locally like every member but never serves fan-out
reads, is never promotable, and stays read-only.

Cargo features (opt-in, default build stays socket-only):

```toml
[dependencies]
hakocluster = { git = "https://github.com/hakodb/hakocluster", features = ["tailer-mesh"] }  # join a LAN mesh room
# hakocluster = { git = "https://github.com/hakodb/hakocluster", features = ["tailer-cloud"] } # join a hub, or host this box's room
```

```rust
let mut cfg = ClusterConfig::default();
cfg.tailer_mesh = Some(TailerMesh { name: "box-a".into(), room_key: "…".into(), port: 17891 });
// or: cfg.tailer_cloud = Some(TailerCloud { client_id, auth_token, mode: TailerCloudMode::Client { room_name, room_key, server_url } });
// or server mode: TailerCloudMode::Server { room_name, room_key, bind_addr }
let cluster = Cluster::open_with_config(&["/data/a", "/data/b"], cfg)?;
```

Rules, all fail-closed at open: mesh and cloud are exclusive; knobs
without their cargo feature refuse; any tailer off-unix refuses. C ABI
takes the same shape as JSON (`tailer_mesh` / `tailer_cloud` objects
in the open config; malformed shapes fail parse).

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
