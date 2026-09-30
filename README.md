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
hakocluster = "0.2"
```

## Non-Rust consumers (go, pascal, ...)

The C ABI (`src/ffi.rs`, header `hakocluster.h`) speaks JSON across the
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
