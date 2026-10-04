//! C ABI over JSON for non-Rust consumers (go, pascal, ...).
//!
//! Strings cross as NUL-terminated UTF-8; every returned `*mut c_char`
//! is heap-owned — free with [`hk_cluster_string_free`] ( Hakodb's
//! `hk_string_free` is a different symbol; both libs static-link their
//! own, so the cluster ABI carries its own to avoid collisions).
//! [`hk_cluster_last_error`]; data calls return null on error (and on
//! plain key-miss for get). Header: `hakocluster.h` (hand-written, the
//! API is small and stable).
//!
//! Query JSON: `{"collection":"b","where":{"field":"g","op":"eq",
//! "value":"g1"},"limit":20}` (`where` optional; only `eq` for now).
//! Config JSON: `{"durability":"interval","interval_ms":5,
//! "sock_dir":"...","max_lag":5000000,"stagger":"a",
//! "stagger_offset_ms":1}` — all keys optional; `"stagger":"b"` takes
//! `"intervals":[5,7]`, `"c"` selects ManualRotation (which additionally
//! requires `"durability":"manual"`).

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;

use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::query::query::Query;

use crate::{Cluster, ClusterConfig, Databases, DbSpec, StaggerPolicy, valid_db_name};

#[allow(non_camel_case_types)]
pub struct HK_Cluster {
    // ponytail: Arc (not owned Cluster) so registry lookups hand out
    // the SAME cluster behind a fresh box — one mesh, many handles.
    // Call sites deref unchanged.
    inner: Arc<Cluster>,
}

#[allow(non_camel_case_types)]
pub struct HK_Databases {
    inner: Databases,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

macro_rules! shield {
    ($fallback:expr, $block:block) => {
        match catch_unwind(AssertUnwindSafe(|| $block)) {
            Ok(val) => val,
            Err(_) => {
                set_last_error("CRITICAL: cluster panic, aborted");
                $fallback
            }
        }
    };
}

fn set_last_error(msg: impl Into<String>) {
    let m = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("ffi error").expect("static"));
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(m);
    });
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn cstr_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("null pointer".into());
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| "invalid utf8".into())
}

fn ok_string(s: String) -> *mut c_char {
    match CString::new(s) {
        Ok(c) => {
            clear_last_error();
            c.into_raw()
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

/// Free a string returned by any hk_cluster_* call.
#[no_mangle]
pub extern "C" fn hk_cluster_string_free(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            drop(CString::from_raw(value));
        }
    }
}

/// Last error text (borrowed; do not free; valid until the next call on
/// this thread). Null when the last call succeeded.
#[no_mangle]
pub extern "C" fn hk_cluster_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| match slot.borrow().as_ref() {
        Some(c) => c.as_ptr(),
        None => ptr::null(),
    })
}

fn parse_config(json: &str) -> Result<ClusterConfig, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| e.to_string())?;
    parse_config_value(&v)
}

// ponytail: Value-taking core so the registry reuses the single-cluster
// parser verbatim on per-db fragments (no second parser to drift).
fn parse_config_value(v: &serde_json::Value) -> Result<ClusterConfig, String> {
    let mut cfg = ClusterConfig::default();
    let get = |k: &str| v.get(k);
    if let Some(s) = get("durability").and_then(|x| x.as_str()) {
        cfg.durability_mode = match s {
            "manual" => hakodb::config::DurabilityMode::Manual,
            "always" => hakodb::config::DurabilityMode::Always,
            "oncommit" => hakodb::config::DurabilityMode::OnCommit,
            _ => hakodb::config::DurabilityMode::Interval,
        };
    }
    if let Some(ms) = get("interval_ms").and_then(|x| x.as_u64()) {
        cfg.group_commit_interval_ms = ms;
    }
    if let Some(d) = get("sock_dir").and_then(|x| x.as_str()) {
        cfg.sock_dir = d.into();
    }
    if let Some(m) = get("max_lag") {
        cfg.max_replica_lag_versions = m.as_u64();
    }
    if let Some(s) = get("stagger") {
        cfg.stagger = if let Some(arr) = s.get("intervals").and_then(|x| x.as_array()) {
            let mut iv = Vec::with_capacity(arr.len());
            for x in arr {
                iv.push(x.as_u64().ok_or("intervals must be numbers")?);
            }
            StaggerPolicy::PerInstance(iv)
        } else {
            match s.as_str().unwrap_or("a") {
                "c" => StaggerPolicy::ManualRotation,
                _ => StaggerPolicy::StaggeredStart {
                    offset_ms: get("stagger_offset_ms")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(1),
                },
            }
        };
    }
    Ok(cfg)
}

fn parse_paths(json: &str) -> Result<Vec<String>, String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| e.to_string())?;
    v.as_array()
        .ok_or_else(|| "paths must be a JSON array".to_string())
        .and_then(|arr| {
            arr.iter()
                .map(|x| {
                    x.as_str()
                        .map(|s| s.to_string())
                        .ok_or_else(|| "paths must be strings".to_string())
                })
                .collect()
        })
}

fn open_impl(paths_json: &str, cfg_json: Option<&str>) -> Result<*mut HK_Cluster, String> {
    let paths = parse_paths(paths_json)?;
    let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
    let cfg = match cfg_json {
        Some(j) => parse_config(j)?,
        None => ClusterConfig::default(),
    };
    // Default sock dir (next to paths[0]) only works when the caller
    // passes none; FFI callers always pass sock_dir explicitly — but keep
    // the Rust default path intact by requiring it here.
    if cfg.sock_dir.as_os_str().is_empty() {
        return Err("config needs sock_dir".into());
    }
    let cluster = Cluster::open_with_config(&refs, cfg).map_err(|e| e)?;
    Ok(Box::into_raw(Box::new(HK_Cluster { inner: Arc::new(cluster) })))
}

/// Open a cluster: `paths_json` = `["/data/a","/data/b"]`;
/// `config_json` = object per module docs, or null for defaults with
/// `./socks` (prefer explicit `sock_dir`). Null on error (see last_error).
#[no_mangle]
pub extern "C" fn hk_cluster_open(
    paths_json: *const c_char,
    sock_dir: *const c_char,
) -> *mut HK_Cluster {
    shield!(ptr::null_mut(), {
        let pj = match cstr_to_string(paths_json) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        // Convenience overload: default config + explicit sock dir (the
        // 90% consumer shape — one call, no config JSON to build).
        let cfg = if sock_dir.is_null() {
            ClusterConfig::default()
        } else {
            match cstr_to_string(sock_dir) {
                Ok(d) => ClusterConfig {
                    sock_dir: d.into(),
                    ..ClusterConfig::default()
                },
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            }
        };
        let paths = match parse_paths(&pj) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();
        match Cluster::open_with_config(&refs, cfg) {
            Ok(cluster) => {
                clear_last_error();
                Box::into_raw(Box::new(HK_Cluster { inner: Arc::new(cluster) }))
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

/// Full-config open: `config_json` per module docs (stagger/policies,
/// ManualRotation, lag guard). Null `config_json` = defaults.
#[no_mangle]
pub extern "C" fn hk_cluster_open_with_config(
    paths_json: *const c_char,
    config_json: *const c_char,
) -> *mut HK_Cluster {
    shield!(ptr::null_mut(), {
        let pj = match cstr_to_string(paths_json) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let cj = if config_json.is_null() {
            None
        } else {
            match cstr_to_string(config_json) {
                Ok(v) => Some(v),
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            }
        };
        match open_impl(&pj, cj.as_deref()) {
            Ok(h) => {
                clear_last_error();
                h
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

/// Close (also stops socket tasks on unix).
#[no_mangle]
pub extern "C" fn hk_cluster_close(handle: *mut HK_Cluster) {
    if !handle.is_null() {
        shield!((), {
            unsafe {
                drop(Box::from_raw(handle));
            }
        });
    }
}

/// Write a doc given as JSON object; returns the id. Engine stamps _time.
#[no_mangle]
pub extern "C" fn hk_cluster_put(
    handle: *mut HK_Cluster,
    collection: *const c_char,
    doc_id: *const c_char,
    doc_json: *const c_char,
) -> *mut c_char {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null cluster");
            return ptr::null_mut();
        }
        let (col, id, js) = match (
            cstr_to_string(collection),
            cstr_to_string(doc_id),
            cstr_to_string(doc_json),
        ) {
            (Ok(a), Ok(b), Ok(c)) => (a, b, c),
            _ => {
                set_last_error("bad string arg");
                return ptr::null_mut();
            }
        };
        let doc = match json_to_doc(&js) {
            Ok(d) => d,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.put_owned(&col, &id, doc) {
            Ok(assigned) => ok_string(assigned),
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

fn json_to_doc(js: &str) -> Result<HakoDoc, String> {
    let v: serde_json::Value =
        serde_json::from_str(js).map_err(|e| e.to_string())?;
    let obj = v.as_object().ok_or("doc must be a JSON object")?;
    // serde_json Map is a BTreeMap: iteration is sorted, which is exactly
    // the HakoDoc field order invariant. _time is engine-stamped on write.
    let mut fields = Vec::with_capacity(obj.len());
    for (k, val) in obj {
        if k == "_time" {
            continue;
        }
        let hv = Value::from_json(val.clone()).map_err(|e| e)?;
        fields.push((std::sync::Arc::from(k.as_str()), hv));
    }
    Ok(HakoDoc { fields, _time: 0 })
}

fn doc_to_json_str(doc: &HakoDoc) -> Result<String, String> {
    let mut out = Vec::with_capacity(256);
    doc.write_json(&mut out);
    String::from_utf8(out).map_err(|e| e.to_string())
}

/// Point read as JSON (with `_time`). Null on miss or error.
#[no_mangle]
pub extern "C" fn hk_cluster_get(
    handle: *mut HK_Cluster,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut c_char {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null cluster");
            return ptr::null_mut();
        }
        let (col, id) = match (cstr_to_string(collection), cstr_to_string(doc_id)) {
            (Ok(a), Ok(b)) => (a, b),
            _ => {
                set_last_error("bad string arg");
                return ptr::null_mut();
            }
        };
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.get(&col, &id) {
            Ok(Some(doc)) => match doc_to_json_str(&doc) {
                Ok(s) => ok_string(s),
                Err(e) => {
                    set_last_error(e);
                    ptr::null_mut()
                }
            },
            Ok(None) => {
                clear_last_error();
                ptr::null_mut()
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

/// Delete through the designated writer. Returns 0 ok, -1 error.
#[no_mangle]
pub extern "C" fn hk_cluster_delete(
    handle: *mut HK_Cluster,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    shield!(-1, {
        if handle.is_null() {
            set_last_error("null cluster");
            return -1;
        }
        let (col, id) = match (cstr_to_string(collection), cstr_to_string(doc_id)) {
            (Ok(a), Ok(b)) => (a, b),
            _ => {
                set_last_error("bad string arg");
                return -1;
            }
        };
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.delete(&col, &id) {
            Ok(_) => {
                clear_last_error();
                0
            }
            Err(e) => {
                set_last_error(e);
                -1
            }
        }
    })
}

/// Fan-out query. Returns `[{"id":"..","doc":{..}},...]` (ids deduped by
/// construction — one row per id from a single replica).
#[no_mangle]
pub extern "C" fn hk_cluster_query(
    handle: *mut HK_Cluster,
    query_json: *const c_char,
) -> *mut c_char {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null cluster");
            return ptr::null_mut();
        }
        let js = match cstr_to_string(query_json) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let q = match parse_query(&js) {
            Ok(q) => q,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.query(q) {
            Ok(rows) => {
                let mut arr = Vec::with_capacity(rows.len());
                for (id, doc) in &rows {
                    let ds = match doc_to_json_str(doc) {
                        Ok(s) => s,
                        Err(e) => {
                            set_last_error(e);
                            return ptr::null_mut();
                        }
                    };
                    let dv: serde_json::Value =
                        serde_json::from_str(&ds).unwrap_or(serde_json::Value::Null);
                    arr.push(serde_json::json!({"id": id, "doc": dv}));
                }
                ok_string(serde_json::Value::Array(arr).to_string())
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

fn parse_query(js: &str) -> Result<Query, String> {
    let v: serde_json::Value =
        serde_json::from_str(js).map_err(|e| e.to_string())?;
    let col = v
        .get("collection")
        .and_then(|x| x.as_str())
        .ok_or("query needs collection")?;
    let mut q = Query::new(col);
    if let Some(w) = v.get("where") {
        let field = w
            .get("field")
            .and_then(|x| x.as_str())
            .ok_or("where needs field")?;
        let op = w.get("op").and_then(|x| x.as_str()).unwrap_or("eq");
        let val = w.get("value").cloned().unwrap_or(serde_json::Value::Null);
        let hv = Value::from_json(val).map_err(|e| e)?;
        match op {
            "eq" => q = q.where_eq(field, hv),
            _ => return Err(format!("unsupported op '{op}' (eq only for now)")),
        }
    }
    if let Some(n) = v.get("limit").and_then(|x| x.as_u64()) {
        q = q.limit(n as usize);
    }
    Ok(q)
}

/// Manual failover. Returns the promotion epoch, -1 on error.
#[no_mangle]
pub extern "C" fn hk_cluster_promote(handle: *mut HK_Cluster, index: usize) -> i64 {
    shield!(-1, {
        if handle.is_null() {
            set_last_error("null cluster");
            return -1;
        }
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.promote(index) {
            Ok(e) => {
                clear_last_error();
                e as i64
            }
            Err(e) => {
                set_last_error(e);
                -1
            }
        }
    })
}

/// Current promotion epoch.
#[no_mangle]
pub extern "C" fn hk_cluster_epoch(handle: *mut HK_Cluster) -> u64 {
    shield!(0, {
        if handle.is_null() {
            return 0;
        }
        let c: &HK_Cluster = unsafe { &*handle };
        c.inner.epoch()
    })
}

/// Apply the lag guard + report health as JSON:
/// `[{"index":0,"lag":0,"healthy":true},...]`.
#[no_mangle]
pub extern "C" fn hk_cluster_refresh_health(handle: *mut HK_Cluster) -> *mut c_char {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null cluster");
            return ptr::null_mut();
        }
        let c: &HK_Cluster = unsafe { &*handle };
        let rep = c.inner.refresh_health();
        let arr: Vec<serde_json::Value> = rep
            .iter()
            .map(|r| {
                serde_json::json!({"index": r.index, "lag": r.lag_versions, "healthy": r.healthy})
            })
            .collect();
        ok_string(serde_json::Value::Array(arr).to_string())
    })
}

/// Option C rotation tick. Returns 0 ok, -1 error.
#[no_mangle]
pub extern "C" fn hk_cluster_tick_flush(handle: *mut HK_Cluster) -> i32 {
    shield!(-1, {
        if handle.is_null() {
            set_last_error("null cluster");
            return -1;
        }
        let c: &HK_Cluster = unsafe { &*handle };
        match c.inner.tick_flush() {
            Ok(_) => {
                clear_last_error();
                0
            }
            Err(e) => {
                set_last_error(e);
                -1
            }
        }
    })
}

// --- Multidatabase registry (hakocluster#6) ---
//
// Config JSON: `{"sock_root":"...","databases":[{"name":"billing",
// "paths":["/data/b1"],"config":{...}}]}` — per-db `config` reuses the
// module-docs shape; `sock_root` is required (each database meshes
// under `sock_root/{name}`, the mesh boundary). All keys required
// except per-db `config` (defaults).

fn parse_databases(json: &str) -> Result<(Vec<DbSpec>, String), String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|e| e.to_string())?;
    let root = v
        .get("sock_root")
        .and_then(|x| x.as_str())
        .ok_or("config needs sock_root")?
        .to_string();
    let arr = v
        .get("databases")
        .and_then(|x| x.as_array())
        .ok_or("config needs databases[]")?;
    let mut specs = Vec::with_capacity(arr.len());
    for d in arr {
        let name = d
            .get("name")
            .and_then(|x| x.as_str())
            .ok_or("database needs name")?;
        if !valid_db_name(name) {
            return Err(format!("bad database name `{name}`"));
        }
        let paths = d
            .get("paths")
            .and_then(|x| x.as_array())
            .ok_or_else(|| format!("database `{name}` needs paths[]"))?
            .iter()
            .map(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| format!("database `{name}` paths must be strings"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Reuse the single-cluster parser verbatim on the fragment.
        let cfg = match d.get("config") {
            Some(c) => parse_config_value(c)?,
            None => ClusterConfig::default(),
        };
        specs.push(DbSpec { name: name.to_string(), paths, config: cfg });
    }
    Ok((specs, root))
}

/// Open N named databases: returns the registry handle, null on error.
/// Fixed at open (reload re-opens); unknown names are always an error,
/// never a default.
#[no_mangle]
pub extern "C" fn hk_databases_open(config_json: *const c_char) -> *mut HK_Databases {
    shield!(ptr::null_mut(), {
        let js = match cstr_to_string(config_json) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let (specs, root) = match parse_databases(&js) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        match Databases::open(specs, root.into()) {
            Ok(dbs) => {
                clear_last_error();
                Box::into_raw(Box::new(HK_Databases { inner: dbs }))
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

/// Close the registry (stops every mesh).
#[no_mangle]
pub extern "C" fn hk_databases_close(handle: *mut HK_Databases) {
    if !handle.is_null() {
        shield!((), {
            unsafe {
                drop(Box::from_raw(handle));
            }
        });
    }
}

/// Look up a database by exact name: fresh `HK_Cluster` box over the
/// SAME cluster (one mesh, many handles — close with
/// `hk_cluster_close`). Null on unknown name (see last_error); there
/// is no default database.
#[no_mangle]
pub extern "C" fn hk_db_get(handle: *mut HK_Databases, name: *const c_char) -> *mut HK_Cluster {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null databases");
            return ptr::null_mut();
        }
        let n = match cstr_to_string(name) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };
        let dbs: &HK_Databases = unsafe { &*handle };
        match dbs.inner.get(&n) {
            Some(c) => {
                clear_last_error();
                Box::into_raw(Box::new(HK_Cluster { inner: c }))
            }
            None => {
                set_last_error(format!("unknown database `{n}`"));
                ptr::null_mut()
            }
        }
    })
}

/// Declared names in declaration order, as a JSON array string.
#[no_mangle]
pub extern "C" fn hk_databases_names(handle: *mut HK_Databases) -> *mut c_char {
    shield!(ptr::null_mut(), {
        if handle.is_null() {
            set_last_error("null databases");
            return ptr::null_mut();
        }
        let dbs: &HK_Databases = unsafe { &*handle };
        let arr: Vec<serde_json::Value> =
            dbs.inner.names().into_iter().map(serde_json::Value::String).collect();
        ok_string(serde_json::Value::Array(arr).to_string())
    })
}
