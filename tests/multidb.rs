//! Multidatabase registry (hakocluster#6): fixed-at-open, isolation,
//! unknown-always-None, writer independence, FFI roundtrip.
//!
//! N=1 per database everywhere (works on all platforms, no mesh
//! needed — isolation is a routing property, not a sync one).

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use hakocluster::ffi::*;
use hakocluster::{ClusterConfig, Databases, DbSpec};
use hakodb::config::DurabilityMode;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::query::query::Query;

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hako-multidb-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn spec(name: &str, dir: &std::path::Path) -> DbSpec {
    DbSpec {
        name: name.into(),
        paths: vec![dir.to_str().unwrap().into()],
        config: ClusterConfig {
            durability_mode: DurabilityMode::Interval,
            group_commit_interval_ms: 5,
            ..ClusterConfig::default()
        },
    }
}

fn put_kv(dbs: &Databases, db: &str, col: &str, id: &str, v: &str) {
    let c = dbs.get(db).unwrap();
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    let mut owned = HakoDoc::default();
    owned.fields = d.fields;
    owned._time = d._time;
    c.put_owned(col, id, owned).unwrap();
}

fn get_v(dbs: &Databases, db: &str, col: &str, id: &str) -> Option<String> {
    dbs.get(db)
        .unwrap()
        .get(col, id)
        .unwrap()
        .and_then(|d| d.get("v").cloned())
        .and_then(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
}

#[test]
fn registry_isolates_databases() {
    let root = tmp("iso");
    let a = tmp("iso-a");
    let b = tmp("iso-b");
    let dbs = Databases::open(
        vec![spec("billing", &a), spec("portal", &b)],
        root.clone(),
    )
    .unwrap();
    assert_eq!(dbs.names(), vec!["billing".to_string(), "portal".to_string()]);
    // Same collection+id, different values per database.
    put_kv(&dbs, "billing", "c", "k1", "one");
    put_kv(&dbs, "portal", "c", "k1", "two");
    assert_eq!(get_v(&dbs, "billing", "c", "k1").as_deref(), Some("one"));
    assert_eq!(get_v(&dbs, "portal", "c", "k1").as_deref(), Some("two"));
    // Unknown name is None (no default database).
    assert!(dbs.get("nope").is_none());
    // Epochs independent (both initial, separate counters).
    assert_eq!(dbs.get("billing").unwrap().epoch(), 0);
    assert_eq!(dbs.get("portal").unwrap().epoch(), 0);
    // Mesh boundary: per-db sock subdirs, never shared.
    assert!(root.join("billing").is_dir());
    assert!(root.join("portal").is_dir());
}

#[test]
fn registry_refuses_garbage() {
    let root = tmp("bad");
    let a = tmp("bad-a");
    let mk = |name: &str| spec(name, &a);
    // Empty registry.
    assert!(Databases::open(vec![], root.clone()).is_err());
    // Duplicate names.
    assert!(Databases::open(vec![mk("x"), mk("x")], root.clone()).is_err());
    // Bad names: empty, traversal, reserved prefix, spaces, overlong.
    for bad in ["", "a/b", "..", "__x", "a b", "a@b", &"x".repeat(129)] {
        assert!(
            Databases::open(vec![mk(bad)], root.clone()).is_err(),
            "name accepted: {bad:?}"
        );
    }
    // No paths.
    let mut s = mk("ok");
    s.paths.clear();
    assert!(Databases::open(vec![s], root.clone()).is_err());
}

// --- FFI (mirrors cluster_ffi.rs handle discipline) ---

fn cs(s: &str) -> *const c_char {
    CString::new(s).unwrap().into_raw() as *const c_char
}

/// Take ownership of a returned string (frees via hk_cluster_string_free
/// after copying out — mirrors what a C consumer does).
unsafe fn take(p: *mut c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let s = CStr::from_ptr(p).to_string_lossy().into_owned();
    hk_cluster_string_free(p);
    Some(s)
}

#[test]
fn ffi_multidb_roundtrip() {
    let da = tmp("ffi-a").to_string_lossy().replace('\\', "\\\\");
    let db = tmp("ffi-b").to_string_lossy().replace('\\', "\\\\");
    let root = tmp("ffi-root").to_string_lossy().replace('\\', "\\\\");
    let cfg = format!(
        r#"{{"sock_root":"{root}","databases":[{{"name":"a","paths":["{da}"]}},{{"name":"b","paths":["{db}"]}}]}}"#
    );
    let h = unsafe { hk_databases_open(cs(&cfg)) };
    assert!(!h.is_null());

    let names = unsafe { take(hk_databases_names(h)) }.unwrap();
    assert!(names.contains("\"a\"") && names.contains("\"b\""), "got: {names}");

    // Unknown name: null + error (no default).
    assert!(unsafe { hk_db_get(h, cs("nope")) }.is_null());
    assert!(!unsafe { hk_cluster_last_error() }.is_null());

    // Same cluster behind fresh boxes: write via one handle...
    let ha = unsafe { hk_db_get(h, cs("a")) };
    assert!(!ha.is_null());
    let id = unsafe { take(hk_cluster_put(ha, cs("c"), cs("k1"), cs(r#"{"v":"one"}"#))) };
    assert_eq!(id.as_deref(), Some("k1"));
    unsafe { hk_cluster_close(ha) };

    // ...read via another handle on the same database...
    let ha2 = unsafe { hk_db_get(h, cs("a")) };
    let doc = unsafe { take(hk_cluster_get(ha2, cs("c"), cs("k1"))) }.expect("get must hit");
    assert!(doc.contains(r#""v":"one""#), "got: {doc}");
    unsafe { hk_cluster_close(ha2) };

    // ...invisible from the other database (isolation across the ABI).
    let hb = unsafe { hk_db_get(h, cs("b")) };
    assert!(unsafe { take(hk_cluster_get(hb, cs("c"), cs("k1"))) }.is_none());
    unsafe { hk_cluster_close(hb) };

    unsafe { hk_databases_close(h) };
}

#[test]
fn ffi_multidb_rejects_garbage() {
    assert!(unsafe { hk_databases_open(cs("not-json")) }.is_null());
    assert!(!unsafe { hk_cluster_last_error() }.is_null());
    let root = tmp("ffi-badroot").to_string_lossy().replace('\\', "\\\\");
    let cfg = format!(r#"{{"sock_root":"{root}","databases":[]}}"#);
    assert!(unsafe { hk_databases_open(cs(&cfg)) }.is_null());
    let cfg = format!(
        r#"{{"sock_root":"{root}","databases":[{{"name":"x","paths":[]}}]}}"#
    );
    assert!(unsafe { hk_databases_open(cs(&cfg)) }.is_null());
}
