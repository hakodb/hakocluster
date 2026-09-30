//! Cluster C ABI over JSON (go/pascal consumers): open/put/get/query/
//! promote/health/tick through raw pointers. Same contract as the Rust
//! API; strings cross as UTF-8, freed with hk_string_free.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use hakocluster::ffi::*;

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

fn tmp(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("hako-cffi-{label}-{nanos}-{}", std::process::id()))
        .to_string_lossy()
        .into_owned()
}

#[test]
fn ffi_put_get_query_roundtrip() {
    let dir = tmp("rt");
    let sock = tmp("rt-sock");
    let paths = format!(r#"["{}"]"#, dir.replace('\\', "\\\\"));
    let h = unsafe { hk_cluster_open(cs(&paths), cs(&sock)) };
    assert!(!h.is_null());

    let id = unsafe {
        take(hk_cluster_put(
            h,
            cs("c"),
            cs("k1"),
            cs(r#"{"v":"one","n":7}"#),
        ))
    };
    assert_eq!(id.as_deref(), Some("k1"));

    let doc = unsafe { take(hk_cluster_get(h, cs("c"), cs("k1"))) };
    let doc = doc.expect("get must hit");
    assert!(doc.contains(r#""v":"one""#), "got: {doc}");
    assert!(doc.contains(r#""_time":"#), "got: {doc}");

    // Missing key reads null (no crash, no leak).
    assert!(unsafe { take(hk_cluster_get(h, cs("c"), cs("nope"))) }.is_none());

    let rows = unsafe {
        take(hk_cluster_query(
            h,
            cs(r#"{"collection":"c","where":{"field":"v","op":"eq","value":"one"},"limit":10}"#),
        ))
    }
    .expect("query must return");
    assert!(rows.contains("k1"), "got: {rows}");

    unsafe { hk_cluster_close(h) };
}

#[test]
fn ffi_promote_health_tick() {
    let dir = tmp("ph");
    let sock = tmp("ph-sock");
    let paths = format!(r#"["{}"]"#, dir.replace('\\', "\\\\"));
    let h = unsafe { hk_cluster_open(cs(&paths), cs(&sock)) };
    assert!(!h.is_null());

    assert_eq!(unsafe { hk_cluster_epoch(h) }, 0);
    assert_eq!(unsafe { hk_cluster_promote(h, 0) }, 0); // no-op self
    assert!(unsafe { hk_cluster_promote(h, 9) } < 0); // out of range
    let health = unsafe { take(hk_cluster_refresh_health(h)) }.unwrap();
    assert!(health.contains("healthy"), "got: {health}");
    assert_eq!(unsafe { hk_cluster_tick_flush(h) }, 0);

    unsafe { hk_cluster_close(h) };
}

#[test]
fn ffi_open_rejects_garbage() {
    let sock = tmp("bad-sock");
    assert!(unsafe { hk_cluster_open(cs("not-json"), cs(&sock)) }.is_null());
    assert!(!unsafe { hk_cluster_last_error() }.is_null());
}
