//! hakocluster Fase 2: stagger policy + lag guard (design:
//! hakodb/hakocluster#1, phase 2).
//!
//! Stagger timing test is unix-only (N>1 needs peering). Validation +
//! single-node shape tests run everywhere.

use std::time::{Duration, Instant};

use hakocluster::{Cluster, ClusterConfig, StaggerPolicy};
use hakodb::config::DurabilityMode;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hako-clu2-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn cfg(sock: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        sock_dir: sock.to_path_buf(),
        ..ClusterConfig::default()
    }
}

fn put_kv(c: &Cluster, col: &str, id: &str, v: &str) {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    c.put_owned(col, id, d).unwrap();
}

fn poll_until(label: &str, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(15) {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timeout waiting for {label}");
}

#[test]
fn stagger_policies_accepted_single_node() {
    let dir = tmp("s1");
    let sock = tmp("s1-sock");
    let mut c1 = cfg(&sock);
    c1.stagger = StaggerPolicy::StaggeredStart { offset_ms: 1 };
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], c1).unwrap();
    assert_eq!(c.instance_count(), 1);

    let dir = tmp("s2");
    let mut c2 = cfg(&sock);
    c2.stagger = StaggerPolicy::PerInstance(vec![5]);
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], c2).unwrap();
    assert_eq!(c.instance_count(), 1);
}

#[test]
fn per_instance_len_mismatch_fails_fast() {
    let dir = tmp("mm");
    let sock = tmp("mm-sock");
    let mut bad = cfg(&sock);
    // N=1 but two intervals supplied — refuse before opening anything.
    bad.stagger = StaggerPolicy::PerInstance(vec![5, 7]);
    let err = Cluster::open_with_config(&[dir.to_str().unwrap()], bad).unwrap_err();
    assert!(err.contains("PerInstance"), "unexpected error: {err}");
}

#[test]
fn health_guard_disabled_single_node() {
    let dir = tmp("h1");
    let sock = tmp("h1-sock");
    let mut c1 = cfg(&sock);
    c1.max_replica_lag_versions = None;
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], c1).unwrap();
    put_kv(&c, "c", "k1", "one");
    let rep = c.refresh_health();
    assert_eq!(rep.len(), 1);
    assert!(rep[0].healthy);
    assert_eq!(c.healthy_count(), 1);
}

#[cfg(unix)]
#[test]
fn staggered_start_spaces_opens() {
    let a = tmp("st-a");
    let b = tmp("st-b");
    let sock = tmp("st-sock");
    let mut c1 = cfg(&sock);
    c1.stagger = StaggerPolicy::StaggeredStart { offset_ms: 100 };
    let t = Instant::now();
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        c1,
    )
    .unwrap();
    // Lower bound only (sleep guarantees at-least) — deterministic.
    assert!(t.elapsed() >= Duration::from_millis(100));
    assert_eq!(c.instance_count(), 2);
}

#[cfg(unix)]
#[test]
fn lag_guard_ejects_and_readmits() {
    let a = tmp("lg-a");
    let b = tmp("lg-b");
    let sock = tmp("lg-sock");
    let mut c1 = cfg(&sock);
    // Custom interval => strict-time: no flush for 30s, so unflushed
    // buffered writes are deterministically invisible to the tailer.
    c1.group_commit_interval_ms = 30_000;
    c1.max_replica_lag_versions = Some(5);
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        c1,
    )
    .unwrap();
    poll_until("meshed", || c.peer_count() == 2);

    // 10 buffered writes, invisible to the tailer: the replica has no
    // "c" versions at all, so lag = the writer's write-micros (huge).
    for i in 0..10 {
        put_kv(&c, "c", &format!("k{i}"), "v");
    }
    let rep = c.refresh_health();
    assert!(rep[0].healthy);
    assert!(!rep[1].healthy, "replica should eject while far behind");
    assert!(rep[1].lag_versions > 5);
    assert_eq!(c.healthy_count(), 1);

    // Ejected replica serves nothing: all reads land on the writer.
    for _ in 0..10 {
        assert!(c.get("c", "k0").unwrap().is_some());
    }
    assert_eq!(c.read_counts(), vec![10, 0]);

    // Flush + converge + refresh: re-admitted (hysteresis: lag <= max/2).
    c.writer().flush().unwrap();
    poll_until("replica converged", || {
        c.instances()[1].get("c", "k9").ok().flatten().is_some()
    });
    let rep = c.refresh_health();
    assert!(rep[1].healthy);
    assert_eq!(c.healthy_count(), 2);
    for _ in 0..10 {
        assert!(c.get("c", "k0").unwrap().is_some());
    }
    assert_eq!(c.read_counts(), vec![15, 5]);
}
