//! hakocluster Fase 3: read-only enforcement + manual failover +
//! Option C assessment hooks (design: hakodb/hakocluster#1, phase 3).
//!
//! N>1 tests are unix-only (peering). Single-node shape tests run
//! everywhere.

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
        "hako-clu3-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn cfg(sock: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        sock_dir: sock.to_path_buf(),
        ..ClusterConfig::default()
    }
}

fn kv(v: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    d
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
fn per_instance_intervals_observable_via_getter() {
    let dir = tmp("iv");
    let sock = tmp("iv-sock");
    let mut c1 = cfg(&sock);
    c1.stagger = StaggerPolicy::PerInstance(vec![42]);
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], c1).unwrap();
    assert_eq!(c.instances()[0].group_commit_interval_ms(), 42);
}

#[test]
fn promote_rejects_bad_index() {
    let dir = tmp("pm");
    let sock = tmp("pm-sock");
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], cfg(&sock)).unwrap();
    assert!(c.promote(5).is_err());
    assert_eq!(c.writer_index(), 0);
    // No-op self-promote on a single node.
    c.promote(0).unwrap();
    assert_eq!(c.writer_index(), 0);
}

#[test]
fn manual_rotation_requires_manual_durability() {
    let dir = tmp("mr");
    let sock = tmp("mr-sock");
    // Interval + ManualRotation: refuse (would silently never flush).
    let mut bad = cfg(&sock);
    bad.stagger = StaggerPolicy::ManualRotation;
    let err =
        Cluster::open_with_config(&[dir.to_str().unwrap()], bad).unwrap_err();
    assert!(err.contains("Manual"), "unexpected error: {err}");

    // Manual + ManualRotation: opens.
    let dir = tmp("mr2");
    let mut ok = cfg(&sock);
    ok.durability_mode = DurabilityMode::Manual;
    ok.stagger = StaggerPolicy::ManualRotation;
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], ok).unwrap();
    assert_eq!(c.instance_count(), 1);
}

#[test]
fn manual_flush_latency_smoke() {
    // Option C assessment anchor: time a real Manual flush of buffered
    // writes. Generous bound (regression tripwire, not a benchmark);
    // the printed ms is the assessment number.
    let dir = tmp("fl");
    let sock = tmp("fl-sock");
    let mut c1 = cfg(&sock);
    c1.durability_mode = DurabilityMode::Manual;
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], c1).unwrap();
    for i in 0..2000 {
        c.put_owned("c", &format!("k{i}"), kv("v")).unwrap();
    }
    let t = Instant::now();
    c.writer().flush().unwrap();
    let ms = t.elapsed().as_millis();
    println!("manual flush 2000 docs: {ms}ms");
    assert!(ms < 30_000, "flush pathologically slow: {ms}ms");
}

#[cfg(unix)]
#[test]
fn replicas_open_read_only_writer_stays_writable() {
    let a = tmp("ro-a");
    let b = tmp("ro-b");
    let sock = tmp("ro-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    // Direct local write to a replica refuses (fail-closed engine side).
    assert!(c.instances()[1].put_owned("c", "k9", kv("nope")).is_err());
    // Writer accepts; cluster routing works.
    c.put_owned("c", "k1", kv("one")).unwrap();
    assert!(c.writer().get("c", "k1").unwrap().is_some());
}

#[cfg(unix)]
#[test]
fn promote_switches_writer_and_replication_follows() {
    let a = tmp("pf-a");
    let b = tmp("pf-b");
    let sock = tmp("pf-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    poll_until("meshed", || c.peer_count() == 2);

    c.put_owned("c", "k0", kv("zero")).unwrap();
    c.writer().flush().unwrap();
    poll_until("k0 converged", || {
        c.instances()[1].get("c", "k0").ok().flatten().is_some()
    });

    // Manual failover: operator fenced the old writer first (no fencing
    // before phase 4 — documented).
    c.promote(1).unwrap();
    assert_eq!(c.writer_index(), 1);

    // Routes to the new writer; the old one refuses local writes now.
    c.put_owned("c", "k1", kv("one")).unwrap();
    assert!(c.instances()[1].get("c", "k1").unwrap().is_some());
    assert!(c.instances()[0].get("c", "k1").unwrap().is_none());
    assert!(c.instances()[0].put_owned("c", "kx", kv("nope")).is_err());

    // Read-only does NOT block replicated ingest: k1 still converges to
    // the old writer via the mesh.
    c.instances()[1].flush().unwrap();
    poll_until("k1 converged back", || {
        c.instances()[0].get("c", "k1").ok().flatten().is_some()
    });
}

#[cfg(unix)]
#[test]
fn manual_rotation_tick_flushes_replicas_into_view() {
    let a = tmp("tk-a");
    let b = tmp("tk-b");
    let sock = tmp("tk-sock");
    let mut c1 = cfg(&sock);
    c1.durability_mode = DurabilityMode::Manual;
    c1.stagger = StaggerPolicy::ManualRotation;
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        c1,
    )
    .unwrap();
    poll_until("meshed", || c.peer_count() == 2);

    // Buffered writes are invisible to the file tailer before any flush.
    c.put_owned("c", "k1", kv("one")).unwrap();
    c.put_owned("c", "k2", kv("two")).unwrap();
    std::thread::sleep(Duration::from_millis(700));
    assert!(c.instances()[1].get("c", "k1").unwrap().is_none());

    // One rotation tick flushes the writer; the replica converges.
    c.tick_flush().unwrap();
    poll_until("tick converged", || {
        c.instances()[1].get("c", "k1").ok().flatten().is_some()
    });
    assert!(c.instances()[1].get("c", "k2").unwrap().is_some());
}
