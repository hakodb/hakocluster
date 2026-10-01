//! hakocluster Fase 1: open + read fan-out + designated-writer routing +
//! socket peering (design: hakodb/hakocluster#1).
//!
//! Peering tests are unix-only (socket_sync compiles out on Windows —
//! same rule as hakodb itself). Routing/fan-out shape tests run everywhere.

use std::time::{Duration, Instant};

use hakocluster::{Cluster, ClusterConfig};
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
        "hako-cluster-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn cfg(sock: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        durability_mode: DurabilityMode::Interval,
        group_commit_interval_ms: 5,
        sock_dir: sock.to_path_buf(),
        ..ClusterConfig::default()
    }
}

fn put_kv(c: &Cluster, col: &str, id: &str, v: &str) {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    let mut owned = HakoDoc::default();
    owned.fields = d.fields;
    owned._time = d._time;
    c.put_owned(col, id, owned).unwrap();
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
fn single_instance_open_works_everywhere() {
    let dir = tmp("solo");
    let sock = tmp("solo-sock");
    let c = Cluster::open_with_config(
        &[dir.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    assert_eq!(c.instance_count(), 1);
    put_kv(&c, "c", "k1", "one");
    assert_eq!(
        c.get("c", "k1").unwrap().unwrap().get("v"),
        Some(&Value::String("one".into()))
    );
    let rows = c.query(Query::new("c").limit(10)).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn writes_route_to_designated_writer() {
    let dir = tmp("w0");
    let sock = tmp("w0-sock");
    let c =
        Cluster::open_with_config(&[dir.to_str().unwrap()], cfg(&sock)).unwrap();
    put_kv(&c, "c", "k1", "one");
    // Immediate on the writer — no sync wait involved.
    assert_eq!(
        c.writer().get("c", "k1").unwrap().unwrap().get("v"),
        Some(&Value::String("one".into()))
    );
    c.delete("c", "k1").unwrap();
    assert!(c.writer().get("c", "k1").unwrap().is_none());
}

#[test]
fn read_fanout_distributes_evenly() {
    let dir = tmp("rr");
    let sock = tmp("rr-sock");
    let c =
        Cluster::open_with_config(&[dir.to_str().unwrap()], cfg(&sock)).unwrap();
    put_kv(&c, "c", "k1", "one");
    for _ in 0..30 {
        assert!(c.get("c", "k1").unwrap().is_some());
    }
    // Single instance serves all 30.
    assert_eq!(c.read_counts(), vec![30]);
}

#[cfg(not(unix))]
#[test]
fn multi_instance_without_peering_fails_closed() {
    let a = tmp("w-a");
    let b = tmp("w-b");
    let sock = tmp("w-sock");
    let err = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap_err();
    assert!(err.contains("unix"), "unexpected error: {err}");
}

#[cfg(unix)]
#[test]
fn two_instances_peer_and_replica_converges() {
    let a = tmp("p-a");
    let b = tmp("p-b");
    let sock = tmp("p-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    assert_eq!(c.instance_count(), 2);
    // One connection per pair, counted on both sides (dialer + accepter).
    poll_until("mesh peered", || c.peer_count() == 2);

    put_kv(&c, "c", "k1", "one");
    c.writer().flush().unwrap();
    // Replica trails the writer (no read-your-write across instances in
    // phase 1) but converges via snapshot/live tail.
    poll_until("replica k1", || {
        c.instances()[1].get("c", "k1").ok().flatten().is_some()
    });
    assert_eq!(
        c.instances()[1]
            .get("c", "k1")
            .unwrap()
            .unwrap()
            .get("v"),
        Some(&Value::String("one".into()))
    );

    // Delete converges too.
    c.delete("c", "k1").unwrap();
    c.writer().flush().unwrap();
    poll_until("replica delete k1", || {
        c.instances()[1].get("c", "k1").ok().flatten().is_none()
    });
}

#[cfg(unix)]
#[test]
fn fanout_spreads_reads_across_replicas() {
    let a = tmp("f-a");
    let b = tmp("f-b");
    let sock = tmp("f-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    put_kv(&c, "c", "k1", "one");
    c.writer().flush().unwrap();
    poll_until("replica k1", || {
        c.instances()[1].get("c", "k1").ok().flatten().is_some()
    });
    for _ in 0..30 {
        assert!(c.get("c", "k1").unwrap().is_some());
    }
    // Round-robin: 15/15.
    assert_eq!(c.read_counts(), vec![15, 15]);
}

#[test]
fn read_index_round_robins_single_node() {
    let dir = tmp("ri");
    let sock = tmp("ri-sock");
    let c =
        Cluster::open_with_config(&[dir.to_str().unwrap()], cfg(&sock)).unwrap();
    assert_eq!((c.read_index(), c.read_index()), (0, 0));
}

#[cfg(unix)]
#[test]
fn read_index_alternates_two_nodes() {
    let a = tmp("ri-a");
    let b = tmp("ri-b");
    let sock = tmp("ri-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();
    assert_eq!(
        (c.read_index(), c.read_index(), c.read_index(), c.read_index()),
        (0, 1, 0, 1)
    );
}
