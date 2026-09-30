//! hakocluster Fase 4: promotion epochs + promotion log (design:
//! hakodb/hakocluster#1, phase 4; lease granting itself needs balancer
//! HA first — see hakobalancer, filed separately).
//!
//! Epochs order promotions for operators; the actual fence stays the
//! read-only flags from phase 3. No auto-failover in this phase.

use hakocluster::{Cluster, ClusterConfig};

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hako-clu4-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn cfg(sock: &std::path::Path) -> ClusterConfig {
    ClusterConfig {
        sock_dir: sock.to_path_buf(),
        ..ClusterConfig::default()
    }
}

#[test]
fn epoch_starts_zero_and_self_promote_is_noop() {
    let dir = tmp("ep");
    let sock = tmp("ep-sock");
    let c = Cluster::open_with_config(&[dir.to_str().unwrap()], cfg(&sock)).unwrap();
    assert_eq!(c.epoch(), 0);
    assert!(c.promotion_log().is_empty());
    // Promoting the current writer changes nothing (idempotent).
    assert_eq!(c.promote(0).unwrap(), 0);
    assert_eq!(c.epoch(), 0);
    assert!(c.promotion_log().is_empty());
}

#[cfg(unix)]
#[test]
fn promotions_bump_epoch_and_log_order() {
    let a = tmp("pe-a");
    let b = tmp("pe-b");
    let sock = tmp("pe-sock");
    let c = Cluster::open_with_config(
        &[a.to_str().unwrap(), b.to_str().unwrap()],
        cfg(&sock),
    )
    .unwrap();

    assert_eq!(c.promote(1).unwrap(), 1);
    assert_eq!(c.epoch(), 1);
    // Idempotent re-promote: no new epoch, no log spam.
    assert_eq!(c.promote(1).unwrap(), 1);
    assert_eq!(c.epoch(), 1);

    assert_eq!(c.promote(0).unwrap(), 2);
    assert_eq!(c.epoch(), 2);
    assert_eq!(c.writer_index(), 0);

    let log = c.promotion_log();
    assert_eq!(log.len(), 2);
    assert_eq!((log[0].epoch, log[0].writer), (1, 1));
    assert_eq!((log[1].epoch, log[1].writer), (2, 0));
    // Sane timestamps, ordered.
    assert!(log[0].at_ms > 0);
    assert!(log[1].at_ms >= log[0].at_ms);
}
