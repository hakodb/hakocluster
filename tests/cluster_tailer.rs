//! Tailer validation (issue #23): fail-closed contract, all platforms.
//! Functional N+1 tests are unix-only (socket mesh); they live behind
//! matching cfgs below so Windows CI covers the contract too.

use hakocluster::{Cluster, ClusterConfig, TailerCloud, TailerMesh};

fn cfg() -> ClusterConfig {
    ClusterConfig::default()
}

fn mesh_knob() -> TailerMesh {
    TailerMesh {
        name: "tailer-1".into(),
        room_key: "room-secret".into(),
        port: 0,
    }
}

fn cloud_knob() -> TailerCloud {
    TailerCloud {
        client_id: "tailer-1".into(),
        room_name: "ops".into(),
        room_key: "room-secret".into(),
        auth_token: "tok".into(),
        server_url: "ws://127.0.0.1:9".into(),
    }
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("hc-tailer-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn both_knobs_rejected() {
    let dir = tmpdir("both");
    let mut c = cfg();
    c.tailer_mesh = Some(mesh_knob());
    c.tailer_cloud = Some(cloud_knob());
    let err = Cluster::open_with_config(&[dir.to_string_lossy().as_ref()], c)
        .expect_err("mesh+cloud must not coexist");
    assert!(err.contains("exclusive"), "unexpected: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Unix-only (on Windows the unix gate fires first by design): knob
/// without its cargo feature must fail naming the feature.
#[cfg(all(unix, not(feature = "tailer-mesh")))]
#[test]
fn mesh_knob_needs_feature() {
    let dir = tmpdir("meshfeat");
    let mut c = cfg();
    c.tailer_mesh = Some(mesh_knob());
    let err = Cluster::open_with_config(&[dir.to_string_lossy().as_ref()], c)
        .expect_err("mesh knob without feature must fail");
    assert!(err.contains("tailer-mesh"), "unexpected: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Same for the cloud lane.
#[cfg(all(unix, not(feature = "tailer-cloud")))]
#[test]
fn cloud_knob_needs_feature() {
    let dir = tmpdir("cloudfeat");
    let mut c = cfg();
    c.tailer_cloud = Some(cloud_knob());
    let err = Cluster::open_with_config(&[dir.to_string_lossy().as_ref()], c)
        .expect_err("cloud knob without feature must fail");
    assert!(err.contains("tailer-cloud"), "unexpected: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(not(unix))]
#[test]
fn tailer_unix_only() {
    let dir = tmpdir("unixonly");
    let mut c = cfg();
    c.tailer_mesh = Some(mesh_knob());
    let err = Cluster::open_with_config(&[dir.to_string_lossy().as_ref()], c)
        .expect_err("tailer off-unix must fail");
    assert!(err.contains("unix"), "unexpected: {err}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// N+1 mesh with a live bridge (no peer needed: serve+discover run,
/// peer count stays 0). Unix + transport feature only.
#[cfg(all(unix, feature = "tailer-mesh"))]
#[test]
fn mesh_tailer_meshes_locally() {
    let dir = tmpdir("meshn1");
    let a = dir.join("a");
    let mut c = cfg();
    c.tailer_mesh = Some(mesh_knob());
    let cluster =
        Cluster::open_with_config(&[a.to_string_lossy().as_ref()], c).expect("open N+1");
    // Serving member + derived tailer.
    assert_eq!(cluster.instance_count(), 2);
    assert_eq!(cluster.tailer_index(), Some(1));
    // Tailer never serves fan-out and is never promotable.
    for _ in 0..50 {
        assert_ne!(cluster.read_index(), 1);
    }
    assert!(cluster.promote(1).is_err());
    assert_eq!(cluster.writer_index(), 0);
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Cloud tailer opens against a dead hub (client retries in the
/// background; open itself must succeed — bridge failure is async).
/// Unix + transport feature only.
#[cfg(all(unix, feature = "tailer-cloud"))]
#[test]
fn cloud_tailer_opens_without_server() {
    let dir = tmpdir("cloudn1");
    let a = dir.join("a");
    let mut c = cfg();
    c.tailer_cloud = Some(cloud_knob());
    let cluster =
        Cluster::open_with_config(&[a.to_string_lossy().as_ref()], c).expect("open N+1");
    assert_eq!(cluster.instance_count(), 2);
    assert_eq!(cluster.tailer_index(), Some(1));
    assert!(cluster.promote(1).is_err());
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}
