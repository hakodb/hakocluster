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
    // Unique socket dir per test: the default is relative ("socks/"),
    // and tests in one binary share a working directory.
    c.sock_dir = dir.join("socks");
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
    c.sock_dir = dir.join("socks");
    c.tailer_cloud = Some(cloud_knob());
    let cluster =
        Cluster::open_with_config(&[a.to_string_lossy().as_ref()], c).expect("open N+1");
    assert_eq!(cluster.instance_count(), 2);
    assert_eq!(cluster.tailer_index(), Some(1));
    assert!(cluster.promote(1).is_err());
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

/// FFI config shapes (parse-level, all platforms): missing keys fail
/// loud with the key named. A tailer-server mode was tried and removed:
/// the server broadcast only serves room-owned collections, so a box-
/// local dataset could never broadcast — hubs run standalone.
#[test]
fn ffi_cloud_shapes_fail_closed() {
    use std::ffi::{CStr, CString};
    use hakocluster::ffi as ffi_mod;

    fn try_open(cfg_json: &str) -> Result<(), String> {
        let dir = std::env::temp_dir().join(format!(
            "hc-tailer-ffi-{}-{}",
            std::process::id(),
            cfg_json.len()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let paths = format!("[\"{}\"]", dir.join("a").to_string_lossy().replace('\\', "/"));
        let p = CString::new(paths).unwrap();
        let c = CString::new(cfg_json).unwrap();
        let h = ffi_mod::hk_cluster_open_with_config(p.as_ptr(), c.as_ptr());
        if h.is_null() {
            let e = unsafe { CStr::from_ptr(ffi_mod::hk_cluster_last_error()) }
                .to_string_lossy()
                .into_owned();
            let _ = std::fs::remove_dir_all(&dir);
            Err(e)
        } else {
            ffi_mod::hk_cluster_close(h);
            let _ = std::fs::remove_dir_all(&dir);
            Ok(())
        }
    }

    // Unknown mode key is just unknown (flat client-only shape).
    let e = try_open(r#"{"tailer_cloud":{"client_id":"t","auth_token":"k","mode":"hub"}}"#)
        .expect_err("missing client keys must fail");
    assert!(e.contains("room_name"), "unexpected: {e}");
    // Missing server_url.
    let e = try_open(
        r#"{"tailer_cloud":{"client_id":"t","auth_token":"k","room_name":"ops","room_key":"rk"}}"#,
    )
    .expect_err("missing url must fail");
    assert!(e.contains("server_url"), "unexpected: {e}");
}

/// End-to-end across boxes on loopback (unix + cloud feature): a
/// STANDALONE hub plus two client-tailer boxes. A tailer-server was
/// tried first and failed for a structural reason (server broadcast
/// only serves room-owned collections; box-local writes land plain) —
/// this is the supported topology. Write on A reads on B.
#[cfg(all(unix, feature = "tailer-cloud"))]
#[test]
fn cloud_tailer_bridges_two_boxes() {
    // Free port without racing the world for long: bind, read, drop.
    let port: u16 = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
        l.local_addr().expect("addr").port()
    };
    let room = format!("e2e-{}", std::process::id());

    // Standalone hub (its own runtime; lives until the end of the test).
    let hub_rt = tokio::runtime::Runtime::new().expect("hub runtime");
    use hakodb::engine::Hako;
    let hub_dir = tmpdir("hub");
    let hub_db = std::sync::Arc::new(
        Hako::open(
            hub_dir.join("db").to_string_lossy().as_ref(),
            Default::default(),
        )
        .expect("hub db"),
    );
    let hub = hakodb::cloud_sync::CloudSync::server(hub_db.clone(), "hub-1", "shared-secret");
    hub_rt
        .block_on(hub.start(&format!("127.0.0.1:{port}")))
        .expect("hub serves");

    let client_knob = |id: &str| TailerCloud {
        client_id: id.into(),
        room_name: room.clone(),
        room_key: "shared-secret".into(),
        auth_token: "shared-secret".into(),
        server_url: format!("ws://127.0.0.1:{port}"),
    };

    let dir_a = tmpdir("bridgeA");
    let dir_b = tmpdir("bridgeB");
    let mut ca = cfg();
    ca.sock_dir = dir_a.join("socks");
    ca.tailer_cloud = Some(client_knob("box-a"));
    let mut cb = cfg();
    cb.sock_dir = dir_b.join("socks");
    cb.tailer_cloud = Some(client_knob("box-b"));
    let box_a = Cluster::open_with_config(&[dir_a.join("a").to_string_lossy().as_ref()], ca)
        .expect("box A opens");
    let box_b = Cluster::open_with_config(&[dir_b.join("a").to_string_lossy().as_ref()], cb)
        .expect("box B opens");

    // Write on A's writer; poll B until the bridge delivers (or timeout).
    {
        use hakodb::document::hako_doc::HakoDoc;
        use hakodb::document::value::Value;
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(42));
        box_a.put("t", "k1", &doc).expect("write on A");
    }
    let mut seen = false;
    for _ in 0..300 {
        if box_b.get("t", "k1").expect("read on B").is_some() {
            seen = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    assert!(seen, "bridge did not deliver A->B within 60s");
    drop(box_a);
    drop(box_b);
    hub.stop();
    drop(hub);
    drop(hub_db);
    drop(hub_rt);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
    let _ = std::fs::remove_dir_all(&hub_dir);
}
