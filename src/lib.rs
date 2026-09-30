//! hakocluster: dispatcher over N hakodb instances (Fase 1).
//!
//! Full design: hakodb/hakocluster#1. Fase 1 scope only:
//! - `Cluster::open[_with_config]` — one `Hako` per data dir + full-mesh
//!   `socket_sync` peering (one connection per pair, `i` dials `j > i`).
//! - Reads (`get`/`query`) fan out round-robin across all instances.
//! - Writes (`put`/`put_owned`/`delete`) route to the designated writer,
//!   `instances[0]` by convention.
//! - Replicated writes join each instance's normal write/flush queue (that
//!   is `SocketSync`'s own behavior); the cluster adds no flush paths.
//!
//! Non-unix: `socket_sync` compiles out (same rule as hakodb), so peering
//! is unavailable — `open` with N > 1 fails closed; N = 1 works as a
//! degenerate single-node cluster.

use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::engine::Hako;
use hakodb::query::query::Query;

#[cfg(unix)]
use hakodb::socket_sync::SocketSync;

/// Fase 1 cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Durability for every instance (tests use Interval: the socket tailer
    /// reads the WAL file, so only flushed bytes replicate live).
    pub durability_mode: hakodb::config::DurabilityMode,
    /// Per-instance group-commit window (staggering is phase 2; Fase 1
    /// uses the same value everywhere).
    pub group_commit_interval_ms: u64,
    /// Directory holding one `instance-{i}.sock` per member.
    pub sock_dir: PathBuf,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            durability_mode: hakodb::config::DurabilityMode::Interval,
            group_commit_interval_ms: 5,
            sock_dir: PathBuf::from("socks"),
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("instances", &self.instances.len())
            .field("reads", &self.read_counts())
            .finish()
    }
}

/// One clustered engine: N instances, one logical dataset.
pub struct Cluster {
    instances: Vec<Instance>,
    /// Round-robin cursor for read fan-out.
    rr: AtomicUsize,
    /// Held so the socket tasks outlive `open` (unix only).
    #[cfg(unix)]
    _rt: tokio::runtime::Runtime,
}

struct Instance {
    db: Arc<Hako>,
    /// Reads served (fan-out accounting; phase 2 least-busy input).
    reads: AtomicU64,
    #[cfg(unix)]
    _sync: SocketSync,
}

impl Cluster {
    /// Open with default config; socket dir is `socks/` next to `paths[0]`.
    pub fn open(paths: &[&str]) -> Result<Self, String> {
        let sock_dir = Path::new(paths.first().ok_or("need ≥1 path")?)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("socks");
        Self::open_with_config(
            paths,
            ClusterConfig {
                sock_dir,
                ..ClusterConfig::default()
            },
        )
    }

    /// Open one `Hako` per data dir and mesh the instances with
    /// `socket_sync` (unix only — see module docs).
    pub fn open_with_config(paths: &[&str], cfg: ClusterConfig) -> Result<Self, String> {
        if paths.is_empty() {
            return Err("need ≥1 path".into());
        }
        #[cfg(not(unix))]
        if paths.len() > 1 {
            return Err(
                "socket peering is unix-only: N>1 clusters need a unix host".into(),
            );
        }

        let mut dbs = Vec::with_capacity(paths.len());
        for p in paths {
            let mut hc = HakoConfig::default();
            hc.durability_mode = cfg.durability_mode;
            hc.group_commit_interval_ms = cfg.group_commit_interval_ms;
            dbs.push(Arc::new(
                Hako::open(p, hc).map_err(|e| format!("open {p}: {e}"))?,
            ));
        }

        #[cfg(unix)]
        {
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| format!("tokio runtime: {e}"))?;
            std::fs::create_dir_all(&cfg.sock_dir)
                .map_err(|e| format!("sock_dir: {e}"))?;
            let socks: Vec<PathBuf> = (0..dbs.len())
                .map(|i| cfg.sock_dir.join(format!("instance-{i}.sock")))
                .collect();
            // Serve all, then dial the mesh (one connection per pair:
            // i dials j > i; traffic is bidirectional per connection).
            let mut instances = Vec::with_capacity(dbs.len());
            for (db, sock) in dbs.into_iter().zip(socks.iter()) {
                let sync = SocketSync::new(db.clone(), vec![]);
                sync.serve(sock.to_string_lossy().as_ref())
                    .map_err(|e| format!("serve {}: {e}", sock.display()))?;
                instances.push(Instance {
                    db,
                    reads: AtomicU64::new(0),
                    _sync: sync,
                });
            }
            for i in 0..instances.len() {
                for j in (i + 1)..instances.len() {
                    let path = socks[j].to_string_lossy().into_owned();
                    rt.block_on(instances[i]._sync.dial(&path))
                        .map_err(|e| format!("dial {}: {e}", socks[j].display()))?;
                }
            }
            return Ok(Self {
                instances,
                rr: AtomicUsize::new(0),
                _rt: rt,
            });
        }

        #[cfg(not(unix))]
        Ok(Self {
            instances: dbs
                .into_iter()
                .map(|db| Instance {
                    db,
                    reads: AtomicU64::new(0),
                })
                .collect(),
            rr: AtomicUsize::new(0),
        })
    }

    /// All instances, index 0 conventionally the designated writer.
    pub fn instances(&self) -> Vec<Arc<Hako>> {
        self.instances.iter().map(|i| i.db.clone()).collect()
    }

    /// The designated writer (index 0). All cluster writes route here.
    pub fn writer(&self) -> &Arc<Hako> {
        &self.instances[0].db
    }

    /// Member count.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Reads served per instance (fan-out accounting).
    pub fn read_counts(&self) -> Vec<u64> {
        self.instances
            .iter()
            .map(|i| i.reads.load(Ordering::Relaxed))
            .collect()
    }

    /// Total live socket peerings (0 off-unix).
    pub fn peer_count(&self) -> usize {
        #[cfg(unix)]
        return self.instances.iter().map(|i| i._sync.peer_count()).sum();
        #[cfg(not(unix))]
        return 0;
    }

    /// Next read replica, round-robin.
    fn pick(&self) -> &Instance {
        let n = self.instances.len();
        // ponytail: wrapping_add, not checked math — a counter that runs
        // for centuries is the only overflow story, and modulo is safe.
        let i = self.rr.fetch_add(1, Ordering::Relaxed) % n;
        &self.instances[i]
    }

    // --- Write path: designated writer only (issue #1, phase 1). ---

    /// Route a write to the designated writer.
    pub fn put(&self, col: &str, id: &str, doc: &HakoDoc) -> Result<String, String> {
        self.writer()
            .put(col, id, doc)
            .map_err(|e| e.to_string())
    }

    /// Owned-doc variant (skips the deep clone when the caller owns the doc).
    pub fn put_owned(&self, col: &str, id: &str, doc: HakoDoc) -> Result<String, String> {
        self.writer()
            .put_owned(col, id, doc)
            .map_err(|e| e.to_string())
    }

    /// Route a delete to the designated writer.
    pub fn delete(&self, col: &str, id: &str) -> Result<String, String> {
        self.writer().delete(col, id).map_err(|e| e.to_string())
    }

    // --- Read path: round-robin fan-out (issue #1, phase 1). ---

    /// Fan-out point read. No read-your-write across instances in phase 1:
    /// replicas trail the writer by <= the socket tail interval.
    pub fn get(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<HakoDoc>, String> {
        let inst = self.pick();
        inst.reads.fetch_add(1, Ordering::Relaxed);
        inst.db.get(collection, doc_id).map_err(|e| e.to_string())
    }

    /// Fan-out query (same lag contract as [`Self::get`]).
    pub fn query(&self, query: Query) -> Result<Vec<(String, HakoDoc)>, String> {
        let inst = self.pick();
        inst.reads.fetch_add(1, Ordering::Relaxed);
        inst.db.query(query).map_err(|e| e.to_string())
    }
}

#[cfg(unix)]
impl Drop for Cluster {
    fn drop(&mut self) {
        for i in &self.instances {
            i._sync.stop();
        }
        // Runtime drop aborts the accept/tail tasks.
    }
}
