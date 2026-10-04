//! hakocluster: dispatcher over N hakodb instances (Fase 2).
//!
//! Full design: hakodb/hakocluster#1.
//! - `Cluster::open[_with_config]` — one `Hako` per data dir + full-mesh
//!   `socket_sync` peering (one connection per pair, `i` dials `j > i`).
//! - Reads (`get`/`query`) fan out round-robin across healthy instances.
//! - Writes (`put`/`put_owned`/`delete`) route to the designated writer,
//!   `instances[0]` by convention.
//! - Fase 2 adds: stagger policy (same interval + spaced opens, or
//!   per-instance intervals) and a lag guard (eject replicas trailing the
//!   writer by more than `max_replica_lag_versions`, re-admit at half).
//! - Replicated writes join each instance's normal write/flush queue (that
//!   is `SocketSync`'s own behavior); the cluster adds no flush paths.
//!
//! Non-unix: `socket_sync` compiles out (same rule as hakodb), so peering
//! is unavailable — `open` with N > 1 fails closed; N = 1 works as a
//! degenerate single-node cluster.

pub mod ffi;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::engine::Hako;
use hakodb::query::query::Query;

#[cfg(unix)]
use hakodb::socket_sync::SocketSync;

/// Flush-cadence stagger policy (issue #1: flush cadence is the ONLY
/// knob — never skip the queue, no direct-flush paths).
#[derive(Debug, Clone)]
pub enum StaggerPolicy {
    /// Option A (default): same interval everywhere; instance opens spaced
    /// by `offset_ms` so group-commit phases don't coincide. The engine's
    /// `last_sync` starts at WAL open, so spaced opens = phased flushes.
    /// Zero coordination protocol.
    StaggeredStart { offset_ms: u64 },
    /// Option B: per-instance intervals (e.g. co-prime-ish 5/7/11). Spreads
    /// load without start-order dependence; durability lag per instance is
    /// slightly uneven (bounded by the engine's 30s clamp). Length must
    /// equal the instance count.
    PerInstance(Vec<u64>),
    /// Option C: Manual durability everywhere; the deployer calls
    /// `tick_flush()` at its own cadence and the cluster flushes one
    /// instance per call in rotation, so fsync storms never coincide.
    /// Requires `durability_mode = Manual` (refused otherwise — an
    /// Interval engine would flush behind the rotation's back). Maximum
    /// control, caller-driven: no background thread, works everywhere.
    ManualRotation,
}

impl Default for StaggerPolicy {
    fn default() -> Self {
        Self::StaggeredStart { offset_ms: 1 }
    }
}

/// Fase 2 cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Durability for every instance (tests use Interval: the socket tailer
    /// reads the WAL file, so only flushed bytes replicate live).
    pub durability_mode: hakodb::config::DurabilityMode,
    /// Group-commit window used when `stagger` doesn't override it
    /// (StaggeredStart and the default).
    pub group_commit_interval_ms: u64,
    /// Flush-cadence stagger across instances (default: 1ms-spaced starts).
    pub stagger: StaggerPolicy,
    /// Lag guard: eject a replica from fan-out when it trails the writer
    /// by more than this many versions (version-map delta). Versions are
    /// write-micros, so the delta doubles as staleness: a healthy replica
    /// trails by ~ the socket tail cadence (500ms = 500_000). Default
    /// 5_000_000 (~10x the tail) tolerates jitter without flapping.
    /// Re-admit at half the threshold (hysteresis). `None` disables.
    /// The writer (index 0) never ejects.
    pub max_replica_lag_versions: Option<u64>,
    /// Directory holding one `instance-{i}.sock` per member.
    pub sock_dir: PathBuf,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            durability_mode: hakodb::config::DurabilityMode::Interval,
            group_commit_interval_ms: 5,
            stagger: StaggerPolicy::StaggeredStart { offset_ms: 1 },
            max_replica_lag_versions: Some(5_000_000),
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
    /// Designated writer index (manual failover moves it).
    writer_index: AtomicUsize,
    /// Promotion epoch (1-based; 0 = initial writer, never promoted).
    epoch: AtomicU64,
    /// Append-only promotion audit (promotions are rare; Vec is fine).
    promotions: std::sync::Mutex<Vec<Promotion>>,
    /// Option C rotation cursor for [`Cluster::tick_flush`].
    flush_rr: AtomicUsize,
    /// Lag-guard threshold (`None` = disabled). Stored so
    /// `refresh_health` stays a pure read of instance state.
    max_lag: Option<u64>,
    /// Runtime hosting the socket tasks (unix only): owned when `open`
    /// runs outside any runtime (plain sync callers, tests), shared when
    /// called inside one (servers like hakobackend run `#[tokio::main]` —
    /// nesting runtimes panics, and blocking is illegal there, so dials
    /// are spawned instead of awaited; peering converges asynchronously).
    #[cfg(unix)]
    _rt: Rt,
}

#[cfg(unix)]
enum Rt {
    Owned(tokio::runtime::Runtime),
    Shared(tokio::runtime::Handle),
}

struct Instance {
    db: Arc<Hako>,
    /// Reads served (fan-out accounting; phase 2 least-busy input).
    reads: AtomicU64,
    /// False = ejected by the lag guard, skipped by fan-out.
    healthy: AtomicBool,
    #[cfg(unix)]
    _sync: std::sync::Arc<SocketSync>,
}

/// One promotion record: which epoch moved the writer where, and when.
/// Operators use epochs to order promotions (a higher epoch supersedes);
/// the actual fence is the read-only flags [`Cluster::promote`] sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Promotion {
    /// 1-based promotion counter (0 = no promotion yet, initial writer).
    pub epoch: u64,
    /// New designated writer index.
    pub writer: usize,
    /// Wall millis at promotion (operator audit only, never a lease).
    pub at_ms: u64,
}

/// Per-instance health from [`Cluster::refresh_health`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaHealth {
    /// Instance index (0 = designated writer, always healthy).
    pub index: usize,
    /// Max version-map delta vs the writer (0 = converged). Versions are
    /// write-micros, so this doubles as staleness in micros.
    pub lag_versions: u64,
    /// In fan-out rotation or ejected.
    pub healthy: bool,
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

        // Resolve the per-instance group-commit window. StaggeredStart
        // shares one value; PerInstance overrides per index (length
        // validated up front — fail before opening anything).
        // ManualRotation forces Manual durability (validated, not
        // silently overridden — an Interval engine would flush behind
        // the rotation's back, defeating the point).
        let intervals: Vec<u64> = match &cfg.stagger {
            StaggerPolicy::StaggeredStart { .. } => {
                vec![cfg.group_commit_interval_ms; paths.len()]
            }
            StaggerPolicy::PerInstance(v) => {
                if v.len() != paths.len() {
                    return Err(format!(
                        "PerInstance needs one interval per path (got {} for {})",
                        v.len(),
                        paths.len()
                    ));
                }
                v.clone()
            }
            StaggerPolicy::ManualRotation => {
                if cfg.durability_mode != hakodb::config::DurabilityMode::Manual {
                    return Err(
                        "ManualRotation needs durability_mode = Manual".into(),
                    );
                }
                vec![cfg.group_commit_interval_ms; paths.len()]
            }
        };
        let stagger_gap =
            match cfg.stagger {
                StaggerPolicy::StaggeredStart { offset_ms } => offset_ms,
                StaggerPolicy::PerInstance(_) | StaggerPolicy::ManualRotation => 0,
            };

        let mut dbs = Vec::with_capacity(paths.len());
        for (n, p) in paths.iter().enumerate() {
            if n > 0 && stagger_gap > 0 {
                // Phase the group-commit windows: each WAL's last_sync
                // starts at its own open, so spaced opens = phased flushes.
                std::thread::sleep(std::time::Duration::from_millis(stagger_gap));
            }
            let mut hc = HakoConfig::default();
            hc.durability_mode = cfg.durability_mode;
            hc.group_commit_interval_ms = intervals[n];
            dbs.push(Arc::new(
                Hako::open(p, hc).map_err(|e| format!("open {p}: {e}"))?,
            ));
        }

        #[cfg(unix)]
        {
            let rt = match tokio::runtime::Handle::try_current() {
                Ok(h) => Rt::Shared(h),
                Err(_) => Rt::Owned(
                    tokio::runtime::Runtime::new()
                        .map_err(|e| format!("tokio runtime: {e}"))?,
                ),
            };
            std::fs::create_dir_all(&cfg.sock_dir)
                .map_err(|e| format!("sock_dir: {e}"))?;
            let socks: Vec<PathBuf> = (0..dbs.len())
                .map(|i| cfg.sock_dir.join(format!("instance-{i}.sock")))
                .collect();
            // Serve all, then dial the mesh (one connection per pair:
            // i dials j > i; traffic is bidirectional per connection).
            // ponytail: serve() needs a reactor context (UnixListener::
            // from_std) — see the mesh block below for how each runtime
            // shape provides it.
            let mut instances = Vec::with_capacity(dbs.len());
            for db in &dbs {
                instances.push(Instance {
                    db: db.clone(),
                    reads: AtomicU64::new(0),
                    healthy: AtomicBool::new(true),
                    _sync: std::sync::Arc::new(SocketSync::new(db.clone(), vec![])),
                });
            }
            // Fail-closed both sides (synchronous, deterministic from
            // boot): only the designated writer accepts local writes.
            // Replicated ingest bypasses the flag by design, so replicas
            // keep converging while read-only.
            for (n, inst) in instances.iter().enumerate() {
                inst.db.set_read_only(n != 0);
            }
            // Mesh setup: serve needs a reactor context
            // (UnixListener::from_std) in both cases.
            let syncs: Vec<(std::sync::Arc<SocketSync>, PathBuf)> = instances
                .iter()
                .zip(socks.iter())
                .map(|(inst, sock)| (inst._sync.clone(), sock.clone()))
                .collect();
            let mesh = async move {
                for (sync, sock) in &syncs {
                    sync.serve(sock.to_string_lossy().as_ref()).map_err(|e| {
                        format!("serve {}: {e}", sock.display())
                    })?;
                }
                for i in 0..syncs.len() {
                    for j in (i + 1)..syncs.len() {
                        let path = syncs[j].1.to_string_lossy().into_owned();
                        // ponytail: short retry for boot-order races (the
                        // other side serves a moment later). Peer RESTART
                        // healing is out of phase-1 scope (no re-meshing);
                        // the backend driver layer retries for that.
                        let mut last = String::new();
                        let mut ok = false;
                        for _ in 0..5 {
                            match syncs[i].0.dial(&path).await {
                                Ok(_) => {
                                    ok = true;
                                    break;
                                }
                                Err(e) => {
                                    last = e.to_string();
                                    tokio::time::sleep(
                                        std::time::Duration::from_secs(1),
                                    )
                                    .await;
                                }
                            }
                        }
                        if !ok {
                            return Err(format!(
                                "dial {}: {last} (mesh incomplete)",
                                syncs[j].1.display()
                            ));
                        }
                    }
                }
                Ok::<(), String>(())
            };
            match &rt {
                Rt::Owned(r) => r.block_on(mesh)?,
                Rt::Shared(h) => {
                    h.spawn(async move {
                        if let Err(e) = mesh.await {
                            eprintln!("[hakocluster] mesh failed: {e}");
                        }
                    });
                }
            }
            return Ok(Self {
                instances,
                rr: AtomicUsize::new(0),
                writer_index: AtomicUsize::new(0),
                epoch: AtomicU64::new(0),
                promotions: std::sync::Mutex::new(Vec::new()),
                flush_rr: AtomicUsize::new(0),
                max_lag: cfg.max_replica_lag_versions,
                _rt: rt,
            });
        }

        #[cfg(not(unix))]
        Ok(Self {
            instances: dbs
                .into_iter()
                .enumerate()
                .map(|(n, db)| {
                    db.set_read_only(n != 0);
                    Instance {
                        db,
                        reads: AtomicU64::new(0),
                        healthy: AtomicBool::new(true),
                    }
                })
                .collect(),
            rr: AtomicUsize::new(0),
            writer_index: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
            promotions: std::sync::Mutex::new(Vec::new()),
            flush_rr: AtomicUsize::new(0),
            max_lag: cfg.max_replica_lag_versions,
        })
    }

    /// All instances, index 0 conventionally the designated writer.
    pub fn instances(&self) -> Vec<Arc<Hako>> {
        self.instances.iter().map(|i| i.db.clone()).collect()
    }

    /// The designated writer. All cluster writes route here.
    pub fn writer(&self) -> &Arc<Hako> {
        &self.instances[self.writer_index.load(Ordering::Relaxed)].db
    }

    /// Designated writer index (0 by convention; [`Self::promote`] moves it).
    pub fn writer_index(&self) -> usize {
        self.writer_index.load(Ordering::Relaxed)
    }

    /// Manual failover: move the designated writer to `index`. Returns
    /// the promotion epoch (1-based; re-promoting the current writer is
    /// a no-op returning the current epoch without logging). Every other
    /// instance is set read-only (in-process, so always reachable — no
    /// partial-failure story here). NO fencing and NO auto-detect: the
    /// operator must fence the old writer first; two live writers diverge
    /// under LWW. Lease granting (auto-failover) needs balancer HA first
    /// and lives there, not here (see hakobalancer).
    pub fn promote(&self, index: usize) -> Result<u64, String> {
        if index >= self.instances.len() {
            return Err(format!(
                "promote: index {index} out of range (n={})",
                self.instances.len()
            ));
        }
        if index == self.writer_index.load(Ordering::Relaxed) {
            return Ok(self.epoch.load(Ordering::Relaxed));
        }
        for (n, inst) in self.instances.iter().enumerate() {
            inst.db.set_read_only(n != index);
        }
        self.writer_index.store(index, Ordering::Relaxed);
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.promotions
            .lock()
            .unwrap()
            .push(Promotion { epoch, writer: index, at_ms });
        Ok(epoch)
    }

    /// Current promotion epoch (0 = initial writer, never promoted).
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Promotion audit log, oldest first.
    pub fn promotion_log(&self) -> Vec<Promotion> {
        self.promotions.lock().unwrap().clone()
    }

    /// Option C rotation tick: flush ONE instance (round-robin) so its
    /// buffered Manual writes reach the WAL file (and the socket tailer).
    /// The deployer calls this at its own flush cadence; fsync storms
    /// never coincide because only one instance flushes per tick.
    pub fn tick_flush(&self) -> Result<(), String> {
        let n = self.instances.len();
        let i = self.flush_rr.fetch_add(1, Ordering::Relaxed) % n;
        self.instances[i]
            .db
            .flush()
            .map_err(|e| format!("tick_flush instance {i}: {e}"))
    }

    /// Member count.
    pub fn instance_count(&self) -> usize {
        self.instances.len()
    }

    /// Next read replica index, round-robin over healthy instances
    /// (ejected ones skipped; the writer always qualifies). Driver
    /// entry-point for fan-out without reimplementing picking.
    pub fn read_index(&self) -> usize {
        let n = self.instances.len();
        // ponytail: wrapping_add, not checked math — a counter that runs
        // for centuries is the only overflow story, and modulo is safe.
        let start = self.rr.fetch_add(1, Ordering::Relaxed);
        for k in 0..n {
            let i = (start + k) % n;
            if self.instances[i].healthy.load(Ordering::Relaxed) {
                return i;
            }
        }
        // Unreachable (writer never ejects) — fail closed to the writer.
        self.writer_index.load(Ordering::Relaxed)
    }

    /// Record a served read for fan-out accounting (drivers that pick
    /// via read_index() and serve through their own handles call this;
    /// Cluster::get/query do it internally). Out-of-range is a no-op.
    pub fn note_read(&self, index: usize) {
        if let Some(inst) = self.instances.get(index) {
            inst.reads.fetch_add(1, Ordering::Relaxed);
        }
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

    /// Recompute replica lag vs the writer and apply the guard: eject past
    /// `max_replica_lag_versions`, re-admit at half (hysteresis). The
    /// designated writer never ejects. `None` disables ejection (lag still
    /// reported). Explicit call — no background thread in phase 2; drive
    /// it from the deployer's own tick.
    pub fn refresh_health(&self) -> Vec<ReplicaHealth> {
        let w = self.writer_index.load(Ordering::Relaxed);
        let base = self.instances[w].db.get_version_map();
        let mut out = Vec::with_capacity(self.instances.len());
        for (n, inst) in self.instances.iter().enumerate() {
            let lag = if n == w {
                0
            } else {
                let m = inst.db.get_version_map();
                base.iter()
                    .map(|(col, wv)| wv.saturating_sub(*m.get(col).unwrap_or(&0)) as u64)
                    .max()
                    .unwrap_or(0)
            };
            let healthy = match (n == w, self.max_lag) {
                (true, _) => true,
                (false, None) => true,
                (false, Some(max)) => {
                    let cur = inst.healthy.load(Ordering::Relaxed);
                    // ponytail: hysteresis in one expression — eject past
                    // max, re-admit at/below half, otherwise hold state.
                    if lag > max {
                        false
                    } else if lag <= max / 2 {
                        true
                    } else {
                        cur
                    }
                }
            };
            inst.healthy.store(healthy, Ordering::Relaxed);
            out.push(ReplicaHealth {
                index: n,
                lag_versions: lag,
                healthy,
            });
        }
        out
    }

    /// Instances currently in fan-out rotation.
    pub fn healthy_count(&self) -> usize {
        self.instances
            .iter()
            .filter(|i| i.healthy.load(Ordering::Relaxed))
            .count()
    }

    /// Next read replica, round-robin over healthy instances (ejected ones
    /// are skipped; the writer always qualifies).
    fn pick(&self) -> &Instance {
        &self.instances[self.read_index()]
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

// --- Multidatabase registry (hakocluster#6): one process, N named
// databases. Fixed at open (reload re-opens; no runtime membership
// mutation). Each database is a full `Cluster` with its own mesh —
// NEVER meshed across databases (converging divergent data is
// corruption shaped as operation, see #2). Unknown names are ALWAYS
// 404/None, even opt-in auto-create does not exist here: storage may
// be fresh-empty on open (as today), but the NAME must be declared.

/// Database name gate: same discipline as collection segments
/// (`[A-Za-z0-9_-]`, 1–128, no `__` prefix). The name becomes a sock
/// subdir, so traversal shapes are refused here, not downstream.
pub fn valid_db_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        && !s.starts_with("__")
}

/// One named database declaration for [`Databases::open`].
#[derive(Debug, Clone)]
pub struct DbSpec {
    pub name: String,
    pub paths: Vec<String>,
    pub config: ClusterConfig,
}

/// N named databases in one process. Selection is by exact name;
/// there is no default database (a silent default routes writes
/// somewhere the operator did not choose).
pub struct Databases {
    dbs: HashMap<String, Arc<Cluster>>,
    /// Declaration order (deterministic listing).
    order: Vec<String>,
    /// The single shared runtime, present only when this registry
    /// minted it (FFI sync callers). Server-embedded use shares the
    /// server runtime instead — N databases must never mean N runtimes.
    #[cfg(unix)]
    _rt: Option<tokio::runtime::Runtime>,
}

impl Databases {
    /// Open every declared database. Each gets `sock_root/{name}` as
    /// its mesh dir (doubles as the mesh boundary: no cross-database
    /// peering is representable). Fail-closed: bad/duplicate/empty
    /// declarations and any per-db open failure refuse the whole
    /// registry (already-opened siblings drop cleanly via `Drop`).
    pub fn open(specs: Vec<DbSpec>, sock_root: PathBuf) -> Result<Self, String> {
        if specs.is_empty() {
            return Err("need ≥1 database".into());
        }
        let mut seen = HashSet::new();
        for s in &specs {
            if !valid_db_name(&s.name) {
                return Err(format!("bad database name `{}`", s.name));
            }
            if !seen.insert(s.name.clone()) {
                return Err(format!("duplicate database `{}`", s.name));
            }
            if s.paths.is_empty() {
                return Err(format!("database `{}` needs ≥1 path", s.name));
            }
        }
        // ponytail: open inside the shared context when we mint the
        // runtime, so every Cluster takes Shared (one runtime total).
        // No Cluster API change: try_current succeeds inside block_on.
        #[cfg(unix)]
        {
            if tokio::runtime::Handle::try_current().is_ok() {
                let (dbs, order) = Self::open_all(specs, &sock_root)?;
                Ok(Self { dbs, order, _rt: None })
            } else {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| format!("tokio runtime: {e}"))?;
                let out = rt.block_on(async { Self::open_all(specs, &sock_root) });
                let (dbs, order) = out?;
                Ok(Self { dbs, order, _rt: Some(rt) })
            }
        }
        #[cfg(not(unix))]
        {
            let (dbs, order) = Self::open_all(specs, &sock_root)?;
            Ok(Self { dbs, order })
        }
    }

    fn open_all(
        specs: Vec<DbSpec>,
        sock_root: &Path,
    ) -> Result<(HashMap<String, Arc<Cluster>>, Vec<String>), String> {
        let mut dbs = HashMap::with_capacity(specs.len());
        let mut order = Vec::with_capacity(specs.len());
        for s in specs {
            let mut cfg = s.config;
            cfg.sock_dir = sock_root.join(&s.name);
            let refs: Vec<&str> = s.paths.iter().map(|p| p.as_str()).collect();
            let c = Cluster::open_with_config(&refs, cfg)
                .map_err(|e| format!("database `{}`: {e}", s.name))?;
            order.push(s.name.clone());
            dbs.insert(s.name, Arc::new(c));
        }
        Ok((dbs, order))
    }

    /// Exact-name lookup. `None` = unknown (caller 404s). No default.
    pub fn get(&self, name: &str) -> Option<Arc<Cluster>> {
        self.dbs.get(name).cloned()
    }

    /// Declared names in declaration order.
    pub fn names(&self) -> Vec<String> {
        self.order.clone()
    }
}
