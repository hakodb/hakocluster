//! hakocluster: dispatcher over N hakodb instances (skeleton).
//!
//! Target shape (see issue #1 for the full design):
//! - `Cluster::open(paths)` — one `Hako` per data dir + `SocketSync`
//!   peering between them.
//! - Reads (`get`/`query`/...) fan out across healthy replicas.
//! - Writes route per policy (single designated writer, or sharded
//!   multi-writer with LWW convergence + staggered group-commit).
//! - Replicated (socket_sync) writes join each instance's normal
//!   write/flush queue — never a direct flush.

use std::sync::Arc;

use hakodb::engine::Hako;

/// One clustered engine: N instances, one logical dataset.
pub struct Cluster {
    /// All instances, index 0 conventionally the designated writer.
    pub instances: Vec<Arc<Hako>>,
}

impl Cluster {
    /// Open one `Hako` per data dir. Peering (socket_sync) and routing
    /// policies land here as the design in issue #1 is implemented.
    pub fn open(_paths: &[&str]) -> Result<Self, String> {
        Err("not implemented yet (see issue #1)".into())
    }
}
