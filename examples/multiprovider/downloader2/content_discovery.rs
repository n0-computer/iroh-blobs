use std::collections::BTreeMap;

use futures_lite::stream::StreamExt;
use futures_util::stream::BoxStream;
use iroh::NodeId;
use serde::{Deserialize, Serialize};

use crate::Hash;

/// Announce kind
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum AnnounceKind {
    /// The peer supposedly has some of the data.
    Partial = 0,
    /// The peer supposedly has the complete data.
    #[default]
    Complete,
}

/// Options for finding peers
#[derive(Debug, Default)]
pub struct FindPeersOpts {
    /// Kind of announce
    #[allow(dead_code)]
    pub kind: AnnounceKind,
}

/// A pluggable content discovery mechanism
pub trait ContentDiscovery: std::fmt::Debug + Send + 'static {
    /// Find peers that have the given blob.
    ///
    /// The returned stream is a handle for the discovery task. It should be an
    /// infinite stream that only stops when it is dropped.
    fn find_peers(&mut self, hash: Hash, opts: FindPeersOpts) -> BoxStream<'static, NodeId>;
}

/// A boxed content discovery
pub type BoxedContentDiscovery = Box<dyn ContentDiscovery>;

/// A simple static content discovery mechanism
#[derive(Debug)]
pub struct StaticContentDiscovery {
    info: BTreeMap<Hash, Vec<NodeId>>,
    default: Vec<NodeId>,
}

impl StaticContentDiscovery {
    /// Create a new static content discovery mechanism
    pub fn new(mut info: BTreeMap<Hash, Vec<NodeId>>, mut default: Vec<NodeId>) -> Self {
        default.sort();
        default.dedup();
        for (_, peers) in info.iter_mut() {
            peers.sort();
            peers.dedup();
        }
        Self { info, default }
    }
}

impl ContentDiscovery for StaticContentDiscovery {
    fn find_peers(&mut self, hash: Hash, _opts: FindPeersOpts) -> BoxStream<'static, NodeId> {
        let peers = self.info.get(&hash).unwrap_or(&self.default).clone();
        Box::pin(futures_lite::stream::iter(peers).chain(futures_lite::stream::pending()))
    }
}
