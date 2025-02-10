//! Downloader version that supports range downloads and downloads from multiple sources.
//!
//! The entry point is the [Downloader::builder] function, which creates a downloader
//! builder. The downloader is highly configurable.
//!
//! Content discovery is configurable via the [ContentDiscovery] trait.
//! Bitfield subscriptions are configurable via the [BitfieldSubscription] trait.
//! Download planning is configurable via the [DownloadPlanner] trait.
//!
//! After creating a downloader, you can schedule downloads using the
//! [Downloader::download] function. The function returns a future that
//! resolves once the download is complete. The download can be cancelled by
//! dropping the future.
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    marker::PhantomData,
    sync::Arc,
    time::Instant,
};

use crate::{
    get::{
        fsm::{BlobContentNext, ConnectedNext, EndBlobNext},
        Stats,
    },
    protocol::{GetRequest, RangeSpec, RangeSpecSeq},
    store::{BaoBatchWriter, MapEntryMut, Store},
    util::local_pool::{self, LocalPool, LocalPoolHandle},
    Hash,
};
use anyhow::Context;
use bao_tree::{io::BaoContentItem, ChunkNum, ChunkRanges};
use futures_lite::StreamExt;
use futures_util::{stream::BoxStream, FutureExt};
use iroh::{Endpoint, NodeId};
use range_collections::range_set::RangeSetRange;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, trace};

mod planners;
use planners::*;

mod state;
use state::*;

mod actor;
use actor::*;

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, derive_more::From,
)]
struct DownloadId(u64);

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, derive_more::From,
)]
struct ObserveId(u64);

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, derive_more::From,
)]
struct DiscoveryId(u64);

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, derive_more::From,
)]
struct PeerDownloadId(u64);

#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, derive_more::From,
)]
struct BitfieldSubscriptionId(u64);

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

/// A pluggable bitfield subscription mechanism
pub trait BitfieldSubscription: std::fmt::Debug + Send + 'static {
    /// Subscribe to a bitfield
    fn subscribe(
        &mut self,
        peer: BitfieldPeer,
        hash: Hash,
    ) -> BoxStream<'static, BitfieldSubscriptionEvent>;
}

/// A boxed bitfield subscription
pub type BoxedBitfieldSubscription = Box<dyn BitfieldSubscription>;

/// An event from a bitfield subscription
#[derive(Debug)]
pub enum BitfieldSubscriptionEvent {
    /// Set the bitfield to the given ranges
    Bitfield {
        /// The entire bitfield
        ranges: ChunkRanges,
    },
    /// Update the bitfield with the given ranges
    BitfieldUpdate {
        /// The ranges that were added
        added: ChunkRanges,
        /// The ranges that were removed
        removed: ChunkRanges,
    },
}

/// Events from observing a local bitfield
#[derive(Debug)]
pub enum ObserveEvent {
    /// Set the bitfield to the given ranges
    Bitfield {
        /// The entire bitfield
        ranges: ChunkRanges,
    },
    /// Update the bitfield with the given ranges
    BitfieldUpdate {
        /// The ranges that were added
        added: ChunkRanges,
        /// The ranges that were removed
        removed: ChunkRanges,
    },
}

/// A download request
#[derive(Debug, Clone)]
pub struct DownloadRequest {
    /// The blob we are downloading
    pub hash: Hash,
    /// The ranges we are interested in
    pub ranges: ChunkRanges,
}

/// A request to observe the local bitmap for a blob
#[derive(Debug, Clone)]
pub struct ObserveRequest {
    /// The blob we are observing
    pub hash: Hash,
    /// The ranges we are interested in
    pub ranges: ChunkRanges,
    /// Buffer size
    pub buffer: usize,
}

#[derive(Debug)]
struct IdGenerator<T = u64> {
    next_id: u64,
    _p: PhantomData<T>,
}

impl<T> Default for IdGenerator<T> {
    fn default() -> Self {
        Self {
            next_id: 0,
            _p: PhantomData,
        }
    }
}

impl<T> IdGenerator<T>
where
    T: From<u64> + Copy,
{
    fn next(&mut self) -> T {
        let id = self.next_id;
        self.next_id += 1;
        T::from(id)
    }
}

/// Peer for a bitfield subscription
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum BitfieldPeer {
    /// The local bitfield
    Local,
    /// A bitfield from a remote peer
    Remote(NodeId),
}

/// A downloader that allows range downloads and downloads from multiple peers.
#[derive(Debug, Clone)]
pub struct Downloader {
    send: mpsc::Sender<UserCommand>,
    _task: Arc<AbortOnDropHandle<()>>,
}

/// A builder for a downloader
#[derive(Debug)]
pub struct DownloaderBuilder<S> {
    endpoint: Endpoint,
    store: S,
    discovery: Option<BoxedContentDiscovery>,
    subscribe_bitfield: Option<BoxedBitfieldSubscription>,
    local_pool: Option<LocalPool>,
    planner: Option<BoxedDownloadPlanner>,
}

impl<S> DownloaderBuilder<S> {
    /// Set the content discovery
    pub fn discovery<D: ContentDiscovery>(self, discovery: D) -> Self {
        Self {
            discovery: Some(Box::new(discovery)),
            ..self
        }
    }

    /// Set the bitfield subscription
    pub fn bitfield_subscription<B: BitfieldSubscription>(self, value: B) -> Self {
        Self {
            subscribe_bitfield: Some(Box::new(value)),
            ..self
        }
    }

    /// Set the local pool
    pub fn local_pool(self, local_pool: LocalPool) -> Self {
        Self {
            local_pool: Some(local_pool),
            ..self
        }
    }

    /// Set the download planner
    pub fn planner<P: DownloadPlanner>(self, planner: P) -> Self {
        Self {
            planner: Some(Box::new(planner)),
            ..self
        }
    }

    /// Build the downloader
    pub fn build(self) -> Downloader
    where
        S: Store,
    {
        let store = self.store;
        let discovery = self.discovery.expect("discovery not set");
        let local_pool = self.local_pool.unwrap_or_else(|| LocalPool::single());
        let planner = self
            .planner
            .unwrap_or_else(|| Box::new(StripePlanner2::new(0, 10)));
        let subscribe_bitfield = self.subscribe_bitfield.unwrap_or_else(|| {
            Box::new(SimpleBitfieldSubscription::new(
                self.endpoint.clone(),
                store.clone(),
                local_pool.handle().clone(),
            ))
        });
        Downloader::new(
            self.endpoint,
            store,
            discovery,
            subscribe_bitfield,
            local_pool,
            planner,
        )
    }
}

impl Downloader {
    /// Create a new download
    ///
    /// The download will be cancelled if the returned future is dropped.
    pub async fn download(&self, request: DownloadRequest) -> anyhow::Result<()> {
        let (send, recv) = tokio::sync::oneshot::channel::<()>();
        self.send
            .send(UserCommand::Download {
                request,
                done: send,
            })
            .await?;
        recv.await?;
        Ok(())
    }

    /// Observe a local bitmap
    pub async fn observe(
        &self,
        request: ObserveRequest,
    ) -> anyhow::Result<tokio::sync::mpsc::Receiver<ObserveEvent>> {
        let (send, recv) = tokio::sync::mpsc::channel(request.buffer);
        self.send
            .send(UserCommand::Observe { request, send })
            .await?;
        Ok(recv)
    }

    /// Create a new downloader builder
    pub fn builder<S: Store>(endpoint: Endpoint, store: S) -> DownloaderBuilder<S> {
        DownloaderBuilder {
            endpoint,
            store,
            discovery: None,
            subscribe_bitfield: None,
            local_pool: None,
            planner: None,
        }
    }

    /// Create a new downloader
    fn new<S: Store>(
        endpoint: Endpoint,
        store: S,
        discovery: BoxedContentDiscovery,
        subscribe_bitfield: BoxedBitfieldSubscription,
        local_pool: LocalPool,
        planner: Box<dyn DownloadPlanner>,
    ) -> Self {
        let actor = DownloaderActor::new(
            endpoint,
            store,
            discovery,
            subscribe_bitfield,
            local_pool,
            planner,
        );
        let (send, recv) = tokio::sync::mpsc::channel(256);
        let task = Arc::new(spawn(async move { actor.run(recv).await }));
        Self { send, _task: task }
    }
}

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

/// A bitfield subscription that just returns nothing for local and everything(*) for remote
///
/// * Still need to figure out how to deal with open ended chunk ranges.
#[allow(dead_code)]
#[derive(Debug)]
struct TestBitfieldSubscription;

impl BitfieldSubscription for TestBitfieldSubscription {
    fn subscribe(
        &mut self,
        peer: BitfieldPeer,
        _hash: Hash,
    ) -> BoxStream<'static, BitfieldSubscriptionEvent> {
        let ranges = match peer {
            BitfieldPeer::Local => ChunkRanges::empty(),
            BitfieldPeer::Remote(_) => {
                ChunkRanges::from(ChunkNum(0)..ChunkNum(1024 * 1024 * 1024 * 1024))
            }
        };
        Box::pin(
            futures_lite::stream::once(BitfieldSubscriptionEvent::Bitfield { ranges })
                .chain(futures_lite::stream::pending()),
        )
    }
}

/// A simple bitfield subscription that gets the valid ranges from a remote node, and the bitmap from a local store
#[derive(Debug)]
pub struct SimpleBitfieldSubscription<S> {
    endpoint: Endpoint,
    store: S,
    local_pool: LocalPoolHandle,
}

impl<S> SimpleBitfieldSubscription<S> {
    /// Create a new bitfield subscription
    pub fn new(endpoint: Endpoint, store: S, local_pool: LocalPoolHandle) -> Self {
        Self {
            endpoint,
            store,
            local_pool,
        }
    }
}

async fn get_valid_ranges_local<S: Store>(hash: &Hash, store: S) -> anyhow::Result<ChunkRanges> {
    if let Some(entry) = store.get_mut(&hash).await? {
        crate::get::db::valid_ranges::<S>(&entry).await
    } else {
        Ok(ChunkRanges::empty())
    }
}

async fn get_valid_ranges_remote(
    endpoint: &Endpoint,
    id: NodeId,
    hash: &Hash,
) -> anyhow::Result<ChunkRanges> {
    let conn = endpoint.connect(id, crate::ALPN).await?;
    let (size, _) = crate::get::request::get_verified_size(&conn, &hash).await?;
    let chunks = (size + 1023) / 1024;
    Ok(ChunkRanges::from(ChunkNum(0)..ChunkNum(chunks)))
}

impl<S: Store> BitfieldSubscription for SimpleBitfieldSubscription<S> {
    fn subscribe(
        &mut self,
        peer: BitfieldPeer,
        hash: Hash,
    ) -> BoxStream<'static, BitfieldSubscriptionEvent> {
        let (send, recv) = tokio::sync::oneshot::channel();
        match peer {
            BitfieldPeer::Local => {
                let store = self.store.clone();
                self.local_pool.spawn_detached(move || async move {
                    match get_valid_ranges_local(&hash, store).await {
                        Ok(ranges) => {
                            send.send(ranges).ok();
                        }
                        Err(e) => {
                            tracing::error!("error getting bitfield: {e}");
                        }
                    };
                });
            }
            BitfieldPeer::Remote(id) => {
                let endpoint = self.endpoint.clone();
                tokio::spawn(async move {
                    match get_valid_ranges_remote(&endpoint, id, &hash).await {
                        Ok(ranges) => {
                            send.send(ranges).ok();
                        }
                        Err(cause) => {
                            tracing::error!("error getting bitfield: {cause}");
                        }
                    }
                });
            }
        }
        Box::pin(
            async move {
                let ranges = match recv.await {
                    Ok(ev) => ev,
                    Err(_) => ChunkRanges::empty(),
                };
                BitfieldSubscriptionEvent::Bitfield { ranges }
            }
            .into_stream(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::net_protocol::Blobs;

    use super::*;
    use bao_tree::ChunkNum;
    use iroh::{protocol::Router, SecretKey};
    use testresult::TestResult;

    fn print_bitfield(iter: impl IntoIterator<Item = bool>) -> String {
        let mut chars = String::new();
        for x in iter {
            chars.push(if x { '█' } else { ' ' });
        }
        chars
    }

    fn as_bool_iter(x: &ChunkRanges, max: u64) -> impl Iterator<Item = bool> {
        let max = x
            .iter()
            .last()
            .map(|x| match x {
                RangeSetRange::RangeFrom(_) => max,
                RangeSetRange::Range(x) => x.end.0,
            })
            .unwrap_or_default();
        let res = (0..max)
            .map(move |i| x.contains(&ChunkNum(i)))
            .collect::<Vec<_>>();
        res.into_iter()
    }

    fn peer(id: u8) -> NodeId {
        let mut secret = [0; 32];
        secret[0] = id;
        SecretKey::from(secret).public()
    }

    #[test]
    fn test_planner_1() {
        let mut planner = StripePlanner2::new(0, 4);
        let hash = Hash::new(b"test");
        let mut ranges = make_range_map(&[chunk_ranges([0..50]), chunk_ranges([50..100])]);
        println!("");
        print_range_map(&ranges);
        println!("planning");
        planner.plan(hash, &mut ranges);
        print_range_map(&ranges);
    }

    #[test]
    fn test_planner_2() {
        let mut planner = StripePlanner2::new(0, 4);
        let hash = Hash::new(b"test");
        let mut ranges = make_range_map(&[
            chunk_ranges([0..100]),
            chunk_ranges([0..100]),
            chunk_ranges([0..100]),
        ]);
        println!("");
        print_range_map(&ranges);
        println!("planning");
        planner.plan(hash, &mut ranges);
        print_range_map(&ranges);
    }

    #[test]
    fn test_planner_3() {
        let mut planner = StripePlanner2::new(0, 4);
        let hash = Hash::new(b"test");
        let mut ranges = make_range_map(&[
            chunk_ranges([0..100]),
            chunk_ranges([0..110]),
            chunk_ranges([0..120]),
            chunk_ranges([0..50]),
        ]);
        println!("");
        print_range_map(&ranges);
        println!("planning");
        planner.plan(hash, &mut ranges);
        print_range_map(&ranges);
    }

    fn make_range_map(ranges: &[ChunkRanges]) -> BTreeMap<NodeId, ChunkRanges> {
        let mut res = BTreeMap::new();
        for (i, range) in ranges.iter().enumerate() {
            res.insert(peer(i as u8), range.clone());
        }
        res
    }

    fn print_range_map(ranges: &BTreeMap<NodeId, ChunkRanges>) {
        for (peer, ranges) in ranges {
            let x = print_bitfield(as_bool_iter(ranges, 100));
            println!("{peer}: {x}");
        }
    }

    #[test]
    fn test_is_superset() {
        let local = ChunkRanges::from(ChunkNum(0)..ChunkNum(100));
        let request = ChunkRanges::from(ChunkNum(0)..ChunkNum(50));
        assert!(local.is_superset(&request));
    }

    #[cfg(feature = "rpc")]
    async fn make_test_node(data: &[u8]) -> anyhow::Result<(Router, NodeId, Hash)> {
        // let noop_subscriber = tracing_subscriber::fmt::Subscriber::builder()
        //     .with_writer(io::sink) // all output is discarded
        //     .with_max_level(tracing::level_filters::LevelFilter::OFF) // effectively disable logging
        //     .finish();
        // let noop_dispatch = tracing::Dispatch::new(noop_subscriber);
        let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
        let node_id = endpoint.node_id();
        let store = crate::store::mem::Store::new();
        let blobs = Blobs::builder(store).build(&endpoint);
        let hash = blobs
            .client()
            .add_bytes(bytes::Bytes::copy_from_slice(data))
            .await?
            .hash;
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(crate::ALPN, blobs)
            .spawn()
            .await?;
        Ok((router, node_id, hash))
    }

    /// Create chunk ranges from an array of u64 ranges
    pub fn chunk_ranges(ranges: impl IntoIterator<Item = Range<u64>>) -> ChunkRanges {
        let mut res = ChunkRanges::empty();
        for range in ranges.into_iter() {
            res |= ChunkRanges::from(ChunkNum(range.start)..ChunkNum(range.end));
        }
        res
    }

    pub fn noop_planner() -> BoxedDownloadPlanner {
        Box::new(NoopPlanner)
    }

    /// Checks if an exact event is present exactly once in a list of events
    pub fn has_one_event(evs: &[Event], ev: &Event) -> bool {
        evs.iter().filter(|e| *e == ev).count() == 1
    }

    pub fn has_all_events(evs: &[Event], evs2: &[&Event]) -> bool {
        evs2.iter().all(|ev| has_one_event(evs, ev))
    }

    pub fn has_one_event_matching(evs: &[Event], f: impl Fn(&Event) -> bool) -> bool {
        evs.iter().filter(|e| f(e)).count() == 1
    }

    #[tokio::test]
    #[cfg(feature = "rpc")]
    async fn downloader_driver_smoke() -> TestResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (_router1, peer, hash) = make_test_node(b"test").await?;
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![crate::protocol::ALPN.to_vec()])
            .discovery_n0()
            .bind()
            .await?;
        let discovery = StaticContentDiscovery {
            info: BTreeMap::new(),
            default: vec![peer],
        };
        let bitfield_subscription = TestBitfieldSubscription;
        let downloader = Downloader::builder(endpoint, store)
            .discovery(discovery)
            .bitfield_subscription(bitfield_subscription)
            .build();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let fut = downloader.download(DownloadRequest {
            hash,
            ranges: chunk_ranges([0..1]),
        });
        fut.await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "rpc")]
    async fn downloader_driver_large() -> TestResult<()> {
        use std::collections::BTreeSet;

        let _ = tracing_subscriber::fmt::try_init();
        let data = vec![0u8; 1024 * 1024];
        let mut nodes = vec![];
        for _i in 0..4 {
            nodes.push(make_test_node(&data).await?);
        }
        let peers = nodes.iter().map(|(_, peer, _)| *peer).collect::<Vec<_>>();
        let hashes = nodes
            .iter()
            .map(|(_, _, hash)| *hash)
            .collect::<BTreeSet<_>>();
        let hash = *hashes.iter().next().unwrap();
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![crate::protocol::ALPN.to_vec()])
            .discovery_n0()
            .bind()
            .await?;
        let discovery = StaticContentDiscovery {
            info: BTreeMap::new(),
            default: peers,
        };
        let downloader = Downloader::builder(endpoint, store)
            .discovery(discovery)
            .planner(StripePlanner2::new(0, 8))
            .build();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let fut = downloader.download(DownloadRequest {
            hash,
            ranges: chunk_ranges([0..1024]),
        });
        fut.await?;
        Ok(())
    }
}
