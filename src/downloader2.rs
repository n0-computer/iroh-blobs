//! Downloader version that supports range downloads and downloads from multiple sources.
//!
//! # Structure
//!
//! The [DownloaderState] is a synchronous state machine containing the logic.
//! It gets commands and produces events. It does not do any IO and also does
//! not have any time dependency. So [DownloaderState::apply_and_get_evs] is a
//! pure function of the state and the command and can therefore be tested
//! easily.
//!
//! In several places, requests are identified by a unique id. It is the responsibility
//! of the caller to generate unique ids. We could use random uuids here, but using
//! integers simplifies testing.
//!
//! Inside the state machine, we use [ChunkRanges] to represent avaiability bitmaps.
//! We treat operations on such bitmaps as very cheap, which is the case as long as
//! the bitmaps are not very fragmented. We can introduce an even more optimized
//! bitmap type, or prevent fragmentation.
//!
//! The [DownloaderDriver] is the asynchronous driver for the state machine. It
//! owns the actual tasks that perform IO.
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    sync::Arc,
};

use crate::{
    get::{
        fsm::{AtInitial, BlobContentNext, ConnectedNext, EndBlobNext},
        Stats,
    }, protocol::{GetRequest, RangeSpec, RangeSpecSeq}, store::{BaoBatchWriter, MapEntryMut, Store}, util::local_pool::{self, LocalPool}, Hash
};
use anyhow::Context;
use bao_tree::{io::BaoContentItem, ChunkNum, ChunkRanges};
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
use iroh::{discovery, Endpoint, NodeId};
use quinn::Chunk;
use range_collections::range_set::RangeSetRange;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use tracing::{debug, error, info, trace};

/// todo: make newtypes?
type DownloadId = u64;
type BitmapSubscriptionId = u64;
type DiscoveryId = u64;
type PeerDownloadId = u64;

/// Announce kind
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum AnnounceKind {
    /// The peer supposedly has some of the data.
    Partial = 0,
    /// The peer supposedly has the complete data.
    #[default]
    Complete,
}

#[derive(Default)]
struct FindPeersOpts {
    /// Kind of announce
    kind: AnnounceKind,
}

/// A pluggable content discovery mechanism
trait ContentDiscovery: Send + 'static {
    /// Find peers that have the given blob.
    ///
    /// The returned stream is a handle for the discovery task. It should be an
    /// infinite stream that only stops when it is dropped.
    fn find_peers(&mut self, hash: Hash, opts: FindPeersOpts) -> impl Stream<Item = NodeId> + Send + Unpin + 'static;
}

/// Global information about a peer
#[derive(Debug, Default)]
struct PeerState {
    /// Executed downloads, to calculate the average download speed.
    ///
    /// This gets updated as soon as possible when new data has been downloaded.
    download_history: VecDeque<(Duration, (u64, u64))>,
}

/// Information about one blob on one peer
struct PeerBlobState {
    /// The subscription id for the subscription
    subscription_id: BitmapSubscriptionId,
    /// The number of subscriptions this peer has
    subscription_count: usize,
    /// chunk ranges this peer reports to have
    ranges: ChunkRanges,
}

impl PeerBlobState {
    fn new(subscription_id: BitmapSubscriptionId) -> Self {
        Self { subscription_id, subscription_count: 1, ranges: ChunkRanges::empty() }
    }
}

#[derive(Debug)]
struct DownloadRequest {
    /// The blob we are downloading
    hash: Hash,
    /// The ranges we are interested in
    ranges: ChunkRanges,
}

struct DownloadState {
    /// The request this state is for
    request: DownloadRequest,
    /// Ongoing downloads
    peer_downloads: BTreeMap<NodeId, PeerDownloadState>,
}

impl DownloadState {
    fn new(request: DownloadRequest) -> Self {
        Self { request, peer_downloads: BTreeMap::new() }
    }
}

struct PeerDownloadState {
    id: PeerDownloadId,
    ranges: ChunkRanges,
}

struct DownloaderState {
    // all peers I am tracking for any download
    peers: BTreeMap<NodeId, PeerState>,
    // all bitmaps I am tracking, both for myself and for remote peers
    //
    // each item here corresponds to an active subscription
    bitmaps: BTreeMap<(Option<NodeId>, Hash), PeerBlobState>,
    // all active downloads
    //
    // these are user downloads. each user download gets split into one or more
    // peer downloads.
    downloads: BTreeMap<DownloadId, DownloadState>,
    // discovery tasks
    //
    // there is a discovery task for each blob we are interested in.
    discovery: BTreeMap<Hash, DiscoveryId>,

    // Counters to generate unique ids for various requests.
    // We could use uuid here, but using integers simplifies testing.
    //
    // the next subscription id
    next_subscription_id: BitmapSubscriptionId,
    // the next discovery id
    next_discovery_id: u64,
    // the next peer download id
    next_peer_download_id: u64,
}

impl DownloaderState {
    fn new() -> Self {
        Self {
            peers: BTreeMap::new(),
            downloads: BTreeMap::new(),
            bitmaps: BTreeMap::new(),
            discovery: BTreeMap::new(),
            next_subscription_id: 0,
            next_discovery_id: 0,
            next_peer_download_id: 0,
        }
    }
}

enum Command {
    /// A user request to start a download.
    StartDownload {
        /// The download request
        request: DownloadRequest,
        /// The unique id, to be assigned by the caller
        id: u64,
    },
    /// A user request to abort a download.
    StopDownload { id: u64 },
    /// A full bitmap for a blob and a peer
    Bitmap {
        /// The peer that sent the bitmap.
        peer: Option<NodeId>,
        /// The blob for which the bitmap is
        hash: Hash,
        /// The complete bitmap
        bitmap: ChunkRanges,
    },
    /// An update of a bitmap for a hash
    ///
    /// This is used both to update the bitmap of remote peers, and to update
    /// the local bitmap.
    BitmapUpdate {
        /// The peer that sent the update.
        peer: Option<NodeId>,
        /// The blob that was updated.
        hash: Hash,
        /// The ranges that were added
        added: ChunkRanges,
        /// The ranges that were removed
        removed: ChunkRanges,
    },
    /// A chunk was downloaded, but not yet stored
    /// 
    /// This can only be used for updating peer stats, not for completing downloads.
    ChunksDownloaded {
        /// Time when the download was received
        time: Duration,
        /// The peer that sent the chunk
        peer: NodeId,
        /// The blob that was downloaded
        hash: Hash,
        /// The ranges that were added locally
        added: ChunkRanges,
    },
    /// Stop tracking a peer for all blobs, for whatever reason
    DropPeer { peer: NodeId },
    /// A peer has been discovered
    PeerDiscovered { peer: NodeId, hash: Hash },
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    SubscribeBitmap {
        peer: Option<NodeId>,
        hash: Hash,
        /// The unique id of the subscription
        id: u64,
    },
    UnsubscribeBitmap {
        /// The unique id of the subscription
        id: u64,
    },
    StartDiscovery {
        hash: Hash,
        /// The unique id of the discovery task
        id: u64,
    },
    StopDiscovery {
        /// The unique id of the discovery task
        id: u64,
    },
    StartPeerDownload {
        /// The unique id of the peer download task
        id: u64,
        peer: NodeId,
        hash: Hash,
        ranges: ChunkRanges,
    },
    StopPeerDownload {
        /// The unique id of the peer download task
        id: u64,
    },
    DownloadComplete {
        /// The unique id of the user download
        id: u64,
    },
    /// An error that stops processing the command
    Error { message: String },
}

impl DownloaderState {
    fn next_subscription_id(&mut self) -> u64 {
        let id = self.next_subscription_id;
        self.next_subscription_id += 1;
        id
    }

    fn next_discovery_id(&mut self) -> u64 {
        let id = self.next_discovery_id;
        self.next_discovery_id += 1;
        id
    }

    fn next_peer_download_id(&mut self) -> u64 {
        let id = self.next_peer_download_id;
        self.next_peer_download_id += 1;
        id
    }

    fn count_providers(&self, hash: Hash) -> usize {
        self.bitmaps.iter().filter(|((peer, x), _)| peer.is_some() && *x == hash).count()
    }

    /// Apply a command and return the events that were generated
    fn apply_and_get_evs(&mut self, cmd: Command) -> Vec<Event> {
        let mut evs = vec![];
        self.apply(cmd, &mut evs);
        evs
    }

    /// Apply a command
    fn apply(&mut self, cmd: Command, evs: &mut Vec<Event>) {
        if let Err(cause) = self.apply0(cmd, evs) {
            evs.push(Event::Error { message: format!("{cause}") });
        }
    }

    /// Apply a command and bail out on error
    fn apply0(&mut self, cmd: Command, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        match cmd {
            Command::StartDownload { request, id } => {
                // ids must be uniquely assigned by the caller!
                anyhow::ensure!(!self.downloads.contains_key(&id), "duplicate download request {id}");
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitmaps.get_mut(&(None, request.hash)) {
                    // just increment the count
                    state.subscription_count += 1;
                } else {
                    // create a new subscription
                    let subscription_id = self.next_subscription_id();
                    evs.push(Event::SubscribeBitmap { peer: None, hash: request.hash, id: subscription_id });
                    self.bitmaps.insert((None, request.hash), PeerBlobState::new(subscription_id));
                }
                if !self.discovery.contains_key(&request.hash) {
                    // start a discovery task
                    let discovery_id = self.next_discovery_id();
                    evs.push(Event::StartDiscovery { hash: request.hash, id: discovery_id });
                    self.discovery.insert(request.hash, discovery_id);
                }
                self.downloads.insert(id, DownloadState::new(request));
            }
            Command::StopDownload { id } => {
                let removed = self.downloads.remove(&id).context(format!("removed unknown download {id}"))?;
                self.bitmaps.retain(|(_peer, hash), state| {
                    if *hash == removed.request.hash {
                        state.subscription_count -= 1;
                        if state.subscription_count == 0 {
                            evs.push(Event::UnsubscribeBitmap { id: state.subscription_id });
                            return false;
                        }
                    }
                    true
                });
            }
            Command::PeerDiscovered { peer, hash } => {
                if self.bitmaps.contains_key(&(Some(peer), hash)) {
                    // we already have a subscription for this peer
                    return Ok(());
                };
                // check if anybody needs this peer
                if !self.downloads.iter().any(|(_id, state)| state.request.hash == hash) {
                    return Ok(());
                }
                // create a peer state if it does not exist
                let _state = self.peers.entry(peer).or_default();
                // create a new subscription
                let subscription_id = self.next_subscription_id();
                evs.push(Event::SubscribeBitmap { peer: Some(peer), hash, id: subscription_id });
                self.bitmaps.insert((Some(peer), hash), PeerBlobState::new(subscription_id));
            }
            Command::DropPeer { peer } => {
                self.bitmaps.retain(|(p, _), state| {
                    if *p == Some(peer) {
                        // todo: should we emit unsubscribe evs here?
                        evs.push(Event::UnsubscribeBitmap { id: state.subscription_id });
                        return false;
                    } else {
                        return true;
                    }
                });
                self.peers.remove(&peer);
            }
            Command::Bitmap { peer, hash, bitmap } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("bitmap for unknown peer {peer:?} and hash {hash}"))?;
                let _chunks = total_chunks(&bitmap).context("open range")?;
                state.ranges = bitmap;
                self.rebalance_downloads(hash, evs)?;
            }
            Command::BitmapUpdate { peer, hash, added, removed } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("bitmap update for unknown peer {peer:?} and hash {hash}"))?;
                state.ranges |= added;
                state.ranges &= !removed;
                self.rebalance_downloads(hash, evs)?;
            }
            Command::ChunksDownloaded { time, peer, hash, added } => {
                let state = self.bitmaps.get_mut(&(None, hash)).context(format!("chunks downloaded before having local bitmap for {hash}"))?;
                let total_downloaded = total_chunks(&added).context("open range")?;
                let total_before = total_chunks(&state.ranges).context("open range")?;
                state.ranges |= added;
                let total_after = total_chunks(&state.ranges).context("open range")?;
                let useful_downloaded = total_after - total_before;
                let peer = self.peers.get_mut(&peer).context(format!("performing download before having peer state for {peer}"))?;
                peer.download_history.push_back((time, (total_downloaded, useful_downloaded)));
                self.rebalance_downloads(hash, evs)?;
            }
        }
        Ok(())
    }

    fn rebalance_downloads(&mut self, hash: Hash, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let Some(self_state) = self.bitmaps.get(&(None, hash)) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        let mut completed = vec![];
        for (id, download) in self.downloads.iter_mut().filter(|(_id, download)| download.request.hash == hash) {
            let remaining = &download.request.ranges - &self_state.ranges;
            if remaining.is_empty() {
                // cancel all downloads, if needed
                for (_, peer_download) in &download.peer_downloads {
                    evs.push(Event::StopPeerDownload { id: peer_download.id });
                }
                // notify the user that the download is complete
                evs.push(Event::DownloadComplete { id: *id });
                // mark the download for later removal
                completed.push(*id);
                continue;
            }
            let mut candidates = vec![];
            for ((peer, _), bitmap) in self.bitmaps.iter().filter(|((peer, x), _)| peer.is_some() && *x == hash) {
                let intersection = &bitmap.ranges & &remaining;
                if !intersection.is_empty() {
                    candidates.push((peer.unwrap(), intersection));
                }
            }
            for (_, state) in &download.peer_downloads {
                // stop all downloads
                evs.push(Event::StopPeerDownload { id: state.id });
            }
            download.peer_downloads.clear();
            for (peer, ranges) in candidates {
                let id = self.next_peer_download_id;
                self.next_peer_download_id += 1;
                evs.push(Event::StartPeerDownload { id, peer, hash, ranges: ranges.clone() });
                download.peer_downloads.insert(peer, PeerDownloadState { id, ranges });
            }
        }
        for id in completed {
            self.downloads.remove(&id);
        }
        Ok(())
    }
}

fn total_chunks(chunks: &ChunkRanges) -> Option<u64> {
    let mut total = 0;
    for range in chunks.iter() {
        match range {
            RangeSetRange::RangeFrom(_range) => return None,
            RangeSetRange::Range(range) => total += range.end.0 - range.start.0,
        }
    }
    Some(total)
}

#[derive(Debug, Clone)]
struct Downloader {
    send: mpsc::Sender<UserCommand>,
    task: Arc<AbortOnDropHandle<()>>,
}

impl Downloader {
    async fn download(&self, request: DownloadRequest) -> anyhow::Result<()> {
        let (send, recv) = tokio::sync::oneshot::channel::<()>();
        self.send.send(UserCommand::Download { request, done: send }).await?;
        recv.await?;
        Ok(())
    }

    fn new<S: Store, D: ContentDiscovery>(endpoint: Endpoint, store: S, discovery: D, local_pool: LocalPool) -> Self {
        let actor = DownloaderActor::new(endpoint, store, discovery, local_pool);
        let (send, recv) = tokio::sync::mpsc::channel(256);
        let task = Arc::new(spawn(async move { actor.run(recv).await }));
        Self { send, task }
    }
}

/// An user-facing command
#[derive(Debug)]
enum UserCommand {
    Download { request: DownloadRequest, done: tokio::sync::oneshot::Sender<()> },
}

struct DownloaderActor<S, D> {
    local_pool: LocalPool,
    endpoint: Endpoint,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    state: DownloaderState,
    store: S,
    discovery: D,
    download_futs: BTreeMap<DownloadId, tokio::sync::oneshot::Sender<()>>,
    peer_download_tasks: BTreeMap<PeerDownloadId, local_pool::Run<anyhow::Result<()>>>,
    discovery_tasks: BTreeMap<DiscoveryId, AbortOnDropHandle<()>>,
    bitmap_subscription_tasks: BTreeMap<BitmapSubscriptionId, AbortOnDropHandle<()>>,
    next_download_id: DownloadId,
}

impl<S: Store, D: ContentDiscovery> DownloaderActor<S, D> {
    fn new(endpoint: Endpoint, store: S, discovery: D, local_pool: LocalPool) -> Self {
        let (send, recv) = mpsc::channel(256);
        Self {
            local_pool,
            endpoint,
            state: DownloaderState::new(),
            store,
            discovery,
            peer_download_tasks: BTreeMap::new(),
            discovery_tasks: BTreeMap::new(),
            bitmap_subscription_tasks: BTreeMap::new(),
            download_futs: BTreeMap::new(),
            command_tx: send,
            command_rx: recv,
            next_download_id: 0,
        }
    }

    async fn run(mut self, mut channel: mpsc::Receiver<UserCommand>) {
        loop {
            tokio::select! {
                biased;
                Some(cmd) = self.command_rx.recv() => {
                    let evs = self.state.apply_and_get_evs(cmd);
                    for ev in evs {
                        self.handle_event(ev, 0);
                    }
                },
                Some(cmd) = channel.recv() => {
                    debug!("user command {cmd:?}");
                    match cmd {
                        UserCommand::Download {
                            request, done,
                        } => {
                            let id = self.next_download_id();
                            self.download_futs.insert(id, done);
                            self.command_tx.send(Command::StartDownload { request, id }).await.ok();
                        }
                    }
                },
            }
        }
    }

    fn next_download_id(&mut self) -> DownloadId {
        let id = self.next_download_id;
        self.next_download_id += 1;
        id
    }

    fn handle_event(&mut self, ev: Event, depth: usize) {
        trace!("handle_event {ev:?} {depth}");
        match ev {
            Event::SubscribeBitmap { peer, hash, id } => {
                let send = self.command_tx.clone();
                let task = spawn(async move {
                    let cmd = if peer.is_none() {
                        // we don't have any data, for now
                        Command::Bitmap { peer: None, hash, bitmap: ChunkRanges::empty() }
                    } else {
                        // all peers have all the data, for now
                        Command::Bitmap { peer, hash, bitmap: ChunkRanges::from(ChunkNum(0)..ChunkNum(16)) }
                    };
                    send.send(cmd).await.ok();
                    futures_lite::future::pending().await
                });
                self.bitmap_subscription_tasks.insert(id, task);
            }
            Event::StartDiscovery { hash, id } => {
                let send = self.command_tx.clone();
                let mut stream = self.discovery.find_peers(hash, Default::default());
                let task = spawn(async move {
                    // process the infinite discovery stream and send commands
                    while let Some(peer) = stream.next().await {
                        println!("peer discovered for hash {hash}: {peer}");
                        let res = send.send(Command::PeerDiscovered { peer, hash }).await;
                        if res.is_err() {
                            // only reason for this is actor task dropped
                            break;
                        }
                    }
                });
                self.discovery_tasks.insert(id, task);
            }
            Event::StartPeerDownload { id, peer, hash, ranges } => {
                let send = self.command_tx.clone();
                let endpoint = self.endpoint.clone();
                let store = self.store.clone();
                let task = self.local_pool.spawn(move || async move {
                    info!("Connecting to peer {peer}");
                    let conn = endpoint.connect(peer, crate::ALPN).await;
                    info!("Got connection to peer {peer} {}", conn.is_err());
                    println!("{conn:?}");
                    let conn = conn?;
                    let spec = RangeSpec::new(ranges);
                    let initial = crate::get::fsm::start(conn, GetRequest { hash, ranges: RangeSpecSeq::new([spec]) });
                    stream_to_db(initial, store, hash, peer, send).await?;
                    anyhow::Ok(())
                });
                self.peer_download_tasks.insert(id, task);
            }
            Event::UnsubscribeBitmap { id } => {
                self.bitmap_subscription_tasks.remove(&id);
            }
            Event::StopDiscovery { id } => {
                self.discovery_tasks.remove(&id);
            }
            Event::StopPeerDownload { id } => {
                self.peer_download_tasks.remove(&id);
            }
            Event::Error { message } => {
                error!("Error during processing event {}", message);
            }
            _ => {
                println!("event: {:?}", ev);
            }
        }
    }
}

/// A simple static content discovery mechanism
struct StaticContentDiscovery {
    info: BTreeMap<Hash, Vec<NodeId>>,
    default: Vec<NodeId>,
}

impl ContentDiscovery for StaticContentDiscovery {
    fn find_peers(&mut self, hash: Hash, _opts: FindPeersOpts) -> impl Stream<Item = NodeId> + Unpin + 'static {
        let peers = self.info.get(&hash).unwrap_or(&self.default).clone();
        futures_lite::stream::iter(peers).chain(futures_lite::stream::pending())
    }
}

async fn stream_to_db<S: Store>(initial: AtInitial, store: S, hash: Hash, peer: NodeId, sender: mpsc::Sender<Command>) -> io::Result<Stats> {
    // connect
    let connected = initial.next().await?;
    // read the first bytes
    let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
        return Err(io::Error::new(io::ErrorKind::Other, "expected start root"));
    };
    let header = start_root.next();

    // get the size of the content
    let (mut content, size) = header.next().await?;
    let entry = store.get_or_create(hash, size).await?;
    let mut writer = entry.batch_writer().await?;
    let mut batch = Vec::new();
    // manually loop over the content and yield all data
    let done = loop {
        match content.next().await {
            BlobContentNext::More((next, data)) => {
                match data? {
                    BaoContentItem::Parent(parent) => {
                        batch.push(parent.into());
                    },
                    BaoContentItem::Leaf(leaf) => {
                        let added = ChunkRanges::from(ChunkNum(leaf.offset / 1024)..);
                        sender.send(Command::ChunksDownloaded { time: Duration::ZERO, peer, hash, added: added.clone() }).await.ok();
                        batch.push(leaf.into());
                        writer.write_batch(size, std::mem::take(&mut batch)).await?;
                        sender.send(Command::BitmapUpdate { peer: None, hash, added, removed: ChunkRanges::empty() }).await.ok();
                    }
                }
                content = next;
            }
            BlobContentNext::Done(done) => {
                // we are done with the root blob
                break done;
            }
        }
    };
    // close the connection even if there is more data
    let closing = match done.next() {
        EndBlobNext::Closing(closing) => closing,
        EndBlobNext::MoreChildren(more) => more.finish(),
    };
    // close the connection
    let stats = closing.next().await?;
    Ok(stats)
}

fn spawn<F, T>(f: F) -> AbortOnDropHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let task = tokio::spawn(f);
    AbortOnDropHandle::new(task)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::net_protocol::Blobs;

    use super::*;
    use bao_tree::ChunkNum;
    use iroh::protocol::Router;
    use testresult::TestResult;

    #[cfg(feature = "rpc")]
    async fn make_test_node(data: &[u8]) -> anyhow::Result<(Router, NodeId, Hash)> {
        let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
        let node_id = endpoint.node_id();
        let store = crate::store::mem::Store::new();
        let blobs = Blobs::builder(store)
            .build(&endpoint);
        let hash = blobs.client().add_bytes(bytes::Bytes::copy_from_slice(data)).await?.hash;
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(crate::ALPN, blobs)
            .spawn().await?;
        Ok((router, node_id, hash))
    }

    /// Create chunk ranges from an array of u64 ranges
    fn chunk_ranges(ranges: impl IntoIterator<Item = Range<u64>>) -> ChunkRanges {
        let mut res = ChunkRanges::empty();
        for range in ranges.into_iter() {
            res |= ChunkRanges::from(ChunkNum(range.start)..ChunkNum(range.end));
        }
        res
    }

    #[test]
    fn downloader_state_smoke() -> TestResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        // let me = "0000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let unknown_hash = "0000000000000000000000000000000000000000000000000000000000000002".parse()?;
        let mut state = DownloaderState::new();
        let evs = state.apply_and_get_evs(super::Command::StartDownload { request: DownloadRequest { hash, ranges: chunk_ranges([0..64]) }, id: 1 });
        assert!(evs.iter().filter(|e| **e == Event::StartDiscovery { hash, id: 0 }).count() == 1, "starting a download should start a discovery task");
        assert!(evs.iter().filter(|e| **e == Event::SubscribeBitmap { peer: None, hash, id: 0 }).count() == 1, "starting a download should subscribe to the local bitmap");
        println!("{evs:?}");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: None, hash, bitmap: ChunkRanges::all() });
        assert!(evs.iter().any(|e| matches!(e, Event::Error { .. })), "adding an open bitmap should produce an error!");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: None, hash: unknown_hash, bitmap: ChunkRanges::all() });
        assert!(evs.iter().any(|e| matches!(e, Event::Error { .. })), "adding an open bitmap for an unknown hash should produce an error!");
        let initial_bitmap = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: None, hash, bitmap: initial_bitmap.clone() });
        assert!(evs.is_empty());
        assert_eq!(state.bitmaps.get(&(None, hash)).context("bitmap should be present")?.ranges, initial_bitmap, "bitmap should be set to the initial bitmap");
        let evs = state.apply_and_get_evs(Command::BitmapUpdate { peer: None, hash, added: chunk_ranges([16..32]), removed: ChunkRanges::empty() });
        assert!(evs.is_empty());
        assert_eq!(state.bitmaps.get(&(None, hash)).context("bitmap should be present")?.ranges, ChunkRanges::from(ChunkNum(0)..ChunkNum(32)), "bitmap should be updated");
        let evs = state.apply_and_get_evs(super::Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: ChunkRanges::from(ChunkNum(0)..ChunkNum(16)),
        });
        assert!(evs.iter().any(|e| matches!(e, Event::Error { .. })), "download from unknown peer should lead to an error!");
        let evs = state.apply_and_get_evs(Command::PeerDiscovered { peer: peer_a, hash });
        assert!(evs.iter().filter(|e| **e == Event::SubscribeBitmap { peer: Some(peer_a), hash, id: 1 }).count() == 1, "adding a new peer for a hash we are interested in should subscribe to the bitmap");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: Some(peer_a), hash, bitmap: chunk_ranges([0..64]) });
        assert!(evs.iter().filter(|e| **e == Event::StartPeerDownload { id: 0, peer: peer_a, hash, ranges: chunk_ranges([32..64]) }).count() == 1, "bitmap from a peer should start a download");
        let evs = state.apply_and_get_evs(Command::ChunksDownloaded { time: Duration::ZERO, peer: peer_a, hash, added: chunk_ranges([32..64]) });
        assert!(evs.iter().filter(|e| matches!(e, Event::DownloadComplete { .. })).count() == 1, "download should be completed by the data");
        println!("{evs:?}");
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "rpc")]
    async fn downloader_driver_smoke() -> TestResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (router, peer, hash) = make_test_node(b"test").await?;
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder().alpns(vec![crate::protocol::ALPN.to_vec()]).discovery_n0().bind().await?;
        let discovery = StaticContentDiscovery { info: BTreeMap::new(), default: vec![peer] };
        let local_pool = LocalPool::single();
        let downloader = Downloader::new(endpoint, store, discovery, local_pool);
        tokio::time::sleep(Duration::from_secs(2)).await;
        let fut = downloader.download(DownloadRequest { hash, ranges: ChunkRanges::all() });
        fut.await?;
        Ok(())
    }
}
