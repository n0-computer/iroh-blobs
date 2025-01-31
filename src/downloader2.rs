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
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    io,
    sync::Arc,
};

use crate::{
    get::{
        fsm::{AtInitial, BlobContentNext, ConnectedNext, EndBlobNext},
        Stats,
    },
    protocol::{GetRequest, RangeSpec, RangeSpecSeq},
    store::{BaoBatchWriter, MapEntryMut, Store},
    util::local_pool::{self, LocalPool},
    Hash,
};
use anyhow::Context;
use bao_tree::{io::BaoContentItem, ChunkNum, ChunkRanges};
use futures_lite::{Stream, StreamExt};
use iroh::{Endpoint, NodeId};
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

#[derive(Debug, Default)]
struct IdGenerator {
    next_id: u64,
}

impl IdGenerator {

    fn next(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

/// Trait for a download planner.
///
/// A download planner has the option to be stateful and keep track of plans
/// depending on the hash, but many planners will be stateless.
/// 
/// Planners can do whatever they want with the chunk ranges. Usually, they
/// want to deduplicate the ranges, but they could also do other things, like
/// eliminate gaps or even extend ranges. The only thing they should not do is
/// to add new peers to the list of options.
trait DownloadPlanner: Send + 'static {
    /// Make a download plan for a hash, by reducing or eliminating the overlap of chunk ranges
    fn plan(&mut self, hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>);
}

type BoxedDownloadPlanner = Box<dyn DownloadPlanner>;

/// A download planner that just leaves everything as is.
///
/// Data will be downloaded from all peers wherever multiple peers have the same data.
struct NoopPlanner;

impl DownloadPlanner for NoopPlanner {
    fn plan(&mut self, _hash: Hash, _options: &mut BTreeMap<NodeId, ChunkRanges>) {}
}

/// A download planner that fully removes overlap between peers.
/// 
/// It divides files into stripes of a fixed size `1 << stripe_size_log` chunks,
/// and for each stripe decides on a single peer to download from, based on the
/// peer id and a random seed.
struct StripePlanner {
    /// seed for the score function. This can be set to 0 for testing for
    /// maximum determinism, but can be set to a random value for production
    /// to avoid multiple downloaders coming up with the same plan.
    seed: u64,
    /// The log of the stripe size in chunks. This planner is relatively
    /// dumb and does not try to come up with continuous ranges, but you can
    /// just set this to a large value to avoid fragmentation.
    /// 
    /// In the very common case where you have small downloads, this will
    /// frequently just choose a single peer for the entire download.
    ///
    /// This is a feature, not a bug. For small downloads, it is not worth
    /// the effort to come up with a more sophisticated plan.
    stripe_size_log: u8,
}

impl StripePlanner {
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self { seed, stripe_size_log }
    }

    /// The score function to decide which peer to download from.
    fn score(peer: &NodeId, seed: u64, stripe: u64) -> u64 {
        // todo: use fnv? blake3 is a bit overkill
        let mut data = [0u8; 32 + 8 + 8];
        data[..32].copy_from_slice(peer.as_bytes());
        data[32..40].copy_from_slice(&stripe.to_be_bytes());
        data[40..48].copy_from_slice(&seed.to_be_bytes());
        let hash = blake3::hash(&data);
        u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }
}

impl DownloadPlanner for StripePlanner {
    fn plan(&mut self, _hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>) {
        assert!(options.values().all(|x| x.boundaries().len() % 2 == 0), "open ranges not supported");
        options.retain(|_, x| !x.is_empty());
        if options.len() <= 1 {
            return;
        }
        let ranges = get_continuous_ranges(options, self.stripe_size_log).unwrap();
        for range in ranges.windows(2) {
            let start = ChunkNum(range[0]);
            let end = ChunkNum(range[1]);
            let curr = ChunkRanges::from(start..end);
            let stripe = range[0] >> self.stripe_size_log;
            let mut best_peer = None;
            let mut best_score = 0;
            let mut matching = vec![];
            for (peer, peer_ranges) in options.iter_mut() {
                if peer_ranges.contains(&start) {
                    let score = Self::score(peer, self.seed, stripe);
                    if score > best_score && peer_ranges.contains(&start) {
                        best_peer = Some(*peer);
                        best_score = score;
                    }
                    matching.push((peer, peer_ranges));
                }
            }
            for (peer, peer_ranges) in matching {
                if *peer != best_peer.unwrap() {
                    peer_ranges.difference_with(&curr);
                }
            }
        }
        options.retain(|_, x| !x.is_empty());
    }
}

fn get_continuous_ranges(options: &mut BTreeMap<NodeId, ChunkRanges>, stripe_size_log: u8) -> Option<Vec<u64>> {
    let mut ranges = BTreeSet::new();
    for x in options.values() {
        ranges.extend(x.boundaries().iter().map(|x| x.0));
    }
    let min = ranges.iter().next().copied()?;
    let max = ranges.iter().next_back().copied()?;
    // add stripe subdividers
    for i in (min >> stripe_size_log)..(max >> stripe_size_log) {
        let x = i << stripe_size_log;
        if x > min && x < max {
            ranges.insert(x);
        }
    }
    let ranges = ranges.into_iter().collect::<Vec<_>>();
    Some(ranges)
}


/// A download planner that fully removes overlap between peers.
/// 
/// It divides files into stripes of a fixed size `1 << stripe_size_log` chunks,
/// and for each stripe decides on a single peer to download from, based on the
/// peer id and a random seed.
struct StripePlanner2 {
    /// seed for the score function. This can be set to 0 for testing for
    /// maximum determinism, but can be set to a random value for production
    /// to avoid multiple downloaders coming up with the same plan.
    seed: u64,
    /// The log of the stripe size in chunks. This planner is relatively
    /// dumb and does not try to come up with continuous ranges, but you can
    /// just set this to a large value to avoid fragmentation.
    /// 
    /// In the very common case where you have small downloads, this will
    /// frequently just choose a single peer for the entire download.
    ///
    /// This is a feature, not a bug. For small downloads, it is not worth
    /// the effort to come up with a more sophisticated plan.
    stripe_size_log: u8,
}

impl StripePlanner2 {
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self { seed, stripe_size_log }
    }

    /// The score function to decide which peer to download from.
    fn score(peer: &NodeId, seed: u64) -> u64 {
        // todo: use fnv? blake3 is a bit overkill
        let mut data = [0u8; 32 + 8];
        data[..32].copy_from_slice(peer.as_bytes());
        data[32..40].copy_from_slice(&seed.to_be_bytes());
        let hash = blake3::hash(&data);
        u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }
}

impl DownloadPlanner for StripePlanner2 {
    fn plan(&mut self, _hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>) {
        assert!(options.values().all(|x| x.boundaries().len() % 2 == 0), "open ranges not supported");
        options.retain(|_, x| !x.is_empty());
        if options.len() <= 1 {
            return;
        }
        let ranges = get_continuous_ranges(options, self.stripe_size_log).unwrap();
        for range in ranges.windows(2) {
            let start = ChunkNum(range[0]);
            let end = ChunkNum(range[1]);
            let curr = ChunkRanges::from(start..end);
            let stripe = range[0] >> self.stripe_size_log;
            let mut best_peer = None;
            let mut best_score = None;
            let mut matching = vec![];
            for (peer, peer_ranges) in options.iter_mut() {
                if peer_ranges.contains(&start) {
                    matching.push((peer, peer_ranges));
                }
            }
            let mut peer_and_score = matching.iter().map(|(peer, _)| (Self::score(peer, self.seed), peer)).collect::<Vec<_>>();
            peer_and_score.sort();
            let peer_to_rank = peer_and_score.into_iter().enumerate().map(|(i, (_, peer))| (*peer, i as u64)).collect::<BTreeMap<_, _>>();
            let n = matching.len() as u64;
            for (peer, _) in matching.iter() {
                let score = Some((peer_to_rank[*peer] + stripe) % n);
                if score > best_score {
                    best_peer = Some(**peer);
                    best_score = score;
                }
            }
            for (peer, peer_ranges) in matching {
                if *peer != best_peer.unwrap() {
                    peer_ranges.difference_with(&curr);
                }
            }
        }
        options.retain(|_, x| !x.is_empty());
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
    bitmaps: BTreeMap<(BitmapPeer, Hash), PeerBlobState>,
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
    subscription_id_gen: IdGenerator,
    // the next discovery id
    discovery_id_gen: IdGenerator,
    // the next peer download id
    peer_download_id_gen: IdGenerator,
    // the download planner
    planner: Box<dyn DownloadPlanner>,
}

impl DownloaderState {
    fn new(planner: Box<dyn DownloadPlanner>) -> Self {
        Self {
            peers: BTreeMap::new(),
            downloads: BTreeMap::new(),
            bitmaps: BTreeMap::new(),
            discovery: BTreeMap::new(),
            subscription_id_gen: Default::default(),
            discovery_id_gen: Default::default(),
            peer_download_id_gen: Default::default(),
            planner,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
enum BitmapPeer {
    Local,
    Remote(NodeId),
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
        peer: BitmapPeer,
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
        peer: BitmapPeer,
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
        peer: BitmapPeer,
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

    fn count_providers(&self, hash: Hash) -> usize {
        self.bitmaps.iter().filter(|((peer, x), _)| *peer != BitmapPeer::Local && *x == hash).count()
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

    /// Stop a download and clean up
    /// 
    /// This is called both for stopping a download before completion, and for
    /// cleaning up after a successful download.
    /// 
    /// Cleanup involves emitting events for
    /// - stopping all peer downloads
    /// - unsubscribing from bitmaps if needed
    /// - stopping the discovery task if needed
    fn stop_download(&mut self, id: u64, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let removed = self.downloads.remove(&id).context(format!("removed unknown download {id}"))?;
        let removed_hash = removed.request.hash;
        // stop associated peer downloads
        for peer_download in removed.peer_downloads.values() {
            evs.push(Event::StopPeerDownload { id: peer_download.id });
        }
        // unsubscribe from bitmaps that have no more subscriptions
        self.bitmaps.retain(|(_peer, hash), state| {
            if *hash == removed_hash {
                state.subscription_count -= 1;
                if state.subscription_count == 0 {
                    evs.push(Event::UnsubscribeBitmap { id: state.subscription_id });
                    return false;
                }
            }
            true
        });
        let hash_interest = self.downloads.values().filter(|x| x.request.hash == removed.request.hash).count();
        if hash_interest == 0 {
            // stop the discovery task if we were the last one interested in the hash
            let discovery_id = self.discovery.remove(&removed.request.hash).context(format!("removed unknown discovery task for {}", removed.request.hash))?;
            evs.push(Event::StopDiscovery { id: discovery_id });
        }
        Ok(())
    }

    /// Apply a command and bail out on error
    fn apply0(&mut self, cmd: Command, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        match cmd {
            Command::StartDownload { request, id } => {
                // ids must be uniquely assigned by the caller!
                anyhow::ensure!(!self.downloads.contains_key(&id), "duplicate download request {id}");
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitmaps.get_mut(&(BitmapPeer::Local, request.hash)) {
                    // just increment the count
                    state.subscription_count += 1;
                } else {
                    // create a new subscription
                    let subscription_id = self.subscription_id_gen.next();
                    evs.push(Event::SubscribeBitmap { peer: BitmapPeer::Local, hash: request.hash, id: subscription_id });
                    self.bitmaps.insert((BitmapPeer::Local, request.hash), PeerBlobState::new(subscription_id));
                }
                if !self.discovery.contains_key(&request.hash) {
                    // start a discovery task
                    let discovery_id = self.discovery_id_gen.next();
                    evs.push(Event::StartDiscovery { hash: request.hash, id: discovery_id });
                    self.discovery.insert(request.hash, discovery_id);
                }
                self.downloads.insert(id, DownloadState::new(request));
            }
            Command::StopDownload { id } => {
                self.stop_download(id, evs)?;
            }
            Command::PeerDiscovered { peer, hash } => {
                if self.bitmaps.contains_key(&(BitmapPeer::Remote(peer), hash)) {
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
                let subscription_id = self.subscription_id_gen.next();
                evs.push(Event::SubscribeBitmap { peer: BitmapPeer::Remote(peer), hash, id: subscription_id });
                self.bitmaps.insert((BitmapPeer::Remote(peer), hash), PeerBlobState::new(subscription_id));
            }
            Command::DropPeer { peer } => {
                self.bitmaps.retain(|(p, _), state| {
                    if *p == BitmapPeer::Remote(peer) {
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
                if peer == BitmapPeer::Local {
                    self.check_completion(hash, evs)?;
                }
                // we have to call start_downloads even if the local bitmap set, since we don't know in which order local and remote bitmaps arrive
                self.start_downloads(hash, evs)?;
            }
            Command::BitmapUpdate { peer, hash, added, removed } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("bitmap update for unknown peer {peer:?} and hash {hash}"))?;
                state.ranges |= added;
                state.ranges &= !removed;
                if peer == BitmapPeer::Local {
                    self.check_completion(hash, evs)?;
                } else {
                    // a local bitmap update does not make more data available, so we don't need to start downloads
                    self.start_downloads(hash, evs)?;
                }
            }
            Command::ChunksDownloaded { time, peer, hash, added } => {
                let state = self.bitmaps.get_mut(&(BitmapPeer::Local, hash)).context(format!("chunks downloaded before having local bitmap for {hash}"))?;
                let total_downloaded = total_chunks(&added).context("open range")?;
                let total_before = total_chunks(&state.ranges).context("open range")?;
                state.ranges |= added;
                let total_after = total_chunks(&state.ranges).context("open range")?;
                let useful_downloaded = total_after - total_before;
                let peer = self.peers.get_mut(&peer).context(format!("performing download before having peer state for {peer}"))?;
                peer.download_history.push_back((time, (total_downloaded, useful_downloaded)));
            }
        }
        Ok(())
    }

    /// Check for completion of a download or of an individual peer download
    ///
    /// This must be called after each change of the local bitmap for a hash
    /// 
    /// In addition to checking for completion, this also create new peer downloads if a peer download is complete and there is more data available for that peer.
    fn check_completion(&mut self, hash: Hash, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let Some(self_state) = self.bitmaps.get(&(BitmapPeer::Local, hash)) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        let mut completed = vec![];
        for (id, download) in self.downloads.iter_mut().filter(|(_id, download)| download.request.hash == hash) {
            // check if the entire download is complete. If this is the case, peer downloads will be cleaned up later
            if self_state.ranges.is_superset(&download.request.ranges) {
                // notify the user that the download is complete
                evs.push(Event::DownloadComplete { id: *id });
                // remember id for later cleanup
                completed.push(*id);
                // no need to look at individual peer downloads in this case
                continue;
            }
            // check if any peer download is complete, and remove it.
            let mut available = vec![];
            download.peer_downloads.retain(|peer, peer_download| {
                if self_state.ranges.is_superset(&peer_download.ranges) {
                    // stop this peer download.
                    //
                    // Might be a noop if the cause for this local change was the same peer download, but we don't know.
                    evs.push(Event::StopPeerDownload { id: peer_download.id });
                    // mark this peer as available
                    available.push(*peer);
                    false
                } else {
                    true
                }
            });
            // reassign the newly available peers without doing a full rebalance
            if !available.is_empty() {
                // check if any of the available peers can provide something of the remaining data
                let mut remaining = &download.request.ranges - &self_state.ranges;
                // subtract the ranges that are already being taken care of by remaining peer downloads
                for peer_download in download.peer_downloads.values() {
                    remaining.difference_with(&peer_download.ranges);
                }
                // see what the new peers can do for us
                let mut candidates= BTreeMap::new();
                for peer in available {
                    let Some(peer_state) = self.bitmaps.get(&(BitmapPeer::Remote(peer), hash)) else {
                        // weird. we should have a bitmap for this peer since it just completed a download
                        continue;
                    };
                    let intersection = &peer_state.ranges & &remaining;
                    if !intersection.is_empty() {
                        candidates.insert(peer, intersection);
                    }
                }
                // deduplicate the ranges
                self.planner.plan(hash, &mut candidates);
                // start new downloads
                for (peer, ranges) in candidates {
                    let id = self.peer_download_id_gen.next();
                    evs.push(Event::StartPeerDownload { id, peer, hash, ranges: ranges.clone() });
                    download.peer_downloads.insert(peer, PeerDownloadState { id, ranges });
                }
            }
        }
        // cleanup completed downloads, has to happen later to avoid double mutable borrow
        for id in completed {
            self.stop_download(id, evs)?;
        }
        Ok(())
    }

    /// Look at all downloads for a hash and see start peer downloads for those that do not have any yet
    fn start_downloads(&mut self, hash: Hash, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let Some(self_state) = self.bitmaps.get(&(BitmapPeer::Local, hash)) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        for (_id, download) in self.downloads.iter_mut().filter(|(_id, download)| download.request.hash == hash && download.peer_downloads.is_empty()) {
            let remaining = &download.request.ranges - &self_state.ranges;
            let mut candidates = BTreeMap::new();
            for ((peer, _), bitmap) in self.bitmaps.iter().filter(|((peer, x), _)| *peer != BitmapPeer::Local && *x == hash) {
                let BitmapPeer::Remote(peer) = peer else { panic!() };
                let intersection = &bitmap.ranges & &remaining;
                if !intersection.is_empty() {
                    candidates.insert(*peer, intersection);
                }
            }
            self.planner.plan(hash, &mut candidates);
            for (peer, ranges) in candidates {
                info!("  Starting download from {peer} for {hash} {ranges:?}");
                let id = self.peer_download_id_gen.next();
                evs.push(Event::StartPeerDownload { id, peer, hash, ranges: ranges.clone() });
                download.peer_downloads.insert(peer, PeerDownloadState { id, ranges });
            }
        }
        Ok(())
    }

    fn rebalance_downloads(&mut self, hash: Hash, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let Some(self_state) = self.bitmaps.get(&(BitmapPeer::Local, hash)) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        for (id, download) in self.downloads.iter_mut().filter(|(_id, download)| download.request.hash == hash) {
            let remaining = &download.request.ranges - &self_state.ranges;
            let mut candidates = vec![];
            for ((peer, _), bitmap) in self.bitmaps.iter().filter(|((peer, x), _)| *peer != BitmapPeer::Local && *x == hash) {
                let BitmapPeer::Remote(peer) = peer else { panic!(); };
                let intersection = &bitmap.ranges & &remaining;
                if !intersection.is_empty() {
                    candidates.push((*peer, intersection));
                }
            }
            info!("Stopping {} old peer downloads", download.peer_downloads.len());
            for (_, state) in &download.peer_downloads {
                // stop all downloads
                evs.push(Event::StopPeerDownload { id: state.id });
            }
            info!("Creating {} new peer downloads", candidates.len());
            download.peer_downloads.clear();
            for (peer, ranges) in candidates {
                info!("  Starting download from {peer} for {hash} {ranges:?}");
                let id = self.peer_download_id_gen.next();
                evs.push(Event::StartPeerDownload { id, peer, hash, ranges: ranges.clone() });
                download.peer_downloads.insert(peer, PeerDownloadState { id, ranges });
            }
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

    fn new<S: Store, D: ContentDiscovery>(endpoint: Endpoint, store: S, discovery: D, local_pool: LocalPool, planner: Box<dyn DownloadPlanner>) -> Self {
        let actor = DownloaderActor::new(endpoint, store, discovery, local_pool, planner);
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
    download_id_gen: IdGenerator,
}

impl<S: Store, D: ContentDiscovery> DownloaderActor<S, D> {
    fn new(endpoint: Endpoint, store: S, discovery: D, local_pool: LocalPool, planner: Box<dyn DownloadPlanner>) -> Self {
        let (send, recv) = mpsc::channel(256);
        Self {
            local_pool,
            endpoint,
            state: DownloaderState::new(planner),
            store,
            discovery,
            peer_download_tasks: BTreeMap::new(),
            discovery_tasks: BTreeMap::new(),
            bitmap_subscription_tasks: BTreeMap::new(),
            download_futs: BTreeMap::new(),
            command_tx: send,
            command_rx: recv,
            download_id_gen: Default::default(),
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
                            let id = self.download_id_gen.next();
                            self.download_futs.insert(id, done);
                            self.command_tx.send(Command::StartDownload { request, id }).await.ok();
                        }
                    }
                },
            }
        }
    }

    fn handle_event(&mut self, ev: Event, depth: usize) {
        trace!("handle_event {ev:?} {depth}");
        match ev {
            Event::SubscribeBitmap { peer, hash, id } => {
                let send = self.command_tx.clone();
                let task = spawn(async move {
                    let cmd = if peer == BitmapPeer::Local {
                        // we don't have any data, for now
                        Command::Bitmap { peer: BitmapPeer::Local, hash, bitmap: ChunkRanges::empty() }
                    } else {
                        // all peers have all the data, for now
                        Command::Bitmap { peer, hash, bitmap: ChunkRanges::from(ChunkNum(0)..ChunkNum(1024)) }
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
                    let conn = endpoint.connect(peer, crate::ALPN).await?;
                    info!("Got connection to peer {peer}");
                    let spec = RangeSpec::new(ranges);
                    let ranges = RangeSpecSeq::new([spec, RangeSpec::EMPTY]);
                    info!("starting download from {peer} for {hash} {ranges:?}");
                    let request = GetRequest::new(hash, ranges);
                    let initial = crate::get::fsm::start(conn, request);
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
            Event::DownloadComplete { id } => {
                if let Some(done) = self.download_futs.remove(&id) {
                    done.send(()).ok();
                }
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
                    }
                    BaoContentItem::Leaf(leaf) => {
                        let start_chunk = leaf.offset / 1024;
                        let added = ChunkRanges::from(ChunkNum(start_chunk)..ChunkNum(start_chunk + 1));
                        sender.send(Command::ChunksDownloaded { time: Duration::ZERO, peer, hash, added: added.clone() }).await.ok();
                        batch.push(leaf.into());
                        writer.write_batch(size, std::mem::take(&mut batch)).await?;
                        sender.send(Command::BitmapUpdate { peer: BitmapPeer::Local, hash, added, removed: ChunkRanges::empty() }).await.ok();
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

fn print_bitmap(iter: impl IntoIterator<Item = bool>) -> String {
    let mut chars = String::new();
    for x in iter {
        chars.push(if x { '█' } else { ' ' });
    }
    chars
}

fn print_bitmap_compact(iter: impl IntoIterator<Item = bool>) -> String {
    let mut chars = String::new();
    let mut iter = iter.into_iter();

    while let (Some(left), Some(right)) = (iter.next(), iter.next()) {
        let c = match (left, right) {
            (true, true) => '█',  // Both pixels are "on"
            (true, false) => '▌', // Left pixel is "on"
            (false, true) => '▐', // Right pixel is "on"
            (false, false) => ' ', // Both are "off"
        };
        chars.push(c);
    }

    // If there's an odd pixel at the end, print only a left block.
    if let Some(left) = iter.next() {
        chars.push(if left { '▌' } else { ' ' });
    }

    chars
}

fn as_bool_iter(x: &ChunkRanges, max: u64) -> impl Iterator<Item = bool> {
    let max = x.iter().last().map(|x| match x {
        RangeSetRange::RangeFrom(_) => max,
        RangeSetRange::Range(x) => x.end.0,
    }).unwrap_or_default();
    let res = (0..max).map(move |i| x.contains(&ChunkNum(i))).collect::<Vec<_>>();
    res.into_iter()
}

/// Given a set of ranges, make them non-overlapping according to some rules.
fn select_ranges(ranges: &[ChunkRanges], continuity_bonus: u64) -> Vec<Option<usize>> {
    let mut total = vec![0u64; ranges.len()];
    let mut boundaries = BTreeSet::new();
    assert!(ranges.iter().all(|x| x.boundaries().len() % 2 == 0));
    for range in ranges.iter() {
        for x in range.boundaries() {
            boundaries.insert(x.0);
        }
    }
    let max = boundaries.iter().max().copied().unwrap_or(0);
    let mut last_selected = None;
    let mut res = vec![];
    for i in 0..max {
        let mut lowest_score = u64::MAX;
        let mut selected = None;
        for j in 0..ranges.len() {
            if ranges[j].contains(&ChunkNum(i)) {
                let consecutive = last_selected == Some(j);
                let score = if consecutive {
                    total[j].saturating_sub(continuity_bonus)
                } else {
                    total[j]
                };
                if score < lowest_score {
                    lowest_score = score;
                    selected = Some(j);
                }
            }
        }
        res.push(selected);
        if let Some(selected) = selected {
            total[selected] += 1;
        }
        last_selected = selected;
    }
    res
}

fn create_ranges(indexes: impl IntoIterator<Item = Option<usize>>) -> Vec<ChunkRanges> {
    let mut res = vec![];
    for (i, n) in indexes.into_iter().enumerate() {
        let x = i as u64;
        if let Some(n) = n {
            while res.len() <= n {
                res.push(ChunkRanges::empty());
            }
            res[n] |= ChunkRanges::from(ChunkNum(x)..ChunkNum(x + 1));
        }
    }
    res
}

fn print_colored_bitmap(data: &[u8], colors: &[u8]) -> String {
    let mut chars = String::new();
    let mut iter = data.iter();

    while let Some(&left) = iter.next() {
        let right = iter.next(); // Try to fetch the next element
        
        let left_color = colors.get(left as usize).map(|x| *x).unwrap_or_default();
        let right_char = match right {
            Some(&right) => {
                let right_color = colors.get(right as usize).map(|x| *x).unwrap_or_default();
                // Use ANSI escape codes to color left and right halves of `█`
                format!(
                    "\x1b[38;5;{}m\x1b[48;5;{}m▌\x1b[0m", 
                    left_color, right_color
                )
            }
            None => format!("\x1b[38;5;{}m▌\x1b[0m", left_color), // Handle odd-length case
        };

        chars.push_str(&right_char);
    }
    chars
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::net_protocol::Blobs;

    use super::*;
    use bao_tree::ChunkNum;
    use iroh::{protocol::Router, SecretKey};
    use testresult::TestResult;

    #[test]
    fn print_chunk_range() {
        let x = chunk_ranges([0..3, 4..30, 40..50]);
        let s = print_bitmap_compact(as_bool_iter(&x, 50));
        println!("{}", s);
    }

    fn peer(id: u8) -> NodeId {
        let mut secret = [0; 32];
        secret[0] = id;
        SecretKey::from(secret).public()
    }

    #[test]
    fn test_planner() {
        let mut planner = StripePlanner2::new(0, 4);
        let hash = Hash::new(b"test");
        let mut ranges = make_range_map(&[
            chunk_ranges([0..100]),
            chunk_ranges([0..110]),
            chunk_ranges([0..120]),
        ]);
        print_range_map(&ranges);
        println!("planning");
        planner.plan(hash, &mut ranges);
        print_range_map(&ranges);
        println!("---");
        let mut ranges = make_range_map(&[
            chunk_ranges([0..100]),
            chunk_ranges([0..110]),
            chunk_ranges([0..120]),
            chunk_ranges([0..50]),
        ]);
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
            let x = print_bitmap(as_bool_iter(ranges, 100));
            println!("{peer}: {x}");
        }
    }

    #[test]
    fn test_select_ranges() {
        let ranges = [
            chunk_ranges([0..90]),
            chunk_ranges([0..100]),
            chunk_ranges([0..80]),
        ];
        let iter = select_ranges(ranges.as_slice(), 8);
        for (i, range) in ranges.iter().enumerate() {
            let bools = as_bool_iter(&range, 100).collect::<Vec<_>>();
            println!("{i:4}{}", print_bitmap(bools));
        }
        print!("    ");
        for x in &iter {
            print!("{}", x.map(|x| x.to_string()).unwrap_or(" ".to_string()));
        }
        println!();
        let ranges = create_ranges(iter);
        for (i, range) in ranges.iter().enumerate() {
            let bools = as_bool_iter(&range, 100).collect::<Vec<_>>();
            println!("{i:4}{}", print_bitmap(bools));
        }
    }

    #[test]
    fn test_is_superset() {
        let local = ChunkRanges::from(ChunkNum(0)..ChunkNum(100));
        let request = ChunkRanges::from(ChunkNum(0)..ChunkNum(50));
        assert!(local.is_superset(&request));
    }

    #[test]
    fn test_print_colored_bitmap() {
        let bitmap = vec![
            0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2,
        ]; // Notice: odd-length input
    
        println!("{}", print_colored_bitmap(&bitmap, &[1,2,3,4]));
    }

    #[cfg(feature = "rpc")]
    async fn make_test_node(data: &[u8]) -> anyhow::Result<(Router, NodeId, Hash)> {
        let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
        let node_id = endpoint.node_id();
        let store = crate::store::mem::Store::new();
        let blobs = Blobs::builder(store).build(&endpoint);
        let hash = blobs.client().add_bytes(bytes::Bytes::copy_from_slice(data)).await?.hash;
        let router = iroh::protocol::Router::builder(endpoint).accept(crate::ALPN, blobs).spawn().await?;
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

    fn noop_planner() -> BoxedDownloadPlanner {
        Box::new(NoopPlanner)
    }

    /// Checks if an exact event is present exactly once in a list of events
    fn has_one_event(evs: &[Event], ev: &Event) -> bool {
        evs.iter().filter(|e| *e == ev).count() == 1
    }

    fn has_one_event_matching(evs: &[Event], f: impl Fn(&Event) -> bool) -> bool {
        evs.iter().filter(|e| f(e)).count() == 1
    }

    #[test]
    fn downloader_state_smoke() -> TestResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        // let me = "0000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let unknown_hash = "0000000000000000000000000000000000000000000000000000000000000002".parse()?;
        let mut state = DownloaderState::new(noop_planner());
        let evs = state.apply_and_get_evs(super::Command::StartDownload { request: DownloadRequest { hash, ranges: chunk_ranges([0..64]) }, id: 1 });
        assert!(has_one_event(&evs, &Event::StartDiscovery { hash, id: 0 }), "starting a download should start a discovery task");
        assert!(has_one_event(&evs, &Event::SubscribeBitmap { peer: BitmapPeer::Local, hash, id: 0 }), "starting a download should subscribe to the local bitmap");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: BitmapPeer::Local, hash, bitmap: ChunkRanges::all() });
        assert!(has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })), "adding an open bitmap should produce an error!");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: BitmapPeer::Local, hash: unknown_hash, bitmap: ChunkRanges::all() });
        assert!(has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })), "adding an open bitmap for an unknown hash should produce an error!");
        let initial_bitmap = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: BitmapPeer::Local, hash, bitmap: initial_bitmap.clone() });
        assert!(evs.is_empty());
        assert_eq!(state.bitmaps.get(&(BitmapPeer::Local, hash)).context("bitmap should be present")?.ranges, initial_bitmap, "bitmap should be set to the initial bitmap");
        let evs = state.apply_and_get_evs(Command::BitmapUpdate { peer: BitmapPeer::Local, hash, added: chunk_ranges([16..32]), removed: ChunkRanges::empty() });
        assert!(evs.is_empty());
        assert_eq!(state.bitmaps.get(&(BitmapPeer::Local, hash)).context("bitmap should be present")?.ranges, ChunkRanges::from(ChunkNum(0)..ChunkNum(32)), "bitmap should be updated");
        let evs = state.apply_and_get_evs(super::Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: ChunkRanges::from(ChunkNum(0)..ChunkNum(16)),
        });
        assert!(has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })), "download from unknown peer should lead to an error!");
        let evs = state.apply_and_get_evs(Command::PeerDiscovered { peer: peer_a, hash });
        assert!(has_one_event(&evs, &Event::SubscribeBitmap { peer: BitmapPeer::Remote(peer_a), hash, id: 1 }), "adding a new peer for a hash we are interested in should subscribe to the bitmap");
        let evs = state.apply_and_get_evs(Command::Bitmap { peer: BitmapPeer::Remote(peer_a), hash, bitmap: chunk_ranges([0..64]) });
        assert!(has_one_event(&evs, &Event::StartPeerDownload { id: 0, peer: peer_a, hash, ranges: chunk_ranges([32..64]) }), "bitmap from a peer should start a download");
        // ChunksDownloaded just updates the peer stats
        let evs = state.apply_and_get_evs(Command::ChunksDownloaded { time: Duration::ZERO, peer: peer_a, hash, added: chunk_ranges([32..48]) });
        assert!(evs.is_empty());
        // Bitmap update does not yet complete the download
        let evs = state.apply_and_get_evs(Command::BitmapUpdate { peer: BitmapPeer::Local, hash, added: chunk_ranges([32..48]), removed: ChunkRanges::empty() });
        assert!(evs.is_empty());
        // ChunksDownloaded just updates the peer stats
        let evs = state.apply_and_get_evs(Command::ChunksDownloaded { time: Duration::ZERO, peer: peer_a, hash, added: chunk_ranges([48..64]) });
        assert!(evs.is_empty());
        // Final bitmap update for the local bitmap should complete the download
        let evs = state.apply_and_get_evs(Command::BitmapUpdate { peer: BitmapPeer::Local, hash, added: chunk_ranges([48..64]), removed: ChunkRanges::empty() });
        assert!(has_one_event_matching(&evs, |e| matches!(e, Event::DownloadComplete { .. })), "download should be completed by the data");
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "rpc")]
    async fn downloader_driver_smoke() -> TestResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (_router1, peer, hash) = make_test_node(b"test").await?;
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder().alpns(vec![crate::protocol::ALPN.to_vec()]).discovery_n0().bind().await?;
        let discovery = StaticContentDiscovery { info: BTreeMap::new(), default: vec![peer] };
        let local_pool = LocalPool::single();
        let downloader = Downloader::new(endpoint, store, discovery, local_pool, noop_planner());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let fut = downloader.download(DownloadRequest { hash, ranges: chunk_ranges([0..1]) });
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
        for _i in 0..10 {
            nodes.push(make_test_node(&data).await?);
        }
        let peers = nodes.iter().map(|(_, peer, _)| *peer).collect::<Vec<_>>();
        let hashes = nodes.iter().map(|(_, _, hash)| *hash).collect::<BTreeSet<_>>();
        let hash = *hashes.iter().next().unwrap();
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder().alpns(vec![crate::protocol::ALPN.to_vec()]).discovery_n0().bind().await?;
        let discovery = StaticContentDiscovery { info: BTreeMap::new(), default: peers };
        let local_pool = LocalPool::single();
        let downloader = Downloader::new(endpoint, store, discovery, local_pool, noop_planner());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let fut = downloader.download(DownloadRequest { hash, ranges: chunk_ranges([0..1024]) });
        fut.await?;
        Ok(())
    }
}
