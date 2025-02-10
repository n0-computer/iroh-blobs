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
    collections::{BTreeMap, BTreeSet, VecDeque},
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
    subscription_id: BitfieldSubscriptionId,
    /// The number of subscriptions this peer has
    subscription_count: usize,
    /// chunk ranges this peer reports to have
    ranges: ChunkRanges,
}

impl PeerBlobState {
    fn new(subscription_id: BitfieldSubscriptionId) -> Self {
        Self {
            subscription_id,
            subscription_count: 1,
            ranges: ChunkRanges::empty(),
        }
    }
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

struct DownloadState {
    /// The request this state is for
    request: DownloadRequest,
    /// Ongoing downloads
    peer_downloads: BTreeMap<NodeId, PeerDownloadState>,
    /// Set to true if the download needs rebalancing
    needs_rebalancing: bool,
}

impl DownloadState {
    fn new(request: DownloadRequest) -> Self {
        Self {
            request,
            peer_downloads: BTreeMap::new(),
            needs_rebalancing: false,
        }
    }
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

/// Wrapper for the downloads map
///
/// This is so we can later optimize access by fields other than id, such as hash.
#[derive(Default)]
struct Downloads {
    by_id: BTreeMap<DownloadId, DownloadState>,
}

impl Downloads {
    fn remove(&mut self, id: &DownloadId) -> Option<DownloadState> {
        self.by_id.remove(id)
    }

    fn contains_key(&self, id: &DownloadId) -> bool {
        self.by_id.contains_key(id)
    }

    fn insert(&mut self, id: DownloadId, state: DownloadState) {
        self.by_id.insert(id, state);
    }

    fn iter_mut_for_hash(
        &mut self,
        hash: Hash,
    ) -> impl Iterator<Item = (&DownloadId, &mut DownloadState)> {
        self.by_id
            .iter_mut()
            .filter(move |x| x.1.request.hash == hash)
    }

    fn iter(&mut self) -> impl Iterator<Item = (&DownloadId, &DownloadState)> {
        self.by_id.iter()
    }

    /// Iterate over all downloads for a given hash
    fn values_for_hash(&self, hash: Hash) -> impl Iterator<Item = &DownloadState> {
        self.by_id.values().filter(move |x| x.request.hash == hash)
    }

    fn values_mut_for_hash(&mut self, hash: Hash) -> impl Iterator<Item = &mut DownloadState> {
        self.by_id
            .values_mut()
            .filter(move |x| x.request.hash == hash)
    }

    fn by_id_mut(&mut self, id: DownloadId) -> Option<&mut DownloadState> {
        self.by_id.get_mut(&id)
    }

    fn by_peer_download_id_mut(
        &mut self,
        id: PeerDownloadId,
    ) -> Option<(&DownloadId, &mut DownloadState)> {
        self.by_id
            .iter_mut()
            .filter(|(_, v)| v.peer_downloads.iter().any(|(_, state)| state.id == id))
            .next()
    }
}

#[derive(Default)]
struct Bitfields {
    // Counters to generate unique ids for various requests.
    // We could use uuid here, but using integers simplifies testing.
    //
    // the next subscription id
    subscription_id_gen: IdGenerator<BitfieldSubscriptionId>,
    by_peer_and_hash: BTreeMap<(BitfieldPeer, Hash), PeerBlobState>,
}

impl Bitfields {
    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&(BitfieldPeer, Hash), &mut PeerBlobState) -> bool,
    {
        self.by_peer_and_hash.retain(|k, v| f(k, v));
    }

    fn get(&self, key: &(BitfieldPeer, Hash)) -> Option<&PeerBlobState> {
        self.by_peer_and_hash.get(key)
    }

    fn get_local(&self, hash: Hash) -> Option<&PeerBlobState> {
        self.by_peer_and_hash.get(&(BitfieldPeer::Local, hash))
    }

    fn get_mut(&mut self, key: &(BitfieldPeer, Hash)) -> Option<&mut PeerBlobState> {
        self.by_peer_and_hash.get_mut(key)
    }

    fn get_local_mut(&mut self, hash: Hash) -> Option<&mut PeerBlobState> {
        self.by_peer_and_hash.get_mut(&(BitfieldPeer::Local, hash))
    }

    fn insert(&mut self, key: (BitfieldPeer, Hash), value: PeerBlobState) {
        self.by_peer_and_hash.insert(key, value);
    }

    fn contains_key(&self, key: &(BitfieldPeer, Hash)) -> bool {
        self.by_peer_and_hash.contains_key(key)
    }

    fn remote_for_hash(&self, hash: Hash) -> impl Iterator<Item = (&NodeId, &PeerBlobState)> {
        self.by_peer_and_hash
            .iter()
            .filter_map(move |((peer, h), state)| {
                if let BitfieldPeer::Remote(peer) = peer {
                    if *h == hash {
                        Some((peer, state))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
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
pub trait DownloadPlanner: Send + std::fmt::Debug + 'static {
    /// Make a download plan for a hash, by reducing or eliminating the overlap of chunk ranges
    fn plan(&mut self, hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>);
}

/// A boxed download planner
pub type BoxedDownloadPlanner = Box<dyn DownloadPlanner>;

/// A download planner that just leaves everything as is.
///
/// Data will be downloaded from all peers wherever multiple peers have the same data.
#[derive(Debug, Clone, Copy)]
pub struct NoopPlanner;

impl DownloadPlanner for NoopPlanner {
    fn plan(&mut self, _hash: Hash, _options: &mut BTreeMap<NodeId, ChunkRanges>) {}
}

/// A download planner that fully removes overlap between peers.
///
/// It divides files into stripes of a fixed size `1 << stripe_size_log` chunks,
/// and for each stripe decides on a single peer to download from, based on the
/// peer id and a random seed.
#[derive(Debug)]
pub struct StripePlanner {
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
    /// Create a new planner with the given seed and stripe size.
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self {
            seed,
            stripe_size_log,
        }
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
        assert!(
            options.values().all(|x| x.boundaries().len() % 2 == 0),
            "open ranges not supported"
        );
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

fn get_continuous_ranges(
    options: &mut BTreeMap<NodeId, ChunkRanges>,
    stripe_size_log: u8,
) -> Option<Vec<u64>> {
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
#[derive(Debug)]
pub struct StripePlanner2 {
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
    /// Create a new planner with the given seed and stripe size.
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self {
            seed,
            stripe_size_log,
        }
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
        assert!(
            options.values().all(|x| x.boundaries().len() % 2 == 0),
            "open ranges not supported"
        );
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
            let mut peer_and_score = matching
                .iter()
                .map(|(peer, _)| (Self::score(peer, self.seed), peer))
                .collect::<Vec<_>>();
            peer_and_score.sort();
            let peer_to_rank = peer_and_score
                .into_iter()
                .enumerate()
                .map(|(i, (_, peer))| (*peer, i as u64))
                .collect::<BTreeMap<_, _>>();
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
    // all bitfields I am tracking, both for myself and for remote peers
    //
    // each item here corresponds to an active subscription
    bitfields: Bitfields,
    /// Observers for local bitfields
    observers: Observers,
    // all active downloads
    //
    // these are user downloads. each user download gets split into one or more
    // peer downloads.
    downloads: Downloads,
    // discovery tasks
    //
    // there is a discovery task for each blob we are interested in.
    discovery: BTreeMap<Hash, DiscoveryId>,
    // the next discovery id
    discovery_id_gen: IdGenerator<DiscoveryId>,
    // the next peer download id
    peer_download_id_gen: IdGenerator<PeerDownloadId>,
    // the download planner
    planner: Box<dyn DownloadPlanner>,
}

impl DownloaderState {
    fn new(planner: Box<dyn DownloadPlanner>) -> Self {
        Self {
            peers: Default::default(),
            downloads: Default::default(),
            bitfields: Default::default(),
            discovery: Default::default(),
            observers: Default::default(),
            discovery_id_gen: Default::default(),
            peer_download_id_gen: Default::default(),
            planner,
        }
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

#[derive(Debug)]
enum Command {
    /// A user request to start a download.
    StartDownload {
        /// The download request
        request: DownloadRequest,
        /// The unique id, to be assigned by the caller
        id: DownloadId,
    },
    /// A user request to abort a download.
    StopDownload { id: DownloadId },
    /// A full bitfield for a blob and a peer
    Bitfield {
        /// The peer that sent the bitfield.
        peer: BitfieldPeer,
        /// The blob for which the bitfield is
        hash: Hash,
        /// The complete bitfield
        ranges: ChunkRanges,
    },
    /// An update of a bitfield for a hash
    ///
    /// This is used both to update the bitfield of remote peers, and to update
    /// the local bitfield.
    BitfieldUpdate {
        /// The peer that sent the update.
        peer: BitfieldPeer,
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
    /// A peer download has completed
    PeerDownloadComplete {
        id: PeerDownloadId,
        #[allow(dead_code)]
        result: anyhow::Result<Stats>,
    },
    /// Stop tracking a peer for all blobs, for whatever reason
    #[allow(dead_code)]
    DropPeer { peer: NodeId },
    /// A peer has been discovered
    PeerDiscovered { peer: NodeId, hash: Hash },
    ///
    ObserveLocal { id: ObserveId, hash: Hash, ranges: ChunkRanges },
    ///
    StopObserveLocal { id: ObserveId },
    /// A tick from the driver, for rebalancing
    Tick { time: Duration },
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    SubscribeBitfield {
        peer: BitfieldPeer,
        hash: Hash,
        /// The unique id of the subscription
        id: BitfieldSubscriptionId,
    },
    UnsubscribeBitfield {
        /// The unique id of the subscription
        id: BitfieldSubscriptionId,
    },
    LocalBitfield {
        id: ObserveId,
        ranges: ChunkRanges,
    },
    LocalBitfieldUpdate {
        id: ObserveId,
        added: ChunkRanges,
        removed: ChunkRanges,
    },
    StartDiscovery {
        hash: Hash,
        /// The unique id of the discovery task
        id: DiscoveryId,
    },
    StopDiscovery {
        /// The unique id of the discovery task
        id: DiscoveryId,
    },
    StartPeerDownload {
        /// The unique id of the peer download task
        id: PeerDownloadId,
        peer: NodeId,
        hash: Hash,
        ranges: ChunkRanges,
    },
    StopPeerDownload {
        /// The unique id of the peer download task
        id: PeerDownloadId,
    },
    DownloadComplete {
        /// The unique id of the user download
        id: DownloadId,
    },
    /// An error that stops processing the command
    Error { message: String },
}

impl DownloaderState {
    /// Apply a command and return the events that were generated
    fn apply(&mut self, cmd: Command) -> Vec<Event> {
        let mut evs = vec![];
        self.apply_mut(cmd, &mut evs);
        evs
    }

    /// Apply a command, using a mutable reference to the events
    fn apply_mut(&mut self, cmd: Command, evs: &mut Vec<Event>) {
        if let Err(cause) = self.apply_mut_0(cmd, evs) {
            evs.push(Event::Error {
                message: format!("{cause}"),
            });
        }
    }

    /// Stop a download and clean up
    ///
    /// This is called both for stopping a download before completion, and for
    /// cleaning up after a successful download.
    ///
    /// Cleanup involves emitting events for
    /// - stopping all peer downloads
    /// - unsubscribing from bitfields if needed
    /// - stopping the discovery task if needed
    fn stop_download(&mut self, id: DownloadId, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let removed = self
            .downloads
            .remove(&id)
            .context(format!("removed unknown download {id:?}"))?;
        let removed_hash = removed.request.hash;
        // stop associated peer downloads
        for peer_download in removed.peer_downloads.values() {
            evs.push(Event::StopPeerDownload {
                id: peer_download.id,
            });
        }
        // unsubscribe from bitfields that have no more subscriptions
        self.bitfields.retain(|(_peer, hash), state| {
            if *hash == removed_hash {
                state.subscription_count -= 1;
                if state.subscription_count == 0 {
                    evs.push(Event::UnsubscribeBitfield {
                        id: state.subscription_id,
                    });
                    return false;
                }
            }
            true
        });
        let hash_interest = self.downloads.values_for_hash(removed.request.hash).count();
        if hash_interest == 0 {
            // stop the discovery task if we were the last one interested in the hash
            let discovery_id = self
                .discovery
                .remove(&removed.request.hash)
                .context(format!(
                    "removed unknown discovery task for {}",
                    removed.request.hash
                ))?;
            evs.push(Event::StopDiscovery { id: discovery_id });
        }
        Ok(())
    }

    /// Apply a command and bail out on error
    fn apply_mut_0(&mut self, cmd: Command, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        trace!("handle_command {cmd:?}");
        match cmd {
            Command::StartDownload { request, id } => {
                // ids must be uniquely assigned by the caller!
                anyhow::ensure!(
                    !self.downloads.contains_key(&id),
                    "duplicate download request {id:?}"
                );
                let hash = request.hash;
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitfields.get_local_mut(hash) {
                    // just increment the count
                    state.subscription_count += 1;
                } else {
                    // create a new subscription
                    let subscription_id = self.bitfields.subscription_id_gen.next();
                    evs.push(Event::SubscribeBitfield {
                        peer: BitfieldPeer::Local,
                        hash,
                        id: subscription_id,
                    });
                    self.bitfields.insert(
                        (BitfieldPeer::Local, hash),
                        PeerBlobState::new(subscription_id),
                    );
                }
                if !self.discovery.contains_key(&request.hash) {
                    // start a discovery task
                    let id = self.discovery_id_gen.next();
                    evs.push(Event::StartDiscovery { hash, id });
                    self.discovery.insert(request.hash, id);
                }
                self.downloads.insert(id, DownloadState::new(request));
                self.check_completion(hash, Some(id), evs)?;
                self.start_downloads(hash, Some(id), evs)?;
            }
            Command::PeerDownloadComplete { id, .. } => {
                let Some((download_id, download)) = self.downloads.by_peer_download_id_mut(id)
                else {
                    // the download was already removed
                    return Ok(());
                };
                let download_id = *download_id;
                let hash = download.request.hash;
                download.peer_downloads.retain(|_, v| v.id != id);
                self.start_downloads(hash, Some(download_id), evs)?;
            }
            Command::StopDownload { id } => {
                self.stop_download(id, evs)?;
            }
            Command::PeerDiscovered { peer, hash } => {
                if self
                    .bitfields
                    .contains_key(&(BitfieldPeer::Remote(peer), hash))
                {
                    // we already have a subscription for this peer
                    return Ok(());
                };
                // check if anybody needs this peer
                if self.downloads.values_for_hash(hash).next().is_none() {
                    return Ok(());
                }
                // create a peer state if it does not exist
                let _state = self.peers.entry(peer).or_default();
                // create a new subscription
                let subscription_id = self.bitfields.subscription_id_gen.next();
                evs.push(Event::SubscribeBitfield {
                    peer: BitfieldPeer::Remote(peer),
                    hash,
                    id: subscription_id,
                });
                self.bitfields.insert(
                    (BitfieldPeer::Remote(peer), hash),
                    PeerBlobState::new(subscription_id),
                );
            }
            Command::DropPeer { peer } => {
                self.bitfields.retain(|(p, _), state| {
                    if *p == BitfieldPeer::Remote(peer) {
                        // todo: should we emit unsubscribe evs here?
                        evs.push(Event::UnsubscribeBitfield {
                            id: state.subscription_id,
                        });
                        return false;
                    } else {
                        return true;
                    }
                });
                self.peers.remove(&peer);
            }
            Command::Bitfield { peer, hash, ranges } => {
                let state = self.bitfields.get_mut(&(peer, hash)).context(format!(
                    "bitfields for unknown peer {peer:?} and hash {hash}"
                ))?;
                let _chunks = total_chunks(&ranges).context("open range")?;
                if peer == BitfieldPeer::Local {
                    if let Some(observers) = self.observers.get_by_hash(&hash) {
                        for (id, request) in observers {
                            let ranges = &ranges & &request.ranges;
                            evs.push(Event::LocalBitfield {
                                id: *id,
                                ranges,
                            });
                        }
                    }
                    state.ranges = ranges;
                    self.check_completion(hash, None, evs)?;
                } else {
                    // We got an entirely new peer, mark all affected downloads for rebalancing
                    for download in self.downloads.values_mut_for_hash(hash) {
                        if ranges.intersects(&download.request.ranges) {
                            download.needs_rebalancing = true;
                        }
                    }
                    state.ranges = ranges;
                }
                // we have to call start_downloads even if the local bitfield set, since we don't know in which order local and remote bitfields arrive
                self.start_downloads(hash, None, evs)?;
            }
            Command::BitfieldUpdate {
                peer,
                hash,
                added,
                removed,
            } => {
                let state = self.bitfields.get_mut(&(peer, hash)).context(format!(
                    "bitfield update for unknown peer {peer:?} and hash {hash}"
                ))?;
                if peer == BitfieldPeer::Local {
                    if let Some(observers) = self.observers.get_by_hash(&hash) {
                        for (id, request) in observers {
                            let added = &added & &request.ranges;
                            let removed = &removed & &request.ranges;
                            if !added.is_empty() || !removed.is_empty() {
                                evs.push(Event::LocalBitfieldUpdate {
                                    id: *id,
                                    added: &added & &request.ranges,
                                    removed: &removed & &request.ranges,
                                });
                            }
                        }
                    }
                    state.ranges |= added;
                    state.ranges &= !removed;
                    self.check_completion(hash, None, evs)?;
                } else {
                    // We got more data for this hash, mark all affected downloads for rebalancing
                    for download in self.downloads.values_mut_for_hash(hash) {
                        // if removed is non-empty, that is so weird that we just rebalance in any case
                        if !removed.is_empty() || added.intersects(&download.request.ranges) {
                            download.needs_rebalancing = true;
                        }
                    }
                    state.ranges |= added;
                    state.ranges &= !removed;
                    // a local bitfield update does not make more data available, so we don't need to start downloads
                    self.start_downloads(hash, None, evs)?;
                }
            }
            Command::ChunksDownloaded {
                time,
                peer,
                hash,
                added,
            } => {
                let state = self.bitfields.get_local_mut(hash).context(format!(
                    "chunks downloaded before having local bitfield for {hash}"
                ))?;
                let total_downloaded = total_chunks(&added).context("open range")?;
                let total_before = total_chunks(&state.ranges).context("open range")?;
                state.ranges |= added;
                let total_after = total_chunks(&state.ranges).context("open range")?;
                let useful_downloaded = total_after - total_before;
                let peer = self.peers.get_mut(&peer).context(format!(
                    "performing download before having peer state for {peer}"
                ))?;
                peer.download_history
                    .push_back((time, (total_downloaded, useful_downloaded)));
            }
            Command::Tick { time } => {
                let window = 10;
                let horizon = time.saturating_sub(Duration::from_secs(window));
                // clean up download history
                let mut to_rebalance = vec![];
                for (peer, state) in self.peers.iter_mut() {
                    state
                        .download_history
                        .retain(|(duration, _)| *duration > horizon);
                    let mut sum_total = 0;
                    let mut sum_useful = 0;
                    for (_, (total, useful)) in state.download_history.iter() {
                        sum_total += total;
                        sum_useful += useful;
                    }
                    let speed_useful = (sum_useful as f64) / (window as f64);
                    let speed_total = (sum_total as f64) / (window as f64);
                    trace!("peer {peer} download speed {speed_total} cps total, {speed_useful} cps useful");
                }

                for (id, download) in self.downloads.iter() {
                    if !download.needs_rebalancing {
                        // nothing has changed that affects this download
                        continue;
                    }
                    let n_peers = self
                        .bitfields
                        .remote_for_hash(download.request.hash)
                        .count();
                    if download.peer_downloads.len() >= n_peers {
                        // we are already downloading from all peers for this hash
                        continue;
                    }
                    to_rebalance.push(*id);
                }
                for id in to_rebalance {
                    self.rebalance_download(id, evs)?;
                }
            }
            Command::ObserveLocal { id, hash, ranges } => {
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitfields.get_local_mut(hash) {
                    // just increment the count
                    state.subscription_count += 1;
                    // emit the current bitfield
                    evs.push(Event::LocalBitfield { id, ranges: state.ranges.clone() });
                } else {
                    // create a new subscription
                    let subscription_id = self.bitfields.subscription_id_gen.next();
                    evs.push(Event::SubscribeBitfield {
                        peer: BitfieldPeer::Local,
                        hash,
                        id: subscription_id,
                    });
                    self.bitfields.insert(
                        (BitfieldPeer::Local, hash),
                        PeerBlobState::new(subscription_id),
                    );
                }
                self.observers.insert(id, ObserveRequest { hash, ranges, buffer: 0 });
            }
            Command::StopObserveLocal { id } => {
                let request = self.observers.remove(&id).context(format!(
                    "stop observing unknown local bitfield {id:?}"
                ))?;
                let removed_hash = request.hash;
                // unsubscribe from bitfields that have no more subscriptions
                self.bitfields.retain(|(_peer, hash), state| {
                    if *hash == removed_hash {
                        state.subscription_count -= 1;
                        if state.subscription_count == 0 {
                            evs.push(Event::UnsubscribeBitfield {
                                id: state.subscription_id,
                            });
                            return false;
                        }
                    }
                    true
                });
            }
        }
        Ok(())
    }

    /// Check for completion of a download or of an individual peer download
    ///
    /// This must be called after each change of the local bitfield for a hash
    ///
    /// In addition to checking for completion, this also create new peer downloads if a peer download is complete and there is more data available for that peer.
    fn check_completion(
        &mut self,
        hash: Hash,
        just_id: Option<DownloadId>,
        evs: &mut Vec<Event>,
    ) -> anyhow::Result<()> {
        let Some(self_state) = self.bitfields.get_local(hash) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        let mut completed = vec![];
        for (id, download) in self.downloads.iter_mut_for_hash(hash) {
            if just_id.is_some() && just_id != Some(*id) {
                continue;
            }
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
                    evs.push(Event::StopPeerDownload {
                        id: peer_download.id,
                    });
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
                let mut candidates = BTreeMap::new();
                for peer in available {
                    let Some(peer_state) = self.bitfields.get(&(BitfieldPeer::Remote(peer), hash))
                    else {
                        // weird. we should have a bitfield for this peer since it just completed a download
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
                    evs.push(Event::StartPeerDownload {
                        id,
                        peer,
                        hash,
                        ranges: ranges.clone(),
                    });
                    download
                        .peer_downloads
                        .insert(peer, PeerDownloadState { id, ranges });
                }
            }
        }
        // cleanup completed downloads, has to happen later to avoid double mutable borrow
        for id in completed {
            self.stop_download(id, evs)?;
        }
        Ok(())
    }

    /// Look at all downloads for a hash and start peer downloads for those that do not have any yet
    fn start_downloads(
        &mut self,
        hash: Hash,
        just_id: Option<DownloadId>,
        evs: &mut Vec<Event>,
    ) -> anyhow::Result<()> {
        let Some(self_state) = self.bitfields.get_local(hash) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        for (id, download) in self
            .downloads
            .iter_mut_for_hash(hash)
            .filter(|(_, download)| download.peer_downloads.is_empty())
        {
            if just_id.is_some() && just_id != Some(*id) {
                continue;
            }
            let remaining = &download.request.ranges - &self_state.ranges;
            let mut candidates = BTreeMap::new();
            for (peer, bitfield) in self.bitfields.remote_for_hash(hash) {
                let intersection = &bitfield.ranges & &remaining;
                if !intersection.is_empty() {
                    candidates.insert(*peer, intersection);
                }
            }
            self.planner.plan(hash, &mut candidates);
            for (peer, ranges) in candidates {
                info!("  Starting download from {peer} for {hash} {ranges:?}");
                let id = self.peer_download_id_gen.next();
                evs.push(Event::StartPeerDownload {
                    id,
                    peer,
                    hash,
                    ranges: ranges.clone(),
                });
                download
                    .peer_downloads
                    .insert(peer, PeerDownloadState { id, ranges });
            }
        }
        Ok(())
    }

    /// rebalance a single download
    fn rebalance_download(&mut self, id: DownloadId, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        let download = self
            .downloads
            .by_id_mut(id)
            .context(format!("rebalancing unknown download {id:?}"))?;
        download.needs_rebalancing = false;
        tracing::info!("Rebalancing download {id:?} {:?}", download.request);
        let hash = download.request.hash;
        let Some(self_state) = self.bitfields.get_local(hash) else {
            // we don't have the self state yet, so we can't really decide if we need to download anything at all
            return Ok(());
        };
        let remaining = &download.request.ranges - &self_state.ranges;
        let mut candidates = BTreeMap::new();
        for (peer, bitfield) in self.bitfields.remote_for_hash(hash) {
            let intersection = &bitfield.ranges & &remaining;
            if !intersection.is_empty() {
                candidates.insert(*peer, intersection);
            }
        }
        self.planner.plan(hash, &mut candidates);
        info!(
            "Stopping {} old peer downloads",
            download.peer_downloads.len()
        );
        for (_, state) in &download.peer_downloads {
            // stop all downloads
            evs.push(Event::StopPeerDownload { id: state.id });
        }
        info!("Creating {} new peer downloads", candidates.len());
        download.peer_downloads.clear();
        for (peer, ranges) in candidates {
            info!("  Starting download from {peer} for {hash} {ranges:?}");
            let id = self.peer_download_id_gen.next();
            evs.push(Event::StartPeerDownload {
                id,
                peer,
                hash,
                ranges: ranges.clone(),
            });
            download
                .peer_downloads
                .insert(peer, PeerDownloadState { id, ranges });
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

#[derive(Debug, Default)]
struct Observers {
    by_hash_and_id: BTreeMap<Hash, BTreeMap<ObserveId, ObserveRequest>>,
}

impl Observers {
    fn insert(&mut self, id: ObserveId, request: ObserveRequest) {
        self.by_hash_and_id
            .entry(request.hash)
            .or_default()
            .insert(id, request);
    }

    fn remove(&mut self, id: &ObserveId) -> Option<ObserveRequest> {
        for requests in self.by_hash_and_id.values_mut() {
            if let Some(request) = requests.remove(id) {
                return Some(request);
            }
        }
        None
    }

    fn get_by_hash(&self, hash: &Hash) -> Option<&BTreeMap<ObserveId, ObserveRequest>> {
        self.by_hash_and_id.get(hash)
    }
}

/// An user-facing command
#[derive(Debug)]
enum UserCommand {
    Download {
        request: DownloadRequest,
        done: tokio::sync::oneshot::Sender<()>,
    },
    Observe {
        request: ObserveRequest,
        send: tokio::sync::mpsc::Sender<ObserveEvent>,
    },
}

struct DownloaderActor<S> {
    local_pool: LocalPool,
    endpoint: Endpoint,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    state: DownloaderState,
    store: S,
    discovery: BoxedContentDiscovery,
    subscribe_bitfield: BoxedBitfieldSubscription,
    download_futs: BTreeMap<DownloadId, tokio::sync::oneshot::Sender<()>>,
    peer_download_tasks: BTreeMap<PeerDownloadId, local_pool::Run<()>>,
    discovery_tasks: BTreeMap<DiscoveryId, AbortOnDropHandle<()>>,
    bitfield_subscription_tasks: BTreeMap<BitfieldSubscriptionId, AbortOnDropHandle<()>>,
    /// Id generator for download ids
    download_id_gen: IdGenerator<DownloadId>,
    /// Id generator for observe ids
    observe_id_gen: IdGenerator<ObserveId>,
    /// Observers
    observers: BTreeMap<ObserveId, tokio::sync::mpsc::Sender<ObserveEvent>>,
    /// The time when the actor was started, serves as the epoch for time messages to the state machine
    start: Instant,
}

impl<S: Store> DownloaderActor<S> {
    fn new(
        endpoint: Endpoint,
        store: S,
        discovery: BoxedContentDiscovery,
        subscribe_bitfield: BoxedBitfieldSubscription,
        local_pool: LocalPool,
        planner: Box<dyn DownloadPlanner>,
    ) -> Self {
        let (send, recv) = mpsc::channel(256);
        Self {
            local_pool,
            endpoint,
            state: DownloaderState::new(planner),
            store,
            discovery,
            subscribe_bitfield,
            peer_download_tasks: BTreeMap::new(),
            discovery_tasks: BTreeMap::new(),
            bitfield_subscription_tasks: BTreeMap::new(),
            download_futs: BTreeMap::new(),
            command_tx: send,
            command_rx: recv,
            download_id_gen: Default::default(),
            observe_id_gen: Default::default(),
            observers: Default::default(),
            start: Instant::now(),
        }
    }

    async fn run(mut self, mut channel: mpsc::Receiver<UserCommand>) {
        let mut ticks = tokio::time::interval(Duration::from_millis(100));
        loop {
            trace!("downloader actor tick");
            tokio::select! {
                biased;
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
                        UserCommand::Observe { request, send } => {
                            let id = self.observe_id_gen.next();
                            self.command_tx.send(Command::ObserveLocal { id, hash: request.hash, ranges: request.ranges }).await.ok();
                            self.observers.insert(id, send);
                        }
                    }
                },
                Some(cmd) = self.command_rx.recv() => {
                    let evs = self.state.apply(cmd);
                    for ev in evs {
                        self.handle_event(ev);
                    }
                },
                _ = ticks.tick() => {
                    let time = self.start.elapsed();
                    self.command_tx.send(Command::Tick { time }).await.ok();
                    // clean up dropped futures
                    //
                    // todo: is there a better mechanism than periodic checks?
                    // I don't want some cancellation token rube goldberg machine.
                    let mut to_delete = vec![];
                    for (id, fut) in self.download_futs.iter() {
                        if fut.is_closed() {
                            to_delete.push(*id);
                            self.command_tx.send(Command::StopDownload { id: *id }).await.ok();
                        }
                    }
                    for id in to_delete {
                        self.download_futs.remove(&id);
                    }
                    // clean up dropped observers
                    let mut to_delete = vec![];
                    for (id, sender) in self.observers.iter() {
                        if sender.is_closed() {
                            to_delete.push(*id);
                            self.command_tx.send(Command::StopObserveLocal { id: *id }).await.ok();
                        }
                    }
                    for id in to_delete {
                        self.observers.remove(&id);
                    }
                },
            }
        }
    }

    fn handle_event(&mut self, ev: Event) {
        trace!("handle_event {ev:?}");
        match ev {
            Event::SubscribeBitfield { peer, hash, id } => {
                let send = self.command_tx.clone();
                let mut stream = self.subscribe_bitfield.subscribe(peer, hash);
                let task = spawn(async move {
                    while let Some(ev) = stream.next().await {
                        let cmd = match ev {
                            BitfieldSubscriptionEvent::Bitfield { ranges } => {
                                Command::Bitfield { peer, hash, ranges }
                            }
                            BitfieldSubscriptionEvent::BitfieldUpdate { added, removed } => {
                                Command::BitfieldUpdate {
                                    peer,
                                    hash,
                                    added,
                                    removed,
                                }
                            }
                        };
                        send.send(cmd).await.ok();
                    }
                });
                self.bitfield_subscription_tasks.insert(id, task);
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
            Event::StartPeerDownload {
                id,
                peer,
                hash,
                ranges,
            } => {
                let send = self.command_tx.clone();
                let endpoint = self.endpoint.clone();
                let store = self.store.clone();
                let start = self.start;
                let task = self.local_pool.spawn(move || {
                    peer_download_task(id, endpoint, store, hash, peer, ranges, send, start)
                });
                self.peer_download_tasks.insert(id, task);
            }
            Event::UnsubscribeBitfield { id } => {
                self.bitfield_subscription_tasks.remove(&id);
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
            Event::LocalBitfield { id, ranges } => {
                let Some(sender) = self.observers.get(&id) else {
                    return;
                };
                if sender.try_send(ObserveEvent::Bitfield { ranges }).is_err() {
                    // the observer has been dropped
                    self.observers.remove(&id);
                }
            }
            Event::LocalBitfieldUpdate { id, added, removed } => {
                let Some(sender) = self.observers.get(&id) else {
                    return;
                };
                if sender.try_send(ObserveEvent::BitfieldUpdate { added, removed }).is_err() {
                    // the observer has been dropped
                    self.observers.remove(&id);
                }
            }
            Event::Error { message } => {
                error!("Error during processing event {}", message);
            }
        }
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

async fn peer_download_task<S: Store>(
    id: PeerDownloadId,
    endpoint: Endpoint,
    store: S,
    hash: Hash,
    peer: NodeId,
    ranges: ChunkRanges,
    sender: mpsc::Sender<Command>,
    start: Instant,
) {
    let result = peer_download(endpoint, store, hash, peer, ranges, &sender, start).await;
    sender
        .send(Command::PeerDownloadComplete { id, result })
        .await
        .ok();
}

async fn peer_download<S: Store>(
    endpoint: Endpoint,
    store: S,
    hash: Hash,
    peer: NodeId,
    ranges: ChunkRanges,
    sender: &mpsc::Sender<Command>,
    start: Instant,
) -> anyhow::Result<Stats> {
    info!("Connecting to peer {peer}");
    let conn = endpoint.connect(peer, crate::ALPN).await?;
    info!("Got connection to peer {peer}");
    let spec = RangeSpec::new(ranges);
    let ranges = RangeSpecSeq::new([spec, RangeSpec::EMPTY]);
    info!("starting download from {peer} for {hash} {ranges:?}");
    let request = GetRequest::new(hash, ranges);
    let initial = crate::get::fsm::start(conn, request);
    // connect
    let connected = initial.next().await?;
    // read the first bytes
    let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
        return Err(io::Error::new(io::ErrorKind::Other, "expected start root").into());
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
                        let added =
                            ChunkRanges::from(ChunkNum(start_chunk)..ChunkNum(start_chunk + 16));
                        sender
                            .send(Command::ChunksDownloaded {
                                time: start.elapsed(),
                                peer,
                                hash,
                                added: added.clone(),
                            })
                            .await
                            .ok();
                        batch.push(leaf.into());
                        writer.write_batch(size, std::mem::take(&mut batch)).await?;
                        sender
                            .send(Command::BitfieldUpdate {
                                peer: BitfieldPeer::Local,
                                hash,
                                added,
                                removed: ChunkRanges::empty(),
                            })
                            .await
                            .ok();
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

    fn has_all_events(evs: &[Event], evs2: &[&Event]) -> bool {
        evs2.iter().all(|ev| has_one_event(evs, ev))
    }

    fn has_one_event_matching(evs: &[Event], f: impl Fn(&Event) -> bool) -> bool {
        evs.iter().filter(|e| f(e)).count() == 1
    }

    /// Test various things that should produce errors
    #[test]
    fn downloader_state_errors() -> TestResult<()> {
        use BitfieldPeer::*;
        let _ = tracing_subscriber::fmt::try_init();
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let unknown_hash =
            "0000000000000000000000000000000000000000000000000000000000000002".parse()?;
        let mut state = DownloaderState::new(noop_planner());
        let evs = state.apply(Command::Bitfield {
            peer: Local,
            hash,
            ranges: ChunkRanges::all(),
        });
        assert!(
            has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })),
            "adding an open bitfield should produce an error!"
        );
        let evs = state.apply(Command::Bitfield {
            peer: Local,
            hash: unknown_hash,
            ranges: ChunkRanges::all(),
        });
        assert!(
            has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })),
            "adding an open bitfield for an unknown hash should produce an error!"
        );
        let evs = state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([0..16]),
        });
        assert!(
            has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })),
            "download from unknown peer should lead to an error!"
        );
        Ok(())
    }

    /// Test a simple scenario where a download is started and completed
    #[test]
    fn downloader_state_smoke() -> TestResult<()> {
        use BitfieldPeer::*;
        let _ = tracing_subscriber::fmt::try_init();
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let mut state = DownloaderState::new(noop_planner());
        let evs = state.apply(Command::StartDownload {
            request: DownloadRequest {
                hash,
                ranges: chunk_ranges([0..64]),
            },
            id: DownloadId(0),
        });
        assert!(
            has_one_event(
                &evs,
                &Event::StartDiscovery {
                    hash,
                    id: DiscoveryId(0)
                }
            ),
            "starting a download should start a discovery task"
        );
        assert!(
            has_one_event(
                &evs,
                &Event::SubscribeBitfield {
                    peer: Local,
                    hash,
                    id: BitfieldSubscriptionId(0)
                }
            ),
            "starting a download should subscribe to the local bitfield"
        );
        let initial_bitfield = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let evs = state.apply(Command::Bitfield {
            peer: Local,
            hash,
            ranges: initial_bitfield.clone(),
        });
        assert!(evs.is_empty());
        assert_eq!(
            state
                .bitfields
                .get_local(hash)
                .context("bitfield should be present")?
                .ranges,
            initial_bitfield,
            "bitfield should be set to the initial bitfield"
        );
        assert_eq!(
            state
                .bitfields
                .get_local(hash)
                .context("bitfield should be present")?
                .subscription_count,
            1,
            "we have one download interested in the bitfield"
        );
        let evs = state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([16..32]),
            removed: ChunkRanges::empty(),
        });
        assert!(evs.is_empty());
        assert_eq!(
            state
                .bitfields
                .get_local(hash)
                .context("bitfield should be present")?
                .ranges,
            ChunkRanges::from(ChunkNum(0)..ChunkNum(32)),
            "bitfield should be updated"
        );
        let evs = state.apply(Command::PeerDiscovered { peer: peer_a, hash });
        assert!(
            has_one_event(
                &evs,
                &Event::SubscribeBitfield {
                    peer: Remote(peer_a),
                    hash,
                    id: 1.into()
                }
            ),
            "adding a new peer for a hash we are interested in should subscribe to the bitfield"
        );
        let evs = state.apply(Command::Bitfield {
            peer: Remote(peer_a),
            hash,
            ranges: chunk_ranges([0..64]),
        });
        assert!(
            has_one_event(
                &evs,
                &Event::StartPeerDownload {
                    id: PeerDownloadId(0),
                    peer: peer_a,
                    hash,
                    ranges: chunk_ranges([32..64])
                }
            ),
            "bitfield from a peer should start a download"
        );
        // ChunksDownloaded just updates the peer stats
        let evs = state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([32..48]),
        });
        assert!(evs.is_empty());
        // Bitfield update does not yet complete the download
        let evs = state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([32..48]),
            removed: ChunkRanges::empty(),
        });
        assert!(evs.is_empty());
        // ChunksDownloaded just updates the peer stats
        let evs = state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([48..64]),
        });
        assert!(evs.is_empty());
        // Final bitfield update for the local bitfield should complete the download
        let evs = state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([48..64]),
            removed: ChunkRanges::empty(),
        });
        assert!(
            has_one_event_matching(&evs, |e| matches!(e, Event::DownloadComplete { .. })),
            "download should be completed by the data"
        );
        // quick check that everything got cleaned up
        assert!(state.downloads.by_id.is_empty());
        assert!(state.bitfields.by_peer_and_hash.is_empty());
        assert!(state.discovery.is_empty());
        Ok(())
    }

    /// Test a scenario where more data becomes available at the remote peer as the download progresses
    #[test]
    fn downloader_state_incremental() -> TestResult<()> {
        use BitfieldPeer::*;
        let _ = tracing_subscriber::fmt::try_init();
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let mut state = DownloaderState::new(noop_planner());
        // Start a download
        state.apply(Command::StartDownload {
            request: DownloadRequest {
                hash,
                ranges: chunk_ranges([0..64]),
            },
            id: DownloadId(0),
        });
        // Initially, we have nothing
        state.apply(Command::Bitfield {
            peer: Local,
            hash,
            ranges: ChunkRanges::empty(),
        });
        // We have a peer for the hash
        state.apply(Command::PeerDiscovered { peer: peer_a, hash });
        // We have a bitfield from the peer
        let evs = state.apply(Command::Bitfield {
            peer: Remote(peer_a),
            hash,
            ranges: chunk_ranges([0..32]),
        });
        assert!(
            has_one_event(
                &evs,
                &Event::StartPeerDownload {
                    id: 0.into(),
                    peer: peer_a,
                    hash,
                    ranges: chunk_ranges([0..32])
                }
            ),
            "bitfield from a peer should start a download"
        );
        // ChunksDownloaded just updates the peer stats
        state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([0..16]),
        });
        // Bitfield update does not yet complete the download
        state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([0..16]),
            removed: ChunkRanges::empty(),
        });
        // The peer now has more data
        state.apply(Command::Bitfield {
            peer: Remote(peer_a),
            hash,
            ranges: chunk_ranges([32..64]),
        });
        // ChunksDownloaded just updates the peer stats
        state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([16..32]),
        });
        // Complete the first part of the download
        let evs = state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([16..32]),
            removed: ChunkRanges::empty(),
        });
        // This triggers cancellation of the first peer download and starting a new one for the remaining data
        assert!(
            has_one_event(&evs, &Event::StopPeerDownload { id: 0.into() }),
            "first peer download should be stopped"
        );
        assert!(
            has_one_event(
                &evs,
                &Event::StartPeerDownload {
                    id: 1.into(),
                    peer: peer_a,
                    hash,
                    ranges: chunk_ranges([32..64])
                }
            ),
            "second peer download should be started"
        );
        // ChunksDownloaded just updates the peer stats
        state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([32..64]),
        });
        // Final bitfield update for the local bitfield should complete the download
        let evs = state.apply(Command::BitfieldUpdate {
            peer: Local,
            hash,
            added: chunk_ranges([32..64]),
            removed: ChunkRanges::empty(),
        });
        assert!(
            has_all_events(
                &evs,
                &[
                    &Event::StopPeerDownload { id: 1.into() },
                    &Event::DownloadComplete { id: 0.into() },
                    &Event::UnsubscribeBitfield { id: 0.into() },
                    &Event::StopDiscovery { id: 0.into() },
                ]
            ),
            "download should be completed by the data"
        );
        println!("{evs:?}");
        Ok(())
    }

    #[test]
    fn downloader_state_multiple_downloads() -> testresult::TestResult<()> {
        use BitfieldPeer::*;
        // Use a constant hash (the same style as used in other tests).
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        // Create a downloader state with a no‐op planner.
        let mut state = DownloaderState::new(noop_planner());

        // --- Start the first (ongoing) download.
        // Request a range from 0..64.
        let download0 = DownloadId(0);
        let req0 = DownloadRequest {
            hash,
            ranges: chunk_ranges([0..64]),
        };
        let evs0 = state.apply(Command::StartDownload {
            request: req0,
            id: download0,
        });
        // When starting the download, we expect a discovery task to be started
        // and a subscription to the local bitfield to be requested.
        assert!(
            has_one_event(
                &evs0,
                &Event::StartDiscovery {
                    hash,
                    id: DiscoveryId(0)
                }
            ),
            "download0 should start discovery"
        );
        assert!(
            has_one_event(
                &evs0,
                &Event::SubscribeBitfield {
                    peer: Local,
                    hash,
                    id: BitfieldSubscriptionId(0)
                }
            ),
            "download0 should subscribe to the local bitfield"
        );

        // --- Simulate some progress for the first download.
        // Let’s say only chunks 0..32 are available locally.
        let evs1 = state.apply(Command::Bitfield {
            peer: Local,
            hash,
            ranges: chunk_ranges([0..32]),
        });
        // No completion event should be generated for download0 because its full range 0..64 is not yet met.
        assert!(
            evs1.is_empty(),
            "Partial bitfield update should not complete download0"
        );

        // --- Start a second download for the same hash.
        // This new download only requires chunks 0..32 which are already available.
        let download1 = DownloadId(1);
        let req1 = DownloadRequest {
            hash,
            ranges: chunk_ranges([0..32]),
        };
        let evs2 = state.apply(Command::StartDownload {
            request: req1,
            id: download1,
        });
        // Because the local bitfield (0..32) is already a superset of the new download’s request,
        // a DownloadComplete event for download1 should be generated immediately.
        assert!(
            has_one_event(&evs2, &Event::DownloadComplete { id: download1 }),
            "New download should complete immediately"
        );

        // --- Verify state:
        // The ongoing download (download0) should still be present in the state,
        // while the newly completed download (download1) is removed.
        assert!(
            state.downloads.contains_key(&download0),
            "download0 should still be active"
        );
        assert!(
            !state.downloads.contains_key(&download1),
            "download1 should have been cleaned up after completion"
        );

        Ok(())
    }

    /// Test a scenario where more data becomes available at the remote peer as the download progresses
    #[test]
    fn downloader_state_drop() -> TestResult<()> {
        use BitfieldPeer::*;
        let _ = tracing_subscriber::fmt::try_init();
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let mut state = DownloaderState::new(noop_planner());
        // Start a download
        state.apply(Command::StartDownload {
            request: DownloadRequest {
                hash,
                ranges: chunk_ranges([0..64]),
            },
            id: 0.into(),
        });
        // Initially, we have nothing
        state.apply(Command::Bitfield {
            peer: Local,
            hash,
            ranges: ChunkRanges::empty(),
        });
        // We have a peer for the hash
        state.apply(Command::PeerDiscovered { peer: peer_a, hash });
        // We have a bitfield from the peer
        let evs = state.apply(Command::Bitfield {
            peer: Remote(peer_a),
            hash,
            ranges: chunk_ranges([0..32]),
        });
        assert!(
            has_one_event(
                &evs,
                &Event::StartPeerDownload {
                    id: 0.into(),
                    peer: peer_a,
                    hash,
                    ranges: chunk_ranges([0..32])
                }
            ),
            "bitfield from a peer should start a download"
        );
        // Sending StopDownload should stop the download and all associated tasks
        // This is what happens (delayed) when the user drops the download future
        let evs = state.apply(Command::StopDownload { id: 0.into() });
        assert!(has_one_event(
            &evs,
            &Event::StopPeerDownload { id: 0.into() }
        ));
        assert!(has_one_event(
            &evs,
            &Event::UnsubscribeBitfield { id: 0.into() }
        ));
        assert!(has_one_event(
            &evs,
            &Event::UnsubscribeBitfield { id: 1.into() }
        ));
        assert!(has_one_event(&evs, &Event::StopDiscovery { id: 0.into() }));
        Ok(())
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
        tokio::time::sleep(Duration::from_secs(2)).await;
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
