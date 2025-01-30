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
//! The [DownloaderDriver] is the asynchronous driver for the state machine. It
//! owns the actual tasks that perform IO.
use std::collections::{BTreeMap, VecDeque};

use crate::{store::Store, Hash};
use anyhow::Context;
use bao_tree::ChunkRanges;
use futures_lite::Stream;
use iroh::NodeId;
use range_collections::range_set::RangeSetRange;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;

/// todo: make newtypes?
type DownloadId = u64;
type BitmapSubscriptionId = u64;
type DiscoveryId = u64;
type PeerDownloadId = u64;

/// Announce kind
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnnounceKind {
    /// The peer supposedly has some of the data.
    Partial = 0,
    /// The peer supposedly has the complete data.
    Complete,
}

struct FindPeersOpts {
    /// Kind of announce
    kind: AnnounceKind,
}

/// A pluggable content discovery mechanism
trait ContentDiscovery {
    /// Find peers that have the given blob.
    ///
    /// The returned stream is a handle for the discovery task. It should be an
    /// infinite stream that only stops when it is dropped.
    fn find_peers(&mut self, hash: Hash, opts: FindPeersOpts)
        -> impl Stream<Item = NodeId> + Unpin;
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
        Self {
            subscription_id,
            subscription_count: 1,
            ranges: ChunkRanges::empty(),
        }
    }
}

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
        Self {
            request,
            peer_downloads: BTreeMap::new(),
        }
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
    /// A chunk was downloaded
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
        self.bitmaps
            .iter()
            .filter(|((peer, x), _)| peer.is_some() && *x == hash)
            .count()
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
            evs.push(Event::Error {
                message: format!("{cause}"),
            });
        }
    }

    /// Apply a command and bail out on error
    fn apply0(&mut self, cmd: Command, evs: &mut Vec<Event>) -> anyhow::Result<()> {
        match cmd {
            Command::StartDownload { request, id } => {
                // ids must be uniquely assigned by the caller!
                anyhow::ensure!(
                    !self.downloads.contains_key(&id),
                    "duplicate download request {id}"
                );
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitmaps.get_mut(&(None, request.hash)) {
                    // just increment the count
                    state.subscription_count += 1;
                } else {
                    // create a new subscription
                    let subscription_id = self.next_subscription_id();
                    evs.push(Event::SubscribeBitmap {
                        peer: None,
                        hash: request.hash,
                        id: subscription_id,
                    });
                    self.bitmaps
                        .insert((None, request.hash), PeerBlobState::new(subscription_id));
                }
                if !self.discovery.contains_key(&request.hash) {
                    // start a discovery task
                    let discovery_id = self.next_discovery_id();
                    evs.push(Event::StartDiscovery {
                        hash: request.hash,
                        id: discovery_id,
                    });
                    self.discovery.insert(request.hash, discovery_id);
                }
                self.downloads.insert(id, DownloadState::new(request));
            }
            Command::StopDownload { id } => {
                let removed = self
                    .downloads
                    .remove(&id)
                    .context(format!("removed unknown download {id}"))?;
                self.bitmaps.retain(|(_peer, hash), state| {
                    if *hash == removed.request.hash {
                        state.subscription_count -= 1;
                        if state.subscription_count == 0 {
                            evs.push(Event::UnsubscribeBitmap {
                                id: state.subscription_id,
                            });
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
                if !self
                    .downloads
                    .iter()
                    .any(|(_id, state)| state.request.hash == hash)
                {
                    return Ok(());
                }
                // create a peer state if it does not exist
                let _state = self.peers.entry(peer).or_default();
                // create a new subscription
                let subscription_id = self.next_subscription_id();
                evs.push(Event::SubscribeBitmap {
                    peer: Some(peer),
                    hash,
                    id: subscription_id,
                });
                self.bitmaps
                    .insert((Some(peer), hash), PeerBlobState::new(subscription_id));
            }
            Command::DropPeer { peer } => {
                self.bitmaps.retain(|(p, _), state| {
                    if *p == Some(peer) {
                        // todo: should we emit unsubscribe evs here?
                        evs.push(Event::UnsubscribeBitmap {
                            id: state.subscription_id,
                        });
                        return false;
                    } else {
                        return true;
                    }
                });
                self.peers.remove(&peer);
            }
            Command::Bitmap { peer, hash, bitmap } => {
                let state = self
                    .bitmaps
                    .get_mut(&(peer, hash))
                    .context(format!("bitmap for unknown peer {peer:?} and hash {hash}"))?;
                let _chunks = total_chunks(&bitmap).context("open range")?;
                state.ranges = bitmap;
                self.rebalance_downloads(hash, evs)?;
            }
            Command::BitmapUpdate {
                peer,
                hash,
                added,
                removed,
            } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!(
                    "bitmap update for unknown peer {peer:?} and hash {hash}"
                ))?;
                state.ranges |= added;
                state.ranges &= !removed;
                self.rebalance_downloads(hash, evs)?;
            }
            Command::ChunksDownloaded {
                time,
                peer,
                hash,
                added,
            } => {
                let state = self.bitmaps.get_mut(&(None, hash)).context(format!(
                    "chunks downloaded before having local bitmap for {hash}"
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
        for (id, download) in self
            .downloads
            .iter_mut()
            .filter(|(_id, download)| download.request.hash == hash)
        {
            let remaining = &download.request.ranges - &self_state.ranges;
            if remaining.is_empty() {
                // cancel all downloads, if needed
                for (_, peer_download) in &download.peer_downloads {
                    evs.push(Event::StopPeerDownload {
                        id: peer_download.id,
                    });
                }
                // notify the user that the download is complete
                evs.push(Event::DownloadComplete { id: *id });
                // mark the download for later removal
                completed.push(*id);
                continue;
            }
            let mut candidates = vec![];
            for ((peer, _), bitmap) in self
                .bitmaps
                .iter()
                .filter(|((peer, x), _)| peer.is_some() && *x == hash)
            {
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
                evs.push(Event::StartPeerDownload {
                    id,
                    peer,
                    ranges: ranges.clone(),
                });
                download
                    .peer_downloads
                    .insert(peer, PeerDownloadState { id, ranges });
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

struct DownloaderDriver<S, D> {
    state: DownloaderState,
    store: S,
    discovery: D,
    peer_download_tasks: BTreeMap<PeerDownloadId, AbortOnDropHandle<()>>,
    discovery_tasks: BTreeMap<DiscoveryId, AbortOnDropHandle<()>>,
    bitmap_subscription_tasks: BTreeMap<BitmapSubscriptionId, AbortOnDropHandle<()>>,
    next_download_id: DownloadId,
}

impl<S: Store, D: ContentDiscovery> DownloaderDriver<S, D> {
    fn new(me: NodeId, store: S, discovery: D) -> Self {
        Self {
            state: DownloaderState::new(),
            store,
            discovery,
            peer_download_tasks: BTreeMap::new(),
            discovery_tasks: BTreeMap::new(),
            bitmap_subscription_tasks: BTreeMap::new(),
            next_download_id: 0,
        }
    }

    fn next_download_id(&mut self) -> DownloadId {
        let id = self.next_download_id;
        self.next_download_id += 1;
        id
    }

    fn handle_event(&mut self, ev: Event) {
        println!("event: {:?}", ev);
    }

    fn download(&mut self, request: DownloadRequest) -> DownloadId {
        let id = self.next_download_id();
        let evs = self
            .state
            .apply_and_get_evs(Command::StartDownload { request, id });
        for ev in evs {
            self.handle_event(ev);
        }
        id
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use bao_tree::ChunkNum;
    use testresult::TestResult;

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
        // let me = "0000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let peer_a = "1000000000000000000000000000000000000000000000000000000000000000".parse()?;
        let hash = "0000000000000000000000000000000000000000000000000000000000000001".parse()?;
        let unknown_hash =
            "0000000000000000000000000000000000000000000000000000000000000002".parse()?;
        let mut state = DownloaderState::new();
        let evs = state.apply_and_get_evs(super::Command::StartDownload {
            request: DownloadRequest {
                hash,
                ranges: chunk_ranges([0..64]),
            },
            id: 1,
        });
        assert!(
            evs.iter()
                .filter(|e| **e == Event::StartDiscovery { hash, id: 0 })
                .count()
                == 1,
            "starting a download should start a discovery task"
        );
        assert!(
            evs.iter()
                .filter(|e| **e
                    == Event::SubscribeBitmap {
                        peer: None,
                        hash,
                        id: 0
                    })
                .count()
                == 1,
            "starting a download should subscribe to the local bitmap"
        );
        println!("{evs:?}");
        let evs = state.apply_and_get_evs(Command::Bitmap {
            peer: None,
            hash,
            bitmap: ChunkRanges::all(),
        });
        assert!(
            evs.iter().any(|e| matches!(e, Event::Error { .. })),
            "adding an open bitmap should produce an error!"
        );
        let evs = state.apply_and_get_evs(Command::Bitmap {
            peer: None,
            hash: unknown_hash,
            bitmap: ChunkRanges::all(),
        });
        assert!(
            evs.iter().any(|e| matches!(e, Event::Error { .. })),
            "adding an open bitmap for an unknown hash should produce an error!"
        );
        let initial_bitmap = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
        let evs = state.apply_and_get_evs(Command::Bitmap {
            peer: None,
            hash,
            bitmap: initial_bitmap.clone(),
        });
        assert!(evs.is_empty());
        assert_eq!(
            state
                .bitmaps
                .get(&(None, hash))
                .context("bitmap should be present")?
                .ranges,
            initial_bitmap,
            "bitmap should be set to the initial bitmap"
        );
        let evs = state.apply_and_get_evs(Command::BitmapUpdate {
            peer: None,
            hash,
            added: chunk_ranges([16..32]),
            removed: ChunkRanges::empty(),
        });
        assert!(evs.is_empty());
        assert_eq!(
            state
                .bitmaps
                .get(&(None, hash))
                .context("bitmap should be present")?
                .ranges,
            ChunkRanges::from(ChunkNum(0)..ChunkNum(32)),
            "bitmap should be updated"
        );
        let evs = state.apply_and_get_evs(super::Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: ChunkRanges::from(ChunkNum(0)..ChunkNum(16)),
        });
        assert!(
            evs.iter().any(|e| matches!(e, Event::Error { .. })),
            "download from unknown peer should lead to an error!"
        );
        let evs = state.apply_and_get_evs(Command::PeerDiscovered { peer: peer_a, hash });
        assert!(
            evs.iter()
                .filter(|e| **e
                    == Event::SubscribeBitmap {
                        peer: Some(peer_a),
                        hash,
                        id: 1
                    })
                .count()
                == 1,
            "adding a new peer for a hash we are interested in should subscribe to the bitmap"
        );
        let evs = state.apply_and_get_evs(Command::Bitmap {
            peer: Some(peer_a),
            hash,
            bitmap: chunk_ranges([0..64]),
        });
        assert!(
            evs.iter()
                .filter(|e| **e
                    == Event::StartPeerDownload {
                        id: 0,
                        peer: peer_a,
                        ranges: chunk_ranges([32..64])
                    })
                .count()
                == 1,
            "bitmap from a peer should start a download"
        );
        let evs = state.apply_and_get_evs(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([32..64]),
        });
        assert!(
            evs.iter()
                .any(|e| matches!(e, Event::DownloadComplete { .. })),
            "download should be completed by the data"
        );
        println!("{evs:?}");
        Ok(())
    }

    #[tokio::test]
    async fn downloader_driver_smoke() -> TestResult<()> {
        let store = crate::store::mem::Store::new();
        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![crate::protocol::ALPN.to_vec()])
            .bind()
            .await?;
        Ok(())
    }
}
