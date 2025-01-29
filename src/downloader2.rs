//! Downloader version that supports range downloads and downloads from multiple sources.

use std::collections::{BTreeMap, VecDeque};

use anyhow::Context;
use bao_tree::ChunkRanges;
use chrono::Duration;
use futures_lite::Stream;
use iroh::NodeId;
use range_collections::range_set::RangeSetRange;
use serde::{Deserialize, Serialize};
use crate::Hash;

type DownloadId = u64;
type BitmapSubscriptionId = u64;
type DiscoveryId = u64;

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
    fn find_peers(&mut self, hash: Hash, opts: FindPeersOpts) -> impl Stream<Item = NodeId> + Unpin;
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

struct DownloadRequest {
    /// The blob we are downloading
    hash: Hash,
    /// The ranges we are interested in
    ranges: ChunkRanges,
}

struct DownloadState {
    /// The request this state is for
    request: DownloadRequest,

}

struct DownloaderState {
    // my own node id
    me: NodeId,
    // all peers I am tracking for any download
    peers: BTreeMap<NodeId, PeerState>,
    // all bitmaps I am tracking, both for myself and for remote peers
    bitmaps: BTreeMap<(NodeId, Hash), PeerBlobState>,
    // all active downloads
    downloads: BTreeMap<DownloadId, DownloadState>,
    // discovery tasks
    discovery: BTreeMap<Hash, DiscoveryId>,
    // the next subscription id
    next_subscription_id: BitmapSubscriptionId,
    // the next discovery id
    next_discovery_id: u64,
}

impl DownloaderState {
    fn new(me: NodeId) -> Self {
        Self {
            me,
            peers: BTreeMap::new(),
            downloads: BTreeMap::new(),
            bitmaps: BTreeMap::new(),
            discovery: BTreeMap::new(),
            next_subscription_id: 0,
            next_discovery_id: 0,
        }
    }
}

enum Command {
    /// A request to start a download.
    StartDownload {
        /// The download request
        request: DownloadRequest,
        /// The unique id, to be assigned by the caller
        id: u64,
    },
    /// A request to abort a download.
    StopDownload {
        id: u64,
    },
    /// A bitmap for a blob and a peer
    Bitmap {
        /// The peer that sent the bitmap.
        peer: NodeId,
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
        peer: NodeId,
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
    DropPeer {
        peer: NodeId,
    }
}

enum Event {
    SubscribeBitmap {
        peer: NodeId,
        hash: Hash,
        id: u64,
    },
    UnsubscribeBitmap {
        id: u64,
    },
    StartDiscovery {
        hash: Hash,
        id: u64,
    },
    Error {
        message: String,
    },
}

impl DownloaderState {

    fn apply(&mut self, cmd: Command, events: &mut Vec<Event>) {
        if let Err(cause) = self.apply0(cmd, events) {
            events.push(Event::Error { message: format!("{cause}") });
        }
    }

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

    fn count_providers(&self, hash: Hash) -> usize {
        self.bitmaps.iter().filter(|((peer, x), _)| *peer != self.me && *x == hash).count()
    }

    fn apply0(&mut self, cmd: Command, events: &mut Vec<Event>) -> anyhow::Result<()> {
        match cmd {
            Command::StartDownload { request, id } => {
                // ids must be uniquely assigned by the caller!
                anyhow::ensure!(!self.downloads.contains_key(&id), "duplicate download request {id}");
                // either we have a subscription for this blob, or we have to create one
                if let Some(state) = self.bitmaps.get_mut(&(self.me, request.hash)) {
                    // just increment the count
                    state.subscription_count += 1;
                } else {
                    // create a new subscription
                    let subscription_id = self.next_subscription_id();
                    events.push(Event::SubscribeBitmap {
                        peer: self.me,
                        hash: request.hash,
                        id: subscription_id,
                    });
                    self.bitmaps.insert((self.me, request.hash), PeerBlobState { subscription_id, subscription_count: 1, ranges: ChunkRanges::empty() });
                }
                if !self.discovery.contains_key(&request.hash) {
                    // start a discovery task
                    let discovery_id = self.next_discovery_id();
                    events.push(Event::StartDiscovery {
                        hash: request.hash,
                        id: discovery_id,
                    });
                    self.discovery.insert(request.hash, discovery_id);
                }
                self.downloads.insert(id, DownloadState { request });
            }
            Command::StopDownload { id } => {
                let removed = self.downloads.remove(&id).context(format!("removed unknown download {id}"))?;
                self.bitmaps.retain(|(_peer, hash), state| {
                    if *hash == removed.request.hash {
                        state.subscription_count -= 1;
                        if state.subscription_count == 0 {
                            events.push(Event::UnsubscribeBitmap { id: state.subscription_id });
                            return false;
                        }
                    }
                    true
                });
            }
            Command::DropPeer { peer } => {
                anyhow::ensure!(peer != self.me, "not a remote peer");
                self.bitmaps.retain(|(p, _), state| {
                    if *p == peer {
                        // todo: should we emit unsubscribe events here?
                        events.push(Event::UnsubscribeBitmap { id: state.subscription_id });
                        return false;
                    } else {
                        return true;
                    }
                });
                self.peers.remove(&peer);
            }
            Command::Bitmap { peer, hash, bitmap } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("bitmap for unknown peer {peer} and hash {hash}"))?;
                state.ranges = bitmap;
            }
            Command::BitmapUpdate { peer, hash, added, removed } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("bitmap update for unknown peer {peer} and hash {hash}"))?;
                state.ranges |= added;
                state.ranges &= !removed;
            }
            Command::ChunksDownloaded { time, peer, hash, added } => {
                let state = self.bitmaps.get_mut(&(peer, hash)).context(format!("chunks downloaded for unknown peer {peer} and hash {hash}"))?;
                let total_downloaded = total_chunks(&added).context("open range")?;
                let total_before = total_chunks(&state.ranges).context("open range")?;
                state.ranges |= added;
                let total_after = total_chunks(&state.ranges).context("open range")?;
                let useful_downloaded = total_after - total_before;
                let peer = self.peers.entry(peer).or_default();
                peer.download_history.push_back((time, (total_downloaded, useful_downloaded)));
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

#[cfg(test)]
mod tests {

    
}