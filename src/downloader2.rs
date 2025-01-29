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

/// Identifier for a download intent.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, derive_more::Display)]
pub struct IntentId(pub u64);

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
    /// 
    subscription_id: u64,
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
    peers: BTreeMap<NodeId, PeerState>,
    bitmaps: BTreeMap<(NodeId, Hash), PeerBlobState>,
    downloads: BTreeMap<IntentId, DownloadState>,
    me: NodeId,
    next_subscription_id: u64,
}

impl DownloaderState {
    fn new(me: NodeId) -> Self {
        Self {
            me,
            peers: BTreeMap::new(),
            downloads: BTreeMap::new(),
            bitmaps: BTreeMap::new(),
            next_subscription_id: 0,
        }
    }
}

enum Command {
    /// A request to start a download.
    StartDownload {
        /// The download request
        request: DownloadRequest,
        /// The unique id, to be assigned by the caller
        id: IntentId,
    },
    /// A request to abort a download.
    StopDownload {
        id: IntentId,
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
    }
}

enum Event {
    SubscribeBitmap {
        peer: NodeId,
        hash: Hash,
        subscription_id: u64,
    },
    UnsubscribeBitmap {
        subscription_id: u64,
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

    fn apply0(&mut self, cmd: Command, events: &mut Vec<Event>) -> anyhow::Result<()> {
        match cmd {
            Command::StartDownload { request, id } => {
                if self.downloads.contains_key(&id) {
                    anyhow::bail!("duplicate download request {id}");
                }
                if !self.bitmaps.contains_key(&(self.me, request.hash)) {
                    let subscription_id = self.next_subscription_id();
                    events.push(Event::SubscribeBitmap {
                        peer: self.me,
                        hash: request.hash,
                        subscription_id,
                    });
                    self.bitmaps.insert((self.me, request.hash), PeerBlobState { subscription_id, ranges: ChunkRanges::empty() });
                }
                self.downloads.insert(id, DownloadState { request });
            }
            Command::StopDownload { id } => {
                self.downloads.remove(&id);
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