//! The state machine for the downloader, as well as the commands and events.
//!
//! In addition to the state machine, there are also some structs encapsulating
//! a part of the state. These are at this point just wrappers around a single
//! map, but can be made more efficient later if needed without breaking the
//! interface.
use super::*;

#[derive(Debug)]
pub(super) enum Command {
    /// A user request to start a download.
    StartDownload {
        /// The download request
        request: DownloadRequest,
        /// The unique id, to be assigned by the caller
        id: DownloadId,
    },
    /// A user request to abort a download.
    StopDownload { id: DownloadId },
    /// An update of a bitfield for a hash
    ///
    /// This is used both to update the bitfield of remote peers, and to update
    /// the local bitfield.
    BitfieldInfo {
        /// The peer that sent the update.
        peer: BitfieldPeer,
        /// The blob that was updated.
        hash: Hash,
        /// The state or update event
        event: BitfieldEvent,
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
    /// Start observing a local bitfield
    ObserveLocal {
        id: ObserveId,
        hash: Hash,
        ranges: ChunkRanges,
    },
    /// Stop observing a local bitfield
    StopObserveLocal { id: ObserveId },
    /// A tick from the driver, for rebalancing
    Tick { time: Duration },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Event {
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
    LocalBitfieldInfo {
        id: ObserveId,
        event: BitfieldEvent,
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
    Error {
        message: String,
    },
}

pub struct DownloaderState {
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
    pub fn new(planner: Box<dyn DownloadPlanner>) -> Self {
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

impl DownloaderState {
    /// Apply a command and return the events that were generated
    pub(super) fn apply(&mut self, cmd: Command) -> Vec<Event> {
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
                if let std::collections::btree_map::Entry::Vacant(e) =
                    self.discovery.entry(request.hash)
                {
                    // start a discovery task
                    let id = self.discovery_id_gen.next();
                    evs.push(Event::StartDiscovery { hash, id });
                    e.insert(id);
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
                        false
                    } else {
                        true
                    }
                });
                self.peers.remove(&peer);
            }
            Command::BitfieldInfo {
                peer,
                hash,
                event: BitfieldEvent::State(BitfieldState { ranges, size }),
            } => {
                let state = self.bitfields.get_mut(&(peer, hash)).context(format!(
                    "bitfields for unknown peer {peer:?} and hash {hash}"
                ))?;
                let _chunks = total_chunks(&ranges).context("open range")?;
                if peer == BitfieldPeer::Local {
                    // we got a new local bitmap, notify local observers
                    // we must notify all local observers, even if the bitmap is empty
                    state.size.update(size)?;
                    if let Some(observers) = self.observers.get_by_hash(&hash) {
                        for (id, request) in observers {
                            let ranges = &ranges & &request.ranges;
                            evs.push(Event::LocalBitfieldInfo {
                                id: *id,
                                event: BitfieldState {
                                    ranges: ranges.clone(),
                                    size: state.size,
                                }
                                .into(),
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
                    state.size.update(size)?;
                }
                // we have to call start_downloads even if the local bitfield set, since we don't know in which order local and remote bitfields arrive
                self.start_downloads(hash, None, evs)?;
            }
            Command::BitfieldInfo {
                peer,
                hash,
                event:
                    BitfieldEvent::Update(BitfieldUpdate {
                        added,
                        removed,
                        size,
                    }),
            } => {
                let state = self.bitfields.get_mut(&(peer, hash)).context(format!(
                    "bitfield update for unknown peer {peer:?} and hash {hash}"
                ))?;
                if peer == BitfieldPeer::Local {
                    // we got a local bitfield update, notify local observers
                    // for updates we can just notify the observers that have a non-empty intersection with the update
                    state.size.update(size)?;
                    if let Some(observers) = self.observers.get_by_hash(&hash) {
                        for (id, request) in observers {
                            let added = &added & &request.ranges;
                            let removed = &removed & &request.ranges;
                            if !added.is_empty() || !removed.is_empty() {
                                evs.push(Event::LocalBitfieldInfo {
                                    id: *id,
                                    event: BitfieldUpdate {
                                        added: &added & &request.ranges,
                                        removed: &removed & &request.ranges,
                                        size: state.size,
                                    }
                                    .into(),
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
                    state.size.update(size)?;
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
                    evs.push(Event::LocalBitfieldInfo {
                        id,
                        event: BitfieldState {
                            ranges: state.ranges.clone(),
                            size: BaoBlobSizeOpt::Unknown,
                        }
                        .into(),
                    });
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
                self.observers.insert(id, ObserveInfo { hash, ranges });
            }
            Command::StopObserveLocal { id } => {
                let request = self
                    .observers
                    .remove(&id)
                    .context(format!("stop observing unknown local bitfield {id:?}"))?;
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
        let mask = match self_state.size {
            BaoBlobSizeOpt::Verified(size) => ChunkRanges::from(..ChunkNum::chunks(size)),
            _ => ChunkRanges::all(),
        };
        let mut completed = vec![];
        for (id, download) in self.downloads.iter_mut_for_hash(hash) {
            if just_id.is_some() && just_id != Some(*id) {
                continue;
            }
            download.request.ranges &= mask.clone();
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
        for state in download.peer_downloads.values() {
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

struct PeerDownloadState {
    id: PeerDownloadId,
    ranges: ChunkRanges,
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
            .find(|(_, v)| v.peer_downloads.iter().any(|(_, state)| state.id == id))
    }
}

#[derive(Debug)]
struct ObserveInfo {
    hash: Hash,
    ranges: ChunkRanges,
}

#[derive(Debug, Default)]
struct Observers {
    by_hash_and_id: BTreeMap<Hash, BTreeMap<ObserveId, ObserveInfo>>,
}

impl Observers {
    fn insert(&mut self, id: ObserveId, request: ObserveInfo) {
        self.by_hash_and_id
            .entry(request.hash)
            .or_default()
            .insert(id, request);
    }

    fn remove(&mut self, id: &ObserveId) -> Option<ObserveInfo> {
        for requests in self.by_hash_and_id.values_mut() {
            if let Some(request) = requests.remove(id) {
                return Some(request);
            }
        }
        None
    }

    fn get_by_hash(&self, hash: &Hash) -> Option<&BTreeMap<ObserveId, ObserveInfo>> {
        self.by_hash_and_id.get(hash)
    }
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
///
/// Note that for remote peers we can't really trust this information.
/// They could lie about the size, and the ranges could be either wrong or outdated.
struct PeerBlobState {
    /// The subscription id for the subscription
    subscription_id: BitfieldSubscriptionId,
    /// The number of subscriptions this peer has
    subscription_count: usize,
    /// chunk ranges this peer reports to have
    ranges: ChunkRanges,
    /// The minimum reported size of the blob
    size: BaoBlobSizeOpt,
}

impl PeerBlobState {
    fn new(subscription_id: BitfieldSubscriptionId) -> Self {
        Self {
            subscription_id,
            subscription_count: 1,
            ranges: ChunkRanges::empty(),
            size: BaoBlobSizeOpt::Unknown,
        }
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
    #![allow(clippy::single_range_in_vec_init)]

    use testresult::TestResult;

    use super::{
        super::tests::{
            chunk_ranges, has_all_events, has_one_event, has_one_event_matching, noop_planner,
        },
        *,
    };

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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldState {
                ranges: initial_bitfield.clone(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([16..32]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Remote(peer_a),
            hash,
            event: BitfieldState {
                ranges: chunk_ranges([0..64]),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([32..48]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([48..64]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldState::unknown().into(),
        });
        // We have a peer for the hash
        state.apply(Command::PeerDiscovered { peer: peer_a, hash });
        // We have a bitfield from the peer
        let evs = state.apply(Command::BitfieldInfo {
            peer: Remote(peer_a),
            hash,
            event: BitfieldState {
                ranges: chunk_ranges([0..32]),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([0..16]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
        });
        // The peer now has more data
        state.apply(Command::BitfieldInfo {
            peer: Remote(peer_a),
            hash,
            event: BitfieldState {
                ranges: chunk_ranges([32..64]),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
        });
        // ChunksDownloaded just updates the peer stats
        state.apply(Command::ChunksDownloaded {
            time: Duration::ZERO,
            peer: peer_a,
            hash,
            added: chunk_ranges([16..32]),
        });
        // Complete the first part of the download
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([16..32]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldUpdate {
                added: chunk_ranges([32..64]),
                removed: ChunkRanges::empty(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs1 = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldState {
                ranges: chunk_ranges([0..32]),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldState::unknown().into(),
        });
        // We have a peer for the hash
        state.apply(Command::PeerDiscovered { peer: peer_a, hash });
        // We have a bitfield from the peer
        let evs = state.apply(Command::BitfieldInfo {
            peer: Remote(peer_a),
            hash,
            event: BitfieldState {
                ranges: chunk_ranges([0..32]),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash,
            event: BitfieldState {
                ranges: ChunkRanges::all(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
        });
        assert!(
            has_one_event_matching(&evs, |e| matches!(e, Event::Error { .. })),
            "adding an open bitfield should produce an error!"
        );
        let evs = state.apply(Command::BitfieldInfo {
            peer: Local,
            hash: unknown_hash,
            event: BitfieldState {
                ranges: ChunkRanges::all(),
                size: BaoBlobSizeOpt::Unknown,
            }
            .into(),
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
}
