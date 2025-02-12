//! The actor for the downloader
use super::*;

/// An user-facing command
#[derive(Debug)]
pub(super) enum UserCommand {
    Download {
        request: DownloadRequest,
        done: tokio::sync::oneshot::Sender<()>,
    },
    Observe {
        request: ObserveRequest,
        send: tokio::sync::mpsc::Sender<BitfieldEvent>,
    },
}

pub(super) struct DownloaderActor<S> {
    state: DownloaderState,
    local_pool: LocalPool,
    endpoint: Endpoint,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    store: S,
    /// Content discovery
    discovery: BoxedContentDiscovery,
    /// Bitfield subscription
    subscribe_bitfield: BoxedBitfieldSubscription,
    /// Futures for downloads
    download_futs: BTreeMap<DownloadId, tokio::sync::oneshot::Sender<()>>,
    /// Tasks for peer downloads
    peer_download_tasks: BTreeMap<PeerDownloadId, local_pool::Run<()>>,
    /// Tasks for discovery
    discovery_tasks: BTreeMap<DiscoveryId, AbortOnDropHandle<()>>,
    /// Tasks for bitfield subscriptions
    bitfield_subscription_tasks: BTreeMap<BitfieldSubscriptionId, AbortOnDropHandle<()>>,
    /// Id generator for download ids
    download_id_gen: IdGenerator<DownloadId>,
    /// Id generator for observe ids
    observe_id_gen: IdGenerator<ObserveId>,
    /// Observers
    observers: BTreeMap<ObserveId, tokio::sync::mpsc::Sender<BitfieldEvent>>,
    /// The time when the actor was started, serves as the epoch for time messages to the state machine
    start: Instant,
}

impl<S: Store> DownloaderActor<S> {
    pub(super) fn new(
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

    pub(super) async fn run(mut self, mut user_commands: mpsc::Receiver<UserCommand>) {
        let mut ticks = tokio::time::interval(Duration::from_millis(100));
        loop {
            trace!("downloader actor tick");
            tokio::select! {
                biased;
                Some(cmd) = user_commands.recv() => {
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
                    while let Some(event) = stream.next().await {
                        let cmd = Command::BitfieldInfo { peer, hash, event };
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
            Event::LocalBitfieldInfo { id, event } => {
                let Some(sender) = self.observers.get(&id) else {
                    return;
                };
                if sender.try_send(event).is_err() {
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

#[allow(clippy::too_many_arguments)]
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
                            .send(Command::BitfieldInfo {
                                peer: BitfieldPeer::Local,
                                hash,
                                event: BitfieldUpdate {
                                    added,
                                    removed: ChunkRanges::empty(),
                                    size,
                                }
                                .into(),
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

/// Spawn a future and wrap it in a [`AbortOnDropHandle`]
pub(super) fn spawn<F, T>(f: F) -> AbortOnDropHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let task = tokio::spawn(f);
    AbortOnDropHandle::new(task)
}
