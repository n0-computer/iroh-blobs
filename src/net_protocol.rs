//! Adaptation of `iroh-blobs` as an `iroh` protocol.

// TODO: reduce API surface and add documentation
#![allow(missing_docs)]

use std::{collections::BTreeSet, fmt::Debug, ops::DerefMut, sync::Arc};

use anyhow::{anyhow, bail, Result};
use futures_lite::future::Boxed as BoxedFuture;
use futures_util::future::BoxFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler, Endpoint, NodeAddr};
use iroh_base::hash::{BlobFormat, Hash};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::{
    downloader::{DownloadRequest, Downloader},
    get::{
        db::{DownloadProgress, GetState},
        Stats,
    },
    provider::EventSender,
    store::GcConfig,
    util::{
        local_pool::{self, LocalPoolHandle},
        progress::{AsyncChannelProgressSender, ProgressSender},
        SetTagOption,
    },
    HashAndFormat,
};

/// A callback that blobs can ask about a set of hashes that should not be garbage collected.
pub type ProtectCb = Box<dyn Fn(&mut BTreeSet<Hash>) -> BoxFuture<()> + Send + Sync>;

/// The state of the gc loop.
#[derive(derive_more::Debug)]
enum GcState {
    // Gc loop is not yet running. Other protocols can add protect callbacks
    Initial(#[debug(skip)] Vec<ProtectCb>),
    // Gc loop is running. No more protect callbacks can be added.
    Started(#[allow(dead_code)] Option<local_pool::Run<()>>),
}

impl Default for GcState {
    fn default() -> Self {
        Self::Initial(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct Blobs<S> {
    rt: LocalPoolHandle,
    pub(crate) store: S,
    events: EventSender,
    downloader: Downloader,
    #[cfg(feature = "rpc")]
    batches: Arc<tokio::sync::Mutex<BlobBatches>>,
    endpoint: Endpoint,
    gc_state: Arc<std::sync::Mutex<GcState>>,
    #[cfg(feature = "rpc")]
    pub(crate) rpc_handler: Arc<std::sync::OnceLock<crate::rpc::RpcHandler>>,
}

/// Keeps track of all the currently active batch operations of the blobs api.
#[cfg(feature = "rpc")]
#[derive(Debug, Default)]
pub(crate) struct BlobBatches {
    /// Currently active batches
    batches: std::collections::BTreeMap<BatchId, BlobBatch>,
    /// Used to generate new batch ids.
    max: u64,
}

/// A single batch of blob operations
#[cfg(feature = "rpc")]
#[derive(Debug, Default)]
struct BlobBatch {
    /// The tags in this batch.
    tags: std::collections::BTreeMap<HashAndFormat, Vec<crate::TempTag>>,
}

#[cfg(feature = "rpc")]
impl BlobBatches {
    /// Create a new unique batch id.
    pub fn create(&mut self) -> BatchId {
        let id = self.max;
        self.max += 1;
        BatchId(id)
    }

    /// Store a temp tag in a batch identified by a batch id.
    pub fn store(&mut self, batch: BatchId, tt: crate::TempTag) {
        let entry = self.batches.entry(batch).or_default();
        entry.tags.entry(tt.hash_and_format()).or_default().push(tt);
    }

    /// Remove a tag from a batch.
    pub fn remove_one(&mut self, batch: BatchId, content: &HashAndFormat) -> Result<()> {
        if let Some(batch) = self.batches.get_mut(&batch) {
            if let Some(tags) = batch.tags.get_mut(content) {
                tags.pop();
                if tags.is_empty() {
                    batch.tags.remove(content);
                }
                return Ok(());
            }
        }
        // this can happen if we try to upgrade a tag from an expired batch
        anyhow::bail!("tag not found in batch");
    }

    /// Remove an entire batch.
    pub fn remove(&mut self, batch: BatchId) {
        self.batches.remove(&batch);
    }
}

/// Builder for the Blobs protocol handler
#[derive(Debug)]
pub struct Builder<S> {
    store: S,
    events: Option<EventSender>,
}

impl<S: crate::store::Store> Builder<S> {
    /// Set the event sender for the blobs protocol.
    pub fn events(mut self, value: EventSender) -> Self {
        self.events = Some(value);
        self
    }

    /// Build the Blobs protocol handler.
    /// You need to provide a local pool handle and an endpoint.
    pub fn build(self, rt: &LocalPoolHandle, endpoint: &Endpoint) -> Blobs<S> {
        let downloader = Downloader::new(self.store.clone(), endpoint.clone(), rt.clone());
        Blobs::new(
            self.store,
            rt.clone(),
            self.events.unwrap_or_default(),
            downloader,
            endpoint.clone(),
        )
    }
}

impl<S> Blobs<S> {
    /// Create a new Blobs protocol handler builder, given a store.
    pub fn builder(store: S) -> Builder<S> {
        Builder {
            store,
            events: None,
        }
    }
}

impl Blobs<crate::store::mem::Store> {
    /// Create a new memory-backed Blobs protocol handler.
    pub fn memory() -> Builder<crate::store::mem::Store> {
        Self::builder(crate::store::mem::Store::new())
    }
}

impl Blobs<crate::store::fs::Store> {
    /// Load a persistent Blobs protocol handler from a path.
    pub async fn persistent(
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Builder<crate::store::fs::Store>> {
        Ok(Self::builder(crate::store::fs::Store::load(path).await?))
    }
}

impl<S: crate::store::Store> Blobs<S> {
    pub fn new(
        store: S,
        rt: LocalPoolHandle,
        events: EventSender,
        downloader: Downloader,
        endpoint: Endpoint,
    ) -> Self {
        Self {
            rt,
            store,
            events,
            downloader,
            endpoint,
            #[cfg(feature = "rpc")]
            batches: Default::default(),
            gc_state: Default::default(),
            #[cfg(feature = "rpc")]
            rpc_handler: Default::default(),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn rt(&self) -> &LocalPoolHandle {
        &self.rt
    }

    pub fn downloader(&self) -> &Downloader {
        &self.downloader
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Add a callback that will be called before the garbage collector runs.
    ///
    /// This can only be called before the garbage collector has started, otherwise it will return an error.
    pub fn add_protected(&self, cb: ProtectCb) -> Result<()> {
        let mut state = self.gc_state.lock().unwrap();
        match &mut *state {
            GcState::Initial(cbs) => {
                cbs.push(cb);
            }
            GcState::Started(_) => {
                anyhow::bail!("cannot add protected blobs after gc has started");
            }
        }
        Ok(())
    }

    /// Start garbage collection with the given settings.
    pub fn start_gc(&self, config: GcConfig) -> Result<()> {
        let mut state = self.gc_state.lock().unwrap();
        let protected = match state.deref_mut() {
            GcState::Initial(items) => std::mem::take(items),
            GcState::Started(_) => bail!("gc already started"),
        };
        let protected = Arc::new(protected);
        let protected_cb = move || {
            let protected = protected.clone();
            async move {
                let mut set = BTreeSet::new();
                for cb in protected.iter() {
                    cb(&mut set).await;
                }
                set
            }
        };
        let store = self.store.clone();
        let run = self
            .rt
            .spawn(move || async move { store.gc_run(config, protected_cb).await });
        *state = GcState::Started(Some(run));
        Ok(())
    }

    #[cfg(feature = "rpc")]
    pub(crate) async fn batches(&self) -> tokio::sync::MutexGuard<'_, BlobBatches> {
        self.batches.lock().await
    }

    pub(crate) async fn download(
        &self,
        endpoint: Endpoint,
        req: BlobDownloadRequest,
        progress: AsyncChannelProgressSender<DownloadProgress>,
    ) -> Result<()> {
        let BlobDownloadRequest {
            hash,
            format,
            nodes,
            tag,
            mode,
        } = req;
        let hash_and_format = HashAndFormat { hash, format };
        let temp_tag = self.store.temp_tag(hash_and_format);
        let stats = match mode {
            DownloadMode::Queued => {
                self.download_queued(endpoint, hash_and_format, nodes, progress.clone())
                    .await?
            }
            DownloadMode::Direct => {
                self.download_direct_from_nodes(endpoint, hash_and_format, nodes, progress.clone())
                    .await?
            }
        };

        progress.send(DownloadProgress::AllDone(stats)).await.ok();
        match tag {
            SetTagOption::Named(tag) => {
                self.store.set_tag(tag, Some(hash_and_format)).await?;
            }
            SetTagOption::Auto => {
                self.store.create_tag(hash_and_format).await?;
            }
        }
        drop(temp_tag);

        Ok(())
    }

    async fn download_queued(
        &self,
        endpoint: Endpoint,
        hash_and_format: HashAndFormat,
        nodes: Vec<NodeAddr>,
        progress: AsyncChannelProgressSender<DownloadProgress>,
    ) -> Result<Stats> {
        /// Name used for logging when new node addresses are added from gossip.
        const BLOB_DOWNLOAD_SOURCE_NAME: &str = "blob_download";

        let mut node_ids = Vec::with_capacity(nodes.len());
        let mut any_added = false;
        for node in nodes {
            node_ids.push(node.node_id);
            if !node.info.is_empty() {
                endpoint.add_node_addr_with_source(node, BLOB_DOWNLOAD_SOURCE_NAME)?;
                any_added = true;
            }
        }
        let can_download = !node_ids.is_empty() && (any_added || endpoint.discovery().is_some());
        anyhow::ensure!(can_download, "no way to reach a node for download");
        let req = DownloadRequest::new(hash_and_format, node_ids).progress_sender(progress);
        let handle = self.downloader.queue(req).await;
        let stats = handle.await?;
        Ok(stats)
    }

    #[tracing::instrument("download_direct", skip_all, fields(hash=%hash_and_format.hash.fmt_short()))]
    async fn download_direct_from_nodes(
        &self,
        endpoint: Endpoint,
        hash_and_format: HashAndFormat,
        nodes: Vec<NodeAddr>,
        progress: AsyncChannelProgressSender<DownloadProgress>,
    ) -> Result<Stats> {
        let mut last_err = None;
        let mut remaining_nodes = nodes.len();
        let mut nodes_iter = nodes.into_iter();
        'outer: loop {
            match crate::get::db::get_to_db_in_steps(
                self.store.clone(),
                hash_and_format,
                progress.clone(),
            )
            .await?
            {
                GetState::Complete(stats) => return Ok(stats),
                GetState::NeedsConn(needs_conn) => {
                    let (conn, node_id) = 'inner: loop {
                        match nodes_iter.next() {
                            None => break 'outer,
                            Some(node) => {
                                remaining_nodes -= 1;
                                let node_id = node.node_id;
                                if node_id == endpoint.node_id() {
                                    debug!(
                                        ?remaining_nodes,
                                        "skip node {} (it is the node id of ourselves)",
                                        node_id.fmt_short()
                                    );
                                    continue 'inner;
                                }
                                match endpoint.connect(node, crate::protocol::ALPN).await {
                                    Ok(conn) => break 'inner (conn, node_id),
                                    Err(err) => {
                                        debug!(
                                            ?remaining_nodes,
                                            "failed to connect to {}: {err}",
                                            node_id.fmt_short()
                                        );
                                        continue 'inner;
                                    }
                                }
                            }
                        }
                    };
                    match needs_conn.proceed(conn).await {
                        Ok(stats) => return Ok(stats),
                        Err(err) => {
                            warn!(
                                ?remaining_nodes,
                                "failed to download from {}: {err}",
                                node_id.fmt_short()
                            );
                            last_err = Some(err);
                        }
                    }
                }
            }
        }
        match last_err {
            Some(err) => Err(err.into()),
            None => Err(anyhow!("No nodes to download from provided")),
        }
    }
}

impl<S: crate::store::Store> ProtocolHandler for Blobs<S> {
    fn accept(&self, conn: Connecting) -> BoxedFuture<Result<()>> {
        let db = self.store.clone();
        let events = self.events.clone();
        let rt = self.rt.clone();

        Box::pin(async move {
            crate::provider::handle_connection(conn.await?, db, events, rt).await;
            Ok(())
        })
    }

    fn shutdown(&self) -> BoxedFuture<()> {
        let store = self.store.clone();
        Box::pin(async move {
            store.shutdown().await;
        })
    }
}

/// A request to the node to download and share the data specified by the hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobDownloadRequest {
    /// This mandatory field contains the hash of the data to download and share.
    pub hash: Hash,
    /// If the format is [`BlobFormat::HashSeq`], all children are downloaded and shared as
    /// well.
    pub format: BlobFormat,
    /// This mandatory field specifies the nodes to download the data from.
    ///
    /// If set to more than a single node, they will all be tried. If `mode` is set to
    /// [`DownloadMode::Direct`], they will be tried sequentially until a download succeeds.
    /// If `mode` is set to [`DownloadMode::Queued`], the nodes may be dialed in parallel,
    /// if the concurrency limits permit.
    pub nodes: Vec<NodeAddr>,
    /// Optional tag to tag the data with.
    pub tag: SetTagOption,
    /// Whether to directly start the download or add it to the download queue.
    pub mode: DownloadMode,
}

/// Set the mode for whether to directly start the download or add it to the download queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownloadMode {
    /// Start the download right away.
    ///
    /// No concurrency limits or queuing will be applied. It is up to the user to manage download
    /// concurrency.
    Direct,
    /// Queue the download.
    ///
    /// The download queue will be processed in-order, while respecting the downloader concurrency limits.
    Queued,
}

/// Newtype for a batch id
#[derive(Debug, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Ord, Clone, Copy, Hash)]
pub struct BatchId(pub u64);
