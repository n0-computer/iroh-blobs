//! Adaptation of `iroh-blobs` as an `iroh` protocol.

// TODO: reduce API surface and add documentation
#![allow(missing_docs)]

use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use anyhow::{anyhow, Result};
use futures_lite::future::Boxed as BoxedFuture;
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
    util::{
        local_pool::LocalPoolHandle,
        progress::{AsyncChannelProgressSender, ProgressSender},
        SetTagOption,
    },
    HashAndFormat, TempTag,
};

// pub type ProtectCb = Box<dyn Fn(&mut BTreeSet<Hash>) -> BoxFuture<()> + Send + Sync>;
//
// #[derive(derive_more::Debug)]
// enum GcState {
//     Initial(#[debug(skip)] Vec<ProtectCb>),
//     Started(#[allow(dead_code)] Option<local_pool::Run<()>>),
// }
//
// impl Default for GcState {
//     fn default() -> Self {
//         Self::Initial(Vec::new())
//     }
// }

#[derive(Debug)]
pub struct Blobs<S> {
    rt: LocalPoolHandle,
    store: S,
    events: EventSender,
    downloader: Downloader,
    batches: tokio::sync::Mutex<BlobBatches>,
    endpoint: Endpoint,
    #[cfg(feature = "rpc")]
    pub(crate) rpc_handler: Arc<OnceLock<crate::rpc::RpcHandler>>,
}

/// Name used for logging when new node addresses are added from gossip.
const BLOB_DOWNLOAD_SOURCE_NAME: &str = "blob_download";

/// Keeps track of all the currently active batch operations of the blobs api.
#[derive(Debug, Default)]
pub(crate) struct BlobBatches {
    /// Currently active batches
    batches: BTreeMap<BatchId, BlobBatch>,
    /// Used to generate new batch ids.
    max: u64,
}

/// A single batch of blob operations
#[derive(Debug, Default)]
struct BlobBatch {
    /// The tags in this batch.
    tags: BTreeMap<HashAndFormat, Vec<TempTag>>,
}

impl BlobBatches {
    /// Create a new unique batch id.
    pub fn create(&mut self) -> BatchId {
        let id = self.max;
        self.max += 1;
        BatchId(id)
    }

    /// Store a temp tag in a batch identified by a batch id.
    pub fn store(&mut self, batch: BatchId, tt: TempTag) {
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
    gc_config: Option<crate::store::GcConfig>,
}

impl<S: crate::store::Store> Builder<S> {
    /// Set the event sender for the blobs protocol.
    pub fn events(mut self, value: EventSender) -> Self {
        self.events = Some(value);
        self
    }

    pub fn gc_config(mut self, value: crate::store::GcConfig) -> Self {
        self.gc_config = Some(value);
        self
    }

    /// Build the Blobs protocol handler.
    /// You need to provide a local pool handle and an endpoint.
    pub fn build(self, rt: &LocalPoolHandle, endpoint: &Endpoint) -> Arc<Blobs<S>> {
        let downloader = Downloader::new(self.store.clone(), endpoint.clone(), rt.clone());
        Arc::new(Blobs::new(
            self.store,
            rt.clone(),
            self.events.unwrap_or_default(),
            downloader,
            endpoint.clone(),
        ))
    }
}

impl Blobs<crate::store::mem::Store> {
    /// Create a new memory-backed Blobs protocol handler.
    pub fn memory() -> Builder<crate::store::mem::Store> {
        Builder {
            store: crate::store::mem::Store::new(),
            events: None,
            gc_config: None,
        }
    }
}

impl Blobs<crate::store::fs::Store> {
    /// Load a persistent Blobs protocol handler from a path.
    pub async fn persistent(
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Builder<crate::store::fs::Store>> {
        Ok(Builder {
            store: crate::store::fs::Store::load(path).await?,
            events: None,
            gc_config: None,
        })
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
            batches: Default::default(),
            #[cfg(feature = "rpc")]
            rpc_handler: Arc::new(OnceLock::new()),
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

    // pub fn add_protected(&self, cb: ProtectCb) -> Result<()> {
    //     let mut state = self.gc_state.lock().unwrap();
    //     match &mut *state {
    //         GcState::Initial(cbs) => {
    //             cbs.push(cb);
    //         }
    //         GcState::Started(_) => {
    //             anyhow::bail!("cannot add protected blobs after gc has started");
    //         }
    //     }
    //     Ok(())
    // }
    //
    // pub fn start_gc(&self, config: GcConfig) -> Result<()> {
    //     let mut state = self.gc_state.lock().unwrap();
    //     let protected = match state.deref_mut() {
    //         GcState::Initial(items) => std::mem::take(items),
    //         GcState::Started(_) => anyhow::bail!("gc already started"),
    //     };
    //     let protected = Arc::new(protected);
    //     let protected_cb = move || {
    //         let protected = protected.clone();
    //         async move {
    //             let mut set = BTreeSet::new();
    //             for cb in protected.iter() {
    //                 cb(&mut set).await;
    //             }
    //             set
    //         }
    //     };
    //     let store = self.store.clone();
    //     let run = self
    //         .rt
    //         .spawn(move || async move { store.gc_run(config, protected_cb).await });
    //     *state = GcState::Started(Some(run));
    //     Ok(())
    // }

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

// trait BlobsInner: Debug + Send + Sync + 'static {
//     fn shutdown(self: Arc<Self>) -> BoxedFuture<()>;
//     fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>>;
//     fn client(self: Arc<Self>) -> MemClient;
//     fn local_pool_handle(&self) -> &LocalPoolHandle;
//     fn downloader(&self) -> &Downloader;
// }

// #[derive(Debug)]
// struct Blobs2 {
//     inner: Arc<dyn BlobsInner>,
// }

// impl Blobs2 {
//     fn client(&self) -> MemClient {
//         self.inner.clone().client()
//     }

//     fn local_pool_handle(&self) -> &LocalPoolHandle {
//         self.inner.local_pool_handle()
//     }

//     fn downloader(&self) -> &Downloader {
//         self.inner.downloader()
//     }
// }

// impl<S: crate::store::Store> BlobsInner for Blobs<S> {
//     fn shutdown(self: Arc<Self>) -> BoxedFuture<()> {
//         ProtocolHandler::shutdown(self)
//     }

//     fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>> {
//         ProtocolHandler::accept(self, conn)
//     }

//     fn client(self: Arc<Self>) -> MemClient {
//         Blobs::client(self)
//     }

//     fn local_pool_handle(&self) -> &LocalPoolHandle {
//         self.rt()
//     }

//     fn downloader(&self) -> &Downloader {
//         self.downloader()
//     }
// }

// impl ProtocolHandler for Blobs2 {
//     fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>> {
//         self.inner.clone().accept(conn)
//     }

//     fn shutdown(self: Arc<Self>) -> BoxedFuture<()> {
//         self.inner.clone().shutdown()
//     }
// }

impl<S: crate::store::Store> ProtocolHandler for Blobs<S> {
    fn accept(self: Arc<Self>, conn: Connecting) -> BoxedFuture<Result<()>> {
        Box::pin(async move {
            crate::provider::handle_connection(
                conn.await?,
                self.store.clone(),
                self.events.clone(),
                self.rt.clone(),
            )
            .await;
            Ok(())
        })
    }

    fn shutdown(self: Arc<Self>) -> BoxedFuture<()> {
        Box::pin(async move {
            self.store.shutdown().await;
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
