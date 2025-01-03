//! Adaptation of `iroh-blobs` as an `iroh` protocol.
//!
//! A blobs protocol handler wraps a store, so you must first create a store.
//!
//! The entry point to create a blobs protocol handler is [`Blobs::builder`].
use std::{collections::BTreeSet, fmt::Debug, ops::DerefMut, sync::Arc};

use anyhow::{bail, Result};
use futures_lite::future::Boxed as BoxedFuture;
use futures_util::future::BoxFuture;
use iroh::{endpoint::Connecting, protocol::ProtocolHandler, Endpoint};
use tracing::debug;

use crate::{
    downloader::Downloader,
    provider::EventSender,
    store::GcConfig,
    util::local_pool::{self, LocalPoolHandle},
    Hash,
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

#[derive(Debug)]
pub(crate) struct BlobsInner<S> {
    pub(crate) rt: LocalPoolHandle,
    pub(crate) store: S,
    events: EventSender,
    pub(crate) downloader: Downloader,
    pub(crate) endpoint: Endpoint,
    gc_state: std::sync::Mutex<GcState>,
    #[cfg(feature = "rpc")]
    pub(crate) batches: tokio::sync::Mutex<batches::BlobBatches>,
}

/// Blobs protocol handler.
#[derive(Debug, Clone)]
pub struct Blobs<S> {
    pub(crate) inner: Arc<BlobsInner<S>>,
    #[cfg(feature = "rpc")]
    pub(crate) rpc_handler: Arc<std::sync::OnceLock<crate::rpc::RpcHandler>>,
}

pub(crate) mod batches {
    use serde::{Deserialize, Serialize};

    /// Newtype for a batch id
    #[derive(Debug, PartialEq, Eq, PartialOrd, Serialize, Deserialize, Ord, Clone, Copy, Hash)]
    pub struct BatchId(pub u64);

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
        tags: std::collections::BTreeMap<crate::HashAndFormat, Vec<crate::TempTag>>,
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
        pub fn remove_one(
            &mut self,
            batch: BatchId,
            content: &crate::HashAndFormat,
        ) -> anyhow::Result<()> {
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
    /// Create a new Blobs protocol handler.
    ///
    /// This is the low-level constructor that allows you to customize
    /// everything. If you don't need that, consider using [`Blobs::builder`].
    pub fn new(
        store: S,
        rt: LocalPoolHandle,
        events: EventSender,
        downloader: Downloader,
        endpoint: Endpoint,
    ) -> Self {
        Self {
            inner: Arc::new(BlobsInner {
                rt,
                store,
                events,
                downloader,
                endpoint,
                #[cfg(feature = "rpc")]
                batches: Default::default(),
                gc_state: Default::default(),
            }),
            #[cfg(feature = "rpc")]
            rpc_handler: Default::default(),
        }
    }

    /// Get the store.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// Get the event sender.
    pub fn events(&self) -> &EventSender {
        &self.inner.events
    }

    /// Get the local pool handle.
    pub fn rt(&self) -> &LocalPoolHandle {
        &self.inner.rt
    }

    /// Get the downloader.
    pub fn downloader(&self) -> &Downloader {
        &self.inner.downloader
    }

    /// Get the endpoint.
    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    /// Add a callback that will be called before the garbage collector runs.
    ///
    /// This can only be called before the garbage collector has started, otherwise it will return an error.
    pub fn add_protected(&self, cb: ProtectCb) -> Result<()> {
        let mut state = self.inner.gc_state.lock().unwrap();
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
        let mut state = self.inner.gc_state.lock().unwrap();
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
        let store = self.store().clone();
        let run = self
            .rt()
            .spawn(move || async move { store.gc_run(config, protected_cb).await });
        *state = GcState::Started(Some(run));
        Ok(())
    }
}

impl<S: crate::store::Store> ProtocolHandler for Blobs<S> {
    fn accept(&self, conn: Connecting) -> BoxedFuture<Result<()>> {
        let db = self.store().clone();
        let events = self.events().clone();
        let rt = self.rt().clone();

        Box::pin(async move {
            crate::provider::handle_connection(conn.await?, db, events, rt).await;
            Ok(())
        })
    }

    fn shutdown(&self) -> BoxedFuture<()> {
        let store = self.store().clone();
        Box::pin(async move {
            store.shutdown().await;
        })
    }
}
