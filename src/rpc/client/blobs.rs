//! API for blobs management.
//!
//! The main entry point is the [`Client`].
//!
//! ## Interacting with the local blob store
//!
//! ### Importing data
//!
//! There are several ways to import data into the local blob store:
//!
//! - [`add_bytes`](Client::add_bytes)
//!   imports in memory data.
//! - [`add_stream`](Client::add_stream)
//!   imports data from a stream of bytes.
//! - [`add_reader`](Client::add_reader)
//!   imports data from an [async reader](tokio::io::AsyncRead).
//! - [`add_from_path`](Client::add_from_path)
//!   imports data from a file.
//!
//! The last method imports data from a file on the local filesystem.
//! This is the most efficient way to import large amounts of data.
//!
//! ### Exporting data
//!
//! There are several ways to export data from the local blob store:
//!
//! - [`read_to_bytes`](Client::read_to_bytes) reads data into memory.
//! - [`read`](Client::read) creates a [reader](Reader) to read data from.
//! - [`export`](Client::export) eports data to a file on the local filesystem.
//!
//! ## Interacting with remote nodes
//!
//! - [`download`](Client::download) downloads data from a remote node.
//!   remote node.
//!
//! ## Interacting with the blob store itself
//!
//! These are more advanced operations that are usually not needed in normal
//! operation.
//!
//! - [`consistency_check`](Client::consistency_check) checks the internal
//!   consistency of the local blob store.
//! - [`validate`](Client::validate) validates the locally stored data against
//!   their BLAKE3 hashes.
//! - [`delete_blob`](Client::delete_blob) deletes a blob from the local store.
//!
//! ### Batch operations
//!
//! For complex update operations, there is a [`batch`](Client::batch) API that
//! allows you to add multiple blobs in a single logical batch.
//!
//! Operations in a batch return [temporary tags](crate::util::TempTag) that
//! protect the added data from garbage collection as long as the batch is
//! alive.
//!
//! To store the data permanently, a temp tag needs to be upgraded to a
//! permanent tag using [`persist`](crate::rpc::client::blobs::Batch::persist) or
//! [`persist_to`](crate::rpc::client::blobs::Batch::persist_to).
use std::{
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{anyhow, Context as _, Result};
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
use futures_util::SinkExt;
use genawaiter::sync::{Co, Gen};
use iroh::NodeAddr;
use portable_atomic::{AtomicU64, Ordering};
use quic_rpc::{
    client::{BoxStreamSync, BoxedConnector},
    Connector, RpcClient,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::warn;

pub use crate::net_protocol::DownloadMode;
use crate::{
    fetch::progress::DownloadProgress as BytesDownloadProgress,
    format::collection::{Collection, SimpleStore},
    net_protocol::BlobDownloadRequest,
    rpc::proto::RpcService,
    store::{
        BaoBlobSize, ConsistencyCheckProgress, ExportFormat, ExportMode,
        ExportProgress as BytesExportProgress, ValidateProgress,
    },
    util::SetTagOption,
    BlobFormat, Hash, Tag,
};

mod batch;
pub use batch::{AddDirOpts, AddFileOpts, AddReaderOpts, Batch};

use super::{flatten, tags};
use crate::rpc::proto::blobs::{
    AddPathRequest, AddStreamRequest, AddStreamUpdate, BatchCreateRequest, BatchCreateResponse,
    BlobStatusRequest, ConsistencyCheckRequest, CreateCollectionRequest, CreateCollectionResponse,
    DeleteRequest, ExportRequest, ListIncompleteRequest, ListRequest, ReadAtRequest,
    ReadAtResponse, ValidateRequest,
};

/// Iroh blobs client.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client<C = BoxedConnector<RpcService>> {
    pub(super) rpc: RpcClient<RpcService, C>,
}

/// Type alias for a memory-backed client.
pub type MemClient = Client<crate::rpc::MemConnector>;

impl<C> Client<C>
where
    C: Connector<RpcService>,
{
    /// Create a new client
    pub fn new(rpc: RpcClient<RpcService, C>) -> Self {
        Self { rpc }
    }

    /// Get a tags client.
    pub fn tags(&self) -> tags::Client<C> {
        tags::Client::new(self.rpc.clone())
    }

    /// Check if a blob is completely stored on the node.
    ///
    /// Note that this will return false for blobs that are partially stored on
    /// the node.
    pub async fn status(&self, hash: Hash) -> Result<BlobStatus> {
        let status = self.rpc.rpc(BlobStatusRequest { hash }).await??;
        Ok(status.0)
    }

    /// Check if a blob is completely stored on the node.
    ///
    /// This is just a convenience wrapper around `status` that returns a boolean.
    pub async fn has(&self, hash: Hash) -> Result<bool> {
        match self.status(hash).await {
            Ok(BlobStatus::Complete { .. }) => Ok(true),
            Ok(_) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Create a new batch for adding data.
    ///
    /// A batch is a context in which temp tags are created and data is added to the node. Temp tags
    /// are automatically deleted when the batch is dropped, leading to the data being garbage collected
    /// unless a permanent tag is created for it.
    pub async fn batch(&self) -> Result<Batch<C>> {
        let (updates, mut stream) = self.rpc.bidi(BatchCreateRequest).await?;
        let BatchCreateResponse::Id(batch) = stream.next().await.context("expected scope id")??;
        let rpc = self.rpc.clone();
        Ok(Batch::new(batch, rpc, updates, 1024))
    }

    /// Stream the contents of a a single blob.
    ///
    /// Returns a [`Reader`], which can report the size of the blob before reading it.
    pub async fn read(&self, hash: Hash) -> Result<Reader> {
        Reader::from_rpc_read(&self.rpc, hash).await
    }

    /// Read offset + len from a single blob.
    ///
    /// If `len` is `None` it will read the full blob.
    pub async fn read_at(&self, hash: Hash, offset: u64, len: ReadAtLen) -> Result<Reader> {
        Reader::from_rpc_read_at(&self.rpc, hash, offset, len).await
    }

    /// Read all bytes of single blob.
    ///
    /// This allocates a buffer for the full blob. Use only if you know that the blob you're
    /// reading is small. If not sure, use [`Self::read`] and check the size with
    /// [`Reader::size`] before calling [`Reader::read_to_bytes`].
    pub async fn read_to_bytes(&self, hash: Hash) -> Result<Bytes> {
        Reader::from_rpc_read(&self.rpc, hash)
            .await?
            .read_to_bytes()
            .await
    }

    /// Read all bytes of single blob at `offset` for length `len`.
    ///
    /// This allocates a buffer for the full length.
    pub async fn read_at_to_bytes(&self, hash: Hash, offset: u64, len: ReadAtLen) -> Result<Bytes> {
        Reader::from_rpc_read_at(&self.rpc, hash, offset, len)
            .await?
            .read_to_bytes()
            .await
    }

    /// Import a blob from a filesystem path.
    ///
    /// `path` should be an absolute path valid for the file system on which
    /// the node runs.
    /// If `in_place` is true, Iroh will assume that the data will not change and will share it in
    /// place without copying to the Iroh data directory.
    pub async fn add_from_path(
        &self,
        path: PathBuf,
        in_place: bool,
        tag: SetTagOption,
        wrap: WrapOption,
    ) -> Result<AddProgress> {
        let stream = self
            .rpc
            .server_streaming(AddPathRequest {
                path,
                in_place,
                tag,
                wrap,
            })
            .await?;
        Ok(AddProgress::new(stream))
    }

    /// Create a collection from already existing blobs.
    ///
    /// For automatically clearing the tags for the passed in blobs you can set
    /// `tags_to_delete` to those tags, and they will be deleted once the collection is created.
    pub async fn create_collection(
        &self,
        collection: Collection,
        tag: SetTagOption,
        tags_to_delete: Vec<Tag>,
    ) -> anyhow::Result<(Hash, Tag)> {
        let CreateCollectionResponse { hash, tag } = self
            .rpc
            .rpc(CreateCollectionRequest {
                collection,
                tag,
                tags_to_delete,
            })
            .await??;
        Ok((hash, tag))
    }

    /// Write a blob by passing an async reader.
    pub async fn add_reader(
        &self,
        reader: impl AsyncRead + Unpin + Send + 'static,
        tag: SetTagOption,
    ) -> anyhow::Result<AddProgress> {
        const CAP: usize = 1024 * 64; // send 64KB per request by default
        let input = ReaderStream::with_capacity(reader, CAP);
        self.add_stream(input, tag).await
    }

    /// Write a blob by passing a stream of bytes.
    pub async fn add_stream(
        &self,
        input: impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static,
        tag: SetTagOption,
    ) -> anyhow::Result<AddProgress> {
        let (mut sink, progress) = self.rpc.bidi(AddStreamRequest { tag }).await?;
        let mut input = input.map(|chunk| match chunk {
            Ok(chunk) => Ok(AddStreamUpdate::Chunk(chunk)),
            Err(err) => {
                warn!("Abort send, reason: failed to read from source stream: {err:?}");
                Ok(AddStreamUpdate::Abort)
            }
        });
        tokio::spawn(async move {
            if let Err(err) = sink.send_all(&mut input).await {
                // if we get an error in send_all due to the connection being closed, this will just fail again.
                // if we get an error due to something else (serialization or size limit), tell the remote to abort.
                sink.send(AddStreamUpdate::Abort).await.ok();
                warn!("Failed to send input stream to remote: {err:?}");
            }
        });

        Ok(AddProgress::new(progress))
    }

    /// Write a blob by passing bytes.
    pub async fn add_bytes(&self, bytes: impl Into<Bytes>) -> anyhow::Result<AddOutcome> {
        let input = chunked_bytes_stream(bytes.into(), 1024 * 64).map(Ok);
        self.add_stream(input, SetTagOption::Auto).await?.await
    }

    /// Write a blob by passing bytes, setting an explicit tag name.
    pub async fn add_bytes_named(
        &self,
        bytes: impl Into<Bytes>,
        name: impl Into<Tag>,
    ) -> anyhow::Result<AddOutcome> {
        let input = chunked_bytes_stream(bytes.into(), 1024 * 64).map(Ok);
        self.add_stream(input, SetTagOption::Named(name.into()))
            .await?
            .await
    }

    /// Validate hashes on the running node.
    ///
    /// If `repair` is true, repair the store by removing invalid data.
    pub async fn validate(
        &self,
        repair: bool,
    ) -> Result<impl Stream<Item = Result<ValidateProgress>>> {
        let stream = self
            .rpc
            .server_streaming(ValidateRequest { repair })
            .await?;
        Ok(stream.map(|res| res.map_err(anyhow::Error::from)))
    }

    /// Validate hashes on the running node.
    ///
    /// If `repair` is true, repair the store by removing invalid data.
    pub async fn consistency_check(
        &self,
        repair: bool,
    ) -> Result<impl Stream<Item = Result<ConsistencyCheckProgress>>> {
        let stream = self
            .rpc
            .server_streaming(ConsistencyCheckRequest { repair })
            .await?;
        Ok(stream.map(|r| r.map_err(anyhow::Error::from)))
    }

    /// Download a blob from another node and add it to the local database.
    pub async fn download(&self, hash: Hash, node: NodeAddr) -> Result<DownloadProgress> {
        self.download_with_opts(
            hash,
            DownloadOptions {
                format: BlobFormat::Raw,
                nodes: vec![node],
                tag: SetTagOption::Auto,
                mode: DownloadMode::Queued,
            },
        )
        .await
    }

    /// Download a hash sequence from another node and add it to the local database.
    pub async fn download_hash_seq(&self, hash: Hash, node: NodeAddr) -> Result<DownloadProgress> {
        self.download_with_opts(
            hash,
            DownloadOptions {
                format: BlobFormat::HashSeq,
                nodes: vec![node],
                tag: SetTagOption::Auto,
                mode: DownloadMode::Queued,
            },
        )
        .await
    }

    /// Download a blob, with additional options.
    pub async fn download_with_opts(
        &self,
        hash: Hash,
        opts: DownloadOptions,
    ) -> Result<DownloadProgress> {
        let DownloadOptions {
            format,
            nodes,
            tag,
            mode,
        } = opts;
        let stream = self
            .rpc
            .server_streaming(BlobDownloadRequest {
                hash,
                format,
                nodes,
                tag,
                mode,
            })
            .await?;
        Ok(DownloadProgress::new(
            stream.map(|res| res.map_err(anyhow::Error::from)),
        ))
    }

    /// Export a blob from the internal blob store to a path on the node's filesystem.
    ///
    /// `destination` should be an writeable, absolute path on the local node's filesystem.
    ///
    /// If `format` is set to [`ExportFormat::Collection`], and the `hash` refers to a collection,
    /// all children of the collection will be exported. See [`ExportFormat`] for details.
    ///
    /// The `mode` argument defines if the blob should be copied to the target location or moved out of
    /// the internal store into the target location. See [`ExportMode`] for details.
    pub async fn export(
        &self,
        hash: Hash,
        destination: PathBuf,
        format: ExportFormat,
        mode: ExportMode,
    ) -> Result<ExportProgress> {
        let req = ExportRequest {
            hash,
            path: destination,
            format,
            mode,
        };
        let stream = self.rpc.server_streaming(req).await?;
        Ok(ExportProgress::new(
            stream.map(|r| r.map_err(anyhow::Error::from)),
        ))
    }

    /// List all complete blobs.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<BlobInfo>>> {
        let stream = self.rpc.server_streaming(ListRequest).await?;
        Ok(flatten(stream))
    }

    /// List all incomplete (partial) blobs.
    pub async fn list_incomplete(&self) -> Result<impl Stream<Item = Result<IncompleteBlobInfo>>> {
        let stream = self.rpc.server_streaming(ListIncompleteRequest).await?;
        Ok(flatten(stream))
    }

    /// Read the content of a collection.
    pub async fn get_collection(&self, hash: Hash) -> Result<Collection> {
        Collection::load(hash, self).await
    }

    /// List all collections.
    pub fn list_collections(&self) -> Result<impl Stream<Item = Result<CollectionInfo>>> {
        let this = self.clone();
        Ok(Gen::new(|co| async move {
            if let Err(cause) = this.list_collections_impl(&co).await {
                co.yield_(Err(cause)).await;
            }
        }))
    }

    async fn list_collections_impl(&self, co: &Co<Result<CollectionInfo>>) -> Result<()> {
        let tags = self.tags_client();
        let mut tags = tags.list_hash_seq().await?;
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            if let Ok(collection) = self.get_collection(tag.hash).await {
                let info = CollectionInfo {
                    tag: tag.name,
                    hash: tag.hash,
                    total_blobs_count: Some(collection.len() as u64 + 1),
                    total_blobs_size: Some(0),
                };
                co.yield_(Ok(info)).await;
            }
        }
        Ok(())
    }

    /// Delete a blob.
    ///
    /// **Warning**: this operation deletes the blob from the local store even
    /// if it is tagged. You should usually not do this manually, but rely on the
    /// node to remove data that is not tagged.
    pub async fn delete_blob(&self, hash: Hash) -> Result<()> {
        self.rpc.rpc(DeleteRequest { hash }).await??;
        Ok(())
    }

    fn tags_client(&self) -> tags::Client<C> {
        tags::Client::new(self.rpc.clone())
    }
}

impl<C> SimpleStore for Client<C>
where
    C: Connector<RpcService>,
{
    async fn load(&self, hash: Hash) -> anyhow::Result<Bytes> {
        self.read_to_bytes(hash).await
    }
}

/// Defines the way to read bytes.
#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy)]
pub enum ReadAtLen {
    /// Reads all available bytes.
    #[default]
    All,
    /// Reads exactly this many bytes, erroring out on larger or smaller.
    Exact(u64),
    /// Reads at most this many bytes.
    AtMost(u64),
}

impl ReadAtLen {
    /// todo make private again
    pub fn as_result_len(&self, size_remaining: u64) -> u64 {
        match self {
            ReadAtLen::All => size_remaining,
            ReadAtLen::Exact(len) => *len,
            ReadAtLen::AtMost(len) => std::cmp::min(*len, size_remaining),
        }
    }
}

/// Whether to wrap the added data in a collection.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub enum WrapOption {
    /// Do not wrap the file or directory.
    #[default]
    NoWrap,
    /// Wrap the file or directory in a collection.
    Wrap {
        /// Override the filename in the wrapping collection.
        name: Option<String>,
    },
}

/// Status information about a blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobStatus {
    /// The blob is not stored at all.
    NotFound,
    /// The blob is only stored partially.
    Partial {
        /// The size of the currently stored partial blob.
        size: BaoBlobSize,
    },
    /// The blob is stored completely.
    Complete {
        /// The size of the blob.
        size: u64,
    },
}

/// Outcome of a blob add operation.
#[derive(Debug, Clone)]
pub struct AddOutcome {
    /// The hash of the blob
    pub hash: Hash,
    /// The format the blob
    pub format: BlobFormat,
    /// The size of the blob
    pub size: u64,
    /// The tag of the blob
    pub tag: Tag,
}

/// Information about a stored collection.
#[derive(Debug, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Tag of the collection
    pub tag: Tag,

    /// Hash of the collection
    pub hash: Hash,
    /// Number of children in the collection
    ///
    /// This is an optional field, because the data is not always available.
    pub total_blobs_count: Option<u64>,
    /// Total size of the raw data referred to by all links
    ///
    /// This is an optional field, because the data is not always available.
    pub total_blobs_size: Option<u64>,
}

/// Information about a complete blob.
#[derive(Debug, Serialize, Deserialize)]
pub struct BlobInfo {
    /// Location of the blob
    pub path: String,
    /// The hash of the blob
    pub hash: Hash,
    /// The size of the blob
    pub size: u64,
}

/// Information about an incomplete blob.
#[derive(Debug, Serialize, Deserialize)]
pub struct IncompleteBlobInfo {
    /// The size we got
    pub size: u64,
    /// The size we expect
    pub expected_size: u64,
    /// The hash of the blob
    pub hash: Hash,
}

/// Progress stream for blob add operations.
#[derive(derive_more::Debug)]
pub struct AddProgress {
    #[debug(skip)]
    stream:
        Pin<Box<dyn Stream<Item = Result<crate::provider::AddProgress>> + Send + Unpin + 'static>>,
    current_total_size: Arc<AtomicU64>,
}

impl AddProgress {
    fn new(
        stream: (impl Stream<
            Item = Result<impl Into<crate::provider::AddProgress>, impl Into<anyhow::Error>>,
        > + Send
             + Unpin
             + 'static),
    ) -> Self {
        let current_total_size = Arc::new(AtomicU64::new(0));
        let total_size = current_total_size.clone();
        let stream = stream.map(move |item| match item {
            Ok(item) => {
                let item = item.into();
                if let crate::provider::AddProgress::Found { size, .. } = &item {
                    total_size.fetch_add(*size, Ordering::Relaxed);
                }
                Ok(item)
            }
            Err(err) => Err(err.into()),
        });
        Self {
            stream: Box::pin(stream),
            current_total_size,
        }
    }
    /// Finish writing the stream, ignoring all intermediate progress events.
    ///
    /// Returns a [`AddOutcome`] which contains a tag, format, hash and a size.
    /// When importing a single blob, this is the hash and size of that blob.
    /// When importing a collection, the hash is the hash of the collection and the size
    /// is the total size of all imported blobs (but excluding the size of the collection blob
    /// itself).
    pub async fn finish(self) -> Result<AddOutcome> {
        self.await
    }
}

impl Stream for AddProgress {
    type Item = Result<crate::provider::AddProgress>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl Future for AddProgress {
    type Output = Result<AddOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(anyhow!("Response stream ended prematurely")))
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    crate::provider::AddProgress::AllDone { hash, format, tag } => {
                        let outcome = AddOutcome {
                            hash,
                            format,
                            tag,
                            size: self.current_total_size.load(Ordering::Relaxed),
                        };
                        return Poll::Ready(Ok(outcome));
                    }
                    crate::provider::AddProgress::Abort(err) => {
                        return Poll::Ready(Err(err.into()));
                    }
                    _ => {}
                },
            }
        }
    }
}

/// Outcome of a blob download operation.
#[derive(Debug, Clone)]
pub struct DownloadOutcome {
    /// The size of the data we already had locally
    pub local_size: u64,
    /// The size of the data we downloaded from the network
    pub downloaded_size: u64,
    /// Statistics about the download
    pub stats: crate::fetch::Stats,
}

/// Progress stream for blob download operations.
#[derive(derive_more::Debug)]
pub struct DownloadProgress {
    #[debug(skip)]
    stream: Pin<Box<dyn Stream<Item = Result<BytesDownloadProgress>> + Send + Unpin + 'static>>,
    current_local_size: Arc<AtomicU64>,
    current_network_size: Arc<AtomicU64>,
}

impl DownloadProgress {
    /// Create a [`DownloadProgress`] that can help you easily poll the [`BytesDownloadProgress`] stream from your download until it is finished or errors.
    pub fn new(
        stream: (impl Stream<Item = Result<impl Into<BytesDownloadProgress>, impl Into<anyhow::Error>>>
             + Send
             + Unpin
             + 'static),
    ) -> Self {
        let current_local_size = Arc::new(AtomicU64::new(0));
        let current_network_size = Arc::new(AtomicU64::new(0));

        let local_size = current_local_size.clone();
        let network_size = current_network_size.clone();

        let stream = stream.map(move |item| match item {
            Ok(item) => {
                let item = item.into();
                match &item {
                    BytesDownloadProgress::FoundLocal { size, .. } => {
                        local_size.fetch_add(size.value(), Ordering::Relaxed);
                    }
                    BytesDownloadProgress::Found { size, .. } => {
                        network_size.fetch_add(*size, Ordering::Relaxed);
                    }
                    _ => {}
                }

                Ok(item)
            }
            Err(err) => Err(err.into()),
        });
        Self {
            stream: Box::pin(stream),
            current_local_size,
            current_network_size,
        }
    }

    /// Finish writing the stream, ignoring all intermediate progress events.
    ///
    /// Returns a [`DownloadOutcome`] which contains the size of the content we downloaded and the size of the content we already had locally.
    /// When importing a single blob, this is the size of that blob.
    /// When importing a collection, this is the total size of all imported blobs (but excluding the size of the collection blob itself).
    pub async fn finish(self) -> Result<DownloadOutcome> {
        self.await
    }
}

impl Stream for DownloadProgress {
    type Item = Result<BytesDownloadProgress>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl Future for DownloadProgress {
    type Output = Result<DownloadOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(anyhow!("Response stream ended prematurely")))
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    BytesDownloadProgress::AllDone(stats) => {
                        let outcome = DownloadOutcome {
                            local_size: self.current_local_size.load(Ordering::Relaxed),
                            downloaded_size: self.current_network_size.load(Ordering::Relaxed),
                            stats,
                        };
                        return Poll::Ready(Ok(outcome));
                    }
                    BytesDownloadProgress::Abort(err) => {
                        return Poll::Ready(Err(err.into()));
                    }
                    _ => {}
                },
            }
        }
    }
}

/// Outcome of a blob export operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportOutcome {
    /// The total size of the exported data.
    total_size: u64,
}

/// Progress stream for blob export operations.
#[derive(derive_more::Debug)]
pub struct ExportProgress {
    #[debug(skip)]
    stream: Pin<Box<dyn Stream<Item = Result<BytesExportProgress>> + Send + Unpin + 'static>>,
    current_total_size: Arc<AtomicU64>,
}

impl ExportProgress {
    /// Create a [`ExportProgress`] that can help you easily poll the [`BytesExportProgress`] stream from your
    /// download until it is finished or errors.
    pub fn new(
        stream: (impl Stream<Item = Result<impl Into<BytesExportProgress>, impl Into<anyhow::Error>>>
             + Send
             + Unpin
             + 'static),
    ) -> Self {
        let current_total_size = Arc::new(AtomicU64::new(0));
        let total_size = current_total_size.clone();
        let stream = stream.map(move |item| match item {
            Ok(item) => {
                let item = item.into();
                if let BytesExportProgress::Found { size, .. } = &item {
                    let size = size.value();
                    total_size.fetch_add(size, Ordering::Relaxed);
                }

                Ok(item)
            }
            Err(err) => Err(err.into()),
        });
        Self {
            stream: Box::pin(stream),
            current_total_size,
        }
    }

    /// Finish writing the stream, ignoring all intermediate progress events.
    ///
    /// Returns a [`ExportOutcome`] which contains the size of the content we exported.
    pub async fn finish(self) -> Result<ExportOutcome> {
        self.await
    }
}

impl Stream for ExportProgress {
    type Item = Result<BytesExportProgress>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl Future for ExportProgress {
    type Output = Result<ExportOutcome>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    return Poll::Ready(Err(anyhow!("Response stream ended prematurely")))
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    BytesExportProgress::AllDone => {
                        let outcome = ExportOutcome {
                            total_size: self.current_total_size.load(Ordering::Relaxed),
                        };
                        return Poll::Ready(Ok(outcome));
                    }
                    BytesExportProgress::Abort(err) => {
                        return Poll::Ready(Err(err.into()));
                    }
                    _ => {}
                },
            }
        }
    }
}

/// Data reader for a single blob.
///
/// Implements [`AsyncRead`].
#[derive(derive_more::Debug)]
pub struct Reader {
    size: u64,
    response_size: u64,
    is_complete: bool,
    #[debug("StreamReader")]
    stream: tokio_util::io::StreamReader<BoxStreamSync<'static, io::Result<Bytes>>, Bytes>,
}

impl Reader {
    fn new(
        size: u64,
        response_size: u64,
        is_complete: bool,
        stream: BoxStreamSync<'static, io::Result<Bytes>>,
    ) -> Self {
        Self {
            size,
            response_size,
            is_complete,
            stream: StreamReader::new(stream),
        }
    }

    /// todo make private again
    pub async fn from_rpc_read<C>(
        rpc: &RpcClient<RpcService, C>,
        hash: Hash,
    ) -> anyhow::Result<Self>
    where
        C: Connector<RpcService>,
    {
        Self::from_rpc_read_at(rpc, hash, 0, ReadAtLen::All).await
    }

    async fn from_rpc_read_at<C>(
        rpc: &RpcClient<RpcService, C>,
        hash: Hash,
        offset: u64,
        len: ReadAtLen,
    ) -> anyhow::Result<Self>
    where
        C: Connector<RpcService>,
    {
        let stream = rpc
            .server_streaming(ReadAtRequest { hash, offset, len })
            .await?;
        let mut stream = flatten(stream);

        let (size, is_complete) = match stream.next().await {
            Some(Ok(ReadAtResponse::Entry { size, is_complete })) => (size, is_complete),
            Some(Err(err)) => return Err(err),
            Some(Ok(_)) => return Err(anyhow!("Expected header frame, but got data frame")),
            None => return Err(anyhow!("Expected header frame, but RPC stream was dropped")),
        };

        let stream = stream.map(|item| match item {
            Ok(ReadAtResponse::Data { chunk }) => Ok(chunk),
            Ok(_) => Err(io::Error::new(io::ErrorKind::Other, "Expected data frame")),
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, format!("{err}"))),
        });
        let len = len.as_result_len(size.value() - offset);
        Ok(Self::new(size.value(), len, is_complete, Box::pin(stream)))
    }

    /// Total size of this blob.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Whether this blob has been downloaded completely.
    ///
    /// Returns false for partial blobs for which some chunks are missing.
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Read all bytes of the blob.
    pub async fn read_to_bytes(&mut self) -> anyhow::Result<Bytes> {
        let mut buf = Vec::with_capacity(self.response_size as usize);
        self.read_to_end(&mut buf).await?;
        Ok(buf.into())
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl Stream for Reader {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).get_pin_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.get_ref().size_hint()
    }
}

/// Options to configure a download request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadOptions {
    /// The format of the data to download.
    pub format: BlobFormat,
    /// Source nodes to download from.
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

fn chunked_bytes_stream(mut b: Bytes, c: usize) -> impl Stream<Item = Bytes> {
    futures_lite::stream::iter(std::iter::from_fn(move || {
        Some(b.split_to(b.len().min(c))).filter(|x| !x.is_empty())
    }))
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration};

    use iroh::{test_utils::DnsPkarrServer, NodeId, RelayMode, SecretKey};
    use node::Node;
    use rand::RngCore;
    use testresult::TestResult;
    use tokio::{io::AsyncWriteExt, sync::mpsc};

    use super::*;
    use crate::{hashseq::HashSeq, ticket::BlobTicket};

    mod node {
        //! An iroh node that just has the blobs transport
        use std::path::Path;

        use iroh::{protocol::Router, Endpoint, NodeAddr, NodeId};
        use tokio_util::task::AbortOnDropHandle;

        use super::RpcService;
        use crate::{
            downloader::Downloader,
            net_protocol::Blobs,
            provider::{CustomEventSender, EventSender},
            rpc::client::{blobs, tags},
            util::local_pool::LocalPool,
        };

        type RpcClient = quic_rpc::RpcClient<RpcService>;

        /// An iroh node that just has the blobs transport
        #[derive(Debug)]
        pub struct Node {
            router: iroh::protocol::Router,
            client: RpcClient,
            _local_pool: LocalPool,
            _rpc_task: AbortOnDropHandle<()>,
        }

        /// An iroh node builder
        #[derive(Debug)]
        pub struct Builder<S> {
            store: S,
            events: EventSender,
            endpoint: Option<iroh::endpoint::Builder>,
        }

        impl<S: crate::store::Store> Builder<S> {
            /// Sets the event sender
            pub fn blobs_events(self, events: impl CustomEventSender) -> Self {
                Self {
                    events: events.into(),
                    ..self
                }
            }

            /// Set an endpoint builder
            pub fn endpoint(self, endpoint: iroh::endpoint::Builder) -> Self {
                Self {
                    endpoint: Some(endpoint),
                    ..self
                }
            }

            /// Spawns the node
            pub async fn spawn(self) -> anyhow::Result<Node> {
                let store = self.store;
                let events = self.events;
                let endpoint = self
                    .endpoint
                    .unwrap_or_else(|| Endpoint::builder().discovery_n0())
                    .bind()
                    .await?;
                let local_pool = LocalPool::single();
                let mut router = Router::builder(endpoint.clone());

                // Setup blobs
                let downloader =
                    Downloader::new(store.clone(), endpoint.clone(), local_pool.handle().clone());
                let blobs = Blobs::new(
                    store.clone(),
                    local_pool.handle().clone(),
                    events,
                    downloader,
                    endpoint.clone(),
                );
                router = router.accept(crate::ALPN, blobs.clone());

                // Build the router
                let router = router.spawn().await?;

                // Setup RPC
                let (internal_rpc, controller) = quic_rpc::transport::flume::channel(32);
                let internal_rpc = quic_rpc::RpcServer::new(internal_rpc).boxed();
                let _rpc_task = internal_rpc.spawn_accept_loop(move |msg, chan| {
                    blobs.clone().handle_rpc_request(msg, chan)
                });
                let client = quic_rpc::RpcClient::new(controller).boxed();
                Ok(Node {
                    router,
                    client,
                    _rpc_task,
                    _local_pool: local_pool,
                })
            }
        }

        impl Node {
            /// Creates a new node with memory storage
            pub fn memory() -> Builder<crate::store::mem::Store> {
                Builder {
                    store: crate::store::mem::Store::new(),
                    events: Default::default(),
                    endpoint: None,
                }
            }

            /// Creates a new node with persistent storage
            pub async fn persistent(
                path: impl AsRef<Path>,
            ) -> anyhow::Result<Builder<crate::store::fs::Store>> {
                Ok(Builder {
                    store: crate::store::fs::Store::load(path).await?,
                    events: Default::default(),
                    endpoint: None,
                })
            }

            /// Returns the node id
            pub fn node_id(&self) -> NodeId {
                self.router.endpoint().node_id()
            }

            /// Returns the node address
            pub async fn node_addr(&self) -> anyhow::Result<NodeAddr> {
                self.router.endpoint().node_addr().await
            }

            /// Shuts down the node
            pub async fn shutdown(self) -> anyhow::Result<()> {
                self.router.shutdown().await
            }

            /// Returns an in-memory blobs client
            pub fn blobs(&self) -> blobs::Client {
                blobs::Client::new(self.client.clone())
            }

            /// Returns an in-memory tags client
            pub fn tags(&self) -> tags::Client {
                tags::Client::new(self.client.clone())
            }
        }
    }

    #[tokio::test]
    async fn test_blob_create_collection() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;

        // create temp file
        let temp_dir = tempfile::tempdir().context("tempdir")?;

        let in_root = temp_dir.path().join("in");
        tokio::fs::create_dir_all(in_root.clone())
            .await
            .context("create dir all")?;

        let mut paths = Vec::new();
        for i in 0..5 {
            let path = in_root.join(format!("test-{i}"));
            let size = 100;
            let mut buf = vec![0u8; size];
            rand::thread_rng().fill_bytes(&mut buf);
            let mut file = tokio::fs::File::create(path.clone())
                .await
                .context("create file")?;
            file.write_all(&buf.clone()).await.context("write_all")?;
            file.flush().await.context("flush")?;
            paths.push(path);
        }

        let blobs = node.blobs();

        let mut collection = Collection::default();
        let mut tags = Vec::new();
        // import files
        for path in &paths {
            let import_outcome = blobs
                .add_from_path(
                    path.to_path_buf(),
                    false,
                    SetTagOption::Auto,
                    WrapOption::NoWrap,
                )
                .await
                .context("import file")?
                .finish()
                .await
                .context("import finish")?;

            collection.push(
                path.file_name().unwrap().to_str().unwrap().to_string(),
                import_outcome.hash,
            );
            tags.push(import_outcome.tag);
        }

        let (hash, tag) = blobs
            .create_collection(collection, SetTagOption::Auto, tags)
            .await?;

        let collections: Vec<_> = blobs.list_collections()?.try_collect().await?;

        assert_eq!(collections.len(), 1);
        {
            let CollectionInfo {
                tag,
                hash,
                total_blobs_count,
                ..
            } = &collections[0];
            assert_eq!(tag, tag);
            assert_eq!(hash, hash);
            // 5 blobs + 1 meta
            assert_eq!(total_blobs_count, &Some(5 + 1));
        }

        // check that "temp" tags have been deleted
        let tags: Vec<_> = node.tags().list().await?.try_collect().await?;
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].hash, hash);
        assert_eq!(tags[0].name, tag);
        assert_eq!(tags[0].format, BlobFormat::HashSeq);

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_read_at() -> Result<()> {
        // let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;

        // create temp file
        let temp_dir = tempfile::tempdir().context("tempdir")?;

        let in_root = temp_dir.path().join("in");
        tokio::fs::create_dir_all(in_root.clone())
            .await
            .context("create dir all")?;

        let path = in_root.join("test-blob");
        let size = 1024 * 128;
        let buf: Vec<u8> = (0..size).map(|i| i as u8).collect();
        let mut file = tokio::fs::File::create(path.clone())
            .await
            .context("create file")?;
        file.write_all(&buf.clone()).await.context("write_all")?;
        file.flush().await.context("flush")?;

        let blobs = node.blobs();

        let import_outcome = blobs
            .add_from_path(
                path.to_path_buf(),
                false,
                SetTagOption::Auto,
                WrapOption::NoWrap,
            )
            .await
            .context("import file")?
            .finish()
            .await
            .context("import finish")?;

        let hash = import_outcome.hash;

        // Read everything
        let res = blobs.read_to_bytes(hash).await?;
        assert_eq!(&res, &buf[..]);

        // Read at smaller than blob_get_chunk_size
        let res = blobs
            .read_at_to_bytes(hash, 0, ReadAtLen::Exact(100))
            .await?;
        assert_eq!(res.len(), 100);
        assert_eq!(&res[..], &buf[0..100]);

        let res = blobs
            .read_at_to_bytes(hash, 20, ReadAtLen::Exact(120))
            .await?;
        assert_eq!(res.len(), 120);
        assert_eq!(&res[..], &buf[20..140]);

        // Read at equal to blob_get_chunk_size
        let res = blobs
            .read_at_to_bytes(hash, 0, ReadAtLen::Exact(1024 * 64))
            .await?;
        assert_eq!(res.len(), 1024 * 64);
        assert_eq!(&res[..], &buf[0..1024 * 64]);

        let res = blobs
            .read_at_to_bytes(hash, 20, ReadAtLen::Exact(1024 * 64))
            .await?;
        assert_eq!(res.len(), 1024 * 64);
        assert_eq!(&res[..], &buf[20..(20 + 1024 * 64)]);

        // Read at larger than blob_get_chunk_size
        let res = blobs
            .read_at_to_bytes(hash, 0, ReadAtLen::Exact(10 + 1024 * 64))
            .await?;
        assert_eq!(res.len(), 10 + 1024 * 64);
        assert_eq!(&res[..], &buf[0..(10 + 1024 * 64)]);

        let res = blobs
            .read_at_to_bytes(hash, 20, ReadAtLen::Exact(10 + 1024 * 64))
            .await?;
        assert_eq!(res.len(), 10 + 1024 * 64);
        assert_eq!(&res[..], &buf[20..(20 + 10 + 1024 * 64)]);

        // full length
        let res = blobs.read_at_to_bytes(hash, 20, ReadAtLen::All).await?;
        assert_eq!(res.len(), 1024 * 128 - 20);
        assert_eq!(&res[..], &buf[20..]);

        // size should be total
        let reader = blobs.read_at(hash, 0, ReadAtLen::Exact(20)).await?;
        assert_eq!(reader.size(), 1024 * 128);
        assert_eq!(reader.response_size, 20);

        // last chunk - exact
        let res = blobs
            .read_at_to_bytes(hash, 1024 * 127, ReadAtLen::Exact(1024))
            .await?;
        assert_eq!(res.len(), 1024);
        assert_eq!(res, &buf[1024 * 127..]);

        // last chunk - open
        let res = blobs
            .read_at_to_bytes(hash, 1024 * 127, ReadAtLen::All)
            .await?;
        assert_eq!(res.len(), 1024);
        assert_eq!(res, &buf[1024 * 127..]);

        // last chunk - larger
        let mut res = blobs
            .read_at(hash, 1024 * 127, ReadAtLen::AtMost(2048))
            .await?;
        assert_eq!(res.size, 1024 * 128);
        assert_eq!(res.response_size, 1024);
        let res = res.read_to_bytes().await?;
        assert_eq!(res.len(), 1024);
        assert_eq!(res, &buf[1024 * 127..]);

        // out of bounds - too long
        let res = blobs
            .read_at(hash, 0, ReadAtLen::Exact(1024 * 128 + 1))
            .await;
        let err = res.unwrap_err();
        assert!(err.to_string().contains("out of bound"));

        // out of bounds - offset larger than blob
        let res = blobs.read_at(hash, 1024 * 128 + 1, ReadAtLen::All).await;
        let err = res.unwrap_err();
        assert!(err.to_string().contains("out of range"));

        // out of bounds - offset + length too large
        let res = blobs
            .read_at(hash, 1024 * 127, ReadAtLen::Exact(1025))
            .await;
        let err = res.unwrap_err();
        assert!(err.to_string().contains("out of bound"));

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_get_collection() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;

        // create temp file
        let temp_dir = tempfile::tempdir().context("tempdir")?;

        let in_root = temp_dir.path().join("in");
        tokio::fs::create_dir_all(in_root.clone())
            .await
            .context("create dir all")?;

        let mut paths = Vec::new();
        for i in 0..5 {
            let path = in_root.join(format!("test-{i}"));
            let size = 100;
            let mut buf = vec![0u8; size];
            rand::thread_rng().fill_bytes(&mut buf);
            let mut file = tokio::fs::File::create(path.clone())
                .await
                .context("create file")?;
            file.write_all(&buf.clone()).await.context("write_all")?;
            file.flush().await.context("flush")?;
            paths.push(path);
        }

        let blobs = node.blobs();

        let mut collection = Collection::default();
        let mut tags = Vec::new();
        // import files
        for path in &paths {
            let import_outcome = blobs
                .add_from_path(
                    path.to_path_buf(),
                    false,
                    SetTagOption::Auto,
                    WrapOption::NoWrap,
                )
                .await
                .context("import file")?
                .finish()
                .await
                .context("import finish")?;

            collection.push(
                path.file_name().unwrap().to_str().unwrap().to_string(),
                import_outcome.hash,
            );
            tags.push(import_outcome.tag);
        }

        let (hash, _tag) = blobs
            .create_collection(collection, SetTagOption::Auto, tags)
            .await?;

        let collection = blobs.get_collection(hash).await?;

        // 5 blobs
        assert_eq!(collection.len(), 5);

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_share() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;

        // create temp file
        let temp_dir = tempfile::tempdir().context("tempdir")?;

        let in_root = temp_dir.path().join("in");
        tokio::fs::create_dir_all(in_root.clone())
            .await
            .context("create dir all")?;

        let path = in_root.join("test-blob");
        let size = 1024 * 128;
        let buf: Vec<u8> = (0..size).map(|i| i as u8).collect();
        let mut file = tokio::fs::File::create(path.clone())
            .await
            .context("create file")?;
        file.write_all(&buf.clone()).await.context("write_all")?;
        file.flush().await.context("flush")?;

        let blobs = node.blobs();

        let import_outcome = blobs
            .add_from_path(
                path.to_path_buf(),
                false,
                SetTagOption::Auto,
                WrapOption::NoWrap,
            )
            .await
            .context("import file")?
            .finish()
            .await
            .context("import finish")?;

        // let ticket = blobs
        //     .share(import_outcome.hash, BlobFormat::Raw, Default::default())
        //     .await?;
        // assert_eq!(ticket.hash(), import_outcome.hash);

        let status = blobs.status(import_outcome.hash).await?;
        assert_eq!(status, BlobStatus::Complete { size });

        Ok(())
    }

    #[derive(Debug, Clone)]
    struct BlobEvents {
        sender: mpsc::Sender<crate::provider::Event>,
    }

    impl BlobEvents {
        fn new(cap: usize) -> (Self, mpsc::Receiver<crate::provider::Event>) {
            let (s, r) = mpsc::channel(cap);
            (Self { sender: s }, r)
        }
    }

    impl crate::provider::CustomEventSender for BlobEvents {
        fn send(&self, event: crate::provider::Event) -> futures_lite::future::Boxed<()> {
            let sender = self.sender.clone();
            Box::pin(async move {
                sender.send(event).await.ok();
            })
        }

        fn try_send(&self, event: crate::provider::Event) {
            self.sender.try_send(event).ok();
        }
    }

    #[tokio::test]
    async fn test_blob_provide_events() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let (node1_events, mut node1_events_r) = BlobEvents::new(16);
        let node1 = node::Node::memory()
            .blobs_events(node1_events)
            .spawn()
            .await?;

        let (node2_events, mut node2_events_r) = BlobEvents::new(16);
        let node2 = node::Node::memory()
            .blobs_events(node2_events)
            .spawn()
            .await?;

        let import_outcome = node1.blobs().add_bytes(&b"hello world"[..]).await?;

        // Download in node2
        let node1_addr = node1.node_addr().await?;
        let res = node2
            .blobs()
            .download(import_outcome.hash, node1_addr)
            .await?
            .await?;
        dbg!(&res);
        assert_eq!(res.local_size, 0);
        assert_eq!(res.downloaded_size, 11);

        node1.shutdown().await?;
        node2.shutdown().await?;

        let mut ev1 = Vec::new();
        while let Some(ev) = node1_events_r.recv().await {
            ev1.push(ev);
        }
        // assert_eq!(ev1.len(), 3);
        assert!(matches!(
            ev1[0],
            crate::provider::Event::ClientConnected { .. }
        ));
        assert!(matches!(
            ev1[1],
            crate::provider::Event::GetRequestReceived { .. }
        ));
        assert!(matches!(
            ev1[2],
            crate::provider::Event::TransferProgress { .. }
        ));
        assert!(matches!(
            ev1[3],
            crate::provider::Event::TransferCompleted { .. }
        ));
        dbg!(&ev1);

        let mut ev2 = Vec::new();
        while let Some(ev) = node2_events_r.recv().await {
            ev2.push(ev);
        }

        // Node 2 did not provide anything
        assert!(ev2.is_empty());
        Ok(())
    }
    /// Download a existing blob from oneself
    #[tokio::test]
    async fn test_blob_get_self_existing() -> TestResult<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;
        let node_id = node.node_id();
        let blobs = node.blobs();

        let AddOutcome { hash, size, .. } = blobs.add_bytes("foo").await?;

        // Direct
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::Raw,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Direct,
                },
            )
            .await?
            .await?;

        assert_eq!(res.local_size, size);
        assert_eq!(res.downloaded_size, 0);

        // Queued
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::Raw,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Queued,
                },
            )
            .await?
            .await?;

        assert_eq!(res.local_size, size);
        assert_eq!(res.downloaded_size, 0);

        Ok(())
    }

    /// Download a missing blob from oneself
    #[tokio::test]
    async fn test_blob_get_self_missing() -> TestResult<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;
        let node_id = node.node_id();
        let blobs = node.blobs();

        let hash = Hash::from_bytes([0u8; 32]);

        // Direct
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::Raw,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Direct,
                },
            )
            .await?
            .await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string().as_str(),
            "No nodes to download from provided"
        );

        // Queued
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::Raw,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Queued,
                },
            )
            .await?
            .await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().to_string().as_str(),
            "No provider nodes found"
        );

        Ok(())
    }

    /// Download a existing collection. Check that things succeed and no download is performed.
    #[tokio::test]
    async fn test_blob_get_existing_collection() -> TestResult<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;
        // We use a nonexisting node id because we just want to check that this succeeds without
        // hitting the network.
        let node_id = NodeId::from_bytes(&[0u8; 32])?;
        let blobs = node.blobs();

        let mut collection = Collection::default();
        let mut tags = Vec::new();
        let mut size = 0;
        for value in ["iroh", "is", "cool"] {
            let import_outcome = blobs.add_bytes(value).await.context("add bytes")?;
            collection.push(value.to_string(), import_outcome.hash);
            tags.push(import_outcome.tag);
            size += import_outcome.size;
        }

        let (hash, _tag) = blobs
            .create_collection(collection, SetTagOption::Auto, tags)
            .await?;

        // load the hashseq and collection header manually to calculate our expected size
        let hashseq_bytes = blobs.read_to_bytes(hash).await?;
        size += hashseq_bytes.len() as u64;
        let hashseq = HashSeq::try_from(hashseq_bytes)?;
        let collection_header_bytes = blobs
            .read_to_bytes(hashseq.into_iter().next().expect("header to exist"))
            .await?;
        size += collection_header_bytes.len() as u64;

        // Direct
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::HashSeq,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Direct,
                },
            )
            .await?
            .await
            .context("direct (download)")?;

        assert_eq!(res.local_size, size);
        assert_eq!(res.downloaded_size, 0);

        // Queued
        let res = blobs
            .download_with_opts(
                hash,
                DownloadOptions {
                    format: BlobFormat::HashSeq,
                    nodes: vec![node_id.into()],
                    tag: SetTagOption::Auto,
                    mode: DownloadMode::Queued,
                },
            )
            .await?
            .await
            .context("queued")?;

        assert_eq!(res.local_size, size);
        assert_eq!(res.downloaded_size, 0);

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(target_os = "windows", ignore = "flaky")]
    async fn test_blob_delete_mem() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let node = node::Node::memory().spawn().await?;

        let res = node.blobs().add_bytes(&b"hello world"[..]).await?;

        let hashes: Vec<_> = node.blobs().list().await?.try_collect().await?;
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].hash, res.hash);

        // delete
        node.blobs().delete_blob(res.hash).await?;

        let hashes: Vec<_> = node.blobs().list().await?.try_collect().await?;
        assert!(hashes.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_blob_delete_fs() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let dir = tempfile::tempdir()?;
        let node = node::Node::persistent(dir.path()).await?.spawn().await?;

        let res = node.blobs().add_bytes(&b"hello world"[..]).await?;

        let hashes: Vec<_> = node.blobs().list().await?.try_collect().await?;
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].hash, res.hash);

        // delete
        node.blobs().delete_blob(res.hash).await?;

        let hashes: Vec<_> = node.blobs().list().await?.try_collect().await?;
        assert!(hashes.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_ticket_multiple_addrs() -> TestResult<()> {
        let _guard = iroh_test::logging::setup();

        let node = Node::memory().spawn().await?;
        let hash = node
            .blobs()
            .add_bytes(Bytes::from_static(b"hello"))
            .await?
            .hash;

        let addr = node.node_addr().await?;
        let ticket = BlobTicket::new(addr, hash, BlobFormat::Raw)?;
        println!("addrs: {:?}", ticket.node_addr());
        assert!(!ticket.node_addr().direct_addresses.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_node_add_blob_stream() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        use std::io::Cursor;
        let node = Node::memory().spawn().await?;

        let blobs = node.blobs();
        let input = vec![2u8; 1024 * 256]; // 265kb so actually streaming, chunk size is 64kb
        let reader = Cursor::new(input.clone());
        let progress = blobs.add_reader(reader, SetTagOption::Auto).await?;
        let outcome = progress.finish().await?;
        let hash = outcome.hash;
        let output = blobs.read_to_bytes(hash).await?;
        assert_eq!(input, output.to_vec());
        Ok(())
    }

    #[tokio::test]
    async fn test_node_add_tagged_blob_event() -> Result<()> {
        let _guard = iroh_test::logging::setup();

        let node = Node::memory().spawn().await?;

        let _got_hash = tokio::time::timeout(Duration::from_secs(10), async move {
            let mut stream = node
                .blobs()
                .add_from_path(
                    Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"),
                    false,
                    SetTagOption::Auto,
                    WrapOption::NoWrap,
                )
                .await?;

            while let Some(progress) = stream.next().await {
                match progress? {
                    crate::provider::AddProgress::AllDone { hash, .. } => {
                        return Ok(hash);
                    }
                    crate::provider::AddProgress::Abort(e) => {
                        anyhow::bail!("Error while adding data: {e}");
                    }
                    _ => {}
                }
            }
            anyhow::bail!("stream ended without providing data");
        })
        .await
        .context("timeout")?
        .context("get failed")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_via_relay() -> Result<()> {
        let _guard = iroh_test::logging::setup();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await?;

        let endpoint1 = iroh::Endpoint::builder()
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .insecure_skip_relay_cert_verify(true);
        let node1 = Node::memory().endpoint(endpoint1).spawn().await?;
        let endpoint2 = iroh::Endpoint::builder()
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .insecure_skip_relay_cert_verify(true);
        let node2 = Node::memory().endpoint(endpoint2).spawn().await?;
        let AddOutcome { hash, .. } = node1.blobs().add_bytes(b"foo".to_vec()).await?;

        // create a node addr with only a relay URL, no direct addresses
        let addr = NodeAddr::new(node1.node_id()).with_relay_url(relay_url);
        node2.blobs().download(hash, addr).await?.await?;
        assert_eq!(
            node2
                .blobs()
                .read_to_bytes(hash)
                .await
                .context("get")?
                .as_ref(),
            b"foo"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "flaky"]
    async fn test_download_via_relay_with_discovery() -> Result<()> {
        let _guard = iroh_test::logging::setup();
        let (relay_map, _relay_url, _guard) = iroh::test_utils::run_relay_server().await?;
        let dns_pkarr_server = DnsPkarrServer::run().await?;

        let secret1 = SecretKey::generate();
        let endpoint1 = iroh::Endpoint::builder()
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .insecure_skip_relay_cert_verify(true)
            .dns_resolver(dns_pkarr_server.dns_resolver())
            .secret_key(secret1.clone())
            .discovery(dns_pkarr_server.discovery(secret1));
        let node1 = Node::memory().endpoint(endpoint1).spawn().await?;
        let secret2 = SecretKey::generate();
        let endpoint2 = iroh::Endpoint::builder()
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .insecure_skip_relay_cert_verify(true)
            .dns_resolver(dns_pkarr_server.dns_resolver())
            .secret_key(secret2.clone())
            .discovery(dns_pkarr_server.discovery(secret2));
        let node2 = Node::memory().endpoint(endpoint2).spawn().await?;
        let hash = node1.blobs().add_bytes(b"foo".to_vec()).await?.hash;

        // create a node addr with node id only
        let addr = NodeAddr::new(node1.node_id());
        node2.blobs().download(hash, addr).await?.await?;
        assert_eq!(
            node2
                .blobs()
                .read_to_bytes(hash)
                .await
                .context("get")?
                .as_ref(),
            b"foo"
        );
        Ok(())
    }
}
