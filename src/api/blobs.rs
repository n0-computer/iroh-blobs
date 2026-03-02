//! API to interact with a local blob store
//!
//! This API is for local interactions with the blob store, such as importing
//! and exporting blobs, observing the bitfield of a blob, and deleting blobs.
//!
//! The main entry point is the [`Blobs`] struct.
use std::{
    collections::BTreeMap,
    future::{Future, IntoFuture},
    io,
    num::NonZeroU64,
    path::{Path, PathBuf},
    pin::Pin,
};

pub use bao_tree::io::mixed::EncodedItem;
use bao_tree::{
    io::{
        fsm::{ResponseDecoder, ResponseDecoderNext},
        BaoContentItem, Leaf,
    },
    BaoTree, ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use genawaiter::sync::Gen;
use iroh_io::AsyncStreamWriter;
use irpc::channel::{mpsc, oneshot};
use n0_error::AnyError;
use n0_future::{future, stream, Stream, StreamExt};
use range_collections::{range_set::RangeSetRange, RangeSet2};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use tracing::trace;
mod reader;
pub use reader::BlobReader;

// Public reexports from the proto module.
//
// Due to the fact that the proto module is hidden from docs by default,
// these will appear in the docs as if they were declared here.
pub use super::proto::{
    AddProgressItem, Bitfield, BlobDeleteRequest as DeleteOptions, BlobStatus,
    ExportBaoRequest as ExportBaoOptions, ExportMode, ExportPathRequest as ExportOptions,
    ExportProgressItem, ExportRangesRequest as ExportRangesOptions,
    ImportBaoRequest as ImportBaoOptions, ImportMode, ObserveRequest as ObserveOptions,
};
use super::{
    proto::{
        BatchResponse, BlobStatusRequest, ClearProtectedRequest, CreateTempTagRequest,
        ExportBaoRequest, ExportRangesItem, ImportBaoRequest, ImportByteStreamRequest,
        ImportBytesRequest, ImportPathRequest, ListRequest, Scope,
    },
    remote::HashSeqChunk,
    tags::TagInfo,
    ApiClient, RequestResult, Tags,
};
use crate::{
    api::proto::{BatchRequest, ImportByteStreamUpdate},
    provider::events::ClientResult,
    store::IROH_BLOCK_SIZE,
    util::{temp_tag::TempTag, RecvStreamAsyncStreamReader},
    BlobFormat, Hash, HashAndFormat,
};

/// Options for adding bytes to the store.
#[derive(Debug)]
pub struct AddBytesOptions {
    /// The bytes to import.
    pub data: Bytes,
    /// The format to use for the imported blob.
    pub format: BlobFormat,
}

impl<T: Into<Bytes>> From<(T, BlobFormat)> for AddBytesOptions {
    fn from(item: (T, BlobFormat)) -> Self {
        let (data, format) = item;
        Self {
            data: data.into(),
            format,
        }
    }
}

/// API for local blob operations: importing, exporting, observing, and listing.
///
/// Obtain a `Blobs` reference from a [`crate::api::Store`] via [`Deref`] or
/// [`crate::api::Store::blobs`]. `Blobs` is a cheap ref-cast over an RPC
/// client, so cloning it is cheap.
///
/// Most operations return a progress handle (e.g. [`AddProgress`],
/// [`ExportProgress`]) which implement [`IntoFuture`] for a single-await
/// result, or expose a `.stream()` method for detailed progress events.
#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Blobs {
    client: ApiClient,
}

impl Blobs {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

    /// Opens a batch scope for adding blobs.
    ///
    /// Within a batch, blobs added via [`Batch::add_bytes`] etc. are protected
    /// from garbage collection for the duration of the batch. When the `Batch`
    /// is dropped, the protection is released and any unreferenced blobs may be
    /// collected.
    ///
    /// Use this when you need to add multiple blobs and then create a tag
    /// referencing all of them atomically.
    pub async fn batch(&self) -> irpc::Result<Batch<'_>> {
        let msg = BatchRequest;
        trace!("{msg:?}");
        let (tx, rx) = self.client.client_streaming(msg, 32).await?;
        let scope = rx.await?;

        Ok(Batch {
            scope,
            blobs: self,
            _tx: tx,
        })
    }

    /// Create a reader for the given hash. The reader implements [`tokio::io::AsyncRead`] and [`tokio::io::AsyncSeek`]
    /// and therefore can be used to read the blob's content.
    ///
    /// Any access to parts of the blob that are not present will result in an error.
    ///
    /// Example:
    /// ```rust
    /// use iroh_blobs::{store::mem::MemStore, api::blobs::Blobs};
    /// use tokio::io::AsyncReadExt;
    ///
    /// # async fn example() -> n0_error::Result<()> {
    /// let store = MemStore::new();
    /// let tag = store.add_slice(b"Hello, world!").await?;
    /// let mut reader = store.reader(tag.hash);
    /// let mut buf = String::new();
    /// reader.read_to_string(&mut buf).await?;
    /// assert_eq!(buf, "Hello, world!");
    /// # Ok(())
    /// }
    /// ```
    pub fn reader(&self, hash: impl Into<Hash>) -> BlobReader {
        self.reader_with_opts(ReaderOptions { hash: hash.into() })
    }

    /// Create a reader for the given options. The reader implements [`tokio::io::AsyncRead`] and [`tokio::io::AsyncSeek`]
    /// and therefore can be used to read the blob's content.
    ///
    /// Any access to parts of the blob that are not present will result in an error.
    pub fn reader_with_opts(&self, options: ReaderOptions) -> BlobReader {
        BlobReader::new(self.clone(), options)
    }

    /// Delete a blob.
    ///
    /// This function is not public, because it does not work as expected when called manually,
    /// because blobs are protected from deletion. This is only called from the gc task, which
    /// clears the protections before.
    ///
    /// Users should rely only on garbage collection for blob deletion.
    pub(crate) async fn delete_with_opts(&self, options: DeleteOptions) -> RequestResult<()> {
        trace!("{options:?}");
        self.client.rpc(options).await??;
        Ok(())
    }

    /// See [`Self::delete_with_opts`].
    pub(crate) async fn delete(
        &self,
        hashes: impl IntoIterator<Item = impl Into<Hash>>,
    ) -> RequestResult<()> {
        self.delete_with_opts(DeleteOptions {
            hashes: hashes.into_iter().map(Into::into).collect(),
            force: false,
        })
        .await
    }

    /// Imports bytes from a byte slice into the store as a raw blob.
    ///
    /// This copies the slice into a `Bytes` before sending. For owned data,
    /// prefer [`Blobs::add_bytes`] to avoid the copy.
    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> AddProgress<'_> {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    /// Imports bytes into the store as a raw blob.
    ///
    /// For format control (e.g. importing as `HashSeq`), use
    /// [`Blobs::add_bytes_with_opts`].
    pub fn add_bytes(&self, data: impl Into<bytes::Bytes>) -> AddProgress<'_> {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    /// Imports bytes into the store with full control over format and other options.
    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> AddProgress<'_> {
        let options = options.into();
        let request = ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(request)
    }

    fn add_bytes_impl(&self, options: ImportBytesRequest) -> AddProgress<'_> {
        trace!("{options:?}");
        let this = self.clone();
        let stream = Gen::new(|co| async move {
            let mut receiver = match this.client.server_streaming(options, 32).await {
                Ok(receiver) => receiver,
                Err(cause) => {
                    co.yield_(AddProgressItem::Error(cause.into())).await;
                    return;
                }
            };
            loop {
                match receiver.recv().await {
                    Ok(Some(item)) => co.yield_(item).await,
                    Err(cause) => {
                        co.yield_(AddProgressItem::Error(cause.into())).await;
                        break;
                    }
                    Ok(None) => break,
                }
            }
        });
        AddProgress::new(self, stream)
    }

    /// Imports a file from the filesystem with full control over import mode and format.
    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> AddProgress<'_> {
        let options = options.into();
        self.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: Scope::GLOBAL,
        })
    }

    fn add_path_with_opts_impl(&self, options: ImportPathRequest) -> AddProgress<'_> {
        trace!("{:?}", options);
        let client = self.client.clone();
        let stream = Gen::new(|co| async move {
            let mut receiver = match client.server_streaming(options, 32).await {
                Ok(receiver) => receiver,
                Err(cause) => {
                    co.yield_(AddProgressItem::Error(cause.into())).await;
                    return;
                }
            };
            loop {
                match receiver.recv().await {
                    Ok(Some(item)) => co.yield_(item).await,
                    Err(cause) => {
                        co.yield_(AddProgressItem::Error(cause.into())).await;
                        break;
                    }
                    Ok(None) => break,
                }
            }
        });
        AddProgress::new(self, stream)
    }

    /// Imports a file from the filesystem as a raw blob.
    ///
    /// The file is copied into the store. For other import modes (e.g. move or
    /// reflink), use [`Blobs::add_path_with_opts`].
    pub fn add_path(&self, path: impl AsRef<Path>) -> AddProgress<'_> {
        self.add_path_with_opts(AddPathOptions {
            path: path.as_ref().to_owned(),
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
        })
    }

    /// Imports data from an async byte stream into the store as a raw blob.
    ///
    /// The stream is consumed in chunks. Use this when the data comes from a
    /// source that does not fit in memory all at once.
    pub async fn add_stream(
        &self,
        data: impl Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    ) -> AddProgress<'_> {
        let inner = ImportByteStreamRequest {
            format: crate::BlobFormat::Raw,
            scope: Scope::default(),
        };
        let client = self.client.clone();
        let stream = Gen::new(|co| async move {
            let (sender, mut receiver) = match client.bidi_streaming(inner, 32, 32).await {
                Ok(x) => x,
                Err(cause) => {
                    co.yield_(AddProgressItem::Error(cause.into())).await;
                    return;
                }
            };
            let recv = async {
                loop {
                    match receiver.recv().await {
                        Ok(Some(item)) => co.yield_(item).await,
                        Err(cause) => {
                            co.yield_(AddProgressItem::Error(cause.into())).await;
                            break;
                        }
                        Ok(None) => break,
                    }
                }
            };
            let send = async {
                tokio::pin!(data);
                while let Some(item) = data.next().await {
                    sender.send(ImportByteStreamUpdate::Bytes(item?)).await?;
                }
                sender.send(ImportByteStreamUpdate::Done).await?;
                n0_error::Ok(())
            };
            let _ = tokio::join!(send, recv);
        });
        AddProgress::new(self, stream)
    }

    /// Exports byte ranges from a blob.
    ///
    /// Ranges are given in bytes. The returned progress handle yields
    /// [`ExportRangesItem`] events; note that the store rounds up to chunk
    /// boundaries (16 KiB), so you may receive slightly more data than
    /// requested. Use [`ExportRangesProgress::concatenate`] to collect the
    /// ranges into a single buffer, clipped to the exact requested bounds.
    pub fn export_ranges(
        &self,
        hash: impl Into<Hash>,
        ranges: impl Into<RangeSet2<u64>>,
    ) -> ExportRangesProgress {
        self.export_ranges_with_opts(ExportRangesOptions {
            hash: hash.into(),
            ranges: ranges.into(),
        })
    }

    /// Exports byte ranges from a blob with full control over options.
    pub fn export_ranges_with_opts(&self, options: ExportRangesOptions) -> ExportRangesProgress {
        trace!("{options:?}");
        ExportRangesProgress::new(
            options.ranges.clone(),
            self.client.server_streaming(options, 32),
        )
    }

    /// Exports a blob in BAO-encoded format with full control over options.
    ///
    /// `local_update_cap` controls the internal channel buffer size; 32 is a
    /// reasonable default for most cases.
    pub fn export_bao_with_opts(
        &self,
        options: ExportBaoOptions,
        local_update_cap: usize,
    ) -> ExportBaoProgress {
        trace!("{options:?}");
        ExportBaoProgress::new(self.client.server_streaming(options, local_update_cap))
    }

    /// Exports a blob in BAO-encoded format for the specified chunk ranges.
    ///
    /// The BAO encoding includes size headers and BLAKE3 hash tree nodes
    /// needed to verify each leaf. This is the format consumed by
    /// [`Blobs::import_bao_bytes`] on the receiving side.
    ///
    /// `ranges` are expressed in BLAKE3 chunk units (1024 bytes each), not
    /// bytes. Use `ChunkRanges::all()` to export the full blob.
    pub fn export_bao(
        &self,
        hash: impl Into<Hash>,
        ranges: impl Into<ChunkRanges>,
    ) -> ExportBaoProgress {
        self.export_bao_with_opts(
            ExportBaoRequest {
                hash: hash.into(),
                ranges: ranges.into(),
            },
            32,
        )
    }

    /// Export a single chunk from the given hash, at the given offset.
    pub async fn export_chunk(
        &self,
        hash: impl Into<Hash>,
        offset: u64,
    ) -> super::ExportBaoResult<Leaf> {
        let base = ChunkNum::full_chunks(offset);
        let ranges = ChunkRanges::from(base..base + 1);
        let mut stream = self.export_bao(hash, ranges).stream();
        while let Some(item) = stream.next().await {
            match item {
                EncodedItem::Leaf(leaf) => return Ok(leaf),
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Err(io::Error::other("unexpected end of stream").into())
    }

    /// Get the entire blob into a Bytes
    ///
    /// This will run out of memory when called for very large blobs, so be careful!
    pub async fn get_bytes(&self, hash: impl Into<Hash>) -> super::ExportBaoResult<Bytes> {
        self.export_bao(hash.into(), ChunkRanges::all())
            .data_to_bytes()
            .await
    }

    /// Observe the bitfield of the given hash.
    pub fn observe(&self, hash: impl Into<Hash>) -> ObserveProgress {
        self.observe_with_opts(ObserveOptions { hash: hash.into() })
    }

    /// Observes the bitfield of a blob with full control over options.
    pub fn observe_with_opts(&self, options: ObserveOptions) -> ObserveProgress {
        trace!("{:?}", options);
        if options.hash == Hash::EMPTY {
            return ObserveProgress::new(async move {
                let (tx, rx) = mpsc::channel(1);
                tx.send(Bitfield::complete(0)).await.ok();
                Ok(rx)
            });
        }
        ObserveProgress::new(self.client.server_streaming(options, 32))
    }

    /// Exports a blob to the filesystem with full control over options.
    pub fn export_with_opts(&self, options: ExportOptions) -> ExportProgress {
        trace!("{:?}", options);
        ExportProgress::new(self.client.server_streaming(options, 32))
    }

    /// Exports a blob to a file on the filesystem.
    ///
    /// The blob is copied to `target`. For other export modes (e.g. move or
    /// reflink), use [`Blobs::export_with_opts`].
    pub fn export(&self, hash: impl Into<Hash>, target: impl AsRef<Path>) -> ExportProgress {
        let options = ExportOptions {
            hash: hash.into(),
            mode: ExportMode::Copy,
            target: target.as_ref().to_owned(),
        };
        self.export_with_opts(options)
    }

    /// Import BaoContentItems from a stream.
    ///
    /// The store assumes that these are already verified and in the correct order.
    #[cfg_attr(feature = "hide-proto-docs", doc(hidden))]
    pub async fn import_bao(
        &self,
        hash: impl Into<Hash>,
        size: NonZeroU64,
        local_update_cap: usize,
    ) -> irpc::Result<ImportBaoHandle> {
        let options = ImportBaoRequest {
            hash: hash.into(),
            size,
        };
        self.import_bao_with_opts(options, local_update_cap).await
    }

    #[cfg_attr(feature = "hide-proto-docs", doc(hidden))]
    pub async fn import_bao_with_opts(
        &self,
        options: ImportBaoOptions,
        local_update_cap: usize,
    ) -> irpc::Result<ImportBaoHandle> {
        trace!("{:?}", options);
        ImportBaoHandle::new(self.client.client_streaming(options, local_update_cap)).await
    }

    #[cfg_attr(feature = "hide-proto-docs", doc(hidden))]
    pub async fn import_bao_reader<R: crate::util::RecvStream>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> RequestResult<R> {
        let mut size = [0; 8];
        reader.recv_exact(&mut size).await?;
        let size = u64::from_le_bytes(size);
        let Some(size) = NonZeroU64::new(size) else {
            return if hash == Hash::EMPTY {
                Ok(reader)
            } else {
                Err(io::Error::other("invalid size for hash").into())
            };
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(
            hash.into(),
            ranges,
            tree,
            RecvStreamAsyncStreamReader::new(reader),
        );
        let options = ImportBaoOptions { hash, size };
        let handle = self.import_bao_with_opts(options, 32).await?;
        let driver = async move {
            let reader = loop {
                match decoder.next().await {
                    ResponseDecoderNext::More((rest, item)) => {
                        handle.tx.send(item?).await?;
                        decoder = rest;
                    }
                    ResponseDecoderNext::Done(reader) => break reader,
                };
            };
            drop(handle.tx);
            io::Result::Ok(reader)
        };
        let fut = async move { handle.rx.await.map_err(io::Error::other)? };
        let (reader, res) = tokio::join!(driver, fut);
        res?;
        Ok(reader?.into_inner())
    }

    #[cfg_attr(feature = "hide-proto-docs", doc(hidden))]
    pub async fn import_bao_bytes(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        data: impl Into<Bytes>,
    ) -> RequestResult<()> {
        self.import_bao_reader(hash, ranges, data.into()).await?;
        Ok(())
    }

    /// Lists all blobs currently stored.
    ///
    /// Yields the hash of each complete or partial blob. Use
    /// [`BlobsListProgress::hashes`] to collect them into a `Vec`, or
    /// [`BlobsListProgress::stream`] to iterate lazily.
    pub fn list(&self) -> BlobsListProgress {
        let msg = ListRequest;
        let client = self.client.clone();
        BlobsListProgress::new(client.server_streaming(msg, 32))
    }

    /// Returns the storage status of a blob: complete, partial, or not found.
    pub async fn status(&self, hash: impl Into<Hash>) -> irpc::Result<BlobStatus> {
        let hash = hash.into();
        let msg = BlobStatusRequest { hash };
        self.client.rpc(msg).await
    }

    /// Returns `true` if the store holds a complete blob for the given hash.
    pub async fn has(&self, hash: impl Into<Hash>) -> irpc::Result<bool> {
        match self.status(hash).await? {
            BlobStatus::Complete { .. } => Ok(true),
            _ => Ok(false),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn clear_protected(&self) -> RequestResult<()> {
        let msg = ClearProtectedRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }
}

/// A progress handle for an add operation that runs inside a [`Batch`] scope.
///
/// Like [`AddProgress`] but, when awaited, returns a [`TempTag`] that is
/// protected until the enclosing batch is dropped.
pub struct BatchAddProgress<'a>(AddProgress<'a>);

impl<'a> IntoFuture for BatchAddProgress<'a> {
    type Output = RequestResult<TempTag>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.temp_tag())
    }
}

impl<'a> BatchAddProgress<'a> {
    /// Completes the add and writes a persistent tag with the given name.
    ///
    /// Returns the [`HashAndFormat`] of the stored blob.
    pub async fn with_named_tag(self, name: impl AsRef<[u8]>) -> RequestResult<HashAndFormat> {
        self.0.with_named_tag(name).await
    }

    /// Completes the add and writes an auto-named persistent tag.
    ///
    /// Returns [`TagInfo`] with the generated tag name and the blob's hash.
    pub async fn with_tag(self) -> RequestResult<TagInfo> {
        self.0.with_tag().await
    }

    /// Returns the underlying stream of [`AddProgressItem`] events.
    pub async fn stream(self) -> impl Stream<Item = AddProgressItem> {
        self.0.stream().await
    }

    /// Completes the add and returns a batch-scoped [`TempTag`].
    ///
    /// The blob is protected from GC until the enclosing batch is dropped.
    pub async fn temp_tag(self) -> RequestResult<TempTag> {
        self.0.temp_tag().await
    }
}

/// A scope in which blobs added are protected from garbage collection.
///
/// Created by [`Blobs::batch`]. All blobs added within a batch are kept alive
/// until the `Batch` is dropped, at which point protection is released.
/// This lets you add multiple blobs and then tag them atomically without
/// risking GC between additions.
pub struct Batch<'a> {
    scope: Scope,
    blobs: &'a Blobs,
    _tx: mpsc::Sender<BatchResponse>,
}

impl<'a> Batch<'a> {
    /// Imports bytes into the store as a raw blob, protected within this batch.
    pub fn add_bytes(&self, data: impl Into<Bytes>) -> BatchAddProgress<'_> {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        BatchAddProgress(self.blobs.add_bytes_impl(options))
    }

    /// Imports bytes with explicit format options, protected within this batch.
    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> BatchAddProgress<'_> {
        let options = options.into();
        BatchAddProgress(self.blobs.add_bytes_impl(ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: self.scope,
        }))
    }

    /// Imports bytes from a byte slice as a raw blob, protected within this batch.
    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> BatchAddProgress<'_> {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        BatchAddProgress(self.blobs.add_bytes_impl(options))
    }

    /// Imports a file from the filesystem with explicit options, protected within this batch.
    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> BatchAddProgress<'_> {
        let options = options.into();
        BatchAddProgress(self.blobs.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: self.scope,
        }))
    }

    /// Creates a batch-scoped [`TempTag`] for a blob that was added externally.
    ///
    /// Use this to protect a blob that you added via other means (e.g. via
    /// a download) within the lifetime of this batch.
    pub async fn temp_tag(&self, value: impl Into<HashAndFormat>) -> irpc::Result<TempTag> {
        let value = value.into();
        let msg = CreateTempTagRequest {
            scope: self.scope,
            value,
        };
        self.blobs.client.rpc(msg).await
    }
}

/// Options for importing data from a filesystem path.
#[derive(Debug)]
pub struct AddPathOptions {
    /// Path to the file to import.
    pub path: PathBuf,
    /// The format to assign to the imported blob.
    pub format: BlobFormat,
    /// How to move the file into the store (copy, move, or reflink).
    pub mode: ImportMode,
}

/// A progress handle for an import operation.
///
/// Internally this is a stream of [`AddProgressItem`] items. Working with this
/// stream directly can be inconvenient, so this struct provides some convenience
/// methods to work with the result.
///
/// It also implements [`IntoFuture`], so you can await it to get the [`TagInfo`] that
/// contains the hash of the added content and also protects the content.
///
/// If you want access to the stream, you can use the [`AddProgress::stream`] method.
pub struct AddProgress<'a> {
    blobs: &'a Blobs,
    inner: stream::Boxed<AddProgressItem>,
}

impl<'a> IntoFuture for AddProgress<'a> {
    type Output = RequestResult<TagInfo>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.with_tag())
    }
}

impl<'a> AddProgress<'a> {
    fn new(blobs: &'a Blobs, stream: impl Stream<Item = AddProgressItem> + Send + 'static) -> Self {
        Self {
            blobs,
            inner: Box::pin(stream),
        }
    }

    /// Completes the import and returns a globally-scoped [`TempTag`] protecting the blob.
    ///
    /// The blob is protected from GC for as long as the `TempTag` is alive.
    pub async fn temp_tag(self) -> RequestResult<TempTag> {
        let mut stream = self.inner;
        while let Some(item) = stream.next().await {
            match item {
                AddProgressItem::Done(tt) => return Ok(tt),
                AddProgressItem::Error(e) => return Err(e.into()),
                _ => {}
            }
        }
        Err(io::Error::other("unexpected end of stream").into())
    }

    /// Completes the import and writes a persistent tag with the given name.
    ///
    /// Returns the [`HashAndFormat`] of the stored blob.
    pub async fn with_named_tag(self, name: impl AsRef<[u8]>) -> RequestResult<HashAndFormat> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let haf = tt.hash_and_format();
        let tags = Tags::ref_from_sender(&blobs.client);
        tags.set(name, haf).await?;
        drop(tt);
        Ok(haf)
    }

    /// Completes the import and writes an auto-named persistent tag.
    ///
    /// Returns [`TagInfo`] containing the generated tag name and the blob's hash.
    pub async fn with_tag(self) -> RequestResult<TagInfo> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let hash = tt.hash();
        let format = tt.format();
        let tags = Tags::ref_from_sender(&blobs.client);
        let name = tags.create(tt.hash_and_format()).await?;
        drop(tt);
        Ok(TagInfo { name, hash, format })
    }

    /// Returns the underlying stream of raw [`AddProgressItem`] progress events.
    pub async fn stream(self) -> impl Stream<Item = AddProgressItem> {
        self.inner
    }
}

/// Options for creating a [`BlobReader`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderOptions {
    /// The hash of the blob to read.
    pub hash: Hash,
}

/// An observe result. Awaiting this will return the current state.
///
/// Calling [`ObserveProgress::stream`] will return a stream of updates, where
/// the first item is the current state and subsequent items are updates.
pub struct ObserveProgress {
    inner: future::Boxed<irpc::Result<mpsc::Receiver<Bitfield>>>,
}

impl IntoFuture for ObserveProgress {
    type Output = RequestResult<Bitfield>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut rx = self.inner.await?;
            match rx.recv().await? {
                Some(bitfield) => Ok(bitfield),
                None => Err(io::Error::other("unexpected end of stream").into()),
            }
        })
    }
}

impl ObserveProgress {
    fn new(
        fut: impl Future<Output = irpc::Result<mpsc::Receiver<Bitfield>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Waits until the blob is fully downloaded and returns the complete bitfield.
    ///
    /// This drives the stream until a [`Bitfield`] with `is_complete()` is
    /// received, then returns it. If the stream ends before that, an error is
    /// returned.
    pub async fn await_completion(self) -> RequestResult<Bitfield> {
        let mut stream = self.stream().await?;
        while let Some(item) = stream.next().await {
            if item.is_complete() {
                return Ok(item);
            }
        }
        Err(io::Error::other("unexpected end of stream").into())
    }

    /// Returns an infinite stream of bitfields. The first bitfield is the
    /// current state, and the following bitfields are updates.
    ///
    /// Once a blob is complete, there will be no more updates.
    pub async fn stream(self) -> irpc::Result<impl Stream<Item = Bitfield>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

/// A progress handle for an export operation.
///
/// Internally this is a stream of [`ExportProgress`] items. Working with this
/// stream directly can be inconvenient, so this struct provides some convenience
/// methods to work with the result.
///
/// To get the underlying stream, use the [`ExportProgress::stream`] method.
///
/// It also implements [`IntoFuture`], so you can await it to get the size of the
/// exported blob.
pub struct ExportProgress {
    inner: future::Boxed<irpc::Result<mpsc::Receiver<ExportProgressItem>>>,
}

impl IntoFuture for ExportProgress {
    type Output = RequestResult<u64>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.finish())
    }
}

impl ExportProgress {
    fn new(
        fut: impl Future<Output = irpc::Result<mpsc::Receiver<ExportProgressItem>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Returns the underlying stream of raw [`ExportProgressItem`] progress events.
    pub async fn stream(self) -> impl Stream<Item = ExportProgressItem> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(e) => {
                    co.yield_(ExportProgressItem::Error(e.into())).await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }

    /// Drives the export to completion and returns the number of bytes written.
    pub async fn finish(self) -> RequestResult<u64> {
        let mut rx = self.inner.await?;
        let mut size = None;
        loop {
            match rx.recv().await? {
                Some(ExportProgressItem::Done) => break,
                Some(ExportProgressItem::Size(s)) => size = Some(s),
                Some(ExportProgressItem::Error(cause)) => return Err(cause.into()),
                _ => {}
            }
        }
        if let Some(size) = size {
            Ok(size)
        } else {
            Err(io::Error::other("unexpected end of stream").into())
        }
    }
}

/// A handle for an ongoing BAO import operation.
///
/// Send verified [`bao_tree::io::BaoContentItem`]s to the store via `tx`.
/// Drop `tx` when done, then await `rx` to learn whether the import succeeded.
pub struct ImportBaoHandle {
    /// Channel for sending BAO content items (leaves and parent nodes) to the store.
    pub tx: mpsc::Sender<BaoContentItem>,
    /// Receives the import result once `tx` is dropped.
    pub rx: oneshot::Receiver<super::Result<()>>,
}

impl ImportBaoHandle {
    pub(crate) async fn new(
        fut: impl Future<
                Output = irpc::Result<(
                    mpsc::Sender<BaoContentItem>,
                    oneshot::Receiver<super::Result<()>>,
                )>,
            > + Send
            + 'static,
    ) -> irpc::Result<Self> {
        let (tx, rx) = fut.await?;
        Ok(Self { tx, rx })
    }
}

/// A progress handle for a [`Blobs::list`] operation.
///
/// Use [`BlobsListProgress::hashes`] to collect all hashes at once, or
/// [`BlobsListProgress::stream`] to iterate lazily.
pub struct BlobsListProgress {
    inner: future::Boxed<irpc::Result<mpsc::Receiver<super::Result<Hash>>>>,
}

impl BlobsListProgress {
    fn new(
        fut: impl Future<Output = irpc::Result<mpsc::Receiver<super::Result<Hash>>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Collects all blob hashes into a `Vec`.
    pub async fn hashes(self) -> RequestResult<Vec<Hash>> {
        let mut rx: mpsc::Receiver<Result<Hash, super::Error>> = self.inner.await?;
        let mut hashes = Vec::new();
        while let Some(item) = rx.recv().await? {
            hashes.push(item?);
        }
        Ok(hashes)
    }

    /// Returns a lazy stream of blob hashes.
    pub async fn stream(self) -> irpc::Result<impl Stream<Item = super::Result<Hash>>> {
        let mut rx = self.inner.await?;
        Ok(Gen::new(|co| async move {
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        }))
    }
}

/// A progress handle for a bao export operation.
///
/// Internally, this is a stream of [`EncodedItem`]s. Using this stream directly
/// is often inconvenient, so there are a number of higher level methods to
/// process the stream.
///
/// You can get access to the underlying stream using the [`ExportBaoProgress::stream`] method.
pub struct ExportRangesProgress {
    ranges: RangeSet2<u64>,
    inner: future::Boxed<irpc::Result<mpsc::Receiver<ExportRangesItem>>>,
}

impl ExportRangesProgress {
    fn new(
        ranges: RangeSet2<u64>,
        fut: impl Future<Output = irpc::Result<mpsc::Receiver<ExportRangesItem>>> + Send + 'static,
    ) -> Self {
        Self {
            ranges,
            inner: Box::pin(fut),
        }
    }
}

impl ExportRangesProgress {
    /// A raw stream of [`ExportRangesItem`]s.
    ///
    /// Ranges will be rounded up to chunk boundaries. So if you request a
    /// range of 0..100, you will get the entire first chunk, 0..1024.
    ///
    /// It is up to the caller to clip the ranges to the requested ranges.
    pub fn stream(self) -> impl Stream<Item = ExportRangesItem> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(e) => {
                    co.yield_(ExportRangesItem::Error(e.into())).await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }

    /// Concatenate all the data into a single `Bytes`.
    pub async fn concatenate(self) -> RequestResult<Vec<u8>> {
        let mut rx = self.inner.await?;
        let mut data = BTreeMap::new();
        while let Some(item) = rx.recv().await? {
            match item {
                ExportRangesItem::Size(_) => {}
                ExportRangesItem::Data(leaf) => {
                    data.insert(leaf.offset, leaf.data);
                }
                ExportRangesItem::Error(cause) => return Err(cause.into()),
            }
        }
        let mut res = Vec::new();
        for range in self.ranges.iter() {
            let (start, end) = match range {
                RangeSetRange::RangeFrom(range) => (*range.start, u64::MAX),
                RangeSetRange::Range(range) => (*range.start, *range.end),
            };
            for (offset, data) in data.iter() {
                let cstart = *offset;
                let cend = *offset + (data.len() as u64);
                if cstart >= end || cend <= start {
                    continue;
                }
                let start = start.max(cstart);
                let end = end.min(cend);
                let data = &data[(start - cstart) as usize..(end - cstart) as usize];
                res.extend_from_slice(data);
            }
        }
        Ok(res)
    }
}

/// A progress handle for a bao export operation.
///
/// Internally, this is a stream of [`EncodedItem`]s. Using this stream directly
/// is often inconvenient, so there are a number of higher level methods to
/// process the stream.
///
/// You can get access to the underlying stream using the [`ExportBaoProgress::stream`] method.
pub struct ExportBaoProgress {
    inner: future::Boxed<irpc::Result<mpsc::Receiver<EncodedItem>>>,
}

impl ExportBaoProgress {
    fn new(
        fut: impl Future<Output = irpc::Result<mpsc::Receiver<EncodedItem>>> + Send + 'static,
    ) -> Self {
        Self {
            inner: Box::pin(fut),
        }
    }

    /// Interprets this blob as a hash sequence and returns a stream of hashes.
    ///
    /// Errors will be reported, but the iterator will nevertheless continue.
    /// If you get an error despite having asked for ranges that should be present,
    /// this means that the data is corrupted. It can still make sense to continue
    /// to get all non-corrupted sections.
    pub fn hashes_with_index(
        self,
    ) -> impl Stream<Item = std::result::Result<(u64, Hash), AnyError>> {
        let mut stream = self.stream();
        Gen::new(|co| async move {
            while let Some(item) = stream.next().await {
                let leaf = match item {
                    EncodedItem::Leaf(leaf) => leaf,
                    EncodedItem::Error(e) => {
                        co.yield_(Err(AnyError::from_std(e))).await;
                        continue;
                    }
                    _ => continue,
                };
                let slice = match HashSeqChunk::try_from(leaf) {
                    Ok(slice) => slice,
                    Err(e) => {
                        co.yield_(Err(e)).await;
                        continue;
                    }
                };
                let offset = slice.base();
                for (o, hash) in slice.into_iter().enumerate() {
                    co.yield_(Ok((offset + o as u64, hash))).await;
                }
            }
        })
    }

    /// Same as [`Self::hashes_with_index`], but without the indexes.
    pub fn hashes(self) -> impl Stream<Item = std::result::Result<Hash, AnyError>> {
        self.hashes_with_index().map(|x| x.map(|(_, hash)| hash))
    }

    /// Collects the full BAO-encoded bytes (size header + hash tree + leaves) into a `Vec`.
    pub async fn bao_to_vec(self) -> RequestResult<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

    /// Collects the raw data payload (leaves only, no hash-tree headers) into `Bytes`.
    pub async fn data_to_bytes(self) -> super::ExportBaoResult<Bytes> {
        let mut rx = self.inner.await?;
        let mut data = Vec::new();
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Leaf(leaf) => {
                    data.push(leaf.data);
                }
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        if data.len() == 1 {
            Ok(data.pop().unwrap())
        } else {
            let mut out = Vec::new();
            for item in data {
                out.extend_from_slice(&item);
            }
            Ok(out.into())
        }
    }

    /// Collects the raw data payload into a `Vec<u8>`.
    pub async fn data_to_vec(self) -> super::ExportBaoResult<Vec<u8>> {
        let mut rx = self.inner.await?;
        let mut data = Vec::new();
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Leaf(leaf) => {
                    data.extend_from_slice(&leaf.data);
                }
                EncodedItem::Parent(_) => {}
                EncodedItem::Size(_) => {}
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(data)
    }

    /// Writes the full BAO-encoded stream (size header, hash-tree nodes, and leaves) to `target`.
    pub async fn write<W: AsyncStreamWriter>(self, target: &mut W) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    target.write(&size.to_le_bytes()).await?;
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    target.write(&data).await?;
                }
                EncodedItem::Leaf(leaf) => {
                    target.write_bytes(leaf.data).await?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

    /// Write quinn variant that also feeds a progress writer.
    pub(crate) async fn write_with_progress<W: crate::util::SendStream>(
        self,
        writer: &mut W,
        progress: &mut impl WriteProgress,
        hash: &Hash,
        index: u64,
    ) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    progress.send_transfer_started(index, hash, size).await;
                    writer.send(&size.to_le_bytes()).await?;
                    progress.log_other_write(8);
                }
                EncodedItem::Parent(parent) => {
                    let mut data = [0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    writer.send(&data).await?;
                    progress.log_other_write(64);
                }
                EncodedItem::Leaf(leaf) => {
                    let len = leaf.data.len();
                    writer.send_bytes(leaf.data).await?;
                    progress
                        .notify_payload_write(index, leaf.offset, len)
                        .await?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

    /// Returns the BAO stream as a stream of raw byte chunks.
    ///
    /// Each chunk is either the 8-byte size header, a 64-byte hash-tree parent
    /// node, or a data leaf. The chunks arrive in BAO wire order.
    pub fn into_byte_stream(self) -> impl Stream<Item = super::Result<Bytes>> {
        self.stream().filter_map(|item| match item {
            EncodedItem::Size(size) => {
                let size = size.to_le_bytes().to_vec().into();
                Some(Ok(size))
            }
            EncodedItem::Parent(parent) => {
                let mut data = vec![0u8; 64];
                data[..32].copy_from_slice(parent.pair.0.as_bytes());
                data[32..].copy_from_slice(parent.pair.1.as_bytes());
                Some(Ok(data.into()))
            }
            EncodedItem::Leaf(leaf) => Some(Ok(leaf.data)),
            EncodedItem::Done => None,
            EncodedItem::Error(cause) => Some(Err(cause.into())),
        })
    }

    /// Returns the underlying stream of [`EncodedItem`]s.
    ///
    /// Items arrive in BAO order: size header first, then interleaved hash-tree
    /// parents and data leaves, ending with a `Done` or `Error` sentinel.
    pub fn stream(self) -> impl Stream<Item = EncodedItem> {
        Gen::new(|co| async move {
            let mut rx = match self.inner.await {
                Ok(rx) => rx,
                Err(cause) => {
                    co.yield_(EncodedItem::Error(io::Error::other(cause).into()))
                        .await;
                    return;
                }
            };
            while let Ok(Some(item)) = rx.recv().await {
                co.yield_(item).await;
            }
        })
    }
}

pub(crate) trait WriteProgress {
    /// Notify the progress writer that a payload write has happened.
    async fn notify_payload_write(&mut self, index: u64, offset: u64, len: usize) -> ClientResult;

    /// Log a write of some other data.
    fn log_other_write(&mut self, len: usize);

    /// Notify the progress writer that a transfer has started.
    async fn send_transfer_started(&mut self, index: u64, hash: &Hash, size: u64);
}
