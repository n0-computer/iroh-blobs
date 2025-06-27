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
use iroh_io::{AsyncStreamReader, TokioStreamReader};
use irpc::channel::{mpsc, oneshot};
use n0_future::{future, stream, Stream, StreamExt};
use quinn::SendStream;
use range_collections::{range_set::RangeSetRange, RangeSet2};
use ref_cast::RefCast;
use tokio::io::AsyncWriteExt;
use tracing::trace;

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
    provider::StreamContext,
    store::IROH_BLOCK_SIZE,
    util::temp_tag::TempTag,
    BlobFormat, Hash, HashAndFormat,
};

/// Options for adding bytes.
#[derive(Debug)]
pub struct AddBytesOptions {
    pub data: Bytes,
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

/// Blobs API
#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Blobs {
    client: ApiClient,
}

impl Blobs {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

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

    pub async fn delete_with_opts(&self, options: DeleteOptions) -> RequestResult<()> {
        trace!("{options:?}");
        self.client.rpc(options).await??;
        Ok(())
    }

    pub async fn delete(
        &self,
        hashes: impl IntoIterator<Item = impl Into<Hash>>,
    ) -> RequestResult<()> {
        self.delete_with_opts(DeleteOptions {
            hashes: hashes.into_iter().map(Into::into).collect(),
            force: false,
        })
        .await
    }

    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> AddProgress {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    pub fn add_bytes(&self, data: impl Into<bytes::Bytes>) -> AddProgress {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(options)
    }

    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> AddProgress {
        let options = options.into();
        let request = ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: Scope::GLOBAL,
        };
        self.add_bytes_impl(request)
    }

    fn add_bytes_impl(&self, options: ImportBytesRequest) -> AddProgress {
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

    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> AddProgress {
        let options = options.into();
        self.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: Scope::GLOBAL,
        })
    }

    fn add_path_with_opts_impl(&self, options: ImportPathRequest) -> AddProgress {
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

    pub fn add_path(&self, path: impl AsRef<Path>) -> AddProgress {
        self.add_path_with_opts(AddPathOptions {
            path: path.as_ref().to_owned(),
            mode: ImportMode::Copy,
            format: BlobFormat::Raw,
        })
    }

    pub async fn add_stream(
        &self,
        data: impl Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    ) -> AddProgress {
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
                anyhow::Ok(())
            };
            let _ = tokio::join!(send, recv);
        });
        AddProgress::new(self, stream)
    }

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

    pub fn export_ranges_with_opts(&self, options: ExportRangesOptions) -> ExportRangesProgress {
        trace!("{options:?}");
        ExportRangesProgress::new(
            options.ranges.clone(),
            self.client.server_streaming(options, 32),
        )
    }

    pub fn export_bao_with_opts(
        &self,
        options: ExportBaoOptions,
        local_update_cap: usize,
    ) -> ExportBaoProgress {
        trace!("{options:?}");
        ExportBaoProgress::new(self.client.server_streaming(options, local_update_cap))
    }

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

    pub fn export_with_opts(&self, options: ExportOptions) -> ExportProgress {
        trace!("{:?}", options);
        ExportProgress::new(self.client.server_streaming(options, 32))
    }

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
    async fn import_bao_reader<R: AsyncStreamReader>(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        mut reader: R,
    ) -> RequestResult<R> {
        let size = u64::from_le_bytes(reader.read::<8>().await.map_err(super::Error::other)?);
        let Some(size) = NonZeroU64::new(size) else {
            return if hash == Hash::EMPTY {
                Ok(reader)
            } else {
                Err(super::Error::other("invalid size for hash").into())
            };
        };
        let tree = BaoTree::new(size.get(), IROH_BLOCK_SIZE);
        let mut decoder = ResponseDecoder::new(hash.into(), ranges, tree, reader);
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
        Ok(reader?)
    }

    #[cfg_attr(feature = "hide-proto-docs", doc(hidden))]
    pub async fn import_bao_quinn(
        &self,
        hash: Hash,
        ranges: ChunkRanges,
        stream: &mut iroh::endpoint::RecvStream,
    ) -> RequestResult<()> {
        let reader = TokioStreamReader::new(stream);
        self.import_bao_reader(hash, ranges, reader).await?;
        Ok(())
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

    pub fn list(&self) -> BlobsListProgress {
        let msg = ListRequest;
        let client = self.client.clone();
        BlobsListProgress::new(client.server_streaming(msg, 32))
    }

    pub async fn status(&self, hash: impl Into<Hash>) -> irpc::Result<BlobStatus> {
        let hash = hash.into();
        let msg = BlobStatusRequest { hash };
        self.client.rpc(msg).await
    }

    pub async fn has(&self, hash: impl Into<Hash>) -> irpc::Result<bool> {
        match self.status(hash).await? {
            BlobStatus::Complete { .. } => Ok(true),
            _ => Ok(false),
        }
    }

    pub(crate) async fn clear_protected(&self) -> RequestResult<()> {
        let msg = ClearProtectedRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }
}

/// A progress handle for a batch scoped add operation.
pub struct BatchAddProgress<'a>(AddProgress<'a>);

impl<'a> IntoFuture for BatchAddProgress<'a> {
    type Output = RequestResult<TempTag>;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.temp_tag())
    }
}

impl<'a> BatchAddProgress<'a> {
    pub async fn with_named_tag(self, name: impl AsRef<[u8]>) -> RequestResult<HashAndFormat> {
        self.0.with_named_tag(name).await
    }

    pub async fn with_tag(self) -> RequestResult<TagInfo> {
        self.0.with_tag().await
    }

    pub async fn stream(self) -> impl Stream<Item = AddProgressItem> {
        self.0.stream().await
    }

    pub async fn temp_tag(self) -> RequestResult<TempTag> {
        self.0.temp_tag().await
    }
}

/// A batch of operations that modify the blob store.
pub struct Batch<'a> {
    scope: Scope,
    blobs: &'a Blobs,
    _tx: mpsc::Sender<BatchResponse>,
}

impl<'a> Batch<'a> {
    pub fn add_bytes(&self, data: impl Into<Bytes>) -> BatchAddProgress {
        let options = ImportBytesRequest {
            data: data.into(),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        BatchAddProgress(self.blobs.add_bytes_impl(options))
    }

    pub fn add_bytes_with_opts(&self, options: impl Into<AddBytesOptions>) -> BatchAddProgress {
        let options = options.into();
        BatchAddProgress(self.blobs.add_bytes_impl(ImportBytesRequest {
            data: options.data,
            format: options.format,
            scope: self.scope,
        }))
    }

    pub fn add_slice(&self, data: impl AsRef<[u8]>) -> BatchAddProgress {
        let options = ImportBytesRequest {
            data: Bytes::copy_from_slice(data.as_ref()),
            format: crate::BlobFormat::Raw,
            scope: self.scope,
        };
        BatchAddProgress(self.blobs.add_bytes_impl(options))
    }

    pub fn add_path_with_opts(&self, options: impl Into<AddPathOptions>) -> BatchAddProgress {
        let options = options.into();
        BatchAddProgress(self.blobs.add_path_with_opts_impl(ImportPathRequest {
            path: options.path,
            mode: options.mode,
            format: options.format,
            scope: self.scope,
        }))
    }

    pub async fn temp_tag(&self, value: impl Into<HashAndFormat>) -> irpc::Result<TempTag> {
        let value = value.into();
        let msg = CreateTempTagRequest {
            scope: self.scope,
            value,
        };
        self.blobs.client.rpc(msg).await
    }
}

/// Options for adding data from a file system path.
#[derive(Debug)]
pub struct AddPathOptions {
    pub path: PathBuf,
    pub format: BlobFormat,
    pub mode: ImportMode,
}

/// A progress handle for an import operation.
///
/// Internally this is a stream of [`AddProgressItem`] items. Working with this
/// stream directly can be inconvenient, so this struct provides some convenience
/// methods to work with the result.
///
/// It also implements [`IntoFuture`], so you can await it to get the [`TempTag`] that
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

    pub async fn temp_tag(self) -> RequestResult<TempTag> {
        let mut stream = self.inner;
        while let Some(item) = stream.next().await {
            match item {
                AddProgressItem::Done(tt) => return Ok(tt),
                AddProgressItem::Error(e) => return Err(e.into()),
                _ => {}
            }
        }
        Err(super::Error::other("unexpected end of stream").into())
    }

    pub async fn with_named_tag(self, name: impl AsRef<[u8]>) -> RequestResult<HashAndFormat> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let haf = *tt.hash_and_format();
        let tags = Tags::ref_from_sender(&blobs.client);
        tags.set(name, *tt.hash_and_format()).await?;
        drop(tt);
        Ok(haf)
    }

    pub async fn with_tag(self) -> RequestResult<TagInfo> {
        let blobs = self.blobs.clone();
        let tt = self.temp_tag().await?;
        let hash = *tt.hash();
        let format = tt.format();
        let tags = Tags::ref_from_sender(&blobs.client);
        let name = tags.create(*tt.hash_and_format()).await?;
        drop(tt);
        Ok(TagInfo { name, hash, format })
    }

    pub async fn stream(self) -> impl Stream<Item = AddProgressItem> {
        self.inner
    }
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
                None => Err(super::Error::other("unexpected end of stream").into()),
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

    pub async fn await_completion(self) -> RequestResult<Bitfield> {
        let mut stream = self.stream().await?;
        while let Some(item) = stream.next().await {
            if item.is_complete() {
                return Ok(item);
            }
        }
        Err(super::Error::other("unexpected end of stream").into())
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
            Err(super::Error::other("unexpected end of stream").into())
        }
    }
}

/// A handle for an ongoing bao import operation.
pub struct ImportBaoHandle {
    pub tx: mpsc::Sender<BaoContentItem>,
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

/// A progress handle for a blobs list operation.
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

    pub async fn hashes(self) -> RequestResult<Vec<Hash>> {
        let mut rx: mpsc::Receiver<Result<Hash, super::Error>> = self.inner.await?;
        let mut hashes = Vec::new();
        while let Some(item) = rx.recv().await? {
            hashes.push(item?);
        }
        Ok(hashes)
    }

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
    pub async fn stream(self) -> impl Stream<Item = ExportRangesItem> {
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
    ) -> impl Stream<Item = std::result::Result<(u64, Hash), anyhow::Error>> {
        let mut stream = self.stream();
        Gen::new(|co| async move {
            while let Some(item) = stream.next().await {
                let leaf = match item {
                    EncodedItem::Leaf(leaf) => leaf,
                    EncodedItem::Error(e) => {
                        co.yield_(Err(e.into())).await;
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
    pub fn hashes(self) -> impl Stream<Item = std::result::Result<Hash, anyhow::Error>> {
        self.hashes_with_index().map(|x| x.map(|(_, hash)| hash))
    }

    pub async fn bao_to_vec(self) -> RequestResult<Vec<u8>> {
        let mut data = Vec::new();
        let mut stream = self.into_byte_stream();
        while let Some(item) = stream.next().await {
            println!("item: {item:?}");
            data.extend_from_slice(&item?);
        }
        Ok(data)
    }

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

    pub async fn write_quinn(self, target: &mut quinn::SendStream) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    target.write_u64_le(size).await?;
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    target.write_all(&data).await.map_err(io::Error::from)?;
                }
                EncodedItem::Leaf(leaf) => {
                    target
                        .write_chunk(leaf.data)
                        .await
                        .map_err(io::Error::from)?;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

    /// Write quinn variant that also feeds a progress writer.
    pub(crate) async fn write_quinn_with_progress(
        self,
        writer: &mut SendStream,
        progress: &mut impl WriteProgress,
        hash: &Hash,
        index: u64,
    ) -> super::ExportBaoResult<()> {
        let mut rx = self.inner.await?;
        while let Some(item) = rx.recv().await? {
            match item {
                EncodedItem::Size(size) => {
                    progress.send_transfer_started(index, hash, size).await;
                    writer.write_u64_le(size).await?;
                    progress.log_other_write(8);
                }
                EncodedItem::Parent(parent) => {
                    let mut data = vec![0u8; 64];
                    data[..32].copy_from_slice(parent.pair.0.as_bytes());
                    data[32..].copy_from_slice(parent.pair.1.as_bytes());
                    writer.write_all(&data).await.map_err(io::Error::from)?;
                    progress.log_other_write(64);
                }
                EncodedItem::Leaf(leaf) => {
                    let len = leaf.data.len();
                    writer
                        .write_chunk(leaf.data)
                        .await
                        .map_err(io::Error::from)?;
                    progress.notify_payload_write(index, leaf.offset, len).await;
                }
                EncodedItem::Done => break,
                EncodedItem::Error(cause) => return Err(cause.into()),
            }
        }
        Ok(())
    }

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
            EncodedItem::Error(cause) => Some(Err(super::Error::other(cause))),
        })
    }

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
    async fn notify_payload_write(&mut self, index: u64, offset: u64, len: usize);

    /// Log a write of some other data.
    fn log_other_write(&mut self, len: usize);

    /// Notify the progress writer that a transfer has started.
    async fn send_transfer_started(&mut self, index: u64, hash: &Hash, size: u64);
}

impl WriteProgress for StreamContext {
    async fn notify_payload_write(&mut self, index: u64, offset: u64, len: usize) {
        StreamContext::notify_payload_write(self, index, offset, len);
    }

    fn log_other_write(&mut self, len: usize) {
        StreamContext::log_other_write(self, len);
    }

    async fn send_transfer_started(&mut self, index: u64, hash: &Hash, size: u64) {
        StreamContext::send_transfer_started(self, index, hash, size).await
    }
}
