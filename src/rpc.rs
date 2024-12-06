//! Provides a rpc protocol as well as a client for the protocol

use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use client::{
    blobs::{BlobInfo, BlobStatus, IncompleteBlobInfo, MemClient, WrapOption},
    tags::TagInfo,
    MemConnector,
};
use futures_buffered::BufferedStreamExt;
use futures_lite::StreamExt;
use futures_util::{FutureExt, Stream};
use genawaiter::sync::{Co, Gen};
use iroh_base::hash::{BlobFormat, HashAndFormat};
use iroh_io::AsyncSliceReader;
use proto::{
    blobs::{
        AddPathRequest, AddPathResponse, AddStreamRequest, AddStreamResponse, AddStreamUpdate,
        BatchAddPathRequest, BatchAddPathResponse, BatchAddStreamRequest, BatchAddStreamResponse,
        BatchAddStreamUpdate, BatchCreateRequest, BatchCreateResponse, BatchCreateTempTagRequest,
        BatchUpdate, BlobStatusRequest, BlobStatusResponse, ConsistencyCheckRequest,
        CreateCollectionRequest, CreateCollectionResponse, DeleteRequest, DownloadResponse,
        ExportRequest, ExportResponse, ListIncompleteRequest, ListRequest, ReadAtRequest,
        ReadAtResponse, ValidateRequest,
    },
    tags::{
        CreateRequest as TagsCreateRequest, DeleteRequest as TagDeleteRequest,
        ListRequest as TagListRequest, SetRequest as TagsSetRequest, SyncMode,
    },
    Request, RpcError, RpcResult, RpcService,
};
use quic_rpc::{
    server::{ChannelTypes, RpcChannel, RpcServerError},
    RpcClient, RpcServer,
};
use tokio_util::task::AbortOnDropHandle;

use crate::{
    export::ExportProgress,
    format::collection::Collection,
    get::db::DownloadProgress,
    net_protocol::{BlobDownloadRequest, Blobs},
    provider::{AddProgress, BatchAddPathProgress},
    store::{ConsistencyCheckProgress, ImportProgress, MapEntry, ValidateProgress},
    util::{
        progress::{AsyncChannelProgressSender, ProgressSender},
        SetTagOption,
    },
    Tag,
};
pub mod client;
pub mod proto;

/// Chunk size for getting blobs over RPC
const RPC_BLOB_GET_CHUNK_SIZE: usize = 1024 * 64;
/// Channel cap for getting blobs over RPC
const RPC_BLOB_GET_CHANNEL_CAP: usize = 2;

impl<D: crate::store::Store> Blobs<D> {
    /// Get a client for the blobs protocol
    pub fn client(&self) -> RpcHandler {
        RpcHandler::new(self)
    }

    /// Handle an RPC request
    pub async fn handle_rpc_request<C>(
        self,
        msg: Request,
        chan: RpcChannel<RpcService, C>,
    ) -> std::result::Result<(), RpcServerError<C>>
    where
        C: ChannelTypes<RpcService>,
    {
        match msg {
            Request::Blobs(msg) => self.handle_blobs_request(msg, chan).await,
            Request::Tags(msg) => self.handle_tags_request(msg, chan).await,
        }
    }

    /// Handle a tags request
    pub async fn handle_tags_request<C>(
        self,
        msg: proto::tags::Request,
        chan: RpcChannel<proto::RpcService, C>,
    ) -> std::result::Result<(), RpcServerError<C>>
    where
        C: ChannelTypes<proto::RpcService>,
    {
        use proto::tags::Request::*;
        match msg {
            Create(msg) => chan.rpc(msg, self, Self::tags_create).await,
            Set(msg) => chan.rpc(msg, self, Self::tags_set).await,
            DeleteTag(msg) => chan.rpc(msg, self, Self::blob_delete_tag).await,
            ListTags(msg) => chan.server_streaming(msg, self, Self::blob_list_tags).await,
        }
    }

    /// Handle a blobs request
    pub async fn handle_blobs_request<C>(
        self,
        msg: proto::blobs::Request,
        chan: RpcChannel<proto::RpcService, C>,
    ) -> std::result::Result<(), RpcServerError<C>>
    where
        C: ChannelTypes<proto::RpcService>,
    {
        use proto::blobs::Request::*;
        match msg {
            List(msg) => chan.server_streaming(msg, self, Self::blob_list).await,
            ListIncomplete(msg) => {
                chan.server_streaming(msg, self, Self::blob_list_incomplete)
                    .await
            }
            CreateCollection(msg) => chan.rpc(msg, self, Self::create_collection).await,
            Delete(msg) => chan.rpc(msg, self, Self::blob_delete_blob).await,
            AddPath(msg) => {
                chan.server_streaming(msg, self, Self::blob_add_from_path)
                    .await
            }
            Download(msg) => chan.server_streaming(msg, self, Self::blob_download).await,
            Export(msg) => chan.server_streaming(msg, self, Self::blob_export).await,
            Validate(msg) => chan.server_streaming(msg, self, Self::blob_validate).await,
            Fsck(msg) => {
                chan.server_streaming(msg, self, Self::blob_consistency_check)
                    .await
            }
            ReadAt(msg) => chan.server_streaming(msg, self, Self::blob_read_at).await,
            AddStream(msg) => chan.bidi_streaming(msg, self, Self::blob_add_stream).await,
            AddStreamUpdate(_msg) => Err(RpcServerError::UnexpectedUpdateMessage),
            BlobStatus(msg) => chan.rpc(msg, self, Self::blob_status).await,
            BatchCreate(msg) => chan.bidi_streaming(msg, self, Self::batch_create).await,
            BatchUpdate(_) => Err(RpcServerError::UnexpectedStartMessage),
            BatchAddStream(msg) => chan.bidi_streaming(msg, self, Self::batch_add_stream).await,
            BatchAddStreamUpdate(_) => Err(RpcServerError::UnexpectedStartMessage),
            BatchAddPath(msg) => {
                chan.server_streaming(msg, self, Self::batch_add_from_path)
                    .await
            }
            BatchCreateTempTag(msg) => chan.rpc(msg, self, Self::batch_create_temp_tag).await,
        }
    }

    async fn blob_status(self, msg: BlobStatusRequest) -> RpcResult<BlobStatusResponse> {
        let blobs = self;
        let entry = blobs
            .store()
            .get(&msg.hash)
            .await
            .map_err(|e| RpcError::new(&e))?;
        Ok(BlobStatusResponse(match entry {
            Some(entry) => {
                if entry.is_complete() {
                    BlobStatus::Complete {
                        size: entry.size().value(),
                    }
                } else {
                    BlobStatus::Partial { size: entry.size() }
                }
            }
            None => BlobStatus::NotFound,
        }))
    }

    async fn blob_list_impl(self, co: &Co<RpcResult<BlobInfo>>) -> io::Result<()> {
        use bao_tree::io::fsm::Outboard;

        let blobs = self;
        let db = blobs.store();
        for blob in db.blobs().await? {
            let blob = blob?;
            let Some(entry) = db.get(&blob).await? else {
                continue;
            };
            let hash = entry.hash();
            let size = entry.outboard().await?.tree().size();
            let path = "".to_owned();
            co.yield_(Ok(BlobInfo { hash, size, path })).await;
        }
        Ok(())
    }

    async fn blob_list_incomplete_impl(
        self,
        co: &Co<RpcResult<IncompleteBlobInfo>>,
    ) -> io::Result<()> {
        let blobs = self;
        let db = blobs.store();
        for hash in db.partial_blobs().await? {
            let hash = hash?;
            let Ok(Some(entry)) = db.get_mut(&hash).await else {
                continue;
            };
            if entry.is_complete() {
                continue;
            }
            let size = 0;
            let expected_size = entry.size().value();
            co.yield_(Ok(IncompleteBlobInfo {
                hash,
                size,
                expected_size,
            }))
            .await;
        }
        Ok(())
    }

    fn blob_list(
        self,
        _msg: ListRequest,
    ) -> impl Stream<Item = RpcResult<BlobInfo>> + Send + 'static {
        Gen::new(|co| async move {
            if let Err(e) = self.blob_list_impl(&co).await {
                co.yield_(Err(RpcError::new(&e))).await;
            }
        })
    }

    fn blob_list_incomplete(
        self,
        _msg: ListIncompleteRequest,
    ) -> impl Stream<Item = RpcResult<IncompleteBlobInfo>> + Send + 'static {
        Gen::new(move |co| async move {
            if let Err(e) = self.blob_list_incomplete_impl(&co).await {
                co.yield_(Err(RpcError::new(&e))).await;
            }
        })
    }

    async fn blob_delete_tag(self, msg: TagDeleteRequest) -> RpcResult<()> {
        self.store()
            .set_tag(msg.name, None)
            .await
            .map_err(|e| RpcError::new(&e))?;
        Ok(())
    }

    async fn blob_delete_blob(self, msg: DeleteRequest) -> RpcResult<()> {
        self.store()
            .delete(vec![msg.hash])
            .await
            .map_err(|e| RpcError::new(&e))?;
        Ok(())
    }

    fn blob_list_tags(self, msg: TagListRequest) -> impl Stream<Item = TagInfo> + Send + 'static {
        tracing::info!("blob_list_tags");
        let blobs = self;
        Gen::new(|co| async move {
            let tags = blobs.store().tags().await.unwrap();
            #[allow(clippy::manual_flatten)]
            for item in tags {
                if let Ok((name, HashAndFormat { hash, format })) = item {
                    if (format.is_raw() && msg.raw) || (format.is_hash_seq() && msg.hash_seq) {
                        co.yield_(TagInfo { name, hash, format }).await;
                    }
                }
            }
        })
    }

    /// Invoke validate on the database and stream out the result
    fn blob_validate(
        self,
        msg: ValidateRequest,
    ) -> impl Stream<Item = ValidateProgress> + Send + 'static {
        let (tx, rx) = async_channel::bounded(1);
        let tx2 = tx.clone();
        let blobs = self;
        tokio::task::spawn(async move {
            if let Err(e) = blobs
                .store()
                .validate(msg.repair, AsyncChannelProgressSender::new(tx).boxed())
                .await
            {
                tx2.send(ValidateProgress::Abort(RpcError::new(&e)))
                    .await
                    .ok();
            }
        });
        rx
    }

    /// Invoke validate on the database and stream out the result
    fn blob_consistency_check(
        self,
        msg: ConsistencyCheckRequest,
    ) -> impl Stream<Item = ConsistencyCheckProgress> + Send + 'static {
        let (tx, rx) = async_channel::bounded(1);
        let tx2 = tx.clone();
        let blobs = self;
        tokio::task::spawn(async move {
            if let Err(e) = blobs
                .store()
                .consistency_check(msg.repair, AsyncChannelProgressSender::new(tx).boxed())
                .await
            {
                tx2.send(ConsistencyCheckProgress::Abort(RpcError::new(&e)))
                    .await
                    .ok();
            }
        });
        rx
    }

    fn blob_add_from_path(self, msg: AddPathRequest) -> impl Stream<Item = AddPathResponse> {
        // provide a little buffer so that we don't slow down the sender
        let (tx, rx) = async_channel::bounded(32);
        let tx2 = tx.clone();
        let rt = self.rt().clone();
        rt.spawn_detached(|| async move {
            if let Err(e) = self.blob_add_from_path0(msg, tx).await {
                tx2.send(AddProgress::Abort(RpcError::new(&*e))).await.ok();
            }
        });
        rx.map(AddPathResponse)
    }

    async fn tags_set(self, msg: TagsSetRequest) -> RpcResult<()> {
        let blobs = self;
        blobs
            .store()
            .set_tag(msg.name, msg.value)
            .await
            .map_err(|e| RpcError::new(&e))?;
        if let SyncMode::Full = msg.sync {
            blobs.store().sync().await.map_err(|e| RpcError::new(&e))?;
        }
        if let Some(batch) = msg.batch {
            if let Some(content) = msg.value.as_ref() {
                blobs
                    .batches()
                    .await
                    .remove_one(batch, content)
                    .map_err(|e| RpcError::new(&*e))?;
            }
        }
        Ok(())
    }

    async fn tags_create(self, msg: TagsCreateRequest) -> RpcResult<Tag> {
        let blobs = self;
        let tag = blobs
            .store()
            .create_tag(msg.value)
            .await
            .map_err(|e| RpcError::new(&e))?;
        if let SyncMode::Full = msg.sync {
            blobs.store().sync().await.map_err(|e| RpcError::new(&e))?;
        }
        if let Some(batch) = msg.batch {
            blobs
                .batches()
                .await
                .remove_one(batch, &msg.value)
                .map_err(|e| RpcError::new(&*e))?;
        }
        Ok(tag)
    }

    fn blob_download(self, msg: BlobDownloadRequest) -> impl Stream<Item = DownloadResponse> {
        let (sender, receiver) = async_channel::bounded(1024);
        let endpoint = self.endpoint().clone();
        let progress = AsyncChannelProgressSender::new(sender);

        let blobs_protocol = self.clone();

        self.rt().spawn_detached(move || async move {
            if let Err(err) = blobs_protocol
                .download(endpoint, msg, progress.clone())
                .await
            {
                progress
                    .send(DownloadProgress::Abort(RpcError::new(&*err)))
                    .await
                    .ok();
            }
        });

        receiver.map(DownloadResponse)
    }

    fn blob_export(self, msg: ExportRequest) -> impl Stream<Item = ExportResponse> {
        let (tx, rx) = async_channel::bounded(1024);
        let progress = AsyncChannelProgressSender::new(tx);
        let rt = self.rt().clone();
        rt.spawn_detached(move || async move {
            let res = crate::export::export(
                self.store(),
                msg.hash,
                msg.path,
                msg.format,
                msg.mode,
                progress.clone(),
            )
            .await;
            match res {
                Ok(()) => progress.send(ExportProgress::AllDone).await.ok(),
                Err(err) => progress
                    .send(ExportProgress::Abort(RpcError::new(&*err)))
                    .await
                    .ok(),
            };
        });
        rx.map(ExportResponse)
    }

    async fn blob_add_from_path0(
        self,
        msg: AddPathRequest,
        progress: async_channel::Sender<AddProgress>,
    ) -> anyhow::Result<()> {
        use std::collections::BTreeMap;

        use crate::store::ImportMode;

        let blobs = self.clone();
        let progress = AsyncChannelProgressSender::new(progress);
        let names = Arc::new(Mutex::new(BTreeMap::new()));
        // convert import progress to provide progress
        let import_progress = progress.clone().with_filter_map(move |x| match x {
            ImportProgress::Found { id, name } => {
                names.lock().unwrap().insert(id, name);
                None
            }
            ImportProgress::Size { id, size } => {
                let name = names.lock().unwrap().remove(&id)?;
                Some(AddProgress::Found { id, name, size })
            }
            ImportProgress::OutboardProgress { id, offset } => {
                Some(AddProgress::Progress { id, offset })
            }
            ImportProgress::OutboardDone { hash, id } => Some(AddProgress::Done { hash, id }),
            _ => None,
        });
        let AddPathRequest {
            wrap,
            path: root,
            in_place,
            tag,
        } = msg;
        // Check that the path is absolute and exists.
        anyhow::ensure!(root.is_absolute(), "path must be absolute");
        anyhow::ensure!(
            root.exists(),
            "trying to add missing path: {}",
            root.display()
        );

        let import_mode = match in_place {
            true => ImportMode::TryReference,
            false => ImportMode::Copy,
        };

        let create_collection = match wrap {
            WrapOption::Wrap { .. } => true,
            WrapOption::NoWrap => root.is_dir(),
        };

        let temp_tag = if create_collection {
            // import all files below root recursively
            let data_sources = crate::util::fs::scan_path(root, wrap)?;
            let blobs = self;

            const IO_PARALLELISM: usize = 4;
            let result: Vec<_> = futures_lite::stream::iter(data_sources)
                .map(|source| {
                    let import_progress = import_progress.clone();
                    let blobs = blobs.clone();
                    async move {
                        let name = source.name().to_string();
                        let (tag, size) = blobs
                            .store()
                            .import_file(
                                source.path().to_owned(),
                                import_mode,
                                BlobFormat::Raw,
                                import_progress,
                            )
                            .await?;
                        let hash = *tag.hash();
                        io::Result::Ok((name, hash, size, tag))
                    }
                })
                .buffered_ordered(IO_PARALLELISM)
                .try_collect()
                .await?;

            // create a collection
            let (collection, _child_tags): (Collection, Vec<_>) = result
                .into_iter()
                .map(|(name, hash, _, tag)| ((name, hash), tag))
                .unzip();

            collection.store(blobs.store()).await?
        } else {
            // import a single file
            let (tag, _size) = blobs
                .store()
                .import_file(root, import_mode, BlobFormat::Raw, import_progress)
                .await?;
            tag
        };

        let hash_and_format = temp_tag.inner();
        let HashAndFormat { hash, format } = *hash_and_format;
        let tag = match tag {
            SetTagOption::Named(tag) => {
                blobs
                    .store()
                    .set_tag(tag.clone(), Some(*hash_and_format))
                    .await?;
                tag
            }
            SetTagOption::Auto => blobs.store().create_tag(*hash_and_format).await?,
        };
        progress
            .send(AddProgress::AllDone {
                hash,
                format,
                tag: tag.clone(),
            })
            .await?;
        Ok(())
    }

    async fn batch_create_temp_tag(self, msg: BatchCreateTempTagRequest) -> RpcResult<()> {
        let blobs = self;
        let tag = blobs.store().temp_tag(msg.content);
        blobs.batches().await.store(msg.batch, tag);
        Ok(())
    }

    fn batch_add_stream(
        self,
        msg: BatchAddStreamRequest,
        stream: impl Stream<Item = BatchAddStreamUpdate> + Send + Unpin + 'static,
    ) -> impl Stream<Item = BatchAddStreamResponse> {
        let (tx, rx) = async_channel::bounded(32);
        let this = self.clone();

        self.rt().spawn_detached(|| async move {
            if let Err(err) = this.batch_add_stream0(msg, stream, tx.clone()).await {
                tx.send(BatchAddStreamResponse::Abort(RpcError::new(&*err)))
                    .await
                    .ok();
            }
        });
        rx
    }

    fn batch_add_from_path(
        self,
        msg: BatchAddPathRequest,
    ) -> impl Stream<Item = BatchAddPathResponse> {
        // provide a little buffer so that we don't slow down the sender
        let (tx, rx) = async_channel::bounded(32);
        let tx2 = tx.clone();
        let this = self.clone();
        self.rt().spawn_detached(|| async move {
            if let Err(e) = this.batch_add_from_path0(msg, tx).await {
                tx2.send(BatchAddPathProgress::Abort(RpcError::new(&*e)))
                    .await
                    .ok();
            }
        });
        rx.map(BatchAddPathResponse)
    }

    async fn batch_add_stream0(
        self,
        msg: BatchAddStreamRequest,
        stream: impl Stream<Item = BatchAddStreamUpdate> + Send + Unpin + 'static,
        progress: async_channel::Sender<BatchAddStreamResponse>,
    ) -> anyhow::Result<()> {
        let blobs = self;
        let progress = AsyncChannelProgressSender::new(progress);

        let stream = stream.map(|item| match item {
            BatchAddStreamUpdate::Chunk(chunk) => Ok(chunk),
            BatchAddStreamUpdate::Abort => {
                Err(io::Error::new(io::ErrorKind::Interrupted, "Remote abort"))
            }
        });

        let import_progress = progress.clone().with_filter_map(move |x| match x {
            ImportProgress::OutboardProgress { offset, .. } => {
                Some(BatchAddStreamResponse::OutboardProgress { offset })
            }
            _ => None,
        });
        let (temp_tag, _len) = blobs
            .store()
            .import_stream(stream, msg.format, import_progress)
            .await?;
        let hash = temp_tag.inner().hash;
        blobs.batches().await.store(msg.batch, temp_tag);
        progress
            .send(BatchAddStreamResponse::Result { hash })
            .await?;
        Ok(())
    }

    async fn batch_add_from_path0(
        self,
        msg: BatchAddPathRequest,
        progress: async_channel::Sender<BatchAddPathProgress>,
    ) -> anyhow::Result<()> {
        let progress = AsyncChannelProgressSender::new(progress);
        // convert import progress to provide progress
        let import_progress = progress.clone().with_filter_map(move |x| match x {
            ImportProgress::Size { size, .. } => Some(BatchAddPathProgress::Found { size }),
            ImportProgress::OutboardProgress { offset, .. } => {
                Some(BatchAddPathProgress::Progress { offset })
            }
            ImportProgress::OutboardDone { hash, .. } => Some(BatchAddPathProgress::Done { hash }),
            _ => None,
        });
        let BatchAddPathRequest {
            path: root,
            import_mode,
            format,
            batch,
        } = msg;
        // Check that the path is absolute and exists.
        anyhow::ensure!(root.is_absolute(), "path must be absolute");
        anyhow::ensure!(
            root.exists(),
            "trying to add missing path: {}",
            root.display()
        );
        let blobs = self;
        let (tag, _) = blobs
            .store()
            .import_file(root, import_mode, format, import_progress)
            .await?;
        let hash = *tag.hash();
        blobs.batches().await.store(batch, tag);

        progress.send(BatchAddPathProgress::Done { hash }).await?;
        Ok(())
    }

    fn blob_add_stream(
        self,
        msg: AddStreamRequest,
        stream: impl Stream<Item = AddStreamUpdate> + Send + Unpin + 'static,
    ) -> impl Stream<Item = AddStreamResponse> {
        let (tx, rx) = async_channel::bounded(32);
        let this = self.clone();

        self.rt().spawn_detached(|| async move {
            if let Err(err) = this.blob_add_stream0(msg, stream, tx.clone()).await {
                tx.send(AddProgress::Abort(RpcError::new(&*err))).await.ok();
            }
        });

        rx.map(AddStreamResponse)
    }

    async fn blob_add_stream0(
        self,
        msg: AddStreamRequest,
        stream: impl Stream<Item = AddStreamUpdate> + Send + Unpin + 'static,
        progress: async_channel::Sender<AddProgress>,
    ) -> anyhow::Result<()> {
        let progress = AsyncChannelProgressSender::new(progress);

        let stream = stream.map(|item| match item {
            AddStreamUpdate::Chunk(chunk) => Ok(chunk),
            AddStreamUpdate::Abort => {
                Err(io::Error::new(io::ErrorKind::Interrupted, "Remote abort"))
            }
        });

        let name_cache = Arc::new(Mutex::new(None));
        let import_progress = progress.clone().with_filter_map(move |x| match x {
            ImportProgress::Found { id: _, name } => {
                let _ = name_cache.lock().unwrap().insert(name);
                None
            }
            ImportProgress::Size { id, size } => {
                let name = name_cache.lock().unwrap().take()?;
                Some(AddProgress::Found { id, name, size })
            }
            ImportProgress::OutboardProgress { id, offset } => {
                Some(AddProgress::Progress { id, offset })
            }
            ImportProgress::OutboardDone { hash, id } => Some(AddProgress::Done { hash, id }),
            _ => None,
        });
        let blobs = self;
        let (temp_tag, _len) = blobs
            .store()
            .import_stream(stream, BlobFormat::Raw, import_progress)
            .await?;
        let hash_and_format = *temp_tag.inner();
        let HashAndFormat { hash, format } = hash_and_format;
        let tag = match msg.tag {
            SetTagOption::Named(tag) => {
                blobs
                    .store()
                    .set_tag(tag.clone(), Some(hash_and_format))
                    .await?;
                tag
            }
            SetTagOption::Auto => blobs.store().create_tag(hash_and_format).await?,
        };
        progress
            .send(AddProgress::AllDone { hash, tag, format })
            .await?;
        Ok(())
    }

    fn blob_read_at(
        self,
        req: ReadAtRequest,
    ) -> impl Stream<Item = RpcResult<ReadAtResponse>> + Send + 'static {
        let (tx, rx) = async_channel::bounded(RPC_BLOB_GET_CHANNEL_CAP);
        let db = self.store().clone();
        self.rt().spawn_detached(move || async move {
            if let Err(err) = read_loop(req, db, tx.clone(), RPC_BLOB_GET_CHUNK_SIZE).await {
                tx.send(RpcResult::Err(RpcError::new(&*err))).await.ok();
            }
        });

        async fn read_loop<D: crate::store::Store>(
            req: ReadAtRequest,
            db: D,
            tx: async_channel::Sender<RpcResult<ReadAtResponse>>,
            max_chunk_size: usize,
        ) -> anyhow::Result<()> {
            let entry = db.get(&req.hash).await?;
            let entry = entry.ok_or_else(|| anyhow!("Blob not found"))?;
            let size = entry.size();

            anyhow::ensure!(
                req.offset <= size.value(),
                "requested offset is out of range: {} > {:?}",
                req.offset,
                size
            );

            let len: usize = req
                .len
                .as_result_len(size.value() - req.offset)
                .try_into()?;

            anyhow::ensure!(
                req.offset + len as u64 <= size.value(),
                "requested range is out of bounds: offset: {}, len: {} > {:?}",
                req.offset,
                len,
                size
            );

            tx.send(Ok(ReadAtResponse::Entry {
                size,
                is_complete: entry.is_complete(),
            }))
            .await?;
            let mut reader = entry.data_reader().await?;

            let (num_chunks, chunk_size) = if len <= max_chunk_size {
                (1, len)
            } else {
                let num_chunks = len / max_chunk_size + (len % max_chunk_size != 0) as usize;
                (num_chunks, max_chunk_size)
            };

            let mut read = 0u64;
            for i in 0..num_chunks {
                let chunk_size = if i == num_chunks - 1 {
                    // last chunk might be smaller
                    len - read as usize
                } else {
                    chunk_size
                };
                let chunk = reader.read_at(req.offset + read, chunk_size).await?;
                let chunk_len = chunk.len();
                if !chunk.is_empty() {
                    tx.send(Ok(ReadAtResponse::Data { chunk })).await?;
                }
                if chunk_len < chunk_size {
                    break;
                } else {
                    read += chunk_len as u64;
                }
            }
            Ok(())
        }

        rx
    }

    fn batch_create(
        self,
        _: BatchCreateRequest,
        mut updates: impl Stream<Item = BatchUpdate> + Send + Unpin + 'static,
    ) -> impl Stream<Item = BatchCreateResponse> {
        let blobs = self;
        async move {
            let batch = blobs.batches().await.create();
            tokio::spawn(async move {
                while let Some(item) = updates.next().await {
                    match item {
                        BatchUpdate::Drop(content) => {
                            // this can not fail, since we keep the batch alive.
                            // therefore it is safe to ignore the result.
                            let _ = blobs.batches().await.remove_one(batch, &content);
                        }
                        BatchUpdate::Ping => {}
                    }
                }
                blobs.batches().await.remove(batch);
            });
            BatchCreateResponse::Id(batch)
        }
        .into_stream()
    }

    async fn create_collection(
        self,
        req: CreateCollectionRequest,
    ) -> RpcResult<CreateCollectionResponse> {
        let CreateCollectionRequest {
            collection,
            tag,
            tags_to_delete,
        } = req;

        let blobs = self;

        let temp_tag = collection
            .store(blobs.store())
            .await
            .map_err(|e| RpcError::new(&*e))?;
        let hash_and_format = temp_tag.inner();
        let HashAndFormat { hash, .. } = *hash_and_format;
        let tag = match tag {
            SetTagOption::Named(tag) => {
                blobs
                    .store()
                    .set_tag(tag.clone(), Some(*hash_and_format))
                    .await
                    .map_err(|e| RpcError::new(&e))?;
                tag
            }
            SetTagOption::Auto => blobs
                .store()
                .create_tag(*hash_and_format)
                .await
                .map_err(|e| RpcError::new(&e))?,
        };

        for tag in tags_to_delete {
            blobs
                .store()
                .set_tag(tag, None)
                .await
                .map_err(|e| RpcError::new(&e))?;
        }

        Ok(CreateCollectionResponse { hash, tag })
    }
}

/// A rpc handler for the blobs rpc protocol
///
/// This struct contains both a task that handles rpc requests and a client
/// that can be used to send rpc requests. Dropping it will stop the handler task,
/// so you need to put it somewhere where it will be kept alive.
#[derive(Debug)]
pub struct RpcHandler {
    /// Client to hand out
    client: MemClient,
    /// Handler task
    _handler: AbortOnDropHandle<()>,
}

impl Deref for RpcHandler {
    type Target = MemClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl RpcHandler {
    fn new<D: crate::store::Store>(blobs: &Blobs<D>) -> Self {
        let blobs = blobs.clone();
        let (listener, connector) = quic_rpc::transport::flume::channel(1);
        let listener = RpcServer::new(listener);
        let client = RpcClient::new(connector);
        let client = MemClient::new(client);
        let _handler = listener
            .spawn_accept_loop(move |req, chan| blobs.clone().handle_rpc_request(req, chan));
        Self { client, _handler }
    }
}
