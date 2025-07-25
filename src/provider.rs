//! The low level server side API
//!
//! Note that while using this API directly is fine, the standard way
//! to provide data is to just register a [`crate::BlobsProtocol`] protocol
//! handler with an [`iroh::Endpoint`](iroh::protocol::Router).
use std::{
    fmt::Debug,
    io,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use anyhow::{Context, Result};
use bao_tree::ChunkRanges;
use iroh::{
    endpoint::{self, RecvStream, SendStream},
    NodeId,
};
use irpc::channel::oneshot;
use n0_future::StreamExt;
use serde::de::DeserializeOwned;
use tokio::{io::AsyncRead, select, sync::mpsc};
use tracing::{debug, debug_span, error, warn, Instrument};

use crate::{
    api::{self, blobs::Bitfield, Store},
    hashseq::HashSeq,
    protocol::{
        ChunkRangesSeq, GetManyRequest, GetRequest, ObserveItem, ObserveRequest, PushRequest,
        Request,
    },
    Hash,
};

/// Provider progress events, to keep track of what the provider is doing.
///
/// ClientConnected ->
///    (GetRequestReceived -> (TransferStarted -> TransferProgress*n)*n -> (TransferCompleted | TransferAborted))*n ->
/// ConnectionClosed
#[derive(Debug)]
pub enum Event {
    /// A new client connected to the provider.
    ClientConnected {
        connection_id: u64,
        node_id: NodeId,
        permitted: oneshot::Sender<bool>,
    },
    /// Connection closed.
    ConnectionClosed { connection_id: u64 },
    /// A new get request was received from the provider.
    GetRequestReceived {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The root hash of the request.
        hash: Hash,
        /// The exact query ranges of the request.
        ranges: ChunkRangesSeq,
    },
    /// A new get request was received from the provider.
    GetManyRequestReceived {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The root hash of the request.
        hashes: Vec<Hash>,
        /// The exact query ranges of the request.
        ranges: ChunkRangesSeq,
    },
    /// A new get request was received from the provider.
    PushRequestReceived {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The root hash of the request.
        hash: Hash,
        /// The exact query ranges of the request.
        ranges: ChunkRangesSeq,
        /// Complete this to permit the request.
        permitted: oneshot::Sender<bool>,
    },
    /// Transfer for the nth blob started.
    TransferStarted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
        index: u64,
        /// The hash of the blob. This is the hash of the request for the first blob, the child hash (index-1) for subsequent blobs.
        hash: Hash,
        /// The size of the blob. This is the full size of the blob, not the size we are sending.
        size: u64,
    },
    /// Progress of the transfer.
    TransferProgress {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// The index of the blob in the request. 0 for the first blob or for raw blob requests.
        index: u64,
        /// The end offset of the chunk that was sent.
        end_offset: u64,
    },
    /// Entire transfer completed.
    TransferCompleted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// Statistics about the transfer.
        stats: Box<TransferStats>,
    },
    /// Entire transfer aborted
    TransferAborted {
        /// The connection id. Multiple requests can be sent over the same connection.
        connection_id: u64,
        /// The request id. There is a new id for each request.
        request_id: u64,
        /// Statistics about the part of the transfer that was aborted.
        stats: Option<Box<TransferStats>>,
    },
}

/// Statistics about a successful or failed transfer.
#[derive(Debug)]
pub struct TransferStats {
    /// The number of bytes sent that are part of the payload.
    pub payload_bytes_sent: u64,
    /// The number of bytes sent that are not part of the payload.
    ///
    /// Hash pairs and the initial size header.
    pub other_bytes_sent: u64,
    /// The number of bytes read from the stream.
    ///
    /// This is the size of the request.
    pub bytes_read: u64,
    /// Total duration from reading the request to transfer completed.
    pub duration: Duration,
}

/// Read the request from the getter.
///
/// Will fail if there is an error while reading, or if no valid request is sent.
///
/// This will read exactly the number of bytes needed for the request, and
/// leave the rest of the stream for the caller to read.
///
/// It is up to the caller do decide if there should be more data.
pub async fn read_request(reader: &mut ProgressReader) -> Result<Request> {
    let mut counting = CountingReader::new(&mut reader.inner);
    let res = Request::read_async(&mut counting).await?;
    reader.bytes_read += counting.read();
    Ok(res)
}

#[derive(Debug)]
pub struct StreamContext {
    /// The connection ID from the connection
    pub connection_id: u64,
    /// The request ID from the recv stream
    pub request_id: u64,
    /// The number of bytes written that are part of the payload
    pub payload_bytes_sent: u64,
    /// The number of bytes written that are not part of the payload
    pub other_bytes_sent: u64,
    /// The number of bytes read from the stream
    pub bytes_read: u64,
    /// The progress sender to send events to
    pub progress: EventSender,
}

/// Wrapper for a [`quinn::SendStream`] with additional per request information.
#[derive(Debug)]
pub struct ProgressWriter {
    /// The quinn::SendStream to write to
    pub inner: SendStream,
    pub(crate) context: StreamContext,
}

impl Deref for ProgressWriter {
    type Target = StreamContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl DerefMut for ProgressWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}

impl StreamContext {
    /// Increase the write count due to a non-payload write.
    pub fn log_other_write(&mut self, len: usize) {
        self.other_bytes_sent += len as u64;
    }

    pub async fn send_transfer_completed(&mut self) {
        self.progress
            .send(|| Event::TransferCompleted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                stats: Box::new(TransferStats {
                    payload_bytes_sent: self.payload_bytes_sent,
                    other_bytes_sent: self.other_bytes_sent,
                    bytes_read: self.bytes_read,
                    duration: Duration::ZERO,
                }),
            })
            .await;
    }

    pub async fn send_transfer_aborted(&mut self) {
        self.progress
            .send(|| Event::TransferAborted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                stats: Some(Box::new(TransferStats {
                    payload_bytes_sent: self.payload_bytes_sent,
                    other_bytes_sent: self.other_bytes_sent,
                    bytes_read: self.bytes_read,
                    duration: Duration::ZERO,
                })),
            })
            .await;
    }

    /// Increase the write count due to a payload write, and notify the progress sender.
    ///
    /// `index` is the index of the blob in the request.
    /// `offset` is the offset in the blob where the write started.
    /// `len` is the length of the write.
    pub fn notify_payload_write(&mut self, index: u64, offset: u64, len: usize) {
        self.payload_bytes_sent += len as u64;
        self.progress.try_send(|| Event::TransferProgress {
            connection_id: self.connection_id,
            request_id: self.request_id,
            index,
            end_offset: offset + len as u64,
        });
    }

    /// Send a get request received event.
    ///
    /// This sends all the required information to make sense of subsequent events such as
    /// [`Event::TransferStarted`] and [`Event::TransferProgress`].
    pub async fn send_get_request_received(&self, hash: &Hash, ranges: &ChunkRangesSeq) {
        self.progress
            .send(|| Event::GetRequestReceived {
                connection_id: self.connection_id,
                request_id: self.request_id,
                hash: *hash,
                ranges: ranges.clone(),
            })
            .await;
    }

    /// Send a get request received event.
    ///
    /// This sends all the required information to make sense of subsequent events such as
    /// [`Event::TransferStarted`] and [`Event::TransferProgress`].
    pub async fn send_get_many_request_received(&self, hashes: &[Hash], ranges: &ChunkRangesSeq) {
        self.progress
            .send(|| Event::GetManyRequestReceived {
                connection_id: self.connection_id,
                request_id: self.request_id,
                hashes: hashes.to_vec(),
                ranges: ranges.clone(),
            })
            .await;
    }

    /// Authorize a push request.
    ///
    /// This will send a request to the event sender, and wait for a response if a
    /// progress sender is enabled. If not, it will always fail.
    ///
    /// We want to make accepting push requests very explicit, since this allows
    /// remote nodes to add arbitrary data to our store.
    #[must_use = "permit should be checked by the caller"]
    pub async fn authorize_push_request(&self, hash: &Hash, ranges: &ChunkRangesSeq) -> bool {
        let mut wait_for_permit = None;
        // send the request, including the permit channel
        self.progress
            .send(|| {
                let (tx, rx) = oneshot::channel();
                wait_for_permit = Some(rx);
                Event::PushRequestReceived {
                    connection_id: self.connection_id,
                    request_id: self.request_id,
                    hash: *hash,
                    ranges: ranges.clone(),
                    permitted: tx,
                }
            })
            .await;
        // wait for the permit, if necessary
        if let Some(wait_for_permit) = wait_for_permit {
            // if somebody does not handle the request, they will drop the channel,
            // and this will fail immediately.
            wait_for_permit.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Send a transfer started event.
    pub async fn send_transfer_started(&self, index: u64, hash: &Hash, size: u64) {
        self.progress
            .send(|| Event::TransferStarted {
                connection_id: self.connection_id,
                request_id: self.request_id,
                index,
                hash: *hash,
                size,
            })
            .await;
    }
}

/// Handle a single connection.
pub async fn handle_connection(
    connection: endpoint::Connection,
    store: Store,
    progress: EventSender,
) {
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id);
    async move {
        let Ok(node_id) = connection.remote_node_id() else {
            warn!("failed to get node id");
            return;
        };
        if !progress
            .authorize_client_connection(connection_id, node_id)
            .await
        {
            debug!("client not authorized to connect");
            return;
        }
        while let Ok((writer, reader)) = connection.accept_bi().await {
            // The stream ID index is used to identify this request.  Requests only arrive in
            // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
            let request_id = reader.id().index();
            let span = debug_span!("stream", stream_id = %request_id);
            let store = store.clone();
            let mut writer = ProgressWriter {
                inner: writer,
                context: StreamContext {
                    connection_id,
                    request_id,
                    payload_bytes_sent: 0,
                    other_bytes_sent: 0,
                    bytes_read: 0,
                    progress: progress.clone(),
                },
            };
            tokio::spawn(
                async move {
                    match handle_stream(store, reader, &mut writer).await {
                        Ok(()) => {
                            writer.send_transfer_completed().await;
                        }
                        Err(err) => {
                            warn!("error: {err:#?}",);
                            writer.send_transfer_aborted().await;
                        }
                    }
                }
                .instrument(span),
            );
        }
        progress
            .send(Event::ConnectionClosed { connection_id })
            .await;
    }
    .instrument(span)
    .await
}

async fn handle_stream(
    store: Store,
    reader: RecvStream,
    writer: &mut ProgressWriter,
) -> Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let mut reader = ProgressReader {
        inner: reader,
        context: StreamContext {
            connection_id: writer.connection_id,
            request_id: writer.request_id,
            payload_bytes_sent: 0,
            other_bytes_sent: 0,
            bytes_read: 0,
            progress: writer.progress.clone(),
        },
    };
    let request = match read_request(&mut reader).await {
        Ok(request) => request,
        Err(e) => {
            // todo: increase invalid requests metric counter
            return Err(e);
        }
    };

    match request {
        Request::Get(request) => {
            // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            reader.inner.read_to_end(0).await?;
            // move the context so we don't lose the bytes read
            writer.context = reader.context;
            handle_get(store, request, writer).await
        }
        Request::GetMany(request) => {
            // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            reader.inner.read_to_end(0).await?;
            // move the context so we don't lose the bytes read
            writer.context = reader.context;
            handle_get_many(store, request, writer).await
        }
        Request::Observe(request) => {
            // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            reader.inner.read_to_end(0).await?;
            handle_observe(store, request, writer).await
        }
        Request::Push(request) => {
            writer.inner.finish()?;
            handle_push(store, request, reader).await
        }
        _ => anyhow::bail!("unsupported request: {request:?}"),
        // Request::Push(request) => handle_push(store, request, writer).await,
    }
}

/// Handle a single get request.
///
/// Requires a database, the request, and a writer.
pub async fn handle_get(
    store: Store,
    request: GetRequest,
    writer: &mut ProgressWriter,
) -> Result<()> {
    let hash = request.hash;
    debug!(%hash, "get received request");

    writer
        .send_get_request_received(&hash, &request.ranges)
        .await;
    let mut hash_seq = None;
    for (offset, ranges) in request.ranges.iter_non_empty_infinite() {
        if offset == 0 {
            send_blob(&store, offset, hash, ranges.clone(), writer).await?;
        } else {
            // todo: this assumes that 1. the hashseq is complete and 2. it is
            // small enough to fit in memory.
            //
            // This should really read the hashseq from the store in chunks,
            // only where needed, so we can deal with holes and large hashseqs.
            let hash_seq = match &hash_seq {
                Some(b) => b,
                None => {
                    let bytes = store.get_bytes(hash).await?;
                    let hs = HashSeq::try_from(bytes)?;
                    hash_seq = Some(hs);
                    hash_seq.as_ref().unwrap()
                }
            };
            let o = usize::try_from(offset - 1).context("offset too large")?;
            let Some(hash) = hash_seq.get(o) else {
                break;
            };
            send_blob(&store, offset, hash, ranges.clone(), writer).await?;
        }
    }

    Ok(())
}

/// Handle a single get request.
///
/// Requires a database, the request, and a writer.
pub async fn handle_get_many(
    store: Store,
    request: GetManyRequest,
    writer: &mut ProgressWriter,
) -> Result<()> {
    debug!("get_many received request");
    writer
        .send_get_many_request_received(&request.hashes, &request.ranges)
        .await;
    let request_ranges = request.ranges.iter_infinite();
    for (child, (hash, ranges)) in request.hashes.iter().zip(request_ranges).enumerate() {
        if !ranges.is_empty() {
            send_blob(&store, child as u64, *hash, ranges.clone(), writer).await?;
        }
    }
    Ok(())
}

/// Handle a single push request.
///
/// Requires a database, the request, and a reader.
pub async fn handle_push(
    store: Store,
    request: PushRequest,
    mut reader: ProgressReader,
) -> Result<()> {
    let hash = request.hash;
    debug!(%hash, "push received request");
    if !reader.authorize_push_request(&hash, &request.ranges).await {
        debug!("push request not authorized");
        return Ok(());
    };
    let mut request_ranges = request.ranges.iter_infinite();
    let root_ranges = request_ranges.next().expect("infinite iterator");
    if !root_ranges.is_empty() {
        // todo: send progress from import_bao_quinn or rename to import_bao_quinn_with_progress
        store
            .import_bao_quinn(hash, root_ranges.clone(), &mut reader.inner)
            .await?;
    }
    if request.ranges.is_blob() {
        debug!("push request complete");
        return Ok(());
    }
    // todo: we assume here that the hash sequence is complete. For some requests this might not be the case. We would need `LazyHashSeq` for that, but it is buggy as of now!
    let hash_seq = store.get_bytes(hash).await?;
    let hash_seq = HashSeq::try_from(hash_seq)?;
    for (child_hash, child_ranges) in hash_seq.into_iter().zip(request_ranges) {
        if child_ranges.is_empty() {
            continue;
        }
        store
            .import_bao_quinn(child_hash, child_ranges.clone(), &mut reader.inner)
            .await?;
    }
    Ok(())
}

/// Send a blob to the client.
pub(crate) async fn send_blob(
    store: &Store,
    index: u64,
    hash: Hash,
    ranges: ChunkRanges,
    writer: &mut ProgressWriter,
) -> api::Result<()> {
    Ok(store
        .export_bao(hash, ranges)
        .write_quinn_with_progress(&mut writer.inner, &mut writer.context, &hash, index)
        .await?)
}

/// Handle a single push request.
///
/// Requires a database, the request, and a reader.
pub async fn handle_observe(
    store: Store,
    request: ObserveRequest,
    writer: &mut ProgressWriter,
) -> Result<()> {
    let mut stream = store.observe(request.hash).stream().await?;
    let mut old = stream
        .next()
        .await
        .ok_or(anyhow::anyhow!("observe stream closed before first value"))?;
    // send the initial bitfield
    send_observe_item(writer, &old).await?;
    // send updates until the remote loses interest
    loop {
        select! {
            new = stream.next() => {
                let new = new.context("observe stream closed")?;
                let diff = old.diff(&new);
                if diff.is_empty() {
                    continue;
                }
                send_observe_item(writer, &diff).await?;
                old = new;
            }
            _ = writer.inner.stopped() => {
                debug!("observer closed");
                break;
            }
        }
    }
    Ok(())
}

async fn send_observe_item(writer: &mut ProgressWriter, item: &Bitfield) -> Result<()> {
    use irpc::util::AsyncWriteVarintExt;
    let item = ObserveItem::from(item);
    let len = writer.inner.write_length_prefixed(item).await?;
    writer.log_other_write(len);
    Ok(())
}

/// Helper to lazyly create an [`Event`], in the case that the event creation
/// is expensive and we want to avoid it if the progress sender is disabled.
pub trait LazyEvent {
    fn call(self) -> Event;
}

impl<T> LazyEvent for T
where
    T: FnOnce() -> Event,
{
    fn call(self) -> Event {
        self()
    }
}

impl LazyEvent for Event {
    fn call(self) -> Event {
        self
    }
}

/// A sender for provider events.
#[derive(Debug, Clone)]
pub struct EventSender(EventSenderInner);

#[derive(Debug, Clone)]
enum EventSenderInner {
    Disabled,
    Enabled(mpsc::Sender<Event>),
}

impl EventSender {
    pub fn new(sender: Option<mpsc::Sender<Event>>) -> Self {
        match sender {
            Some(sender) => Self(EventSenderInner::Enabled(sender)),
            None => Self(EventSenderInner::Disabled),
        }
    }

    /// Send a client connected event, if the progress sender is enabled.
    ///
    /// This will permit the client to connect if the sender is disabled.
    #[must_use = "permit should be checked by the caller"]
    pub async fn authorize_client_connection(&self, connection_id: u64, node_id: NodeId) -> bool {
        let mut wait_for_permit = None;
        self.send(|| {
            let (tx, rx) = oneshot::channel();
            wait_for_permit = Some(rx);
            Event::ClientConnected {
                connection_id,
                node_id,
                permitted: tx,
            }
        })
        .await;
        if let Some(wait_for_permit) = wait_for_permit {
            // if we have events configured, and they drop the channel, we consider that as a no!
            // todo: this will be confusing and needs to be properly documented.
            wait_for_permit.await.unwrap_or(false)
        } else {
            true
        }
    }

    /// Send an ephemeral event, if the progress sender is enabled.
    ///
    /// The event will only be created if the sender is enabled.
    fn try_send(&self, event: impl LazyEvent) {
        match &self.0 {
            EventSenderInner::Enabled(sender) => {
                let value = event.call();
                sender.try_send(value).ok();
            }
            EventSenderInner::Disabled => {}
        }
    }

    /// Send a mandatory event, if the progress sender is enabled.
    ///
    /// The event only be created if the sender is enabled.
    async fn send(&self, event: impl LazyEvent) {
        match &self.0 {
            EventSenderInner::Enabled(sender) => {
                let value = event.call();
                if let Err(err) = sender.send(value).await {
                    error!("failed to send progress event: {:?}", err);
                }
            }
            EventSenderInner::Disabled => {}
        }
    }
}

pub struct ProgressReader {
    inner: RecvStream,
    context: StreamContext,
}

impl Deref for ProgressReader {
    type Target = StreamContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl DerefMut for ProgressReader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}

pub struct CountingReader<R> {
    pub inner: R,
    pub read: u64,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, read: 0 }
    }

    pub fn read(&self) -> u64 {
        self.read
    }
}

impl CountingReader<&mut iroh::endpoint::RecvStream> {
    pub async fn read_to_end_as<T: DeserializeOwned>(&mut self, max_size: usize) -> io::Result<T> {
        let data = self
            .inner
            .read_to_end(max_size)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let value = postcard::from_bytes(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.read += data.len() as u64;
        Ok(value)
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = result {
            this.read += buf.filled().len() as u64;
        }
        result
    }
}
