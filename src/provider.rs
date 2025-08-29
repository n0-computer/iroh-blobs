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
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bao_tree::ChunkRanges;
use iroh::{
    endpoint::{self, RecvStream, SendStream},
    NodeId,
};
use irpc::{channel::oneshot};
use n0_future::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{io::AsyncRead, select, sync::mpsc};
use tracing::{debug, debug_span, error, trace, warn, Instrument};

use crate::{
    api::{self, blobs::{Bitfield, WriteProgress}, Store}, hashseq::HashSeq, protocol::{
        ChunkRangesSeq, GetManyRequest, GetRequest, ObserveItem, ObserveRequest, PushRequest,
        Request,
    }, provider::events::{ClientConnected, ConnectionClosed, GetRequestReceived, RequestTracker}, Hash
};
pub(crate) mod events;
pub use events::EventSender as EventSender2;
pub use events::ProviderMessage;
pub use events::EventMask;
pub use events::AbortReason;

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
#[derive(Debug, Serialize, Deserialize)]
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
pub struct ReaderContext {
    /// The start time of the transfer
    pub t0: Instant,
    /// The connection ID from the connection
    pub connection_id: u64,
    /// The request ID from the recv stream
    pub request_id: u64,
    /// The number of bytes read from the stream
    pub bytes_read: u64,
}

#[derive(Debug)]
pub struct WriterContext {
    /// The start time of the transfer
    pub t0: Instant,
    /// The connection ID from the connection
    pub connection_id: u64,
    /// The request ID from the recv stream
    pub request_id: u64,
    /// The number of bytes read from the stream
    pub bytes_read: u64,
    /// The number of payload bytes written to the stream
    pub payload_bytes_written: u64,
    /// The number of bytes written that are not part of the payload
    pub other_bytes_written: u64,
    /// Way to report progress
    pub tracker: RequestTracker,
}

/// Wrapper for a [`quinn::SendStream`] with additional per request information.
#[derive(Debug)]
pub struct ProgressWriter {
    /// The quinn::SendStream to write to
    pub inner: SendStream,
    pub(crate) context: WriterContext,
}

impl ProgressWriter {
    fn new(inner: SendStream, context: ReaderContext, tracker: RequestTracker) -> Self {
        Self { inner, context: WriterContext {
            connection_id: context.connection_id,
            request_id: context.request_id,
            bytes_read: context.bytes_read,
            t0: context.t0,
            payload_bytes_written: 0,
            other_bytes_written: 0,
            tracker,
        } }
    }

    async fn transfer_aborted(&self) {
        self.tracker.transfer_aborted(|| Some(Box::new(TransferStats {
            payload_bytes_sent: self.payload_bytes_written,
            other_bytes_sent: self.other_bytes_written,
            bytes_read: self.bytes_read,
            duration: self.context.t0.elapsed(),
        }))).await.ok();
    }

    async fn transfer_completed(&self) {
        self.tracker.transfer_completed(|| Box::new(TransferStats {
            payload_bytes_sent: self.payload_bytes_written,
            other_bytes_sent: self.other_bytes_written,
            bytes_read: self.bytes_read,
            duration: self.context.t0.elapsed(),
        })).await.ok();
    }
}

impl Deref for ProgressWriter {
    type Target = WriterContext;

    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl DerefMut for ProgressWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}

/// Handle a single connection.
pub async fn handle_connection(
    connection: endpoint::Connection,
    store: Store,
    progress: EventSender2,
) {
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id);
    async move {
        let Ok(node_id) = connection.remote_node_id() else {
            warn!("failed to get node id");
            return;
        };
        if let Err(cause) =progress
            .client_connected(|| ClientConnected {
                connection_id,
                node_id,
            })
            .await
        {
            debug!("client not authorized to connect: {cause}");
            return;
        }
        while let Ok((writer, reader)) = connection.accept_bi().await {
            // The stream ID index is used to identify this request.  Requests only arrive in
            // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
            let request_id = reader.id().index();
            let span = debug_span!("stream", stream_id = %request_id);
            let store = store.clone();
            let context = ReaderContext {
                t0: Instant::now(),
                connection_id: connection_id,
                request_id: request_id,
                bytes_read: 0,
            };
            let reader = ProgressReader {
                inner: reader,
                context,
            };
            tokio::spawn(
                handle_stream(store, reader, writer, progress.clone())
                .instrument(span),
            );
        }
        progress
            .connection_closed(|| ConnectionClosed { connection_id })
            .await.ok();
    }
    .instrument(span)
    .await
}

async fn handle_stream(
    store: Store,
    mut reader: ProgressReader,
    writer: SendStream,
    progress: EventSender2,
) {
    // 1. Decode the request.
    debug!("reading request");
    let request = match read_request(&mut reader).await {
        Ok(request) => request,
        Err(e) => {
            // todo: event for read request failed
            return;
        }
    };

    match request {
        Request::Get(request) => {
            let tracker = match progress.get_request(|| GetRequestReceived {
                connection_id: reader.context.connection_id,
                request_id: reader.context.request_id,
                request: request.clone(),
            }).await {
                Ok(tracker) => tracker,
                Err(e) => {
                    trace!("Request denied: {}", e);
                    return;
                }
            };
            // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            let res = reader.inner.read_to_end(0).await;
            let mut writer = ProgressWriter::new(writer, reader.context, tracker);
            if res.is_err() {
                writer.transfer_aborted().await;
                return;
            }
            match handle_get(store, request, &mut writer).await {
                Ok(()) => {
                    writer.transfer_completed().await;
                }
                Err(_) => {
                    writer.transfer_aborted().await;
                }
            }
        }
        Request::GetMany(request) => {
            todo!();
            // // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            // reader.inner.read_to_end(0).await?;
            // // move the context so we don't lose the bytes read
            // writer.context = reader.context;
            // handle_get_many(store, request, writer).await
        }
        Request::Observe(request) => {
            todo!();
            // // we expect no more bytes after the request, so if there are more bytes, it is an invalid request.
            // reader.inner.read_to_end(0).await?;
            // handle_observe(store, request, writer).await
        }
        Request::Push(request) => {
            todo!();
            // writer.inner.finish()?;
            // handle_push(store, request, reader).await
        }
        _ => {},
    }
}

/// Handle a single get request.
///
/// Requires a database, the request, and a writer.
pub async fn handle_get(
    store: Store,
    request: GetRequest,
    writer: &mut ProgressWriter,
) -> anyhow::Result<()> {
    let hash = request.hash;
    debug!(%hash, "get received request");
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
    writer.context.log_other_write(len);
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
    context: ReaderContext,
}

impl Deref for ProgressReader {
    type Target = ReaderContext;

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
