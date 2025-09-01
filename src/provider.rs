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
use iroh::endpoint::{self, RecvStream, SendStream};
use n0_future::StreamExt;
use quinn::{ClosedStream, ConnectionError, ReadToEndError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{io::AsyncRead, select};
use tracing::{debug, debug_span, warn, Instrument};

use crate::{
    api::{
        self,
        blobs::{Bitfield, WriteProgress},
        Store,
    },
    hashseq::HashSeq,
    protocol::{GetManyRequest, GetRequest, ObserveItem, ObserveRequest, PushRequest, Request},
    provider::events::{ClientConnected, ClientError, ConnectionClosed, RequestTracker},
    Hash,
};
pub mod events;
use events::EventSender;

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
    /// In most cases this is just the request, for push requests this is
    /// request, size header and hash pairs.
    pub other_bytes_read: u64,
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
pub async fn read_request(context: &mut StreamData) -> Result<Request> {
    let mut counting = CountingReader::new(&mut context.reader);
    let res = Request::read_async(&mut counting).await?;
    context.other_bytes_read += counting.read();
    Ok(res)
}

#[derive(Debug)]
pub struct StreamData {
    t0: Instant,
    connection_id: u64,
    request_id: u64,
    reader: RecvStream,
    writer: SendStream,
    other_bytes_read: u64,
    events: EventSender,
}

impl StreamData {
    pub async fn accept(
        conn: &endpoint::Connection,
        events: &EventSender,
    ) -> Result<Self, ConnectionError> {
        let (writer, reader) = conn.accept_bi().await?;
        Ok(Self {
            t0: Instant::now(),
            connection_id: conn.stable_id() as u64,
            request_id: reader.id().into(),
            reader,
            writer,
            other_bytes_read: 0,
            events: events.clone(),
        })
    }

    /// We are done with reading. Return a ProgressWriter that contains the read stats and connection id
    async fn into_writer(
        mut self,
        tracker: RequestTracker,
    ) -> Result<ProgressWriter, ReadToEndError> {
        let res = self.reader.read_to_end(0).await;
        if let Err(e) = res {
            tracker
                .transfer_aborted(|| Box::new(self.stats()))
                .await
                .ok();
            return Err(e);
        };
        Ok(ProgressWriter::new(
            self.writer,
            WriterContext {
                t0: self.t0,
                connection_id: self.connection_id,
                request_id: self.request_id,
                other_bytes_read: self.other_bytes_read,
                payload_bytes_written: 0,
                other_bytes_written: 0,
                tracker,
            },
        ))
    }

    async fn into_reader(
        mut self,
        tracker: RequestTracker,
    ) -> Result<ProgressReader, ClosedStream> {
        let res = self.writer.finish();
        if let Err(e) = res {
            tracker
                .transfer_aborted(|| Box::new(self.stats()))
                .await
                .ok();
            return Err(e);
        };
        Ok(ProgressReader::new(
            self.reader,
            ReaderContext {
                t0: self.t0,
                connection_id: self.connection_id,
                request_id: self.request_id,
                other_bytes_read: self.other_bytes_read,
                tracker,
            },
        ))
    }

    async fn get_request(
        &self,
        f: impl FnOnce() -> GetRequest,
    ) -> Result<RequestTracker, ClientError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    async fn get_many_request(
        &self,
        f: impl FnOnce() -> GetManyRequest,
    ) -> Result<RequestTracker, ClientError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    async fn push_request(
        &self,
        f: impl FnOnce() -> PushRequest,
    ) -> Result<RequestTracker, ClientError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    async fn observe_request(
        &self,
        f: impl FnOnce() -> ObserveRequest,
    ) -> Result<RequestTracker, ClientError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    fn stats(&self) -> TransferStats {
        TransferStats {
            payload_bytes_sent: 0,
            other_bytes_sent: 0,
            other_bytes_read: self.other_bytes_read,
            duration: self.t0.elapsed(),
        }
    }
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
    pub other_bytes_read: u64,
    /// Progress tracking for the request
    pub tracker: RequestTracker,
}

impl ReaderContext {
    pub fn new(context: StreamData, tracker: RequestTracker) -> Self {
        Self {
            t0: context.t0,
            connection_id: context.connection_id,
            request_id: context.request_id,
            other_bytes_read: context.other_bytes_read,
            tracker,
        }
    }

    pub fn stats(&self) -> TransferStats {
        TransferStats {
            payload_bytes_sent: 0,
            other_bytes_sent: 0,
            other_bytes_read: self.other_bytes_read,
            duration: self.t0.elapsed(),
        }
    }
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
    pub other_bytes_read: u64,
    /// The number of payload bytes written to the stream
    pub payload_bytes_written: u64,
    /// The number of bytes written that are not part of the payload
    pub other_bytes_written: u64,
    /// Way to report progress
    pub tracker: RequestTracker,
}

impl WriterContext {
    pub fn new(context: &StreamData, tracker: RequestTracker) -> Self {
        Self {
            t0: context.t0,
            connection_id: context.connection_id,
            request_id: context.request_id,
            other_bytes_read: context.other_bytes_read,
            payload_bytes_written: 0,
            other_bytes_written: 0,
            tracker,
        }
    }

    pub fn stats(&self) -> TransferStats {
        TransferStats {
            payload_bytes_sent: self.payload_bytes_written,
            other_bytes_sent: self.other_bytes_written,
            other_bytes_read: self.other_bytes_read,
            duration: self.t0.elapsed(),
        }
    }
}

/// Wrapper for a [`quinn::SendStream`] with additional per request information.
#[derive(Debug)]
pub struct ProgressWriter {
    /// The quinn::SendStream to write to
    pub inner: SendStream,
    pub(crate) context: WriterContext,
}

impl ProgressWriter {
    fn new(inner: SendStream, context: WriterContext) -> Self {
        Self { inner, context }
    }

    async fn transfer_aborted(&self) {
        self.tracker
            .transfer_aborted(|| Box::new(self.stats()))
            .await
            .ok();
    }

    async fn transfer_completed(&self) {
        self.tracker
            .transfer_completed(|| Box::new(self.stats()))
            .await
            .ok();
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
    progress: EventSender,
) {
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id);
    async move {
        let Ok(node_id) = connection.remote_node_id() else {
            warn!("failed to get node id");
            return;
        };
        if let Err(cause) = progress
            .client_connected(|| ClientConnected {
                connection_id,
                node_id,
            })
            .await
        {
            debug!("client not authorized to connect: {cause}");
            return;
        }
        while let Ok(context) = StreamData::accept(&connection, &progress).await {
            let span = debug_span!("stream", stream_id = %context.request_id);
            let store = store.clone();
            tokio::spawn(handle_stream(store, context).instrument(span));
        }
        progress
            .connection_closed(|| ConnectionClosed { connection_id })
            .await
            .ok();
    }
    .instrument(span)
    .await
}

async fn handle_stream(store: Store, mut context: StreamData) -> anyhow::Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let request = read_request(&mut context).await?;

    match request {
        Request::Get(request) => {
            let tracker = context.get_request(|| request.clone()).await?;
            let mut writer = context.into_writer(tracker).await?;
            if handle_get(store, request, &mut writer).await.is_ok() {
                writer.transfer_completed().await;
            } else {
                writer.transfer_aborted().await;
            }
        }
        Request::GetMany(request) => {
            let tracker = context.get_many_request(|| request.clone()).await?;
            let mut writer = context.into_writer(tracker).await?;
            if handle_get_many(store, request, &mut writer).await.is_ok() {
                writer.transfer_completed().await;
            } else {
                writer.transfer_aborted().await;
            }
        }
        Request::Observe(request) => {
            let tracker = context.observe_request(|| request.clone()).await?;
            let mut writer = context.into_writer(tracker).await?;
            if handle_observe(store, request, &mut writer).await.is_ok() {
                writer.transfer_completed().await;
            } else {
                writer.transfer_aborted().await;
            }
        }
        Request::Push(request) => {
            let tracker = context.push_request(|| request.clone()).await?;
            let mut reader = context.into_reader(tracker).await?;
            if handle_push(store, request, &mut reader).await.is_ok() {
                reader.transfer_completed().await;
            } else {
                reader.transfer_aborted().await;
            }
        }
        _ => {}
    }
    Ok(())
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
    reader: &mut ProgressReader,
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

pub struct ProgressReader {
    inner: RecvStream,
    context: ReaderContext,
}

impl ProgressReader {
    pub fn new(inner: RecvStream, context: ReaderContext) -> Self {
        Self { inner, context }
    }

    async fn transfer_aborted(&self) {
        self.tracker
            .transfer_aborted(|| Box::new(self.stats()))
            .await
            .ok();
    }

    async fn transfer_completed(&self) {
        self.tracker
            .transfer_completed(|| Box::new(self.stats()))
            .await
            .ok();
    }
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
