//! The low level server side API
//!
//! Note that while using this API directly is fine, the standard way
//! to provide data is to just register a [`crate::BlobsProtocol`] protocol
//! handler with an [`iroh::Endpoint`](iroh::protocol::Router).
use std::{
    fmt::Debug,
    future::Future,
    io,
    time::{Duration, Instant},
};

use anyhow::Result;
use bao_tree::ChunkRanges;
use iroh::endpoint;
use iroh_io::{AsyncStreamReader, AsyncStreamWriter};
use n0_future::StreamExt;
use quinn::{ConnectionError, VarInt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snafu::Snafu;
use tokio::select;
use tracing::{debug, debug_span, warn, Instrument};

use crate::{
    api::{
        blobs::{Bitfield, WriteProgress},
        ExportBaoError, ExportBaoResult, RequestError, Store,
    },
    get::{IrohStreamReader, IrohStreamWriter},
    hashseq::HashSeq,
    protocol::{
        GetManyRequest, GetRequest, ObserveItem, ObserveRequest, PushRequest, Request, ERR_INTERNAL,
    },
    provider::events::{
        ClientConnected, ClientResult, ConnectionClosed, HasErrorCode, ProgressError,
        RequestTracker,
    },
    Hash,
};
pub mod events;
use events::EventSender;

type DefaultWriter = IrohStreamWriter;
type DefaultReader = IrohStreamReader;

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

/// A pair of [`SendStream`] and [`RecvStream`] with additional context data.
#[derive(Debug)]
pub struct StreamPair<R: AsyncStreamReader = DefaultReader, W: AsyncStreamWriter = DefaultWriter> {
    t0: Instant,
    connection_id: u64,
    request_id: u64,
    reader: R,
    writer: W,
    other_bytes_read: u64,
    events: EventSender,
}

impl StreamPair {
    pub async fn accept(
        conn: &endpoint::Connection,
        events: EventSender,
    ) -> Result<Self, ConnectionError> {
        let (writer, reader) = conn.accept_bi().await?;
        Ok(Self::new(
            conn.stable_id() as u64,
            reader.id().into(),
            IrohStreamReader(reader),
            IrohStreamWriter(writer),
            events,
        ))
    }
}

impl<R: AsyncStreamReader, W: AsyncStreamWriter> StreamPair<R, W> {
    pub fn new(
        connection_id: u64,
        request_id: u64,
        reader: R,
        writer: W,
        events: EventSender,
    ) -> Self {
        Self {
            t0: Instant::now(),
            connection_id,
            request_id,
            reader,
            writer,
            other_bytes_read: 0,
            events,
        }
    }

    /// Read the request.
    ///
    /// Will fail if there is an error while reading, or if no valid request is sent.
    ///
    /// This will read exactly the number of bytes needed for the request, and
    /// leave the rest of the stream for the caller to read.
    ///
    /// It is up to the caller do decide if there should be more data.
    pub async fn read_request(&mut self) -> Result<Request> {
        let (res, size) = Request::read_async(&mut self.reader).await?;
        self.other_bytes_read += size as u64;
        Ok(res)
    }

    /// We are done with reading. Return a ProgressWriter that contains the read stats and connection id
    pub async fn into_writer(
        mut self,
        tracker: RequestTracker,
    ) -> Result<ProgressWriter<W>, io::Error> {
        self.reader.expect_eof().await?;
        drop(self.reader);
        Ok(ProgressWriter::new(
            self.writer,
            WriterContext {
                t0: self.t0,
                other_bytes_read: self.other_bytes_read,
                payload_bytes_written: 0,
                other_bytes_written: 0,
                tracker,
            },
        ))
    }

    pub async fn into_reader(
        mut self,
        tracker: RequestTracker,
    ) -> Result<ProgressReader<R>, io::Error> {
        self.writer.sync().await?;
        drop(self.writer);
        Ok(ProgressReader {
            inner: self.reader,
            context: ReaderContext {
                t0: self.t0,
                other_bytes_read: self.other_bytes_read,
                tracker,
            },
        })
    }

    pub async fn get_request(
        &self,
        f: impl FnOnce() -> GetRequest,
    ) -> Result<RequestTracker, ProgressError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    pub async fn get_many_request(
        &self,
        f: impl FnOnce() -> GetManyRequest,
    ) -> Result<RequestTracker, ProgressError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    pub async fn push_request(
        &self,
        f: impl FnOnce() -> PushRequest,
    ) -> Result<RequestTracker, ProgressError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
    }

    pub async fn observe_request(
        &self,
        f: impl FnOnce() -> ObserveRequest,
    ) -> Result<RequestTracker, ProgressError> {
        self.events
            .request(f, self.connection_id, self.request_id)
            .await
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
struct ReaderContext {
    /// The start time of the transfer
    t0: Instant,
    /// The number of bytes read from the stream
    other_bytes_read: u64,
    /// Progress tracking for the request
    tracker: RequestTracker,
}

impl ReaderContext {
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
pub(crate) struct WriterContext {
    /// The start time of the transfer
    t0: Instant,
    /// The number of bytes read from the stream
    other_bytes_read: u64,
    /// The number of payload bytes written to the stream
    payload_bytes_written: u64,
    /// The number of bytes written that are not part of the payload
    other_bytes_written: u64,
    /// Way to report progress
    tracker: RequestTracker,
}

impl WriterContext {
    fn stats(&self) -> TransferStats {
        TransferStats {
            payload_bytes_sent: self.payload_bytes_written,
            other_bytes_sent: self.other_bytes_written,
            other_bytes_read: self.other_bytes_read,
            duration: self.t0.elapsed(),
        }
    }
}

impl WriteProgress for WriterContext {
    async fn notify_payload_write(&mut self, _index: u64, offset: u64, len: usize) -> ClientResult {
        let len = len as u64;
        let end_offset = offset + len;
        self.payload_bytes_written += len;
        self.tracker.transfer_progress(len, end_offset).await
    }

    fn log_other_write(&mut self, len: usize) {
        self.other_bytes_written += len as u64;
    }

    async fn send_transfer_started(&mut self, index: u64, hash: &Hash, size: u64) {
        self.tracker.transfer_started(index, hash, size).await.ok();
    }
}

/// Wrapper for a [`quinn::SendStream`] with additional per request information.
#[derive(Debug)]
pub struct ProgressWriter<W: AsyncStreamWriter = DefaultWriter> {
    /// The quinn::SendStream to write to
    pub inner: W,
    pub(crate) context: WriterContext,
}

impl<W: AsyncStreamWriter> ProgressWriter<W> {
    fn new(inner: W, context: WriterContext) -> Self {
        Self { inner, context }
    }

    async fn transfer_aborted(&self) {
        self.context
            .tracker
            .transfer_aborted(|| Box::new(self.context.stats()))
            .await
            .ok();
    }

    async fn transfer_completed(&self) {
        self.context
            .tracker
            .transfer_completed(|| Box::new(self.context.stats()))
            .await
            .ok();
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
            connection.close(cause.code(), cause.reason());
            debug!("closing connection: {cause}");
            return;
        }
        while let Ok(context) = StreamPair::accept(&connection, progress.clone()).await {
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

/// Describes how to handle errors for a stream.
pub trait ErrorHandler {
    type W: AsyncStreamWriter;
    type R: AsyncStreamReader;
    fn stop(reader: &mut Self::R, code: VarInt) -> impl Future<Output = ()>;
    fn reset(writer: &mut Self::W, code: VarInt) -> impl Future<Output = ()>;
}

async fn handle_read_request_result<H: ErrorHandler, T, E: HasErrorCode>(
    pair: &mut StreamPair<H::R, H::W>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => Ok(x),
        Err(e) => {
            H::reset(&mut pair.writer, e.code()).await;
            Err(e)
        }
    }
}
async fn handle_write_result<H: ErrorHandler, T, E: HasErrorCode>(
    writer: &mut ProgressWriter<H::W>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => {
            writer.transfer_completed().await;
            Ok(x)
        }
        Err(e) => {
            H::reset(&mut writer.inner, e.code()).await;
            writer.transfer_aborted().await;
            Err(e)
        }
    }
}
async fn handle_read_result<H: ErrorHandler, T, E: HasErrorCode>(
    reader: &mut ProgressReader<H::R>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => {
            reader.transfer_completed().await;
            Ok(x)
        }
        Err(e) => {
            H::stop(&mut reader.inner, e.code()).await;
            reader.transfer_aborted().await;
            Err(e)
        }
    }
}
struct IrohErrorHandler;

impl ErrorHandler for IrohErrorHandler {
    type W = DefaultWriter;
    type R = DefaultReader;

    async fn stop(reader: &mut Self::R, code: VarInt) {
        reader.0.stop(code).ok();
    }
    async fn reset(writer: &mut Self::W, code: VarInt) {
        writer.0.reset(code).ok();
    }
}

pub async fn handle_stream(store: Store, mut context: StreamPair) -> anyhow::Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let request = context.read_request().await?;
    type H = IrohErrorHandler;

    match request {
        Request::Get(request) => handle_get::<H>(context, store, request).await?,
        Request::GetMany(request) => handle_get_many::<H>(context, store, request).await?,
        Request::Observe(request) => handle_observe::<H>(context, store, request).await?,
        Request::Push(request) => handle_push::<H>(context, store, request).await?,
        _ => {}
    }
    Ok(())
}

#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum HandleGetError {
    #[snafu(transparent)]
    ExportBao {
        source: ExportBaoError,
    },
    InvalidHashSeq,
    InvalidOffset,
}

impl HasErrorCode for HandleGetError {
    fn code(&self) -> VarInt {
        match self {
            HandleGetError::ExportBao {
                source: ExportBaoError::Progress { source, .. },
            } => source.code(),
            HandleGetError::InvalidHashSeq => ERR_INTERNAL,
            HandleGetError::InvalidOffset => ERR_INTERNAL,
            _ => ERR_INTERNAL,
        }
    }
}

/// Handle a single get request.
///
/// Requires a database, the request, and a writer.
async fn handle_get_impl<W: AsyncStreamWriter>(
    store: Store,
    request: GetRequest,
    writer: &mut ProgressWriter<W>,
) -> Result<(), HandleGetError> {
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
                    let hs =
                        HashSeq::try_from(bytes).map_err(|_| HandleGetError::InvalidHashSeq)?;
                    hash_seq = Some(hs);
                    hash_seq.as_ref().unwrap()
                }
            };
            let o = usize::try_from(offset - 1).map_err(|_| HandleGetError::InvalidOffset)?;
            let Some(hash) = hash_seq.get(o) else {
                break;
            };
            send_blob(&store, offset, hash, ranges.clone(), writer).await?;
        }
    }

    Ok(())
}

pub async fn handle_get<H: ErrorHandler>(
    mut pair: StreamPair<H::R, H::W>,
    store: Store,
    request: GetRequest,
) -> anyhow::Result<()> {
    let res = pair.get_request(|| request.clone()).await;
    let tracker = handle_read_request_result::<H, _, _>(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_get_impl(store, request, &mut writer).await;
    handle_write_result::<H, _, _>(&mut writer, res).await?;
    Ok(())
}

#[derive(Debug, Snafu)]
pub enum HandleGetManyError {
    #[snafu(transparent)]
    ExportBao { source: ExportBaoError },
}

impl HasErrorCode for HandleGetManyError {
    fn code(&self) -> VarInt {
        match self {
            Self::ExportBao {
                source: ExportBaoError::Progress { source, .. },
            } => source.code(),
            _ => ERR_INTERNAL,
        }
    }
}

/// Handle a single get request.
///
/// Requires a database, the request, and a writer.
async fn handle_get_many_impl<W: AsyncStreamWriter>(
    store: Store,
    request: GetManyRequest,
    writer: &mut ProgressWriter<W>,
) -> Result<(), HandleGetManyError> {
    debug!("get_many received request");
    let request_ranges = request.ranges.iter_infinite();
    for (child, (hash, ranges)) in request.hashes.iter().zip(request_ranges).enumerate() {
        if !ranges.is_empty() {
            send_blob(&store, child as u64, *hash, ranges.clone(), writer).await?;
        }
    }
    Ok(())
}

pub async fn handle_get_many<H: ErrorHandler>(
    mut pair: StreamPair<H::R, H::W>,
    store: Store,
    request: GetManyRequest,
) -> anyhow::Result<()> {
    let res = pair.get_many_request(|| request.clone()).await;
    let tracker = handle_read_request_result::<H, _, _>(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_get_many_impl(store, request, &mut writer).await;
    handle_write_result::<H, _, _>(&mut writer, res).await?;
    Ok(())
}

#[derive(Debug, Snafu)]
pub enum HandlePushError {
    #[snafu(transparent)]
    ExportBao {
        source: ExportBaoError,
    },

    InvalidHashSeq,

    #[snafu(transparent)]
    Request {
        source: RequestError,
    },
}

impl HasErrorCode for HandlePushError {
    fn code(&self) -> VarInt {
        match self {
            Self::ExportBao {
                source: ExportBaoError::Progress { source, .. },
            } => source.code(),
            _ => ERR_INTERNAL,
        }
    }
}

/// Handle a single push request.
///
/// Requires a database, the request, and a reader.
async fn handle_push_impl<R: AsyncStreamReader>(
    store: Store,
    request: PushRequest,
    reader: &mut ProgressReader<R>,
) -> Result<(), HandlePushError> {
    let hash = request.hash;
    debug!(%hash, "push received request");
    let mut request_ranges = request.ranges.iter_infinite();
    let root_ranges = request_ranges.next().expect("infinite iterator");
    if !root_ranges.is_empty() {
        // todo: send progress from import_bao_quinn or rename to import_bao_quinn_with_progress
        store
            .import_bao_reader(hash, root_ranges.clone(), &mut reader.inner)
            .await?;
    }
    if request.ranges.is_blob() {
        debug!("push request complete");
        return Ok(());
    }
    // todo: we assume here that the hash sequence is complete. For some requests this might not be the case. We would need `LazyHashSeq` for that, but it is buggy as of now!
    let hash_seq = store.get_bytes(hash).await?;
    let hash_seq = HashSeq::try_from(hash_seq).map_err(|_| HandlePushError::InvalidHashSeq)?;
    for (child_hash, child_ranges) in hash_seq.into_iter().zip(request_ranges) {
        if child_ranges.is_empty() {
            continue;
        }
        store
            .import_bao_reader(child_hash, child_ranges.clone(), &mut reader.inner)
            .await?;
    }
    Ok(())
}

pub async fn handle_push<H: ErrorHandler>(
    mut pair: StreamPair<H::R, H::W>,
    store: Store,
    request: PushRequest,
) -> anyhow::Result<()> {
    let res = pair.push_request(|| request.clone()).await;
    let tracker = handle_read_request_result::<H, _, _>(&mut pair, res).await?;
    let mut reader = pair.into_reader(tracker).await?;
    let res = handle_push_impl(store, request, &mut reader).await;
    handle_read_result::<H, _, _>(&mut reader, res).await?;
    Ok(())
}

/// Send a blob to the client.
pub(crate) async fn send_blob<W: AsyncStreamWriter>(
    store: &Store,
    index: u64,
    hash: Hash,
    ranges: ChunkRanges,
    writer: &mut ProgressWriter<W>,
) -> ExportBaoResult<()> {
    store
        .export_bao(hash, ranges)
        .write_with_progress(&mut writer.inner, &mut writer.context, &hash, index)
        .await
}

#[derive(Debug, Snafu)]
pub enum HandleObserveError {
    ObserveStreamClosed,

    #[snafu(transparent)]
    RemoteClosed {
        source: io::Error,
    },
}

impl HasErrorCode for HandleObserveError {
    fn code(&self) -> VarInt {
        ERR_INTERNAL
    }
}

/// Handle a single push request.
///
/// Requires a database, the request, and a reader.
async fn handle_observe_impl(
    store: Store,
    request: ObserveRequest,
    writer: &mut ProgressWriter,
) -> std::result::Result<(), HandleObserveError> {
    let mut stream = store
        .observe(request.hash)
        .stream()
        .await
        .map_err(|_| HandleObserveError::ObserveStreamClosed)?;
    let mut old = stream
        .next()
        .await
        .ok_or(HandleObserveError::ObserveStreamClosed)?;
    // send the initial bitfield
    send_observe_item(writer, &old).await?;
    // send updates until the remote loses interest
    loop {
        select! {
            new = stream.next() => {
                let new = new.ok_or(HandleObserveError::ObserveStreamClosed)?;
                let diff = old.diff(&new);
                if diff.is_empty() {
                    continue;
                }
                send_observe_item(writer, &diff).await?;
                old = new;
            }
            _ = writer.inner.0.stopped() => {
                debug!("observer closed");
                break;
            }
        }
    }
    Ok(())
}

async fn send_observe_item(writer: &mut ProgressWriter, item: &Bitfield) -> io::Result<()> {
    use irpc::util::AsyncWriteVarintExt;
    let item = ObserveItem::from(item);
    let len = writer.inner.0.write_length_prefixed(item).await?;
    writer.context.log_other_write(len);
    Ok(())
}

pub async fn handle_observe<H: ErrorHandler<W = DefaultWriter>>(
    mut pair: StreamPair<H::R, H::W>,
    store: Store,
    request: ObserveRequest,
) -> anyhow::Result<()> {
    let res = pair.observe_request(|| request.clone()).await;
    let tracker = handle_read_request_result::<H, _, _>(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_observe_impl(store, request, &mut writer).await;
    handle_write_result::<H, _, _>(&mut writer, res).await?;
    Ok(())
}

pub struct ProgressReader<R: AsyncStreamReader = DefaultReader> {
    inner: R,
    context: ReaderContext,
}

impl<R: AsyncStreamReader> ProgressReader<R> {
    async fn transfer_aborted(&self) {
        self.context
            .tracker
            .transfer_aborted(|| Box::new(self.context.stats()))
            .await
            .ok();
    }

    async fn transfer_completed(&self) {
        self.context
            .tracker
            .transfer_completed(|| Box::new(self.context.stats()))
            .await
            .ok();
    }
}

pub(crate) trait RecvStreamExt: AsyncStreamReader {
    async fn expect_eof(&mut self) -> io::Result<()> {
        match self.read_u8().await {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected data",
            )),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn read_u8(&mut self) -> io::Result<u8> {
        let buf = self.read::<1>().await?;
        Ok(buf[0])
    }

    async fn read_to_end_as<T: DeserializeOwned>(
        &mut self,
        max_size: usize,
    ) -> io::Result<(T, usize)> {
        let data = self.read_bytes(max_size).await?;
        self.expect_eof().await?;
        let value = postcard::from_bytes(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok((value, data.len()))
    }

    async fn read_length_prefixed<T: DeserializeOwned>(
        &mut self,
        max_size: usize,
    ) -> io::Result<T> {
        let Some(n) = self.read_varint_u64().await? else {
            return Err(io::ErrorKind::UnexpectedEof.into());
        };
        if n > max_size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "length prefix too large",
            ));
        }
        let n = n as usize;
        let data = self.read_bytes(n).await?;
        let value = postcard::from_bytes(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(value)
    }

    /// Reads a u64 varint from an AsyncRead source, using the Postcard/LEB128 format.
    ///
    /// In Postcard's varint format (LEB128):
    /// - Each byte uses 7 bits for the value
    /// - The MSB (most significant bit) of each byte indicates if there are more bytes (1) or not (0)
    /// - Values are stored in little-endian order (least significant group first)
    ///
    /// Returns the decoded u64 value.
    async fn read_varint_u64(&mut self) -> io::Result<Option<u64>> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;

        loop {
            // We can only shift up to 63 bits (for a u64)
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Varint is too large for u64",
                ));
            }

            // Read a single byte
            let res = self.read_u8().await;
            if shift == 0 {
                if let Err(cause) = res {
                    if cause.kind() == io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    } else {
                        return Err(cause);
                    }
                }
            }

            let byte = res?;

            // Extract the 7 value bits (bits 0-6, excluding the MSB which is the continuation bit)
            let value = (byte & 0x7F) as u64;

            // Add the bits to our result at the current shift position
            result |= value << shift;

            // If the high bit is not set (0), this is the last byte
            if byte & 0x80 == 0 {
                break;
            }

            // Move to the next 7 bits
            shift += 7;
        }

        Ok(Some(result))
    }
}

impl<R: AsyncStreamReader> RecvStreamExt for R {}
