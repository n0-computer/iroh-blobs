//! The low level server side API
//!
//! Note that while using this API directly is fine, the standard way
//! to provide data is to just register a [`crate::BlobsProtocol`] protocol
//! handler with an [`iroh::Endpoint`](iroh::protocol::Router).
use std::{
    fmt::Debug,
    future::Future,
    io,
    ops::DerefMut,
    time::{Duration, Instant},
};

use anyhow::Result;
use bao_tree::ChunkRanges;
use bytes::Bytes;
use iroh::endpoint;
use iroh_io::{AsyncStreamReader, AsyncStreamWriter};
use n0_future::StreamExt;
use quinn::{ConnectionError, ReadExactError, VarInt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snafu::Snafu;
use tokio::{io::AsyncRead, select};
use tracing::{debug, debug_span, warn, Instrument};

use crate::{
    api::{
        blobs::{Bitfield, WriteProgress},
        ExportBaoError, ExportBaoResult, RequestError, Store,
    },
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

type DefaultWriter = iroh::endpoint::SendStream;
type DefaultReader = iroh::endpoint::RecvStream;

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
pub struct StreamPair<
    R: crate::provider::RecvStream = DefaultReader,
    W: crate::provider::SendStream = DefaultWriter,
> {
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
            reader,
            writer,
            events,
        ))
    }
}

impl<R: crate::provider::RecvStream, W: crate::provider::SendStream> StreamPair<R, W> {
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
pub struct ProgressWriter<W: crate::provider::SendStream = DefaultWriter> {
    /// The quinn::SendStream to write to
    pub inner: W,
    pub(crate) context: WriterContext,
}

impl<W: crate::provider::SendStream> ProgressWriter<W> {
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
            tokio::spawn(handle_stream(context, store).instrument(span));
        }
        progress
            .connection_closed(|| ConnectionClosed { connection_id })
            .await
            .ok();
    }
    .instrument(span)
    .await
}

pub trait SendStreamSpecific: Send {
    /// Reset the stream with the given error code.
    fn reset(&mut self, code: VarInt) -> io::Result<()>;
    /// Wait for the stream to be stopped, returning the error code if it was.
    fn stopped(&mut self) -> impl Future<Output = io::Result<Option<VarInt>>> + Send;
}

/// An abstract `iroh::endpoint::SendStream`.
pub trait SendStream: SendStreamSpecific {
    /// Send bytes to the stream. This takes a `Bytes` because iroh can directly use them.
    fn send_bytes(&mut self, bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send;
    /// Send that sends a fixed sized buffer.
    fn send<const L: usize>(
        &mut self,
        buf: &[u8; L],
    ) -> impl Future<Output = io::Result<()>> + Send;
    /// Sync the stream. Not needed for iroh, but needed for intermediate buffered streams such as compression.
    fn sync(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

pub trait RecvStreamSpecific: Send {
    /// Stop the stream with the given error code.
    fn stop(&mut self, code: VarInt) -> io::Result<()>;
}

/// An abstract `iroh::endpoint::RecvStream`.
pub trait RecvStream: RecvStreamSpecific {
    /// Receive up to `len` bytes from the stream, directly into a `Bytes`.
    fn recv_bytes(&mut self, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;
    /// Receive exactly `len` bytes from the stream, directly into a `Bytes`.
    ///
    /// This will return an error if the stream ends before `len` bytes are read.
    ///
    /// Note that this is different from `recv_bytes`, which will return fewer bytes if the stream ends.
    fn recv_bytes_exact(&mut self, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;
    /// Receive exactly `L` bytes from the stream, directly into a `[u8; L]`.
    fn recv<const L: usize>(&mut self) -> impl Future<Output = io::Result<[u8; L]>> + Send;
}

impl SendStream for iroh::endpoint::SendStream {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        Ok(self.write_chunk(bytes).await?)
    }

    async fn send<const L: usize>(&mut self, buf: &[u8; L]) -> io::Result<()> {
        Ok(self.write_all(buf).await?)
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SendStreamSpecific for iroh::endpoint::SendStream {
    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        Ok(self.reset(code)?)
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        Ok(self.stopped().await?)
    }
}

impl RecvStream for iroh::endpoint::RecvStream {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        let mut buf = vec![0; len];
        match self.read_exact(&mut buf).await {
            Err(ReadExactError::FinishedEarly(n)) => {
                buf.truncate(n);
            }
            Err(ReadExactError::ReadError(e)) => {
                return Err(e.into());
            }
            Ok(()) => {}
        };
        Ok(buf.into())
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        let mut buf = vec![0; len];
        self.read_exact(&mut buf).await.map_err(|e| match e {
            ReadExactError::FinishedEarly(0) => io::Error::new(io::ErrorKind::UnexpectedEof, ""),
            ReadExactError::FinishedEarly(_) => io::Error::new(io::ErrorKind::InvalidData, ""),
            ReadExactError::ReadError(e) => e.into(),
        })?;
        Ok(buf.into())
    }

    async fn recv<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        let mut buf = [0; L];
        self.read_exact(&mut buf).await.map_err(|e| match e {
            ReadExactError::FinishedEarly(0) => io::Error::new(io::ErrorKind::UnexpectedEof, ""),
            ReadExactError::FinishedEarly(_) => io::Error::new(io::ErrorKind::InvalidData, ""),
            ReadExactError::ReadError(e) => e.into(),
        })?;
        Ok(buf)
    }
}

impl RecvStreamSpecific for iroh::endpoint::RecvStream {
    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        Ok(self.stop(code)?)
    }
}

impl<R: RecvStream> RecvStream for &mut R {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.deref_mut().recv_bytes(len).await
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        self.deref_mut().recv_bytes_exact(len).await
    }

    async fn recv<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        self.deref_mut().recv::<L>().await
    }
}

impl<R: RecvStreamSpecific> RecvStreamSpecific for &mut R {
    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        self.deref_mut().stop(code)
    }
}

impl<W: SendStream> SendStream for &mut W {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        self.deref_mut().send_bytes(bytes).await
    }

    async fn send<const L: usize>(&mut self, buf: &[u8; L]) -> io::Result<()> {
        self.deref_mut().send(buf).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.deref_mut().sync().await
    }
}

impl<W: SendStreamSpecific> SendStreamSpecific for &mut W {
    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        self.deref_mut().reset(code)
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        self.deref_mut().stopped().await
    }
}

#[derive(Debug)]
pub struct AsyncReadRecvStream<R>(R);

impl<R> AsyncReadRecvStream<R> {
    pub fn new(inner: R) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> R {
        self.0
    }
}

use tokio::io::AsyncReadExt;

impl<R: AsyncRead + Unpin + RecvStreamSpecific> RecvStream for AsyncReadRecvStream<R> {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        let mut res = vec![0; len];
        let mut n = 0;
        loop {
            let read = self.0.read(&mut res[n..]).await?;
            if read == 0 {
                res.truncate(n);
                break;
            }
            n += read;
            if n == len {
                break;
            }
        }
        Ok(res.into())
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        let mut res = vec![0; len];
        self.0.read_exact(&mut res).await?;
        Ok(res.into())
    }

    async fn recv<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        let mut res = [0; L];
        self.0.read_exact(&mut res).await?;
        Ok(res)
    }
}

impl<R: RecvStreamSpecific> RecvStreamSpecific for AsyncReadRecvStream<R> {
    fn stop(&mut self, code: VarInt) -> io::Result<()> {
        self.0.stop(code)
    }
}

impl RecvStream for Bytes {
    async fn recv_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        let n = len.min(self.len());
        let res = self.slice(..n);
        *self = self.slice(n..);
        Ok(res)
    }

    async fn recv_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        if self.len() < len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let res = self.slice(..len);
        *self = self.slice(len..);
        Ok(res)
    }

    async fn recv<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        if self.len() < L {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let mut res = [0; L];
        res.copy_from_slice(&self[..L]);
        *self = self.slice(L..);
        Ok(res)
    }
}

impl RecvStreamSpecific for Bytes {
    fn stop(&mut self, _code: VarInt) -> io::Result<()> {
        Ok(())
    }
}

/// Utility to convert a [tokio::io::AsyncWrite] into an [SendStream].
#[derive(Debug, Clone)]
pub struct AsyncWriteSendStream<W>(W);

impl<W> AsyncWriteSendStream<W> {
    pub fn new(inner: W) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> W {
        self.0
    }
}

use tokio::io::AsyncWriteExt;

impl<W: tokio::io::AsyncWrite + Unpin + SendStreamSpecific> SendStream for AsyncWriteSendStream<W> {
    async fn send_bytes(&mut self, bytes: Bytes) -> io::Result<()> {
        self.0.write_all(&bytes).await
    }

    async fn send<const L: usize>(&mut self, buf: &[u8; L]) -> io::Result<()> {
        self.0.write_all(buf).await
    }

    async fn sync(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
}

impl<W: SendStreamSpecific> SendStreamSpecific for AsyncWriteSendStream<W> {
    fn reset(&mut self, code: VarInt) -> io::Result<()> {
        self.0.reset(code)?;
        Ok(())
    }

    async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
        Ok(self.0.stopped().await?)
    }
}

#[derive(Debug)]
pub struct RecvStreamAsyncStreamReader<R>(R);

impl<R: RecvStream> RecvStreamAsyncStreamReader<R> {
    pub fn new(inner: R) -> Self {
        Self(inner)
    }

    pub fn into_inner(self) -> R {
        self.0
    }
}

impl<R: RecvStream> AsyncStreamReader for RecvStreamAsyncStreamReader<R> {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.0.recv_bytes_exact(len).await
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        self.0.recv::<L>().await
    }
}

/// Describes how to handle errors for a stream.
pub trait ErrorHandler {
    type W: AsyncStreamWriter;
    type R: AsyncStreamReader;
    fn stop(reader: &mut Self::R, code: VarInt) -> impl Future<Output = ()>;
    fn reset(writer: &mut Self::W, code: VarInt) -> impl Future<Output = ()>;
}

async fn handle_read_request_result<
    R: crate::provider::RecvStream,
    W: crate::provider::SendStream,
    T,
    E: HasErrorCode,
>(
    pair: &mut StreamPair<R, W>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => Ok(x),
        Err(e) => {
            pair.writer.reset(e.code()).ok();
            Err(e)
        }
    }
}
async fn handle_write_result<W: crate::provider::SendStream, T, E: HasErrorCode>(
    writer: &mut ProgressWriter<W>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => {
            writer.transfer_completed().await;
            Ok(x)
        }
        Err(e) => {
            writer.inner.reset(e.code()).ok();
            writer.transfer_aborted().await;
            Err(e)
        }
    }
}
async fn handle_read_result<R: crate::provider::RecvStream, T, E: HasErrorCode>(
    reader: &mut ProgressReader<R>,
    r: Result<T, E>,
) -> Result<T, E> {
    match r {
        Ok(x) => {
            reader.transfer_completed().await;
            Ok(x)
        }
        Err(e) => {
            reader.inner.stop(e.code()).ok();
            reader.transfer_aborted().await;
            Err(e)
        }
    }
}

pub async fn handle_stream(mut pair: StreamPair, store: Store) -> anyhow::Result<()> {
    // 1. Decode the request.
    debug!("reading request");
    let request = pair.read_request().await?;

    match request {
        Request::Get(request) => handle_get(pair, store, request).await?,
        Request::GetMany(request) => handle_get_many(pair, store, request).await?,
        Request::Observe(request) => handle_observe(pair, store, request).await?,
        Request::Push(request) => handle_push(pair, store, request).await?,
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
async fn handle_get_impl<W: crate::provider::SendStream>(
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
    writer
        .inner
        .sync()
        .await
        .map_err(|e| HandleGetError::ExportBao { source: e.into() })?;

    Ok(())
}

pub async fn handle_get<R: crate::provider::RecvStream, W: crate::provider::SendStream>(
    mut pair: StreamPair<R, W>,
    store: Store,
    request: GetRequest,
) -> anyhow::Result<()> {
    let res = pair.get_request(|| request.clone()).await;
    let tracker = handle_read_request_result(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_get_impl(store, request, &mut writer).await;
    handle_write_result(&mut writer, res).await?;
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
async fn handle_get_many_impl<W: crate::provider::SendStream>(
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

pub async fn handle_get_many<R: crate::provider::RecvStream, W: crate::provider::SendStream>(
    mut pair: StreamPair<R, W>,
    store: Store,
    request: GetManyRequest,
) -> anyhow::Result<()> {
    let res = pair.get_many_request(|| request.clone()).await;
    let tracker = handle_read_request_result(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_get_many_impl(store, request, &mut writer).await;
    handle_write_result(&mut writer, res).await?;
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
async fn handle_push_impl<R: crate::provider::RecvStream>(
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

pub async fn handle_push<R: crate::provider::RecvStream, W: crate::provider::SendStream>(
    mut pair: StreamPair<R, W>,
    store: Store,
    request: PushRequest,
) -> anyhow::Result<()> {
    let res = pair.push_request(|| request.clone()).await;
    let tracker = handle_read_request_result(&mut pair, res).await?;
    let mut reader = pair.into_reader(tracker).await?;
    let res = handle_push_impl(store, request, &mut reader).await;
    handle_read_result(&mut reader, res).await?;
    Ok(())
}

/// Send a blob to the client.
pub(crate) async fn send_blob<W: crate::provider::SendStream>(
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
async fn handle_observe_impl<W: crate::provider::SendStream>(
    store: Store,
    request: ObserveRequest,
    writer: &mut ProgressWriter<W>,
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
            _ = writer.inner.stopped() => {
                debug!("observer closed");
                break;
            }
        }
    }
    Ok(())
}

async fn send_observe_item<W: crate::provider::SendStream>(
    writer: &mut ProgressWriter<W>,
    item: &Bitfield,
) -> io::Result<()> {
    let item = ObserveItem::from(item);
    let len = writer.inner.write_length_prefixed(item).await?;
    writer.context.log_other_write(len);
    Ok(())
}

pub async fn handle_observe<R: crate::provider::RecvStream, W: crate::provider::SendStream>(
    mut pair: StreamPair<R, W>,
    store: Store,
    request: ObserveRequest,
) -> anyhow::Result<()> {
    let res = pair.observe_request(|| request.clone()).await;
    let tracker = handle_read_request_result(&mut pair, res).await?;
    let mut writer = pair.into_writer(tracker).await?;
    let res = handle_observe_impl(store, request, &mut writer).await;
    handle_write_result(&mut writer, res).await?;
    Ok(())
}

pub struct ProgressReader<R: crate::provider::RecvStream = DefaultReader> {
    inner: R,
    context: ReaderContext,
}

impl<R: crate::provider::RecvStream> ProgressReader<R> {
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

pub(crate) trait RecvStreamExt: crate::provider::RecvStream {
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
        let buf = self.recv::<1>().await?;
        Ok(buf[0])
    }

    async fn read_to_end_as<T: DeserializeOwned>(
        &mut self,
        max_size: usize,
    ) -> io::Result<(T, usize)> {
        let data = self.recv_bytes(max_size).await?;
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
        let data = self.recv_bytes(n).await?;
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

impl<R: crate::provider::RecvStream> RecvStreamExt for R {}

pub(crate) trait SendStreamExt: crate::provider::SendStream {
    async fn write_length_prefixed<T: Serialize>(&mut self, value: T) -> io::Result<usize> {
        let size = postcard::experimental::serialized_size(&value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = Vec::with_capacity(size + 9);
        irpc::util::WriteVarintExt::write_length_prefixed(&mut buf, value)?;
        let n = buf.len();
        self.send_bytes(buf.into()).await?;
        Ok(n)
    }
}

impl<W: crate::provider::SendStream> SendStreamExt for W {}
