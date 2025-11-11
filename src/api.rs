//! The user facing API of the store.
//!
//! This API is both for interacting with an in-process store and for interacting
//! with a remote store via rpc calls.
//!
//! The entry point for the api is the [`Store`] struct. There are several ways
//! to obtain a `Store` instance: it is available via [`Deref`]
//! from the different store implementations
//! (e.g. [`MemStore`](crate::store::mem::MemStore)
//! and [`FsStore`](crate::store::fs::FsStore)) as well as on the
//! [`BlobsProtocol`](crate::BlobsProtocol) iroh protocol handler.
//!
//! You can also [`connect`](Store::connect) to a remote store that is listening
//! to rpc requests.
use std::{io, ops::Deref};

use bao_tree::io::EncodeError;
use iroh::Endpoint;
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use proto::{ShutdownRequest, SyncDbRequest};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, Snafu};
use tags::Tags;

pub mod blobs;
pub mod downloader;
pub mod proto;
pub mod remote;
pub mod tags;
use crate::{api::proto::WaitIdleRequest, provider::events::ProgressError};
pub use crate::{store::util::Tag, util::temp_tag::TempTag};

pub(crate) type ApiClient = irpc::Client<proto::Request>;

#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[allow(missing_docs)]
#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum RequestError {
    /// Request failed due to rpc error.
    #[snafu(display("rpc error: {source}"))]
    Rpc { source: irpc::Error },
    /// Request failed due an actual error.
    #[snafu(display("inner error: {source}"))]
    Inner { source: Error },
}

impl From<irpc::Error> for RequestError {
    fn from(value: irpc::Error) -> Self {
        RpcSnafu.into_error(value)
    }
}

impl From<Error> for RequestError {
    fn from(value: Error) -> Self {
        InnerSnafu.into_error(value)
    }
}

impl From<io::Error> for RequestError {
    fn from(value: io::Error) -> Self {
        InnerSnafu.into_error(value.into())
    }
}

impl From<irpc::channel::mpsc::RecvError> for RequestError {
    fn from(value: irpc::channel::mpsc::RecvError) -> Self {
        RpcSnafu.into_error(value.into())
    }
}

pub type RequestResult<T> = std::result::Result<T, RequestError>;

#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[allow(missing_docs)]
#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum ExportBaoError {
    #[snafu(display("send error: {source}"))]
    Send { source: irpc::channel::SendError },
    #[snafu(display("mpsc recv error: {source}"))]
    MpscRecv {
        source: irpc::channel::mpsc::RecvError,
    },
    #[snafu(display("oneshot recv error: {source}"))]
    OneshotRecv {
        source: irpc::channel::oneshot::RecvError,
    },
    #[snafu(display("request error: {source}"))]
    Request { source: irpc::RequestError },
    #[snafu(display("io error: {source}"))]
    ExportBaoIo { source: io::Error },
    #[snafu(display("encode error: {source}"))]
    ExportBaoInner { source: bao_tree::io::EncodeError },
    #[snafu(display("client error: {source}"))]
    ClientError { source: ProgressError },
}

impl From<ExportBaoError> for Error {
    fn from(e: ExportBaoError) -> Self {
        match e {
            ExportBaoError::Send { source, .. } => Self::Io(source.into()),
            ExportBaoError::MpscRecv { source, .. } => Self::Io(source.into()),
            ExportBaoError::OneshotRecv { source, .. } => Self::Io(source.into()),
            ExportBaoError::Request { source, .. } => Self::Io(source.into()),
            ExportBaoError::ExportBaoIo { source, .. } => Self::Io(source),
            ExportBaoError::ExportBaoInner { source, .. } => Self::Io(source.into()),
            ExportBaoError::ClientError { source, .. } => Self::Io(source.into()),
        }
    }
}

impl From<irpc::Error> for ExportBaoError {
    fn from(e: irpc::Error) -> Self {
        match e {
            irpc::Error::MpscRecv { source, .. } => MpscRecvSnafu.into_error(source),
            irpc::Error::OneshotRecv { source, .. } => OneshotRecvSnafu.into_error(source),
            irpc::Error::Send { source, .. } => SendSnafu.into_error(source),
            irpc::Error::Request { source, .. } => RequestSnafu.into_error(source),
            irpc::Error::Write { source, .. } => ExportBaoIoSnafu.into_error(source.into()),
        }
    }
}

impl From<io::Error> for ExportBaoError {
    fn from(value: io::Error) -> Self {
        ExportBaoIoSnafu.into_error(value)
    }
}

impl From<irpc::channel::mpsc::RecvError> for ExportBaoError {
    fn from(value: irpc::channel::mpsc::RecvError) -> Self {
        MpscRecvSnafu.into_error(value)
    }
}

impl From<irpc::channel::oneshot::RecvError> for ExportBaoError {
    fn from(value: irpc::channel::oneshot::RecvError) -> Self {
        OneshotRecvSnafu.into_error(value)
    }
}

impl From<irpc::channel::SendError> for ExportBaoError {
    fn from(value: irpc::channel::SendError) -> Self {
        SendSnafu.into_error(value)
    }
}

impl From<irpc::RequestError> for ExportBaoError {
    fn from(value: irpc::RequestError) -> Self {
        RequestSnafu.into_error(value)
    }
}

impl From<bao_tree::io::EncodeError> for ExportBaoError {
    fn from(value: bao_tree::io::EncodeError) -> Self {
        ExportBaoInnerSnafu.into_error(value)
    }
}

impl From<ProgressError> for ExportBaoError {
    fn from(value: ProgressError) -> Self {
        ClientSnafu.into_error(value)
    }
}

pub type ExportBaoResult<T> = std::result::Result<T, ExportBaoError>;

#[derive(Debug, derive_more::Display, derive_more::From, Serialize, Deserialize)]
pub enum Error {
    #[serde(with = "crate::util::serde::io_error_serde")]
    Io(io::Error),
}

impl Error {
    pub fn io(
        kind: io::ErrorKind,
        msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::Io(io::Error::new(kind, msg.into()))
    }

    pub fn other<E>(msg: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Io(io::Error::other(msg.into()))
    }
}

impl From<irpc::Error> for Error {
    fn from(e: irpc::Error) -> Self {
        Self::Io(e.into())
    }
}

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        match e {
            RequestError::Rpc { source, .. } => Self::Io(source.into()),
            RequestError::Inner { source, .. } => source,
        }
    }
}

impl From<irpc::channel::mpsc::RecvError> for Error {
    fn from(e: irpc::channel::mpsc::RecvError) -> Self {
        Self::Io(e.into())
    }
}

#[cfg(feature = "rpc")]
impl From<irpc::rpc::WriteError> for Error {
    fn from(e: irpc::rpc::WriteError) -> Self {
        Self::Io(e.into())
    }
}

impl From<irpc::RequestError> for Error {
    fn from(e: irpc::RequestError) -> Self {
        Self::Io(e.into())
    }
}

impl From<irpc::channel::SendError> for Error {
    fn from(e: irpc::channel::SendError) -> Self {
        Self::Io(e.into())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
        }
    }
}

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        match value {
            EncodeError::Io(cause) => Self::Io(cause),
            _ => Self::other(value),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The main entry point for the store API.
#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Store {
    client: ApiClient,
}

impl Deref for Store {
    type Target = blobs::Blobs;

    fn deref(&self) -> &Self::Target {
        blobs::Blobs::ref_from_sender(&self.client)
    }
}

impl Store {
    /// The tags API.
    pub fn tags(&self) -> &Tags {
        Tags::ref_from_sender(&self.client)
    }

    /// The blobs API.
    pub fn blobs(&self) -> &blobs::Blobs {
        blobs::Blobs::ref_from_sender(&self.client)
    }

    /// API for getting blobs from a *single* remote node.
    pub fn remote(&self) -> &remote::Remote {
        remote::Remote::ref_from_sender(&self.client)
    }

    /// Create a downloader for more complex downloads.
    ///
    /// Unlike the other APIs, this creates an object that has internal state,
    /// so don't create it ad hoc but store it somewhere if you need it multiple
    /// times.
    pub fn downloader(&self, endpoint: &Endpoint) -> downloader::Downloader {
        downloader::Downloader::new(self, endpoint)
    }

    /// Connect to a remote store as a rpc client.
    #[cfg(feature = "rpc")]
    pub fn connect(endpoint: quinn::Endpoint, addr: std::net::SocketAddr) -> Self {
        let sender = irpc::Client::quinn(endpoint, addr);
        Store::from_sender(sender)
    }

    /// Listen on a quinn endpoint for incoming rpc connections.
    #[cfg(feature = "rpc")]
    pub async fn listen(self, endpoint: quinn::Endpoint) {
        use irpc::rpc::RemoteService;

        use self::proto::Request;
        let local = self.client.as_local().unwrap().clone();
        let handler = Request::remote_handler(local);
        irpc::rpc::listen::<Request>(endpoint, handler).await
    }

    pub async fn sync_db(&self) -> RequestResult<()> {
        let msg = SyncDbRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }

    pub async fn shutdown(&self) -> irpc::Result<()> {
        let msg = ShutdownRequest;
        self.client.rpc(msg).await?;
        Ok(())
    }

    /// Waits for the store to become completely idle.
    ///
    /// This is mostly useful for tests, where you want to check that e.g. the
    /// store has written all data to disk.
    ///
    /// Note that a store is not guaranteed to become idle, if it is being
    /// interacted with concurrently. So this might wait forever.
    ///
    /// Also note that once you get the callback, the store is not guaranteed to
    /// still be idle. All this tells you that there was a point in time where
    /// the store was idle between the call and the response.
    pub async fn wait_idle(&self) -> irpc::Result<()> {
        let msg = WaitIdleRequest;
        self.client.rpc(msg).await?;
        Ok(())
    }

    pub(crate) fn from_sender(client: ApiClient) -> Self {
        Self { client }
    }

    pub(crate) fn ref_from_sender(client: &ApiClient) -> &Self {
        Self::ref_cast(client)
    }
}
