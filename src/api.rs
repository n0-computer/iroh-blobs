//! The user-facing store API.
//!
//! This module works the same whether the store lives in the same process or
//! on a remote node reached over RPC. The entry point is [`Store`].
//!
//! ## Getting a `Store`
//!
//! - Deref from [`crate::store::mem::MemStore`] or [`crate::store::fs::FsStore`].
//! - Deref from [`crate::BlobsProtocol`] when you have a running iroh router.
//! - [`Store::connect`] to talk to a remote store over QUIC (requires the
//!   `rpc` feature).
//!
//! ## Sub-APIs
//!
//! `Store` itself derefs to [`blobs::Blobs`] for convenience. The other
//! namespaces are accessed via [`Store::tags`] and [`Store::remote`].
use std::{io, ops::Deref};

use bao_tree::io::EncodeError;
use iroh::Endpoint;
use n0_error::{e, stack_error};
use proto::{ShutdownRequest, SyncDbRequest};
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};
use tags::Tags;

pub mod blobs;
pub mod downloader;
pub mod proto;
pub mod remote;
pub mod tags;
use crate::{api::proto::WaitIdleRequest, provider::events::ProgressError};
pub use crate::{store::util::Tag, util::temp_tag::TempTag};

pub(crate) type ApiClient = irpc::Client<proto::Request>;

/// Error returned by store operations that go through the RPC layer.
///
/// An operation can fail either because the RPC transport itself broke
/// (`Rpc` variant) or because the store reported a logical error (`Inner`
/// variant).
#[allow(missing_docs)]
#[non_exhaustive]
#[stack_error(derive, add_meta)]
pub enum RequestError {
    /// Request failed due to rpc error.
    #[error("rpc error: {source}")]
    Rpc { source: irpc::Error },
    /// Request failed due an actual error.
    #[error("inner error: {source}")]
    Inner {
        #[error(std_err)]
        source: Error,
    },
}

impl From<irpc::Error> for RequestError {
    fn from(value: irpc::Error) -> Self {
        e!(RequestError::Rpc, value)
    }
}

impl From<Error> for RequestError {
    fn from(value: Error) -> Self {
        e!(RequestError::Inner, value)
    }
}

impl From<io::Error> for RequestError {
    fn from(value: io::Error) -> Self {
        e!(RequestError::Inner, value.into())
    }
}

impl From<irpc::channel::mpsc::RecvError> for RequestError {
    fn from(value: irpc::channel::mpsc::RecvError) -> Self {
        e!(RequestError::Rpc, value.into())
    }
}

/// Result type for operations that go through the RPC layer.
pub type RequestResult<T> = std::result::Result<T, RequestError>;

/// Error returned during a BAO-format export.
///
/// BAO exports involve streaming data from the store, so there are several
/// distinct failure points: send/receive channel errors, one-shot sync errors,
/// and IO errors from encoding the BAO stream.
#[allow(missing_docs)]
#[non_exhaustive]
#[stack_error(derive, add_meta, from_sources)]
pub enum ExportBaoError {
    #[error("send error")]
    Send { source: irpc::channel::SendError },
    #[error("mpsc recv e api.acp.pro-channelsrror")]
    MpscRecv {
        source: irpc::channel::mpsc::RecvError,
    },
    #[error("oneshot recv error")]
    OneshotRecv {
        source: irpc::channel::oneshot::RecvError,
    },
    #[error("request error")]
    Request { source: irpc::RequestError },
    #[error("io error")]
    ExportBaoIo {
        #[error(std_err)]
        source: io::Error,
    },
    #[error("encode error")]
    ExportBaoInner {
        #[error(std_err)]
        source: bao_tree::io::EncodeError,
    },
    #[error("client error")]
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
            irpc::Error::MpscRecv { source: e, .. } => e!(ExportBaoError::MpscRecv, e),
            irpc::Error::OneshotRecv { source: e, .. } => e!(ExportBaoError::OneshotRecv, e),
            irpc::Error::Send { source: e, .. } => e!(ExportBaoError::Send, e),
            irpc::Error::Request { source: e, .. } => e!(ExportBaoError::Request, e),
            #[cfg(feature = "rpc")]
            irpc::Error::Write { source: e, .. } => e!(ExportBaoError::ExportBaoIo, e.into()),
        }
    }
}

/// Result type for BAO-format export operations.
pub type ExportBaoResult<T> = std::result::Result<T, ExportBaoError>;

/// The primary error type for store operations.
///
/// All store errors ultimately wrap an [`std::io::Error`]. This lets them be
/// passed across RPC boundaries and composed with other IO code without
/// custom serialization logic for every variant.
#[derive(Serialize, Deserialize)]
#[stack_error(derive, std_sources, from_sources)]
pub enum Error {
    /// An IO error from the store or transport layer.
    #[serde(with = "crate::util::serde::io_error_serde")]
    Io(#[error(source)] io::Error),
}

impl Error {
    /// Creates an `Error` from an [`io::ErrorKind`] and a message.
    pub fn io(
        kind: io::ErrorKind,
        msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::Io(io::Error::new(kind, msg.into()))
    }

    /// Creates an `Error` wrapping `msg` as `io::ErrorKind::Other`.
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

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        match value {
            EncodeError::Io(cause) => Self::Io(cause),
            _ => Self::Io(io::Error::other(value)),
        }
    }
}

/// Result type for store operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The main entry point for the store API.
///
/// `Store` is a cheap-to-clone handle (essentially an RPC client). It derefs
/// to [`blobs::Blobs`], so blob operations can be called directly on the store.
/// Tag management is available via [`Store::tags`], and single-node downloads
/// via [`Store::remote`].
///
/// See the [module documentation](self) for ways to obtain a `Store`.
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

    /// Flushes any pending writes to durable storage.
    ///
    /// For the filesystem store this commits any outstanding redb transactions.
    /// For the memory store this is a no-op.
    pub async fn sync_db(&self) -> RequestResult<()> {
        let msg = SyncDbRequest;
        self.client.rpc(msg).await??;
        Ok(())
    }

    /// Shuts the store down cleanly.
    ///
    /// Flushes in-flight writes and frees all store resources. Any subsequent
    /// calls on this `Store` handle or clones of it will fail. When the store
    /// is embedded in an [`iroh::protocol::Router`], the router's shutdown
    /// calls this automatically.
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
