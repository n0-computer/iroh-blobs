//! Error returned from get operations
use std::io;

use iroh::endpoint::{self, ClosedStream};
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use quinn::{ConnectionError, ReadError, WriteError};
use snafu::{Backtrace, IntoError, Snafu};

use crate::{
    api::ExportBaoError,
    get::fsm::{AtBlobHeaderNextError, ConnectedNextError, DecodeError},
};

#[derive(Debug, Snafu)]
pub enum NotFoundCases {
    #[snafu(transparent)]
    AtBlobHeaderNext { source: AtBlobHeaderNextError },
    #[snafu(transparent)]
    Decode { source: DecodeError },
}

#[derive(Debug, Snafu)]
pub enum NoncompliantNodeCases {
    #[snafu(transparent)]
    Connection { source: ConnectionError },
    #[snafu(transparent)]
    Decode { source: DecodeError },
}

#[derive(Debug, Snafu)]
pub enum RemoteResetCases {
    #[snafu(transparent)]
    Read { source: ReadError },
    #[snafu(transparent)]
    Write { source: WriteError },
    #[snafu(transparent)]
    Connection { source: ConnectionError },
}

#[derive(Debug, Snafu)]
pub enum BadRequestCases {
    #[snafu(transparent)]
    Anyhow { source: anyhow::Error },
    #[snafu(transparent)]
    Postcard { source: postcard::Error },
    #[snafu(transparent)]
    ConnectedNext { source: ConnectedNextError },
}

#[derive(Debug, Snafu)]
pub enum LocalFailureCases {
    #[snafu(transparent)]
    Io {
        source: io::Error,
    },
    #[snafu(transparent)]
    Anyhow {
        source: anyhow::Error,
    },
    #[snafu(transparent)]
    IrpcSend {
        source: irpc::channel::SendError,
    },
    #[snafu(transparent)]
    Irpc {
        source: irpc::Error,
    },
    #[snafu(transparent)]
    ExportBao {
        source: ExportBaoError,
    },
    TokioSend {},
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for LocalFailureCases {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        LocalFailureCases::TokioSend {}
    }
}

#[derive(Debug, Snafu)]
pub enum IoCases {
    #[snafu(transparent)]
    Io { source: io::Error },
    #[snafu(transparent)]
    ConnectionError { source: endpoint::ConnectionError },
    #[snafu(transparent)]
    ReadError { source: endpoint::ReadError },
    #[snafu(transparent)]
    WriteError { source: endpoint::WriteError },
    #[snafu(transparent)]
    ClosedStream { source: endpoint::ClosedStream },
    #[snafu(transparent)]
    ConnectedNextError { source: ConnectedNextError },
    #[snafu(transparent)]
    AtBlobHeaderNextError { source: AtBlobHeaderNextError },
}

/// Failures for a get operation
#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum GetError {
    /// Hash not found, or a requested chunk for the hash not found.
    #[snafu(display("Data for hash not found"))]
    NotFound {
        #[snafu(source(from(NotFoundCases, Box::new)))]
        source: Box<NotFoundCases>,
    },
    /// Remote has reset the connection.
    #[snafu(display("Remote has reset the connection"))]
    RemoteReset {
        #[snafu(source(from(RemoteResetCases, Box::new)))]
        source: Box<RemoteResetCases>,
    },
    /// Remote behaved in a non-compliant way.
    #[snafu(display("Remote behaved in a non-compliant way"))]
    NoncompliantNode {
        #[snafu(source(from(NoncompliantNodeCases, Box::new)))]
        source: Box<NoncompliantNodeCases>,
    },

    /// Network or IO operation failed.
    #[snafu(display("A network or IO operation failed"))]
    Io {
        #[snafu(source(from(IoCases, Box::new)))]
        source: Box<IoCases>,
    },
    /// Our download request is invalid.
    #[snafu(display("Our download request is invalid"))]
    BadRequest {
        #[snafu(source(from(BadRequestCases, Box::new)))]
        source: Box<BadRequestCases>,
    },
    /// Operation failed on the local node.
    #[snafu(display("Operation failed on the local node"))]
    LocalFailure {
        #[snafu(source(from(LocalFailureCases, Box::new)))]
        source: Box<LocalFailureCases>,
    },
}

pub type GetResult<T> = std::result::Result<T, GetError>;

impl From<irpc::channel::SendError> for GetError {
    fn from(value: irpc::channel::SendError) -> Self {
        LocalFailureSnafu.into_error(value.into())
    }
}

impl<T: Send + Sync + 'static> From<tokio::sync::mpsc::error::SendError<T>> for GetError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        LocalFailureSnafu.into_error(value.into())
    }
}

impl From<endpoint::ConnectionError> for GetError {
    fn from(value: endpoint::ConnectionError) -> Self {
        // explicit match just to be sure we are taking everything into account
        use endpoint::ConnectionError;
        match value {
            e @ ConnectionError::VersionMismatch => {
                // > The peer doesn't implement any supported version
                // unsupported version is likely a long time error, so this peer is not usable
                NoncompliantNodeSnafu.into_error(e.into())
            }
            e @ ConnectionError::TransportError(_) => {
                // > The peer violated the QUIC specification as understood by this implementation
                // bad peer we don't want to keep around
                NoncompliantNodeSnafu.into_error(e.into())
            }
            e @ ConnectionError::ConnectionClosed(_) => {
                // > The peer's QUIC stack aborted the connection automatically
                // peer might be disconnecting or otherwise unavailable, drop it
                IoSnafu.into_error(e.into())
            }
            e @ ConnectionError::ApplicationClosed(_) => {
                // > The peer closed the connection
                // peer might be disconnecting or otherwise unavailable, drop it
                IoSnafu.into_error(e.into())
            }
            e @ ConnectionError::Reset => {
                // > The peer is unable to continue processing this connection, usually due to having restarted
                RemoteResetSnafu.into_error(e.into())
            }
            e @ ConnectionError::TimedOut => {
                // > Communication with the peer has lapsed for longer than the negotiated idle timeout
                IoSnafu.into_error(e.into())
            }
            e @ ConnectionError::LocallyClosed => {
                // > The local application closed the connection
                // TODO(@divma): don't see how this is reachable but let's just not use the peer
                IoSnafu.into_error(e.into())
            }
            e @ ConnectionError::CidsExhausted => {
                // > The connection could not be created because not enough of the CID space
                // > is available
                IoSnafu.into_error(e.into())
            }
        }
    }
}

impl From<endpoint::ReadError> for GetError {
    fn from(value: endpoint::ReadError) -> Self {
        use endpoint::ReadError;
        match value {
            e @ ReadError::Reset(_) => RemoteResetSnafu.into_error(e.into()),
            ReadError::ConnectionLost(conn_error) => conn_error.into(),
            ReadError::ClosedStream
            | ReadError::IllegalOrderedRead
            | ReadError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                IoSnafu.into_error(value.into())
            }
        }
    }
}
impl From<ClosedStream> for GetError {
    fn from(value: ClosedStream) -> Self {
        IoSnafu.into_error(value.into())
    }
}

impl From<quinn::WriteError> for GetError {
    fn from(value: quinn::WriteError) -> Self {
        use quinn::WriteError;
        match value {
            e @ WriteError::Stopped(_) => RemoteResetSnafu.into_error(e.into()),
            WriteError::ConnectionLost(conn_error) => conn_error.into(),
            WriteError::ClosedStream | WriteError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                IoSnafu.into_error(value.into())
            }
        }
    }
}

impl From<crate::get::fsm::ConnectedNextError> for GetError {
    fn from(value: crate::get::fsm::ConnectedNextError) -> Self {
        use crate::get::fsm::ConnectedNextError::*;
        match value {
            e @ PostcardSer { .. } => {
                // serialization errors indicate something wrong with the request itself
                BadRequestSnafu.into_error(e.into())
            }
            e @ RequestTooBig { .. } => {
                // request will never be sent, drop it
                BadRequestSnafu.into_error(e.into())
            }
            Write { source, .. } => source.into(),
            Closed { source, .. } => source.into(),
            e @ Io { .. } => {
                // io errors are likely recoverable
                IoSnafu.into_error(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::AtBlobHeaderNextError> for GetError {
    fn from(value: crate::get::fsm::AtBlobHeaderNextError) -> Self {
        use crate::get::fsm::AtBlobHeaderNextError::*;
        match value {
            e @ NotFound { .. } => {
                // > This indicates that the provider does not have the requested data.
                // peer might have the data later, simply retry it
                NotFoundSnafu.into_error(e.into())
            }
            EndpointRead { source, .. } => source.into(),
            e @ Io { .. } => {
                // io errors are likely recoverable
                IoSnafu.into_error(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::DecodeError> for GetError {
    fn from(value: crate::get::fsm::DecodeError) -> Self {
        use crate::get::fsm::DecodeError::*;

        match value {
            e @ ChunkNotFound { .. } => NotFoundSnafu.into_error(e.into()),
            e @ ParentNotFound { .. } => NotFoundSnafu.into_error(e.into()),
            e @ LeafNotFound { .. } => NotFoundSnafu.into_error(e.into()),
            e @ ParentHashMismatch { .. } => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                NoncompliantNodeSnafu.into_error(e.into())
            }
            e @ LeafHashMismatch { .. } => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                NoncompliantNodeSnafu.into_error(e.into())
            }
            Read { source, .. } => source.into(),
            DecodeIo { source, .. } => source.into(),
        }
    }
}

impl From<std::io::Error> for GetError {
    fn from(value: std::io::Error) -> Self {
        // generally consider io errors recoverable
        // we might want to revisit this at some point
        IoSnafu.into_error(value.into())
    }
}
