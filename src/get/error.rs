//! Error returned from get operations
use iroh::endpoint;

use crate::util::progress::ProgressSendError;

/// Failures for a fetch operation
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Hash not found.
    #[error("Hash not found")]
    NotFound(#[source] anyhow::Error),
    /// Remote has reset the connection.
    #[error("Remote has reset the connection")]
    RemoteReset(#[source] anyhow::Error),
    /// Remote behaved in a non-compliant way.
    #[error("Remote behaved in a non-compliant way")]
    NoncompliantNode(#[source] anyhow::Error),

    /// Network or IO operation failed.
    #[error("A network or IO operation failed")]
    Io(#[source] anyhow::Error),

    /// Our download request is invalid.
    #[error("Our download request is invalid")]
    BadRequest(#[source] anyhow::Error),
    /// Operation failed on the local node.
    #[error("Operation failed on the local node")]
    LocalFailure(#[source] anyhow::Error),
}

impl From<ProgressSendError> for Error {
    fn from(value: ProgressSendError) -> Self {
        Self::LocalFailure(value.into())
    }
}

impl From<endpoint::ConnectionError> for Error {
    fn from(value: endpoint::ConnectionError) -> Self {
        // explicit match just to be sure we are taking everything into account
        use endpoint::ConnectionError;
        match value {
            e @ ConnectionError::VersionMismatch => {
                // > The peer doesn't implement any supported version
                // unsupported version is likely a long time error, so this peer is not usable
                Error::NoncompliantNode(e.into())
            }
            e @ ConnectionError::TransportError(_) => {
                // > The peer violated the QUIC specification as understood by this implementation
                // bad peer we don't want to keep around
                Error::NoncompliantNode(e.into())
            }
            e @ ConnectionError::ConnectionClosed(_) => {
                // > The peer's QUIC stack aborted the connection automatically
                // peer might be disconnecting or otherwise unavailable, drop it
                Error::Io(e.into())
            }
            e @ ConnectionError::ApplicationClosed(_) => {
                // > The peer closed the connection
                // peer might be disconnecting or otherwise unavailable, drop it
                Error::Io(e.into())
            }
            e @ ConnectionError::Reset => {
                // > The peer is unable to continue processing this connection, usually due to having restarted
                Error::RemoteReset(e.into())
            }
            e @ ConnectionError::TimedOut => {
                // > Communication with the peer has lapsed for longer than the negotiated idle timeout
                Error::Io(e.into())
            }
            e @ ConnectionError::LocallyClosed => {
                // > The local application closed the connection
                // TODO(@divma): don't see how this is reachable but let's just not use the peer
                Error::Io(e.into())
            }
            e @ quinn::ConnectionError::CidsExhausted => {
                // > The connection could not be created because not enough of the CID space
                // > is available
                Error::Io(e.into())
            }
        }
    }
}

impl From<endpoint::ReadError> for Error {
    fn from(value: endpoint::ReadError) -> Self {
        use endpoint::ReadError;
        match value {
            e @ ReadError::Reset(_) => Error::RemoteReset(e.into()),
            ReadError::ConnectionLost(conn_error) => conn_error.into(),
            ReadError::ClosedStream
            | ReadError::IllegalOrderedRead
            | ReadError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                Error::Io(value.into())
            }
        }
    }
}
impl From<quinn::ClosedStream> for Error {
    fn from(value: quinn::ClosedStream) -> Self {
        Error::Io(value.into())
    }
}

impl From<endpoint::WriteError> for Error {
    fn from(value: endpoint::WriteError) -> Self {
        use endpoint::WriteError;
        match value {
            e @ WriteError::Stopped(_) => Error::RemoteReset(e.into()),
            WriteError::ConnectionLost(conn_error) => conn_error.into(),
            WriteError::ClosedStream | WriteError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                Error::Io(value.into())
            }
        }
    }
}

impl From<crate::get::fsm::ConnectedNextError> for Error {
    fn from(value: crate::get::fsm::ConnectedNextError) -> Self {
        use crate::get::fsm::ConnectedNextError::*;
        match value {
            e @ PostcardSer(_) => {
                // serialization errors indicate something wrong with the request itself
                Error::BadRequest(e.into())
            }
            e @ RequestTooBig => {
                // request will never be sent, drop it
                Error::BadRequest(e.into())
            }
            Write(e) => e.into(),
            Closed(e) => e.into(),
            e @ Io(_) => {
                // io errors are likely recoverable
                Error::Io(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::AtBlobHeaderNextError> for Error {
    fn from(value: crate::get::fsm::AtBlobHeaderNextError) -> Self {
        use crate::get::fsm::AtBlobHeaderNextError::*;
        match value {
            e @ NotFound => {
                // > This indicates that the provider does not have the requested data.
                // peer might have the data later, simply retry it
                Error::NotFound(e.into())
            }
            Read(e) => e.into(),
            e @ Io(_) => {
                // io errors are likely recoverable
                Error::Io(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::DecodeError> for Error {
    fn from(value: crate::get::fsm::DecodeError) -> Self {
        use crate::get::fsm::DecodeError::*;

        match value {
            e @ NotFound => Error::NotFound(e.into()),
            e @ ParentNotFound(_) => Error::NotFound(e.into()),
            e @ LeafNotFound(_) => Error::NotFound(e.into()),
            e @ ParentHashMismatch(_) => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                Error::NoncompliantNode(e.into())
            }
            e @ LeafHashMismatch(_) => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                Error::NoncompliantNode(e.into())
            }
            Read(e) => e.into(),
            Io(e) => e.into(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        // generally consider io errors recoverable
        // we might want to revisit this at some point
        Error::Io(value.into())
    }
}
