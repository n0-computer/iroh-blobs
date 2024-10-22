//! Error returned from get operations

use iroh_net::endpoint;

use crate::util::progress::ProgressSendError;

/// Failures for a get operation
#[derive(Debug, thiserror::Error)]
pub enum GetError {
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

impl From<ProgressSendError> for GetError {
    fn from(value: ProgressSendError) -> Self {
        Self::LocalFailure(value.into())
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
                GetError::NoncompliantNode(e.into())
            }
            e @ ConnectionError::TransportError(_) => {
                // > The peer violated the QUIC specification as understood by this implementation
                // bad peer we don't want to keep around
                GetError::NoncompliantNode(e.into())
            }
            e @ ConnectionError::ConnectionClosed(_) => {
                // > The peer's QUIC stack aborted the connection automatically
                // peer might be disconnecting or otherwise unavailable, drop it
                GetError::Io(e.into())
            }
            e @ ConnectionError::ApplicationClosed(_) => {
                // > The peer closed the connection
                // peer might be disconnecting or otherwise unavailable, drop it
                GetError::Io(e.into())
            }
            e @ ConnectionError::Reset => {
                // > The peer is unable to continue processing this connection, usually due to having restarted
                GetError::RemoteReset(e.into())
            }
            e @ ConnectionError::TimedOut => {
                // > Communication with the peer has lapsed for longer than the negotiated idle timeout
                GetError::Io(e.into())
            }
            e @ ConnectionError::LocallyClosed => {
                // > The local application closed the connection
                // TODO(@divma): don't see how this is reachable but let's just not use the peer
                GetError::Io(e.into())
            }
            e @ quinn::ConnectionError::CidsExhausted => {
                // > The connection could not be created because not enough of the CID space
                // > is available
                GetError::Io(e.into())
            }
        }
    }
}

impl From<endpoint::ReadError> for GetError {
    fn from(value: endpoint::ReadError) -> Self {
        use endpoint::ReadError;
        match value {
            e @ ReadError::Reset(_) => GetError::RemoteReset(e.into()),
            ReadError::ConnectionLost(conn_error) => conn_error.into(),
            ReadError::ClosedStream
            | ReadError::IllegalOrderedRead
            | ReadError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                GetError::Io(value.into())
            }
        }
    }
}
impl From<quinn::ClosedStream> for GetError {
    fn from(value: quinn::ClosedStream) -> Self {
        GetError::Io(value.into())
    }
}

impl From<endpoint::WriteError> for GetError {
    fn from(value: endpoint::WriteError) -> Self {
        use endpoint::WriteError;
        match value {
            e @ WriteError::Stopped(_) => GetError::RemoteReset(e.into()),
            WriteError::ConnectionLost(conn_error) => conn_error.into(),
            WriteError::ClosedStream | WriteError::ZeroRttRejected => {
                // all these errors indicate the peer is not usable at this moment
                GetError::Io(value.into())
            }
        }
    }
}

impl From<crate::get::fsm::ConnectedNextError> for GetError {
    fn from(value: crate::get::fsm::ConnectedNextError) -> Self {
        use crate::get::fsm::ConnectedNextError::*;
        match value {
            e @ PostcardSer(_) => {
                // serialization errors indicate something wrong with the request itself
                GetError::BadRequest(e.into())
            }
            e @ RequestTooBig => {
                // request will never be sent, drop it
                GetError::BadRequest(e.into())
            }
            Write(e) => e.into(),
            Closed(e) => e.into(),
            e @ Io(_) => {
                // io errors are likely recoverable
                GetError::Io(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::AtBlobHeaderNextError> for GetError {
    fn from(value: crate::get::fsm::AtBlobHeaderNextError) -> Self {
        use crate::get::fsm::AtBlobHeaderNextError::*;
        match value {
            e @ NotFound => {
                // > This indicates that the provider does not have the requested data.
                // peer might have the data later, simply retry it
                GetError::NotFound(e.into())
            }
            Read(e) => e.into(),
            e @ Io(_) => {
                // io errors are likely recoverable
                GetError::Io(e.into())
            }
        }
    }
}

impl From<crate::get::fsm::DecodeError> for GetError {
    fn from(value: crate::get::fsm::DecodeError) -> Self {
        use crate::get::fsm::DecodeError::*;

        match value {
            e @ NotFound => GetError::NotFound(e.into()),
            e @ ParentNotFound(_) => GetError::NotFound(e.into()),
            e @ LeafNotFound(_) => GetError::NotFound(e.into()),
            e @ ParentHashMismatch(_) => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                GetError::NoncompliantNode(e.into())
            }
            e @ LeafHashMismatch(_) => {
                // TODO(@divma): did the peer sent wrong data? is it corrupted? did we sent a wrong
                // request?
                GetError::NoncompliantNode(e.into())
            }
            Read(e) => e.into(),
            Io(e) => e.into(),
        }
    }
}

impl From<std::io::Error> for GetError {
    fn from(value: std::io::Error) -> Self {
        // generally consider io errors recoverable
        // we might want to revisit this at some point
        GetError::Io(value.into())
    }
}
