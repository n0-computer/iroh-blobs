//! Error returned from get operations
use std::io;

use iroh::endpoint::{ConnectionError, ReadError, VarInt, WriteError};
use n0_error::{stack_error, AnyError};

use crate::get::fsm::{
    AtBlobHeaderNextError, AtClosingNextError, ConnectedNextError, DecodeError, InitialNextError,
};

/// Errors that can occur during a low-level blob get (download) operation.
#[allow(missing_docs)]
#[stack_error(derive, add_meta)]
pub enum GetError {
    #[error(transparent)]
    InitialNext {
        #[error(from)]
        source: InitialNextError,
    },
    #[error(transparent)]
    ConnectedNext {
        #[error(from)]
        source: ConnectedNextError,
    },
    #[error(transparent)]
    AtBlobHeaderNext {
        #[error(from)]
        source: AtBlobHeaderNextError,
    },
    #[error(transparent)]
    Decode {
        #[error(from)]
        source: DecodeError,
    },
    #[error(transparent)]
    IrpcSend {
        #[error(from)]
        source: irpc::channel::SendError,
    },
    #[error(transparent)]
    AtClosingNext {
        #[error(from)]
        source: AtClosingNextError,
    },
    #[error("local failure")]
    LocalFailure { source: AnyError },
    #[error("bad request")]
    BadRequest { source: AnyError },
}

impl GetError {
    /// Returns the QUIC application error code from the remote side, if the
    /// failure was caused by a stream reset or connection close with a code.
    pub fn iroh_error_code(&self) -> Option<VarInt> {
        if let Some(ReadError::Reset(code)) = self
            .remote_read()
            .and_then(|source| source.get_ref())
            .and_then(|e| e.downcast_ref::<iroh::endpoint::ReadError>())
        {
            Some(*code)
        } else if let Some(WriteError::Stopped(code)) = self
            .remote_write()
            .and_then(|source| source.get_ref())
            .and_then(|e| e.downcast_ref::<iroh::endpoint::WriteError>())
        {
            Some(*code)
        } else if let Some(ConnectionError::ApplicationClosed(ac)) = self
            .open()
            .and_then(|source| source.get_ref())
            .and_then(|e| e.downcast_ref::<iroh::endpoint::ConnectionError>())
        {
            Some(ac.error_code)
        } else {
            None
        }
    }

    /// Returns the IO error from writing to the remote, if the failure had that cause.
    pub fn remote_write(&self) -> Option<&io::Error> {
        match self {
            Self::ConnectedNext {
                source: ConnectedNextError::Write { source, .. },
                ..
            } => Some(source),
            _ => None,
        }
    }

    /// Returns the IO error from opening the stream, if the failure had that cause.
    pub fn open(&self) -> Option<&io::Error> {
        match self {
            Self::InitialNext {
                source: InitialNextError::Open { source, .. },
                ..
            } => Some(source),
            _ => None,
        }
    }

    /// Returns the IO error from reading from the remote, if the failure had that cause.
    pub fn remote_read(&self) -> Option<&io::Error> {
        match self {
            Self::AtBlobHeaderNext {
                source: AtBlobHeaderNextError::Read { source, .. },
                ..
            } => Some(source),
            Self::Decode {
                source: DecodeError::Read { source, .. },
                ..
            } => Some(source),
            Self::AtClosingNext {
                source: AtClosingNextError::Read { source, .. },
                ..
            } => Some(source),
            _ => None,
        }
    }

    /// Returns the IO error from writing to local storage, if the failure had that cause.
    pub fn local_write(&self) -> Option<&io::Error> {
        match self {
            Self::Decode {
                source: DecodeError::Write { source, .. },
                ..
            } => Some(source),
            _ => None,
        }
    }
}

/// Result type for low-level blob get operations.
pub type GetResult<T> = std::result::Result<T, GetError>;
