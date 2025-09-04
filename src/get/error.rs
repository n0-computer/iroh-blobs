//! Error returned from get operations
use std::io;

use iroh::endpoint::{ConnectionError, ReadError, VarInt, WriteError};
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use snafu::{Backtrace, Snafu};

use crate::get::fsm::{
    AtBlobHeaderNextError, AtClosingNextError, ConnectedNextError, DecodeError, InitialNextError,
};

/// Failures for a get operation
#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(module)]
pub enum GetError {
    #[snafu(transparent)]
    InitialNext {
        source: InitialNextError,
    },
    #[snafu(transparent)]
    ConnectedNext {
        source: ConnectedNextError,
    },
    #[snafu(transparent)]
    AtBlobHeaderNext {
        source: AtBlobHeaderNextError,
    },
    #[snafu(transparent)]
    Decode {
        source: DecodeError,
    },
    #[snafu(transparent)]
    IrpcSend {
        source: irpc::channel::SendError,
    },
    #[snafu(transparent)]
    AtClosingNext {
        source: AtClosingNextError,
    },
    LocalFailure {
        source: anyhow::Error,
    },
    BadRequest {
        source: anyhow::Error,
    },
}

impl GetError {
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

    pub fn remote_write(&self) -> Option<&io::Error> {
        match self {
            Self::ConnectedNext {
                source: ConnectedNextError::Write { source, .. },
                ..
            } => Some(&source),
            _ => None,
        }
    }

    pub fn open(&self) -> Option<&io::Error> {
        match self {
            Self::InitialNext {
                source: InitialNextError::Open { source, .. },
                ..
            } => Some(&source),
            _ => None,
        }
    }

    pub fn remote_read(&self) -> Option<&io::Error> {
        match self {
            Self::AtBlobHeaderNext {
                source: AtBlobHeaderNextError::Read { source, .. },
                ..
            } => Some(&source),
            Self::Decode {
                source: DecodeError::Read { source, .. },
                ..
            } => Some(&source),
            Self::AtClosingNext {
                source: AtClosingNextError::Read { source, .. },
                ..
            } => Some(&source),
            _ => None,
        }
    }

    pub fn local_write(&self) -> Option<&io::Error> {
        match self {
            Self::Decode {
                source: DecodeError::Write { source, .. },
                ..
            } => Some(&source),
            _ => None,
        }
    }
}

pub type GetResult<T> = std::result::Result<T, GetError>;
