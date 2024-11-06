//! RPC protocol for the iroh-blobs service
use nested_enum_utils::enum_conversions;
use serde::{Deserialize, Serialize};

pub mod blobs;
pub mod tags;

/// quic-rpc service for iroh blobs
#[derive(Debug, Clone)]
pub struct RpcService;

impl quic_rpc::Service for RpcService {
    type Req = Request;
    type Res = Response;
}

#[allow(missing_docs)]
#[enum_conversions]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Blobs(blobs::Request),
    Tags(tags::Request),
}

#[allow(missing_docs)]
#[enum_conversions]
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Blobs(blobs::Response),
    Tags(tags::Response),
}

/// Error type for RPC operations
pub type RpcError = serde_error::Error;
/// Result type for RPC operations
pub type RpcResult<T> = Result<T, RpcError>;
