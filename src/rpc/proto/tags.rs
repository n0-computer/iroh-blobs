//! Tags RPC protocol
use nested_enum_utils::enum_conversions;
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::{RpcResult, RpcService};
use crate::{
    net_protocol::BatchId,
    rpc::client::tags::{DeleteOptions, ListOptions, TagInfo},
    HashAndFormat, Tag,
};

#[allow(missing_docs)]
#[derive(strum::Display, Debug, Serialize, Deserialize)]
#[enum_conversions(super::Request)]
#[rpc_requests(RpcService)]
pub enum Request {
    #[rpc(response = RpcResult<Tag>)]
    Create(CreateRequest),
    #[rpc(response = RpcResult<()>)]
    Set(SetRequest),
    #[rpc(response = RpcResult<()>)]
    DeleteTag(DeleteRequest),
    #[server_streaming(response = TagInfo)]
    ListTags(ListRequest),
}

#[allow(missing_docs)]
#[derive(strum::Display, Debug, Serialize, Deserialize)]
#[enum_conversions(super::Response)]
pub enum Response {
    Create(RpcResult<Tag>),
    ListTags(TagInfo),
    DeleteTag(RpcResult<()>),
}

/// Determine how to sync the db after a modification operation
#[derive(Debug, Serialize, Deserialize, Default)]
pub enum SyncMode {
    /// Fully sync the db
    #[default]
    Full,
    /// Do not sync the db
    None,
}

/// Create a tag
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRequest {
    /// Value of the tag
    pub value: HashAndFormat,
    /// Batch to use, none for global
    pub batch: Option<BatchId>,
    /// Sync mode
    pub sync: SyncMode,
}

/// Set or delete a tag
#[derive(Debug, Serialize, Deserialize)]
pub struct SetRequest {
    /// Name of the tag
    pub name: Tag,
    /// Value of the tag
    pub value: HashAndFormat,
    /// Batch to use, none for global
    pub batch: Option<BatchId>,
    /// Sync mode
    pub sync: SyncMode,
}

/// List all collections
///
/// Lists all collections that have been explicitly added to the database.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListRequest {
    /// List raw tags
    pub raw: bool,
    /// List hash seq tags
    pub hash_seq: bool,
    /// From tag (inclusive)
    pub from: Option<Tag>,
    /// To tag (exclusive)
    pub to: Option<Tag>,
}

impl From<ListOptions> for ListRequest {
    fn from(options: ListOptions) -> Self {
        Self {
            raw: options.raw,
            hash_seq: options.hash_seq,
            from: options.from,
            to: options.to,
        }
    }
}

/// Delete a tag
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// From tag (inclusive)
    pub from: Option<Tag>,
    /// To tag (exclusive)
    pub to: Option<Tag>,
}

impl From<DeleteOptions> for DeleteRequest {
    fn from(options: DeleteOptions) -> Self {
        Self {
            from: options.from,
            to: options.to,
        }
    }
}
