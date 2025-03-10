//! Tags RPC protocol
use bytes::Bytes;
use nested_enum_utils::enum_conversions;
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::{RpcResult, RpcService};
use crate::{
    net_protocol::BatchId,
    rpc::client::tags::TagInfo,
    util::{increment_vec, next_prefix},
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
    /// Value of the tag, None to delete
    pub value: Option<HashAndFormat>,
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
    /// From tag
    pub from: Option<Tag>,
    /// To tag (exclusive)
    pub to: Option<Tag>,
}

impl ListRequest {
    /// List tags with a prefix
    pub fn prefix(prefix: Tag) -> Self {
        let mut to = prefix.0.to_vec();
        let to = if next_prefix(&mut to) {
            Some(Bytes::from(to).into())
        } else {
            None
        };
        Self {
            raw: true,
            hash_seq: true,
            from: Some(prefix),
            to,
        }
    }

    /// List a single tag
    pub fn single(name: Tag) -> Self {
        let mut next = name.0.to_vec();
        increment_vec(&mut next);
        let next = Bytes::from(next).into();
        Self {
            raw: true,
            hash_seq: true,
            from: Some(name),
            to: Some(next),
        }
    }

    /// List all tags
    pub fn all() -> Self {
        Self {
            raw: true,
            hash_seq: true,
            from: None,
            to: None,
        }
    }

    /// List raw tags
    pub fn raw() -> Self {
        Self {
            raw: true,
            hash_seq: false,
            from: None,
            to: None,
        }
    }

    /// List hash seq tags
    pub fn hash_seq() -> Self {
        Self {
            raw: false,
            hash_seq: true,
            from: None,
            to: None,
        }
    }
}

/// Delete a tag
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Name of the tag
    pub name: Tag,
}
