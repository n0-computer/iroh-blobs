//! API for tag management.
//!
//! The purpose of tags is to mark information as important to prevent it
//! from being garbage-collected (if the garbage collector is turned on).
//! Currently this is used for blobs.
//!
//! The main entry point is the [`Client`].
//!
//! You obtain a [`Client`] via [`Iroh::tags()`](crate::client::Iroh::tags).
//!
//! [`Client::list`] can be used to list all tags.
//! [`Client::list_hash_seq`] can be used to list all tags with a hash_seq format.
//!
//! [`Client::delete`] can be used to delete a tag.
use crate::{BlobFormat, Hash, Tag};
use anyhow::Result;
use futures_lite::{Stream, StreamExt};
use quic_rpc::RpcClient;
use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

use crate::rpc::proto::tags::{DeleteRequest, ListRequest};

/// Iroh tags client.
#[derive(Debug, Clone, RefCast)]
#[repr(transparent)]
pub struct Client<C, S> {
    pub(super) rpc: RpcClient<crate::rpc::proto::RpcService, C, S>,
}

impl<C, S> Client<C, S>
where
    C: quic_rpc::ServiceConnection<S>,
    S: quic_rpc::Service,
{
    /// Lists all tags.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
        let stream = self.rpc.server_streaming(ListRequest::all()).await?;
        Ok(stream.map(|res| res.map_err(anyhow::Error::from)))
    }

    /// Lists all tags with a hash_seq format.
    pub async fn list_hash_seq(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
        let stream = self.rpc.server_streaming(ListRequest::hash_seq()).await?;
        Ok(stream.map(|res| res.map_err(anyhow::Error::from)))
    }

    /// Deletes a tag.
    pub async fn delete(&self, name: Tag) -> Result<()> {
        self.rpc.rpc(DeleteRequest { name }).await??;
        Ok(())
    }
}

/// Information about a tag.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagInfo {
    /// Name of the tag
    pub name: Tag,
    /// Format of the data
    pub format: BlobFormat,
    /// Hash of the data
    pub hash: Hash,
}
