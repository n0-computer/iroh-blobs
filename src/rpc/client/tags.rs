//! API for tag management.
//!
//! The purpose of tags is to mark information as important to prevent it
//! from being garbage-collected (if the garbage collector is turned on).
//! Currently this is used for blobs.
//!
//! The main entry point is the [`Client`].
//!
//! [`Client::list`] can be used to list all tags.
//! [`Client::list_hash_seq`] can be used to list all tags with a hash_seq format.
//!
//! [`Client::delete`] can be used to delete a tag.
use anyhow::Result;
use futures_lite::{Stream, StreamExt};
use quic_rpc::{client::BoxedConnector, Connector, RpcClient};
use serde::{Deserialize, Serialize};

use crate::{
    rpc::proto::{
        tags::{DeleteRequest, ListRequest},
        RpcService,
    },
    BlobFormat, Hash, Tag,
};

/// Iroh tags client.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client<C = BoxedConnector<RpcService>> {
    pub(super) rpc: RpcClient<RpcService, C>,
}

/// A client that uses the memory connector.
pub type MemClient = Client<crate::rpc::MemConnector>;

impl<C> Client<C>
where
    C: Connector<RpcService>,
{
    /// Creates a new client
    pub fn new(rpc: RpcClient<RpcService, C>) -> Self {
        Self { rpc }
    }

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
