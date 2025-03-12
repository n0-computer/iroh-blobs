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
use std::ops::{Bound, RangeBounds};

use anyhow::Result;
use futures_lite::{io, Stream, StreamExt};
use quic_rpc::{client::BoxedConnector, Connector, RpcClient};
use serde::{Deserialize, Serialize};

use crate::{
    rpc::proto::{
        tags::{DeleteRequest, ListRequest, SetRequest, SyncMode},
        RpcService,
    },
    BlobFormat, Hash, HashAndFormat, Tag,
};

/// Iroh tags client.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client<C = BoxedConnector<RpcService>> {
    pub(super) rpc: RpcClient<RpcService, C>,
}

/// Options for a list operation.
#[derive(Debug, Clone)]
pub struct ListOptions {
    /// List tags to hash seqs
    pub hash_seq: bool,
    /// List tags to raw blobs
    pub raw: bool,
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

fn tags_from_range<R, E>(range: R) -> (Option<Tag>, Option<Tag>)
where
    R: RangeBounds<E>,
    E: AsRef<[u8]>,
{
    let from = match range.start_bound() {
        Bound::Included(start) => Some(Tag::from(start.as_ref())),
        Bound::Excluded(start) => Some(Tag::from(start.as_ref()).successor()),
        Bound::Unbounded => None,
    };
    let to = match range.end_bound() {
        Bound::Included(end) => Some(Tag::from(end.as_ref()).successor()),
        Bound::Excluded(end) => Some(Tag::from(end.as_ref())),
        Bound::Unbounded => None,
    };
    (from, to)
}

impl ListOptions {
    /// List a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self {
            from,
            to,
            raw: true,
            hash_seq: true,
        }
    }

    /// List tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            raw: true,
            hash_seq: true,
            from: Some(from),
            to,
        }
    }

    /// List a single tag
    pub fn single(name: &[u8]) -> Self {
        let from = Tag::from(name);
        Self {
            to: Some(from.successor()),
            from: Some(from),
            raw: true,
            hash_seq: true,
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

/// Options for a delete operation.
#[derive(Debug, Clone)]
pub struct DeleteOptions {
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl DeleteOptions {
    /// Delete a single tag
    pub fn single(name: &[u8]) -> Self {
        let name = Tag::from(name);
        Self {
            to: Some(name.successor()),
            from: Some(name),
        }
    }

    /// Delete a range of tags
    pub fn range<R, E>(range: R) -> Self
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        let (from, to) = tags_from_range(range);
        Self { from, to }
    }

    /// Delete tags with a prefix
    pub fn prefix(prefix: &[u8]) -> Self {
        let from = Tag::from(prefix);
        let to = from.next_prefix();
        Self {
            from: Some(from),
            to,
        }
    }
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

    /// List all tags with options.
    ///
    /// This is the most flexible way to list tags. All the other list methods are just convenience
    /// methods that call this one with the appropriate options.
    pub async fn list_with_opts(
        &self,
        options: ListOptions,
    ) -> Result<impl Stream<Item = Result<TagInfo>>> {
        let stream = self
            .rpc
            .server_streaming(ListRequest::from(options))
            .await?;
        Ok(stream.map(|res| res.map_err(anyhow::Error::from)))
    }

    /// Set the value for a single tag
    pub async fn set(&self, name: impl AsRef<[u8]>, value: impl Into<HashAndFormat>) -> Result<()> {
        self.rpc
            .rpc(SetRequest {
                name: Tag::from(name.as_ref()),
                value: value.into(),
                batch: None,
                sync: SyncMode::Full,
            })
            .await??;
        Ok(())
    }

    /// Get the value of a single tag
    pub async fn get(&self, name: impl AsRef<[u8]>) -> Result<Option<TagInfo>> {
        let mut stream = self
            .list_with_opts(ListOptions::single(name.as_ref()))
            .await?;
        stream.next().await.transpose()
    }

    /// Rename a tag
    ///
    /// This is done in steps, so it is not atomic!
    pub async fn rename(&self, from: impl AsRef<[u8]>, to: impl AsRef<[u8]>) -> Result<()> {
        let from = from.as_ref();
        let to = to.as_ref();
        let Some(old) = self.get(from.as_ref()).await? else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Tag not found").into());
        };
        self.set(to.as_ref(), old.hash_and_format()).await?;
        self.delete(from.as_ref()).await?;
        Ok(())
    }

    /// List a range of tags
    pub async fn list_range<R, E>(&self, range: R) -> Result<impl Stream<Item = Result<TagInfo>>>
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        self.list_with_opts(ListOptions::range(range)).await
    }

    /// Lists all tags with the given prefix.
    pub async fn list_prefix(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> Result<impl Stream<Item = Result<TagInfo>>> {
        self.list_with_opts(ListOptions::prefix(prefix.as_ref()))
            .await
    }

    /// Lists all tags.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
        self.list_with_opts(ListOptions::all()).await
    }

    /// Lists all tags with a hash_seq format.
    pub async fn list_hash_seq(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
        self.list_with_opts(ListOptions::hash_seq()).await
    }

    /// Deletes a tag.
    pub async fn delete_with_opts(&self, options: DeleteOptions) -> Result<()> {
        self.rpc.rpc(DeleteRequest::from(options)).await??;
        Ok(())
    }

    /// Deletes a tag.
    pub async fn delete(&self, name: impl AsRef<[u8]>) -> Result<()> {
        self.delete_with_opts(DeleteOptions::single(name.as_ref()))
            .await
    }

    /// Deletes a range of tags.
    pub async fn delete_range<R, E>(&self, range: R) -> Result<()>
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        self.delete_with_opts(DeleteOptions::range(range)).await
    }

    /// Delete all tags with the given prefix.
    pub async fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.delete_with_opts(DeleteOptions::prefix(prefix.as_ref()))
            .await
    }

    /// Delete all tags. Use with care. After this, all data will be garbage collected.
    pub async fn delete_all(&self) -> Result<()> {
        self.delete_with_opts(DeleteOptions {
            from: None,
            to: None,
        })
        .await
    }
}

/// Information about a tag.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TagInfo {
    /// Name of the tag
    pub name: Tag,
    /// Format of the data
    pub format: BlobFormat,
    /// Hash of the data
    pub hash: Hash,
}

impl TagInfo {
    /// Create a new tag info.
    pub fn new(name: impl AsRef<[u8]>, value: impl Into<HashAndFormat>) -> Self {
        let name = name.as_ref();
        let value = value.into();
        Self {
            name: Tag::from(name),
            hash: value.hash,
            format: value.format,
        }
    }

    /// Get the hash and format of the tag.
    pub fn hash_and_format(&self) -> HashAndFormat {
        HashAndFormat {
            hash: self.hash,
            format: self.format,
        }
    }
}
