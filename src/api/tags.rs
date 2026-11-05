//! Tags API
//!
//! The main entry point is the [`Tags`] struct.
use std::ops::RangeBounds;

use genawaiter::sync::Gen;
use irpc::{channel::mpsc, RpcMessage};
use n0_future::{Stream, StreamExt};
use ref_cast::RefCast;
use tracing::trace;

pub use super::proto::{
    CreateTagRequest as CreateOptions, DeleteTagsRequest as DeleteOptions,
    ListTagsRequest as ListOptions, RenameTagRequest as RenameOptions, SetTagRequest as SetOptions,
    TagInfo,
};
use super::{
    proto::{CreateTempTagRequest, Scope},
    ApiClient, Tag, TempTag,
};
use crate::{api::proto::ListTempTagsRequest, HashAndFormat};

/// The API for interacting with tags and temp tags.
#[derive(Debug, Clone, ref_cast::RefCast)]
#[repr(transparent)]
pub struct Tags {
    client: ApiClient,
}

impl Tags {
    pub(crate) fn ref_from_sender(sender: &ApiClient) -> &Self {
        Self::ref_cast(sender)
    }

    /// Lists the content protected by temporary tags in all scopes.
    ///
    /// An error is the last item in the stream.
    pub async fn list_temp_tags(
        &self,
    ) -> irpc::Result<impl Stream<Item = super::Result<HashAndFormat>>> {
        let options = ListTempTagsRequest;
        trace!("{:?}", options);
        let rx = self.client.server_streaming(options, 32).await?;
        Ok(list_stream(rx))
    }

    /// List all tags with options.
    ///
    /// This is the most flexible way to list tags. All the other list methods are just convenience
    /// methods that call this one with the appropriate options.
    /// An error is the last item in the stream.
    pub async fn list_with_opts(
        &self,
        options: ListOptions,
    ) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>> {
        trace!("{:?}", options);
        let rx = self.client.server_streaming(options, 32).await?;
        Ok(list_stream(rx))
    }

    /// Get the value of a single tag
    pub async fn get(&self, name: impl AsRef<[u8]>) -> super::RequestResult<Option<TagInfo>> {
        let mut stream = self
            .list_with_opts(ListOptions::single(name.as_ref()))
            .await?;
        Ok(stream.next().await.transpose()?)
    }

    pub async fn set_with_opts(&self, options: SetOptions) -> super::RequestResult<()> {
        trace!("{:?}", options);
        self.client.rpc(options).await??;
        Ok(())
    }

    pub async fn set(
        &self,
        name: impl AsRef<[u8]>,
        value: impl Into<HashAndFormat>,
    ) -> super::RequestResult<()> {
        self.set_with_opts(SetOptions {
            name: Tag::from(name.as_ref()),
            value: value.into(),
        })
        .await
    }

    /// List a range of tags
    pub async fn list_range<R, E>(
        &self,
        range: R,
    ) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>>
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
    ) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListOptions::prefix(prefix.as_ref()))
            .await
    }

    /// Lists all tags.
    pub async fn list(&self) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListOptions::all()).await
    }

    /// Lists all tags with a hash_seq format.
    pub async fn list_hash_seq(&self) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>> {
        self.list_with_opts(ListOptions::hash_seq()).await
    }

    /// Deletes a tag, with full control over options. All other delete methods
    /// wrap this.
    ///
    /// Returns the number of tags actually removed. Attempting to delete a non-existent tag will *not* fail.
    pub async fn delete_with_opts(&self, options: DeleteOptions) -> super::RequestResult<u64> {
        trace!("{:?}", options);
        let deleted = self.client.rpc(options).await??;
        Ok(deleted)
    }

    /// Deletes a tag.
    ///
    /// Returns the number of tags actually removed. Attempting to delete a non-existent tag will *not* fail.
    pub async fn delete(&self, name: impl AsRef<[u8]>) -> super::RequestResult<u64> {
        self.delete_with_opts(DeleteOptions::single(name.as_ref()))
            .await
    }

    /// Deletes a range of tags.
    ///
    /// Returns the number of tags actually removed. Attempting to delete a non-existent tag will *not* fail.
    pub async fn delete_range<R, E>(&self, range: R) -> super::RequestResult<u64>
    where
        R: RangeBounds<E>,
        E: AsRef<[u8]>,
    {
        self.delete_with_opts(DeleteOptions::range(range)).await
    }

    /// Delete all tags with the given prefix.
    ///
    /// Returns the number of tags actually removed. Attempting to delete a non-existent tag will *not* fail.
    pub async fn delete_prefix(&self, prefix: impl AsRef<[u8]>) -> super::RequestResult<u64> {
        self.delete_with_opts(DeleteOptions::prefix(prefix.as_ref()))
            .await
    }

    /// Delete all tags. Use with care. After this, all data will be garbage collected.
    ///
    /// Returns the number of tags actually removed. Attempting to delete a non-existent tag will *not* fail.
    pub async fn delete_all(&self) -> super::RequestResult<u64> {
        self.delete_with_opts(DeleteOptions {
            from: None,
            to: None,
        })
        .await
    }

    /// Rename a tag atomically
    ///
    /// If the tag does not exist, this will return an error.
    pub async fn rename_with_opts(&self, options: RenameOptions) -> super::RequestResult<()> {
        trace!("{:?}", options);
        self.client.rpc(options).await??;
        Ok(())
    }

    /// Rename a tag atomically
    ///
    /// If the tag does not exist, this will return an error.
    pub async fn rename(
        &self,
        from: impl AsRef<[u8]>,
        to: impl AsRef<[u8]>,
    ) -> super::RequestResult<()> {
        self.rename_with_opts(RenameOptions {
            from: Tag::from(from.as_ref()),
            to: Tag::from(to.as_ref()),
        })
        .await
    }

    pub async fn create_with_opts(&self, options: CreateOptions) -> super::RequestResult<Tag> {
        trace!("{:?}", options);
        let rx = self.client.rpc(options);
        Ok(rx.await??)
    }

    pub async fn create(&self, value: impl Into<HashAndFormat>) -> super::RequestResult<Tag> {
        self.create_with_opts(CreateOptions {
            value: value.into(),
        })
        .await
    }

    pub async fn temp_tag(&self, value: impl Into<HashAndFormat>) -> irpc::Result<TempTag> {
        let value = value.into();
        let msg = CreateTempTagRequest {
            scope: Scope::GLOBAL,
            value,
        };
        self.client.rpc(msg).await
    }
}

/// Preserves store and transport errors, and ends the listing at the first error.
fn list_stream<T: RpcMessage>(
    mut rx: mpsc::Receiver<super::Result<T>>,
) -> impl Stream<Item = super::Result<T>> {
    Gen::new(|co| async move {
        loop {
            let item = match rx.recv().await {
                Ok(Some(item)) => item,
                Ok(None) => break,
                Err(cause) => Err(cause.into()),
            };
            let failed = item.is_err();
            co.yield_(item).await;
            if failed {
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use testresult::TestResult;

    use super::*;
    use crate::{
        api::{self, proto::Command, Store},
        Hash,
    };

    /// Keep the sender open after an error, and even send a later item. The client
    /// must deliver the error and end without waiting for the sender to close.
    async fn answer<T: RpcMessage>(tx: mpsc::Sender<api::Result<T>>, value: impl Fn() -> T) {
        tx.send(Ok(value())).await.unwrap();
        tx.send(Err(api::Error::io(
            io::ErrorKind::Unsupported,
            "listing unsupported",
        )))
        .await
        .unwrap();
        tx.send(Ok(value())).await.unwrap();
        tx.closed().await;
    }

    #[tokio::test]
    async fn listings_deliver_items_and_terminal_errors() -> TestResult<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(1);
            let store = Store::from_client(tx);
            let hash = Hash::new("listed");
            let responder = tokio::spawn(async move {
                let Command::ListTags(cmd) = rx.recv().await.unwrap() else {
                    panic!("expected ListTags")
                };
                answer(cmd.tx, || TagInfo::new("listed", hash)).await;
                let Command::ListTempTags(cmd) = rx.recv().await.unwrap() else {
                    panic!("expected ListTempTags")
                };
                answer(cmd.tx, || HashAndFormat::raw(hash)).await;
            });

            let mut named = store.tags().list().await?;
            assert_eq!(named.next().await.unwrap()?, TagInfo::new("listed", hash));
            let api::Error::Io(error) = named.next().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(named.next().await.is_none());
            drop(named);

            let mut temporary = store.tags().list_temp_tags().await?;
            assert_eq!(temporary.next().await.unwrap()?, HashAndFormat::raw(hash));
            let api::Error::Io(error) = temporary.next().await.unwrap().unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert!(temporary.next().await.is_none());
            drop(temporary);
            responder.await?;
            TestResult::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn readonly_listings_are_empty() -> TestResult<()> {
        let store = crate::store::readonly_mem::ReadonlyMemStore::new([b"blob"]);
        assert!(store.tags().list().await?.next().await.is_none());
        assert!(store.tags().list_temp_tags().await?.next().await.is_none());
        Ok(())
    }
}
