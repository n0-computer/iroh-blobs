//! Tags API
//!
//! The main entry point is the [`Tags`] struct.
use std::{io, ops::RangeBounds};

use genawaiter::sync::Gen;
use n0_future::{Stream, StreamExt};
use ref_cast::RefCast;
use tracing::trace;

pub use super::proto::{
    CreateTagRequest as CreateOptions, DeleteTagsRequest as DeleteOptions,
    ListTagsRequest as ListOptions, RenameTagRequest as RenameOptions, SetTagRequest as SetOptions,
    TagInfo,
};
use super::{
    proto::{
        rules::{Step, StreamRules, TagListRules},
        CreateTempTagRequest, ListTagsItem, ListTempTagsItem, Scope,
    },
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
    /// An error is the last item in the stream. Premature closure is reported as an error.
    pub async fn list_temp_tags(
        &self,
    ) -> irpc::Result<impl Stream<Item = super::Result<HashAndFormat>>> {
        let options = ListTempTagsRequest;
        trace!("{:?}", options);
        let mut rx = self.client.server_streaming(options, 32).await?;
        Ok(Gen::new(|co| async move {
            let mut rules = TagListRules;
            loop {
                let item = match rx.recv().await {
                    Ok(Some(item)) => item,
                    Ok(None) => {
                        co.yield_(Err(super::Error::io(
                            io::ErrorKind::UnexpectedEof,
                            "temporary-tag listing ended before completion",
                        )))
                        .await;
                        break;
                    }
                    Err(error) => {
                        co.yield_(Err(error.into())).await;
                        break;
                    }
                };
                let step = match rules.check(&item) {
                    Ok(step) => step,
                    Err(error) => {
                        co.yield_(Err(error.into())).await;
                        break;
                    }
                };
                match item {
                    ListTempTagsItem::Tag(tag) => co.yield_(Ok(tag)).await,
                    ListTempTagsItem::Done => {}
                    ListTempTagsItem::Error(error) => co.yield_(Err(error)).await,
                }
                if step == Step::Last {
                    break;
                }
            }
        }))
    }

    /// List all tags with options.
    ///
    /// This is the most flexible way to list tags. All the other list methods are just convenience
    /// methods that call this one with the appropriate options.
    /// An error is the last item in the stream. Premature closure is reported as an error.
    pub async fn list_with_opts(
        &self,
        options: ListOptions,
    ) -> irpc::Result<impl Stream<Item = super::Result<TagInfo>>> {
        trace!("{:?}", options);
        let mut rx = self.client.server_streaming(options, 32).await?;
        Ok(Gen::new(|co| async move {
            let mut rules = TagListRules;
            loop {
                let item = match rx.recv().await {
                    Ok(Some(item)) => item,
                    Ok(None) => {
                        co.yield_(Err(super::Error::io(
                            io::ErrorKind::UnexpectedEof,
                            "tag listing ended before completion",
                        )))
                        .await;
                        break;
                    }
                    Err(error) => {
                        co.yield_(Err(error.into())).await;
                        break;
                    }
                };
                let step = match rules.check(&item) {
                    Ok(step) => step,
                    Err(error) => {
                        co.yield_(Err(error.into())).await;
                        break;
                    }
                };
                match item {
                    ListTagsItem::Tag(tag) => co.yield_(Ok(tag)).await,
                    ListTagsItem::Done => {}
                    ListTagsItem::Error(error) => co.yield_(Err(error)).await,
                }
                if step == Step::Last {
                    break;
                }
            }
        }))
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

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use testresult::TestResult;

    use super::*;
    use crate::{
        api::{self, proto::Command, Store},
        Hash,
    };

    #[derive(Clone, Copy)]
    enum Ending {
        Done,
        Error,
        Drop,
    }

    // Keep the sender open after a terminal item and send a later item. Clients
    // must stop at the terminal item without waiting for the sender to close.
    async fn answer<T: irpc::RpcMessage>(
        tx: irpc::channel::mpsc::Sender<T>,
        value: impl Fn() -> T,
        done: T,
        error: impl Fn(api::Error) -> T,
        ending: Ending,
        empty: bool,
    ) {
        if !empty {
            tx.send(value()).await.unwrap();
        }
        let terminal = match ending {
            Ending::Done => done,
            Ending::Error => error(api::Error::io(
                io::ErrorKind::Unsupported,
                "listing unsupported",
            )),
            Ending::Drop => return,
        };
        tx.send(terminal).await.unwrap();
        // A remote client may already have stopped receiving at this point.
        tx.send(value()).await.ok();
        tx.closed().await;
    }

    async fn check_listing<T: std::fmt::Debug + PartialEq>(
        mut stream: impl Stream<Item = api::Result<T>> + Unpin,
        expected: T,
        ending: Ending,
        empty: bool,
    ) -> TestResult {
        if !empty {
            assert_eq!(stream.next().await.unwrap()?, expected);
        }
        match ending {
            Ending::Done => {}
            Ending::Error | Ending::Drop => {
                let api::Error::Io(error) = stream.next().await.unwrap().unwrap_err();
                let kind = match ending {
                    Ending::Error => io::ErrorKind::Unsupported,
                    _ => io::ErrorKind::UnexpectedEof,
                };
                assert_eq!(error.kind(), kind);
            }
        }
        assert!(stream.next().await.is_none());
        Ok(())
    }

    async fn listing_endings(remote: bool) -> TestResult {
        tokio::time::timeout(Duration::from_secs(10), async {
            for ending in [Ending::Done, Ending::Error, Ending::Drop] {
                for empty in [false, true] {
                    let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(1);
                    let store = Store::from_client(tx);
                    let hash = Hash::new("listed");
                    let responder = tokio::spawn(async move {
                        let Command::ListTags(cmd) = rx.recv().await.unwrap() else {
                            panic!("expected ListTags")
                        };
                        answer(
                            cmd.tx,
                            || ListTagsItem::Tag(TagInfo::new("listed", hash)),
                            ListTagsItem::Done,
                            ListTagsItem::Error,
                            ending,
                            empty,
                        )
                        .await;
                        let Command::ListTempTags(cmd) = rx.recv().await.unwrap() else {
                            panic!("expected ListTempTags")
                        };
                        answer(
                            cmd.tx,
                            || ListTempTagsItem::Tag(HashAndFormat::raw(hash)),
                            ListTempTagsItem::Done,
                            ListTempTagsItem::Error,
                            ending,
                            empty,
                        )
                        .await;
                    });
                    #[cfg(feature = "rpc")]
                    let (store, listener) = if remote {
                        let addr = "127.0.0.1:0".parse()?;
                        let (server, cert) = irpc::util::make_server_endpoint(addr)?;
                        let client = irpc::util::make_client_endpoint(addr, &[cert.as_ref()])?;
                        let address = server.local_addr()?;
                        let listener = tokio::spawn(store.listen(server));
                        (Store::connect(client, address), Some(listener))
                    } else {
                        (store, None)
                    };
                    #[cfg(not(feature = "rpc"))]
                    assert!(!remote);

                    check_listing(
                        store.tags().list().await?,
                        TagInfo::new("listed", hash),
                        ending,
                        empty,
                    )
                    .await?;
                    check_listing(
                        store.tags().list_temp_tags().await?,
                        HashAndFormat::raw(hash),
                        ending,
                        empty,
                    )
                    .await?;
                    responder.await?;
                    #[cfg(feature = "rpc")]
                    if let Some(listener) = listener {
                        listener.abort();
                    }
                }
            }
            TestResult::Ok(())
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    async fn listings_require_explicit_completion() -> TestResult {
        listing_endings(false).await
    }

    #[cfg(feature = "rpc")]
    #[tokio::test]
    async fn remote_listings_require_explicit_completion() -> TestResult {
        listing_endings(true).await
    }

    #[tokio::test]
    async fn readonly_listings_are_empty() -> TestResult<()> {
        let store = crate::store::readonly_mem::ReadonlyMemStore::new([b"blob"]);
        assert!(store.tags().list().await?.next().await.is_none());
        assert!(store.tags().list_temp_tags().await?.next().await.is_none());
        Ok(())
    }
}
