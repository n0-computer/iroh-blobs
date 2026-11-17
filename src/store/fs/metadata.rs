//! Metadata client and protocol, independent of the database implementation.
//!
//! Requests share an ordered queue with the backend's store-wide commands.
//! Get observes earlier applied changes. Set and acknowledged Update reply when
//! applied, not committed. Delete replies only after commit, including when the
//! entry was absent. A failed batch or shutdown drops outstanding replies.
//! Fatal backend failures must also reach the store supervisor so it can abort
//! workers. File-unreferencing notices are emitted only after commit.

use super::entry_state::EntryState;
use crate::{
    api::{proto::ListTagsRequest, tags::TagInfo},
    util::channel::oneshot,
    Hash,
};
use bytes::Bytes;
use std::io;
use tracing::Span;

mod proto;
pub use proto::*;

/// Client for the shared metadata queue. Entry and store-wide commands use
/// the same concrete sender, without a mapping adapter or boxed send future.
#[derive(Debug, Clone)]
pub struct Metadata {
    sender: tokio::sync::mpsc::Sender<Command>,
}

impl Metadata {
    pub fn new(sender: tokio::sync::mpsc::Sender<Command>) -> Self {
        Self { sender }
    }

    /// Lists tags from a consistent view after all earlier commands.
    pub async fn list_tags(
        &self,
        span: Span,
        request: ListTagsRequest,
    ) -> io::Result<Listing<TagInfo>> {
        let (tx, rx) = oneshot::channel();
        self.send(ListTags { request, tx, span }.into()).await?;
        rx.await
            .map_err(|_| io::Error::other("receive tag listing"))?
    }

    /// Lists blobs and their locations, without inline bytes, from a consistent
    /// view after all earlier commands.
    pub async fn list_blobs(&self, span: Span) -> io::Result<BlobListing> {
        let (tx, rx) = oneshot::channel();
        self.send(ListBlobs { tx, span }.into()).await?;
        rx.await
            .map_err(|_| io::Error::other("receive blob listing"))?
    }

    /// Candidates and roots from one view after all earlier commands.
    pub async fn gc_snapshot(&self, span: Span) -> io::Result<GcSnapshotReply> {
        let (tx, rx) = oneshot::channel();
        self.send(GcSnapshot { tx, span }.into()).await?;
        rx.await
            .map_err(|_| io::Error::other("receive gc snapshot"))?
    }

    /// Forward a store-wide command on the same ordered queue.
    pub async fn send(&self, cmd: Command) -> io::Result<()> {
        self.sender
            .send(cmd)
            .await
            .map_err(|_| io::Error::other("actor down"))
    }

    pub async fn update_await(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: Some(tx),
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await
            .map_err(|_e| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Update the entry state for a hash, without awaiting completion.
    pub async fn update(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        self.sender
            .send(
                Update {
                    hash,
                    state,
                    tx: None,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))
    }

    /// Set the entry state and await completion.
    pub async fn set(&self, hash: Hash, entry_state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Set {
                    hash,
                    state: entry_state,
                    tx,
                    span: Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send update"))?;
        rx.await.map_err(|_| io::Error::other("receive update"))??;
        Ok(())
    }

    /// Get the entry state for a hash, if any.
    pub async fn get(&self, hash: Hash) -> io::Result<Option<EntryState<Bytes>>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                Get {
                    hash,
                    tx,
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .map_err(|_| io::Error::other("send get"))?;
        rx.await.map_err(|_| io::Error::other("receive get"))?
    }

    /// Apply an unconditional delete, answered once it is committed, even if
    /// there was no metadata entry.
    ///
    /// Only the hash's entity sends this, see [`Delete`].
    pub async fn delete(&self, hash: Hash) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let span = Span::current();
        self.sender
            .send(Delete { hash, tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send delete"))?;
        rx.await.map_err(|_| io::Error::other("receive delete"))??;
        Ok(())
    }

    /// A view of this database scoped to a single entry.
    pub fn entry(&self, hash: Hash) -> EntryMeta<'_> {
        EntryMeta { db: self, hash }
    }
}

/// The metadata of a single entry.
///
/// This is all an entity gets to see of the database.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta<'a> {
    db: &'a Metadata,
    hash: Hash,
}

impl EntryMeta<'_> {
    /// The hash of the entry.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Apply an unconditional delete, answered once it is committed.
    pub async fn delete(&self) -> io::Result<()> {
        self.db.delete(self.hash).await
    }

    /// Get the entry state.
    pub async fn get(&self) -> io::Result<Option<EntryState<Bytes>>> {
        self.db.get(self.hash).await
    }

    /// Replace the entry state, and wait for completion.
    pub async fn set(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.set(self.hash, state).await
    }

    /// Merge into the entry state, without waiting for completion.
    pub async fn update(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.update(self.hash, state).await
    }

    /// Merge into the entry state, and wait for completion.
    pub async fn update_await(&self, state: EntryState<Bytes>) -> io::Result<()> {
        self.db.update_await(self.hash, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn entry_commands_share_the_backend_queue() -> testresult::TestResult<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let metadata = Metadata::new(tx.clone());
        let hash = Hash::new("entry");
        let entry = metadata.entry(hash);
        // No reply is needed for update. It must already be enqueued when
        // this returns, before the store-wide snapshot on the same channel.
        entry.update(EntryState::Partial { size: None }).await?;
        let (listing_tx, _listing_rx) = oneshot::channel();
        tx.send(
            ListBlobs {
                tx: listing_tx,
                span: Span::none(),
            }
            .into(),
        )
        .await
        .unwrap();
        let Some(Command::ReadWrite(ReadWriteCommand::Update(update))) = rx.recv().await else {
            panic!("update must precede snapshot");
        };
        assert_eq!(update.hash, hash);
        assert!(update.tx.is_none());
        assert!(matches!(
            rx.recv().await,
            Some(Command::TopLevel(TopLevelCommand::ListBlobs(_)))
        ));

        let (result, ()) = tokio::join!(entry.get(), async {
            let Some(Command::ReadOnly(ReadOnlyCommand::Get(get))) = rx.recv().await else {
                panic!("expected get");
            };
            assert_eq!(get.hash, hash);
            get.tx.send(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "backend error",
            )));
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn delete_waits_for_commit_reply_and_detects_dropped_reply() -> testresult::TestResult<()>
    {
        for acknowledge in [true, false] {
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let metadata = Metadata::new(tx);
            let entry = metadata.entry(Hash::new("entry"));
            let delete = entry.delete();
            tokio::pin!(delete);
            let command = tokio::select! {
                result = &mut delete => panic!("delete returned before reply: {result:?}"),
                command = rx.recv() => command.unwrap(),
            };
            let Command::ReadWrite(ReadWriteCommand::Delete(command)) = command else {
                panic!("expected delete")
            };
            // Receiving the request is not enough to complete deletion.
            tokio::select! {
                biased;
                result = &mut delete => panic!("delete returned before commit: {result:?}"),
                _ = std::future::ready(()) => {}
            }
            if acknowledge {
                command.tx.send(Ok(()));
                delete.await?;
            } else {
                drop(command);
                assert!(delete.await.is_err());
            }
        }
        Ok(())
    }
}
