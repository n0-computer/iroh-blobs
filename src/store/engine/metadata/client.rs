//! The client every part of a store talks to the metadata store through.
use std::io;

use bytes::Bytes;
use tracing::Span;

use super::{
    proto::{
        Delete, GcSnapshot, GcSnapshotReply, Get, ListBlobs, ListTags, Listing, Set, Update,
        UpdateAwait,
    },
    Request,
};
use crate::{
    api::{proto::ListTagsRequest, tags::TagInfo},
    store::fs::entry_state::EntryState,
    util::channel::oneshot,
    Hash,
};

#[derive(Debug, Clone)]
pub struct Db {
    sender: tokio::sync::mpsc::Sender<Request>,
}

impl Db {
    pub fn new(sender: tokio::sync::mpsc::Sender<Request>) -> Self {
        Self { sender }
    }

    /// Lists tags, from a snapshot taken after all earlier commands.
    pub async fn list_tags(
        &self,
        span: tracing::Span,
        request: ListTagsRequest,
    ) -> io::Result<Listing<TagInfo>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(ListTags { request, tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send list tags"))?;
        rx.await
            .map_err(|_| io::Error::other("receive list tags"))?
    }

    /// Lists the hashes of all blobs, from a snapshot taken after all earlier commands.
    pub async fn list_blobs(&self, span: tracing::Span) -> io::Result<Listing<Hash>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(ListBlobs { tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send list blobs"))?;
        rx.await
            .map_err(|_| io::Error::other("receive list blobs"))?
    }

    /// Reads the gc candidates (every blob) and the named tag roots from one
    /// snapshot taken after all earlier commands.
    pub async fn gc_snapshot(&self, span: tracing::Span) -> io::Result<GcSnapshotReply> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(GcSnapshot { tx, span }.into())
            .await
            .map_err(|_| io::Error::other("send gc snapshot"))?;
        rx.await
            .map_err(|_| io::Error::other("receive gc snapshot"))?
    }

    pub async fn update_await(&self, hash: Hash, state: EntryState<Bytes>) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(
                UpdateAwait {
                    hash,
                    state,
                    tx,
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
        let res = rx.await.map_err(|_| io::Error::other("receive get"))?;
        res.state
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

    /// Send a command. This exists so the main actor can directly forward commands.
    ///
    /// This will fail only if the database actor is dead. In that case the main
    /// actor should probably also shut down.
    pub async fn send(&self, cmd: Request) -> io::Result<()> {
        self.sender
            .send(cmd)
            .await
            .map_err(|_e| io::Error::other("actor down"))?;
        Ok(())
    }
}

/// The metadata of a single entry.
///
/// This is all an entity gets to see of the database.
#[derive(Debug, Clone, Copy)]
pub struct EntryMeta<'a> {
    db: &'a Db,
    hash: Hash,
}

impl EntryMeta<'_> {
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
