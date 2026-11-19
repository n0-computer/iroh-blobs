//! The messages of the metadata protocol.
use std::{fmt, io, pin::Pin};

use bytes::Bytes;
use n0_future::Stream;
use tracing::Span;

use crate::{
    api::{self, proto::ListTagsRequest, tags::TagInfo},
    store::{engine::metadata::entry_state::EntryState, util::DD},
    util::channel::oneshot,
    Hash, HashAndFormat,
};

/// Get the entry state for a hash.
///
/// This will read from the blobs table and enrich the result with the content
/// of the inline data and inline outboard tables if necessary.
pub struct Get {
    pub hash: Hash,
    pub tx: oneshot::Sender<GetResult>,
    pub span: Span,
}

impl fmt::Debug for Get {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Get")
            .field("hash", &DD(self.hash.to_hex()))
            .finish_non_exhaustive()
    }
}

/// Delete the entry of one hash, sent by the hash's entity.
///
/// The entity sends this from its `Deleting` state, so none of its writes can
/// follow. The reply comes once the delete is committed, even if no entry
/// existed, and it also detects commands dropped during shutdown or a failed
/// batch, since then it is dropped.
/// Whether the entry may be deleted is decided before, by the main actor's
/// temp tag check. A metadata error fails the store instead of undoing a delete.
#[derive(Debug)]
pub struct Delete {
    pub hash: Hash,
    pub tx: oneshot::Sender<io::Result<()>>,
    pub span: Span,
}

#[derive(Debug)]
pub struct GetResult {
    pub state: io::Result<Option<EntryState<Bytes>>>,
}

/// Get the entry state for a hash.
///
/// This will read from the blobs table and enrich the result with the content
/// of the inline data and inline outboard tables if necessary.
#[derive(Debug)]
pub struct Dump {
    pub tx: oneshot::Sender<n0_error::Result<()>>,
    pub span: Span,
}

/// A stream of rows from a read snapshot, which it owns.
///
/// The metadata actor hands it out and moves on, so it never waits for the reader.
pub type Listing<T> = Pin<Box<dyn Stream<Item = api::Result<T>> + Send + 'static>>;

/// The gc candidates (every blob) and the named tag roots, from one snapshot.
pub type GcSnapshotReply = (Listing<Hash>, Listing<HashAndFormat>);

/// List tags, from a snapshot taken after all earlier commands.
pub struct ListTags {
    pub request: ListTagsRequest,
    pub tx: tokio::sync::oneshot::Sender<io::Result<Listing<TagInfo>>>,
    pub span: Span,
}

impl fmt::Debug for ListTags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListTags")
            .field("request", &self.request)
            .finish_non_exhaustive()
    }
}

/// List the hashes of all blobs, from a snapshot taken after all earlier commands.
pub struct ListBlobs {
    pub tx: tokio::sync::oneshot::Sender<io::Result<Listing<Hash>>>,
    pub span: Span,
}

impl fmt::Debug for ListBlobs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListBlobs").finish_non_exhaustive()
    }
}

/// The gc candidates and roots, from one snapshot taken after all earlier commands.
pub struct GcSnapshot {
    pub tx: tokio::sync::oneshot::Sender<io::Result<GcSnapshotReply>>,
    pub span: Span,
}

impl fmt::Debug for GcSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcSnapshot").finish_non_exhaustive()
    }
}

/// Merge into the entry state of a hash, without a reply.
///
/// Sent by the hash's entity for its writes. If applying it fails, the
/// metadata actor fails, and with it the store, so the entity never runs on
/// a state the metadata does not have.
pub struct Update {
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    pub span: Span,
}

impl fmt::Debug for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .finish_non_exhaustive()
    }
}

/// Merge into the entry state of a hash, replied once applied.
///
/// Applied means visible to later commands, not committed: the reply comes
/// before the batch commits. If the commit fails, the store fails.
pub struct UpdateAwait {
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    pub tx: oneshot::Sender<io::Result<()>>,
    pub span: Span,
}

impl fmt::Debug for UpdateAwait {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateAwait")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .finish_non_exhaustive()
    }
}

pub struct Set {
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    pub tx: oneshot::Sender<io::Result<()>>,
    pub span: Span,
}

impl fmt::Debug for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Set")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .finish_non_exhaustive()
    }
}

/// Modification method: create a new unique tag and set it to a value.
pub use crate::api::proto::CreateTagMsg;
/// Modification method: remove a range of tags.
pub use crate::api::proto::DeleteTagsMsg;
/// Modification method: rename a tag.
pub use crate::api::proto::RenameTagMsg;
/// Modification method: set a tag to a value, or remove it.
pub use crate::api::proto::SetTagMsg;
