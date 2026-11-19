//! The metadata protocol: what a metadata store answers, independent of how it stores it.
//!
//! A metadata store is one actor that receives [`Request`]s on one ordered
//! channel. Entities send the per-entry requests for their own hash, and the
//! main actor sends the store-wide ones. The actor sends unreferenced parts
//! back to the store after each commit, and fails the store by ending with an
//! error.
//!
//! How the actor batches requests into transactions is up to it. The fs store's
//! redb actor converts each request into its own command grouping as it
//! receives it, and groups them by that.
//!
//! # Contract
//!
//! - **One FIFO.** Each request sees the effect of every earlier one, also
//!   before it is committed, and no request overtakes another.
//! - **Applied or committed.** `Set` and `UpdateAwait` are answered once
//!   applied, which means visible to later requests. `Delete` and `SyncDb` are
//!   answered once committed, also a `Delete` of an absent entry. `Update` is
//!   not answered.
//! - **Failure.** If a batch fails, its outstanding replies are dropped and
//!   the actor ends with an error, which aborts the store. Nothing keeps
//!   running on an applied but lost state.
//! - **Listings.** `ListTags`, `ListBlobs` and `GcSnapshot` each read one
//!   consistent snapshot, taken at their place in the FIFO, and reply with a
//!   stream that owns it, so the actor never waits for the reader.
//! - **Unreferenced parts** are sent only after the commit that stopped
//!   referencing them, on an unbounded channel, so the actor never waits on the
//!   store.
//! - **Shutdown** commits everything, records the clean shutdown so the next
//!   open needs no recovery, and replies. Then the actor ends.
use std::{any::Any, fmt, future::Future};

use nested_enum_utils::enum_conversions;
use tokio::sync::mpsc;

mod client;
pub(crate) mod entry_state;
pub(crate) mod parts;
pub(crate) mod proto;

pub(crate) use client::{Db, EntryMeta};
use parts::UnreferencedParts;
use proto::{
    CreateTagMsg, Delete, DeleteTagsMsg, Dump, GcSnapshot, Get, ListBlobs, ListTags, RenameTagMsg,
    Set, SetTagMsg, Update, UpdateAwait,
};

use crate::{
    api::proto::{BlobStatusMsg, ShutdownMsg, SyncDbMsg},
    Hash,
};

/// What a metadata store is started with.
pub(crate) struct MetaChannels {
    /// The requests, in order.
    pub requests: mpsc::Receiver<Request>,
    /// Where to send the parts a commit stopped referencing.
    pub unreferenced: mpsc::UnboundedSender<(Hash, UnreferencedParts)>,
}

/// The running metadata store. It ends with an error to fail the store.
pub(crate) trait MetaTask: Future<Output = Result<(), Failure>> + Send + 'static {}

impl<T: Future<Output = Result<(), Failure>> + Send + 'static> MetaTask for T {}

/// Why a metadata store ended, and what it keeps alive until the store has stopped its workers.
///
/// The redb store keeps its database here, so the database stays locked
/// until no worker can touch files anymore.
pub(crate) struct Failure {
    pub error: n0_error::AnyError,
    _guard: Box<dyn Any + Send>,
}

impl Failure {
    pub fn new(error: n0_error::AnyError, guard: impl Any + Send) -> Self {
        Self {
            error,
            _guard: Box::new(guard),
        }
    }
}

impl fmt::Debug for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Failure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

/// A request to the metadata store.
#[derive(Debug)]
#[enum_conversions()]
pub(crate) enum Request {
    // Per entry, sent by the hash's entity.
    /// Get the entry state, with inline data and outboard.
    Get(Get),
    /// Replace the entry state. Answered once applied.
    Set(Set),
    /// Merge into the entry state. Not answered.
    Update(Update),
    /// Merge into the entry state. Answered once applied.
    UpdateAwait(UpdateAwait),
    /// Make sure the entry is absent. Answered once committed.
    Delete(Delete),

    // Store-wide, sent by the main actor.
    /// Set a tag, or remove it.
    SetTag(SetTagMsg),
    /// Create a new unique tag.
    CreateTag(CreateTagMsg),
    /// Remove a range of tags.
    DeleteTags(DeleteTagsMsg),
    /// Rename a tag.
    RenameTag(RenameTagMsg),
    /// List tags from a snapshot.
    ListTags(ListTags),
    /// The status of a blob.
    BlobStatus(BlobStatusMsg),
    /// List the hashes of all blobs from a snapshot.
    ListBlobs(ListBlobs),
    /// Log the content of the store, for debugging.
    Dump(Dump),
    /// The gc candidates and roots from one snapshot.
    GcSnapshot(GcSnapshot),
    /// Answered once everything before it is committed.
    SyncDb(SyncDbMsg),
    /// Commit, record the clean shutdown, reply, and end.
    Shutdown(ShutdownMsg),
}
