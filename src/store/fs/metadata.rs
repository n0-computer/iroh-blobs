//! The metadata protocol: what a metadata store answers, independent of how it stores it.
//!
//! A metadata store is one actor that receives [`Request`]s on one ordered
//! channel. Entities send the per-entry requests for their own hash, and the
//! main actor sends the store-wide ones. The actor sends unreferenced parts
//! back to the store after each commit, and fails the store by ending with an
//! error.
//!
//! How the actor batches requests into transactions is up to it. The fs store's
//! redb actor groups them by its own [`Command`] classification, which
//! [`Request`] converts to and from.
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
use nested_enum_utils::enum_conversions;

use super::meta::{
    Command, CreateTagMsg, Delete, DeleteTagsMsg, Dump, GcSnapshot, Get, ListBlobs, ListTags,
    ReadOnlyCommand, ReadWriteCommand, RenameTagMsg, Set, SetTagMsg, TopLevelCommand, Update,
    UpdateAwait,
};
use crate::api::proto::{BlobStatusMsg, ShutdownMsg, SyncDbMsg};

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

impl From<Request> for Command {
    fn from(request: Request) -> Self {
        match request {
            Request::Get(x) => ReadOnlyCommand::Get(x).into(),
            Request::Set(x) => ReadWriteCommand::Set(x).into(),
            Request::Update(x) => ReadWriteCommand::Update(x).into(),
            Request::UpdateAwait(x) => ReadWriteCommand::UpdateAwait(x).into(),
            Request::Delete(x) => ReadWriteCommand::Delete(x).into(),
            Request::SetTag(x) => ReadWriteCommand::SetTag(x).into(),
            Request::CreateTag(x) => ReadWriteCommand::CreateTag(x).into(),
            Request::DeleteTags(x) => ReadWriteCommand::DeleteTags(x).into(),
            Request::RenameTag(x) => ReadWriteCommand::RenameTag(x).into(),
            Request::ListTags(x) => TopLevelCommand::ListTags(x).into(),
            Request::BlobStatus(x) => ReadOnlyCommand::GetBlobStatus(x).into(),
            Request::ListBlobs(x) => TopLevelCommand::ListBlobs(x).into(),
            Request::Dump(x) => ReadOnlyCommand::Dump(x).into(),
            Request::GcSnapshot(x) => TopLevelCommand::GcSnapshot(x).into(),
            Request::SyncDb(x) => TopLevelCommand::SyncDb(x).into(),
            Request::Shutdown(x) => TopLevelCommand::Shutdown(x).into(),
        }
    }
}

impl From<Command> for Request {
    fn from(command: Command) -> Self {
        match command {
            Command::ReadOnly(cmd) => match cmd {
                ReadOnlyCommand::Get(x) => Self::Get(x),
                ReadOnlyCommand::Dump(x) => Self::Dump(x),
                ReadOnlyCommand::GetBlobStatus(x) => Self::BlobStatus(x),
            },
            Command::ReadWrite(cmd) => match cmd {
                ReadWriteCommand::Update(x) => Self::Update(x),
                ReadWriteCommand::UpdateAwait(x) => Self::UpdateAwait(x),
                ReadWriteCommand::Set(x) => Self::Set(x),
                ReadWriteCommand::Delete(x) => Self::Delete(x),
                ReadWriteCommand::SetTag(x) => Self::SetTag(x),
                ReadWriteCommand::DeleteTags(x) => Self::DeleteTags(x),
                ReadWriteCommand::RenameTag(x) => Self::RenameTag(x),
                ReadWriteCommand::CreateTag(x) => Self::CreateTag(x),
            },
            Command::TopLevel(cmd) => match cmd {
                TopLevelCommand::SyncDb(x) => Self::SyncDb(x),
                TopLevelCommand::Shutdown(x) => Self::Shutdown(x),
                TopLevelCommand::ListTags(x) => Self::ListTags(x),
                TopLevelCommand::ListBlobs(x) => Self::ListBlobs(x),
                TopLevelCommand::GcSnapshot(x) => Self::GcSnapshot(x),
            },
        }
    }
}
