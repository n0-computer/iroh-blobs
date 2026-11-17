//! Shared metadata commands. Replies contain only backend-independent types.
use super::super::entry_state::EntryState;
use bytes::Bytes;
use n0_future::Stream;
use std::{fmt, io, pin::Pin};

use nested_enum_utils::enum_conversions;
use tracing::Span;

use crate::{
    api::{
        self,
        proto::{BlobStatusMsg, ListTagsRequest, ShutdownMsg, SyncDbMsg},
        tags::TagInfo,
    },
    store::util::DD,
    util::channel::oneshot,
    Hash, HashAndFormat,
};

/// Get the entry state for a hash.
///
/// Inline data and outboards are included in the returned entry.
pub struct Get {
    pub hash: Hash,
    pub tx: oneshot::Sender<io::Result<Option<EntryState<Bytes>>>>,
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

/// Merge entry metadata. An optional reply acknowledges application, not commit.
pub struct Update {
    pub hash: Hash,
    pub state: EntryState<Bytes>,
    /// If present, acknowledge once the merge is applied, before commit.
    pub tx: Option<oneshot::Sender<io::Result<()>>>,
    pub span: Span,
}

impl fmt::Debug for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Update")
            .field("hash", &self.hash)
            .field("state", &DD(self.state.fmt_short()))
            .field("tx", &self.tx.is_some())
            .finish()
    }
}

/// Replace entry metadata and acknowledge application, not commit.
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

/// An owned listing. The backend retains whatever snapshot the stream needs;
/// consuming it does not block the metadata actor.
pub type Listing<T> = Pin<Box<dyn Stream<Item = api::Result<T>> + Send + 'static>>;

/// Blob locations from a consistent view, without inline payloads.
pub type BlobListing = Listing<(Hash, EntryState<()>)>;

/// Candidates and named roots from the same consistent view.
pub type GcSnapshotReply = (Listing<Hash>, Listing<HashAndFormat>);

/// Dump backend metadata for diagnostics.
#[derive(Debug)]
pub struct Dump {
    pub tx: oneshot::Sender<n0_error::Result<()>>,
    pub span: Span,
}

#[derive(derive_more::Debug)]
pub struct ListTags {
    pub request: ListTagsRequest,
    #[debug(skip)]
    pub tx: oneshot::Sender<io::Result<Listing<TagInfo>>>,
    pub span: Span,
}

#[derive(derive_more::Debug)]
pub struct ListBlobs {
    #[debug(skip)]
    pub tx: oneshot::Sender<io::Result<BlobListing>>,
    pub span: Span,
}

#[derive(derive_more::Debug)]
pub struct GcSnapshot {
    #[debug(skip)]
    pub tx: oneshot::Sender<io::Result<GcSnapshotReply>>,
    pub span: Span,
}

/// Modification method: create a new unique tag and set it to a value.
pub use crate::api::proto::CreateTagMsg;
/// Modification method: remove a range of tags.
pub use crate::api::proto::DeleteTagsMsg;
/// Modification method: rename a tag.
pub use crate::api::proto::RenameTagMsg;
/// Modification method: set a tag to a value, or remove it.
pub use crate::api::proto::SetTagMsg;

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadOnlyCommand {
    Get(Get),
    Dump(Dump),
    GetBlobStatus(BlobStatusMsg),
}

impl ReadOnlyCommand {
    pub fn parent_span(&self) -> &Span {
        match self {
            Self::Get(x) => &x.span,
            Self::Dump(x) => &x.span,
            Self::GetBlobStatus(x) => &x.span,
        }
    }
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum ReadWriteCommand {
    Update(Update),
    Set(Set),
    Delete(Delete),
    SetTag(SetTagMsg),
    DeleteTags(DeleteTagsMsg),
    RenameTag(RenameTagMsg),
    CreateTag(CreateTagMsg),
}

impl ReadWriteCommand {
    pub fn parent_span(&self) -> &Span {
        match self {
            Self::Update(x) => &x.span,
            Self::Set(x) => &x.span,
            Self::Delete(x) => &x.span,
            Self::SetTag(x) => &x.span,
            Self::DeleteTags(x) => &x.span,
            Self::RenameTag(x) => &x.span,
            Self::CreateTag(x) => &x.span,
        }
    }
}

#[derive(Debug)]
#[enum_conversions(Command)]
pub enum TopLevelCommand {
    SyncDb(SyncDbMsg),
    Shutdown(ShutdownMsg),
    ListTags(ListTags),
    ListBlobs(ListBlobs),
    GcSnapshot(GcSnapshot),
}

impl TopLevelCommand {
    pub fn parent_span(&self) -> &Span {
        match self {
            Self::SyncDb(x) => &x.span,
            Self::Shutdown(x) => &x.span,
            Self::ListTags(x) => &x.span,
            Self::ListBlobs(x) => &x.span,
            Self::GcSnapshot(x) => &x.span,
        }
    }
}

#[enum_conversions()]
pub enum Command {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
    TopLevel(TopLevelCommand),
}

// Err returns the original command for further dispatch, without allocating.
#[allow(clippy::result_large_err)]
impl Command {
    pub fn non_top_level(self) -> std::result::Result<NonTopLevelCommand, Self> {
        match self {
            Self::ReadOnly(cmd) => Ok(NonTopLevelCommand::ReadOnly(cmd)),
            Self::ReadWrite(cmd) => Ok(NonTopLevelCommand::ReadWrite(cmd)),
            _ => Err(self),
        }
    }

    pub fn read_only(self) -> std::result::Result<ReadOnlyCommand, Self> {
        match self {
            Self::ReadOnly(cmd) => Ok(cmd),
            _ => Err(self),
        }
    }
}

#[derive(Debug)]
#[enum_conversions()]
pub enum NonTopLevelCommand {
    ReadOnly(ReadOnlyCommand),
    ReadWrite(ReadWriteCommand),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly(cmd) => cmd.fmt(f),
            Self::ReadWrite(cmd) => cmd.fmt(f),
            Self::TopLevel(cmd) => cmd.fmt(f),
        }
    }
}
