//! How the redb store groups metadata requests into transactions.
use std::fmt;

use nested_enum_utils::enum_conversions;
use tracing::Span;

pub(crate) use crate::store::engine::metadata::proto::*;
use crate::{
    api::proto::{BlobStatusMsg, ShutdownMsg, SyncDbMsg},
    store::engine::metadata::Request,
};

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
    UpdateAwait(UpdateAwait),
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
            Self::UpdateAwait(x) => &x.span,
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
