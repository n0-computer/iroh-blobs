#![cfg_attr(feature = "hide-proto-docs", doc(hidden))]
//! The protocol that a store implementation needs to implement.
//!
//! A store needs to handle [`Request`]s. It is fine to just return an error for some
//! commands. E.g. an immutable store can just return an error for import commands.
//!
//! Each command consists of a serializable request message and channels for updates
//! and responses. The enum containing the full requests is [`Command`]. These are the
//! commands you will have to handle in a store actor handler.
//!
//! This crate provides a file system based store implementation, [`crate::store::fs::FsStore`],
//! as well as a mutable in-memory store and an immutable in-memory store.
//!
//! The file system store is quite complex and optimized, so to get started take a look at
//! the much simpler memory store.
//!
//! # Listings
//!
//! [`Request::ListBlobs`], [`Request::ListTags`] and [`Request::ListTempTags`]
//! end with an explicit `Done` or `Error`, since the client can not tell from
//! the items when a listing is complete. See [`ListBlobsItem`].
//!
//! # The empty blob
//!
//! The empty blob, [`Hash::EMPTY`], is always present. The client in [`crate::api`]
//! answers requests that read its bytes or delete it without asking the store, so a
//! store never gets such a request, and [`Request::ListBlobs`] must not list it.
//! Like everything else about this protocol, this is not enforced: a store may rely on
//! it, but a client that sends such a request anyway gets unspecified results.
//!
//! File export is the exception: [`Request::ExportPath`] must reach the store even
//! for the empty blob, because the target path is on the store's filesystem. The
//! store creates the empty file without creating a blob entry.
//!
//! An import only learns that its data is empty once it is hashed. At that point a
//! store does nothing but answer with a [`TempTag`] that is not tracked, since the
//! empty blob never needs protection. It does not create an entry for it.
use std::{
    fmt::{self, Debug},
    io,
    num::NonZeroU64,
    ops::{Bound, RangeBounds},
    path::PathBuf,
    pin::Pin,
};

use bao_tree::{
    io::{mixed::EncodedItem, BaoContentItem, Leaf},
    ChunkRanges,
};
use bytes::Bytes;
use irpc::{
    channel::{mpsc, oneshot},
    rpc_requests,
};
use n0_future::Stream;
use range_collections::RangeSet2;
use serde::{Deserialize, Serialize};
pub(crate) mod bitfield;
pub(crate) mod rules;
pub use bitfield::Bitfield;

use crate::{store::util::Tag, util::temp_tag::TempTag, BlobFormat, Hash, HashAndFormat};

#[allow(dead_code)]
pub(crate) trait HashSpecific {
    fn hash(&self) -> Hash;

    fn hash_short(&self) -> impl fmt::Display + Copy + 'static {
        self.hash().fmt_short()
    }
}

impl HashSpecific for ImportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ObserveMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ExportBaoMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ExportRangesMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

impl HashSpecific for ExportPathMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.hash
    }
}

pub type BoxedByteStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync + 'static>>;

impl HashSpecific for CreateTagMsg {
    fn hash(&self) -> crate::Hash {
        self.inner.value.hash
    }
}

#[rpc_requests(message = Command, alias = "Msg", rpc_feature = "rpc")]
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    #[rpc(tx = mpsc::Sender<ListBlobsItem>)]
    ListBlobs(ListRequest),
    #[rpc(tx = oneshot::Sender<Scope>, rx = mpsc::Receiver<BatchResponse>)]
    Batch(BatchRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    DeleteBlobs(BlobDeleteRequest),
    #[rpc(rx = mpsc::Receiver<BaoContentItem>, tx = oneshot::Sender<super::Result<()>>)]
    ImportBao(ImportBaoRequest),
    #[rpc(tx = mpsc::Sender<EncodedItem>)]
    ExportBao(ExportBaoRequest),
    #[rpc(tx = mpsc::Sender<ExportRangesItem>)]
    ExportRanges(ExportRangesRequest),
    #[rpc(tx = mpsc::Sender<Bitfield>)]
    Observe(ObserveRequest),
    #[rpc(tx = oneshot::Sender<BlobStatus>)]
    BlobStatus(BlobStatusRequest),
    #[rpc(tx = mpsc::Sender<AddProgressItem>)]
    ImportBytes(ImportBytesRequest),
    #[rpc(rx = mpsc::Receiver<ImportByteStreamUpdate>, tx = mpsc::Sender<AddProgressItem>)]
    ImportByteStream(ImportByteStreamRequest),
    #[rpc(tx = mpsc::Sender<AddProgressItem>)]
    ImportPath(ImportPathRequest),
    #[rpc(tx = mpsc::Sender<ExportProgressItem>)]
    ExportPath(ExportPathRequest),
    #[rpc(tx = mpsc::Sender<ListTagsItem>)]
    ListTags(ListTagsRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SetTag(SetTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<u64>>)]
    DeleteTags(DeleteTagsRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    RenameTag(RenameTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<Tag>>)]
    CreateTag(CreateTagRequest),
    #[rpc(tx = mpsc::Sender<ListTempTagsItem>)]
    ListTempTags(ListTempTagsRequest),
    #[rpc(tx = oneshot::Sender<TempTag>)]
    CreateTempTag(CreateTempTagRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    SyncDb(SyncDbRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    WaitIdle(WaitIdleRequest),
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(ShutdownRequest),
    #[rpc(tx = oneshot::Sender<super::Result<()>>)]
    ClearProtected(ClearProtectedRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WaitIdleRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncDbRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShutdownRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClearProtectedRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobStatusRequest {
    pub hash: Hash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListRequest;

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRequest;

#[derive(Debug, Serialize, Deserialize)]
pub enum BatchResponse {
    Drop(HashAndFormat),
    Ping,
}

/// Options for force deletion of blobs.
#[derive(Debug, Serialize, Deserialize)]
pub struct BlobDeleteRequest {
    pub hashes: Vec<Hash>,
    pub force: bool,
}

/// Import the given bytes.
#[derive(Serialize, Deserialize)]
pub struct ImportBytesRequest {
    pub data: Bytes,
    pub format: BlobFormat,
    pub scope: Scope,
}

impl fmt::Debug for ImportBytesRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImportBytes")
            .field("data", &self.data.len())
            .field("format", &self.format)
            .field("scope", &self.scope)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportPathRequest {
    pub path: PathBuf,
    pub mode: ImportMode,
    pub format: BlobFormat,
    pub scope: Scope,
}

/// Import bao encoded data for the given hash with the iroh block size.
///
/// The result is just a single item, indicating if a write error occurred.
/// To observe the incoming data more granularly, use the `Observe` command
/// concurrently.
#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBaoRequest {
    pub hash: Hash,
    pub size: NonZeroU64,
}

/// Observe the local bitfield of the given hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct ObserveRequest {
    pub hash: Hash,
}

/// Export the given ranges in bao format, with the iroh block size.
///
/// The returned stream should be verified by the store.
///
/// The response starts with the [`EncodedItem::Size`] of the blob, followed by
/// [`EncodedItem::Parent`] and [`EncodedItem::Leaf`] items for the requested
/// ranges in pre-order, so leaves come in ascending order and end at the size
/// at the latest. A range past the end of the blob stands for the last chunk.
/// The stream is complete with the leaf that reaches the end of the requested
/// ranges or the size, whichever comes first, or right after the size if there
/// is nothing to send. A stream that ends before that was cut short. An
/// [`EncodedItem::Error`] can come at any time, and is the last item.
/// [`EncodedItem::Done`] is not needed, but counts as the last item too.
///
#[doc = include_str!("../../docs/img/export_bao.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportBaoRequest {
    pub hash: Hash,
    pub ranges: ChunkRanges,
}

/// Export the given ranges as chunks, without validation.
///
/// Missing blobs or requested chunks produce an [`std::io::ErrorKind::NotFound`]
/// error. Ranges past the end of the blob are clipped.
///
/// The response starts with the `Size` of the blob, followed by `Data` for the
/// requested ranges clipped to the size, in ascending order. The stream is
/// complete with the `Data` that reaches the end of the last clipped range, or
/// right after the size if there is nothing to send. A stream that ends before
/// that was cut short. An `Error` can come at any time, and is the last item.
///
#[doc = include_str!("../../docs/img/export_ranges.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportRangesRequest {
    pub hash: Hash,
    pub ranges: RangeSet2<u64>,
}

/// Export a file to a target path.
///
/// For an incomplete file, the size might be truncated and gaps will be filled
/// with zeros. If possible, a store implementation should try to write as a
/// sparse file.

#[derive(Debug, Serialize, Deserialize)]
pub struct ExportPathRequest {
    pub hash: Hash,
    pub mode: ExportMode,
    pub target: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportByteStreamRequest {
    pub format: BlobFormat,
    pub scope: Scope,
}

/// The items a client sends to the store for an [`ImportByteStreamRequest`].
///
/// The client sends the data as [`Self::Bytes`] items, and then
/// [`Self::Done`]. Dropping the sender does not complete the stream:
/// the store only imports the data after [`Self::Done`],
/// and treats a stream that ends without it as aborted. So a client that goes
/// away in the middle of an upload never produces a truncated blob.
///
#[doc = include_str!("../../docs/img/import_byte_stream.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub enum ImportByteStreamUpdate {
    Bytes(Bytes),
    Done,
}

/// Options for a list operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTagsRequest {
    /// List tags to hash seqs
    pub hash_seq: bool,
    /// List tags to raw blobs
    pub raw: bool,
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl ListTagsRequest {
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

/// An item of a [`Request::ListBlobs`] listing.
///
/// A listing is zero or more [`Self::Hash`] items, followed by [`Self::Done`]
/// on success or [`Self::Error`] on failure. An empty listing still sends
/// [`Self::Done`]. Dropping the sender does not complete a listing: a store
/// task that panics or is aborted drops its sender, and the client can not
/// tell from the items that some are missing. So a client reports a listing
/// that closes before its terminal item as an error, and stops at the terminal
/// item without waiting for the close.
///
/// [`ListTagsItem`] and [`ListTempTagsItem`] work the same way.
///
#[doc = include_str!("../../docs/img/list.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub enum ListBlobsItem {
    /// A blob in the store.
    Hash(Hash),
    /// The listing is complete.
    Done,
    /// The listing failed, the items before it are not complete.
    Error(super::Error),
}

/// An item of a [`Request::ListTags`] listing.
///
/// Ends like a [blob listing](ListBlobsItem).
#[derive(Debug, Serialize, Deserialize)]
pub enum ListTagsItem {
    /// A matching named tag.
    Tag(TagInfo),
    /// The listing is complete.
    Done,
    /// The listing failed, the items before it are not complete.
    Error(super::Error),
}

/// An item of a [`Request::ListTempTags`] listing.
///
/// Ends like a [blob listing](ListBlobsItem).
#[derive(Debug, Serialize, Deserialize)]
pub enum ListTempTagsItem {
    /// Content protected by a temporary tag.
    Tag(HashAndFormat),
    /// The listing is complete.
    Done,
    /// The listing failed, the items before it are not complete.
    Error(super::Error),
}

/// Information about a tag.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagInfo {
    /// Name of the tag
    pub name: Tag,
    /// Format of the data
    pub format: BlobFormat,
    /// Hash of the data
    pub hash: Hash,
}

impl From<TagInfo> for HashAndFormat {
    fn from(tag_info: TagInfo) -> Self {
        HashAndFormat {
            hash: tag_info.hash,
            format: tag_info.format,
        }
    }
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

pub(crate) fn tags_from_range<R, E>(range: R) -> (Option<Tag>, Option<Tag>)
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

/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTempTagRequest {
    pub scope: Scope,
    pub value: HashAndFormat,
}

/// List all temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTempTagsRequest;

/// Rename a tag atomically
#[derive(Debug, Serialize, Deserialize)]
pub struct RenameTagRequest {
    /// Old tag name
    pub from: Tag,
    /// New tag name
    pub to: Tag,
}

/// Options for a delete operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteTagsRequest {
    /// Optional from tag (inclusive)
    pub from: Option<Tag>,
    /// Optional to tag (exclusive)
    pub to: Option<Tag>,
}

impl DeleteTagsRequest {
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

/// Options for creating a tag or setting it to a new value.
#[derive(Debug, Serialize, Deserialize)]
pub struct SetTagRequest {
    pub name: Tag,
    pub value: HashAndFormat,
}

/// Options for creating a tag
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTagRequest {
    pub value: HashAndFormat,
}

/// Debug tool to exit the process in the middle of a write transaction, for testing.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessExitRequest {
    pub code: i32,
}

/// Progress events for importing from any local source.
///
/// An import first copies the data. The [`Self::Size`] comes once, before the
/// copy for sources of known size such as blobs and files, and after it for
/// streams. [`Self::CopyDone`] follows if the data was copied, which an import
/// that references a file where it is does not do. Then the outboard is
/// computed, and finally [`Self::Done`] comes. Only the size and the final
/// [`Self::Done`] or [`Self::Error`] are guaranteed; [`Self::CopyProgress`] and
/// [`Self::OutboardProgress`] are best effort and may not come at all. Progress
/// never goes backwards or past the size. An [`Self::Error`] can come at any
/// time, and is the last event.
///
#[doc = include_str!("../../docs/img/add_progress.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub enum AddProgressItem {
    /// Progress copying the file into the data directory.
    ///
    /// On most modern systems, copying will be done with copy on write,
    /// so copying will be instantaneous and you won't get any of these.
    ///
    /// The number is the *byte offset* of the copy process.
    ///
    /// This is an ephemeral progress event, so you can't rely on getting
    /// regular updates.
    CopyProgress(u64),
    /// Size of the file or stream has been determined.
    ///
    /// For some input such as blobs or files, the size is immediately known.
    /// For other inputs such as streams, the size is determined by reading
    /// the stream to the end.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these.
    Size(u64),
    /// The copy part of the import operation is done.
    ///
    /// This is only sent if the import copied the data, and then exactly once.
    /// An import that references a file where it is, see
    /// [`ImportMode::TryReference`], does not copy it and does not send this.
    CopyDone,
    /// Progress computing the outboard and root hash of the imported data.
    ///
    /// This is an ephemeral progress event, so you can't rely on getting
    /// regular updates.
    OutboardProgress(u64),
    /// The import is done. Once you get this event the data is available
    /// and protected in the store via the temp tag.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these if the operation was successful.
    ///
    /// This is one of the two possible final events. After this event, there
    /// won't be any more progress events.
    Done(TempTag),
    /// The import failed with an error. Partial data will be deleted.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these if the operation was unsuccessful.
    ///
    /// This is one of the two possible final events. After this event, there
    /// won't be any more progress events.
    Error(#[serde(with = "crate::util::serde::io_error_serde")] io::Error),
}

impl From<io::Error> for AddProgressItem {
    fn from(e: io::Error) -> Self {
        Self::Error(e)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ExportRangesItem {
    /// The size of the file being exported.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these.
    Size(u64),
    /// A range of the file being exported.
    Data(Leaf),
    /// Error while exporting the ranges.
    Error(super::Error),
}

impl From<super::Error> for ExportRangesItem {
    fn from(e: super::Error) -> Self {
        Self::Error(e)
    }
}

impl From<Leaf> for ExportRangesItem {
    fn from(leaf: Leaf) -> Self {
        Self::Data(leaf)
    }
}

/// Progress events for exporting to a local file.
///
/// Exporting does not involve outboard computation, so the events are simpler
/// than [`AddProgressItem`]. The [`Self::Size`] comes first, then the data is
/// copied, and finally [`Self::Done`] comes. Only the size and the final
/// [`Self::Done`] or [`Self::Error`] are guaranteed; [`Self::CopyProgress`] is
/// best effort and may not come at all. Progress never goes backwards or past
/// the size. An [`Self::Error`] can come at any time, and is the last event.
///
#[doc = include_str!("../../docs/img/export_progress.svg")]
#[derive(Debug, Serialize, Deserialize)]
pub enum ExportProgressItem {
    /// The size of the file being exported.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these.
    Size(u64),
    /// Progress copying the file to the target directory.
    ///
    /// On many modern systems, copying will be done with copy on write,
    /// so copying will be instantaneous and you won't get any of these.
    ///
    /// This is an ephemeral progress event, so you can't rely on getting
    /// regular updates.
    CopyProgress(u64),
    /// The export is done. Once you get this event the data is available.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these if the operation was successful.
    ///
    /// This is one of the two possible final events. After this event, there
    /// won't be any more progress events.
    Done,
    /// The export failed with an error.
    ///
    /// This is a guaranteed progress event, so you can rely on getting exactly
    /// one of these if the operation was unsuccessful.
    ///
    /// This is one of the two possible final events. After this event, there
    /// won't be any more progress events.
    Error(super::Error),
}

impl From<super::Error> for ExportProgressItem {
    fn from(e: super::Error) -> Self {
        Self::Error(e)
    }
}

/// The import mode describes how files will be imported.
///
/// This is a hint to the import trait method. For some implementations, this
/// does not make any sense. E.g. an in memory implementation will always have
/// to copy the file into memory. Also, a disk based implementation might choose
/// to copy small files even if the mode is `Reference`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ImportMode {
    /// This mode will copy the file into the database before hashing.
    ///
    /// This is the safe default because the file can not be accidentally modified
    /// after it has been imported.
    #[default]
    Copy,
    /// This mode will try to reference the file in place and assume it is unchanged after import.
    ///
    /// This has a large performance and storage benefit, but it is less safe since
    /// the file might be modified after it has been imported.
    ///
    /// Stores are allowed to ignore this mode and always copy the file, e.g.
    /// if the file is very small or if the store does not support referencing files.
    TryReference,
}

/// The import mode describes how files will be imported.
///
/// This is a hint to the import trait method. For some implementations, this
/// does not make any sense. E.g. an in memory implementation will always have
/// to copy the file into memory. Also, a disk based implementation might choose
/// to copy small files even if the mode is `Reference`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
pub enum ExportMode {
    /// This mode will copy the file to the target directory.
    ///
    /// This is the safe default because the file can not be accidentally modified
    /// after it has been exported.
    #[default]
    Copy,
    /// This mode will try to move the file to the target directory and then reference it from
    /// the database.
    ///
    /// This has a large performance and storage benefit, but it is less safe since
    /// the file might be modified in the target directory after it has been exported.
    ///
    /// Stores are allowed to ignore this mode and always copy the file, e.g.
    /// if the file is very small or if the store does not support referencing files.
    TryReference,
}

/// Status information about a blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobStatus {
    /// The blob is not stored at all.
    NotFound,
    /// The blob is only stored partially.
    Partial {
        /// The size of the currently stored partial blob.
        size: Option<u64>,
    },
    /// The blob is stored completely.
    Complete {
        /// The size of the blob.
        size: u64,
    },
}

/// A scope for a write operation.
#[derive(
    Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display,
)]
pub struct Scope(pub(crate) u64);

impl Scope {
    pub const GLOBAL: Self = Self(0);
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 == 0 {
            write!(f, "Global")
        } else {
            f.debug_tuple("Scope").field(&self.0).finish()
        }
    }
}
