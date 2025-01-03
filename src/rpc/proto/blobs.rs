//! RPC requests and responses for the blob service.
use std::path::PathBuf;

use bytes::Bytes;
use iroh::NodeAddr;
use nested_enum_utils::enum_conversions;
use quic_rpc_derive::rpc_requests;
use serde::{Deserialize, Serialize};

use super::{RpcError, RpcResult, RpcService};
pub use crate::get::progress::DownloadProgressEvent;
use crate::{
    format::collection::Collection,
    net_protocol::batches::BatchId,
    rpc::client::blobs::{
        BlobInfo, BlobStatus, DownloadMode, IncompleteBlobInfo, ReadAtLen, WrapOption,
    },
    store::{
        BaoBlobSize, ConsistencyCheckProgress, ExportFormat, ExportMode, ExportProgress,
        ImportMode, ValidateProgress,
    },
    util::SetTagOption,
    BlobFormat, Hash, HashAndFormat, Tag,
};

#[allow(missing_docs)]
#[derive(strum::Display, Debug, Serialize, Deserialize)]
#[enum_conversions(super::Request)]
#[rpc_requests(RpcService)]
pub enum Request {
    #[server_streaming(response = RpcResult<ReadAtResponse>)]
    ReadAt(ReadAtRequest),
    #[bidi_streaming(update = AddStreamUpdate, response = AddStreamResponse)]
    AddStream(AddStreamRequest),
    AddStreamUpdate(AddStreamUpdate),
    #[server_streaming(response = AddPathResponse)]
    AddPath(AddPathRequest),
    #[server_streaming(response = DownloadResponse)]
    Download(BlobDownloadRequest),
    #[server_streaming(response = ExportResponse)]
    Export(ExportRequest),
    #[server_streaming(response = RpcResult<BlobInfo>)]
    List(ListRequest),
    #[server_streaming(response = RpcResult<IncompleteBlobInfo>)]
    ListIncomplete(ListIncompleteRequest),
    #[rpc(response = RpcResult<()>)]
    Delete(DeleteRequest),
    #[server_streaming(response = ValidateProgress)]
    Validate(ValidateRequest),
    #[server_streaming(response = ConsistencyCheckProgress)]
    Fsck(ConsistencyCheckRequest),
    #[rpc(response = RpcResult<CreateCollectionResponse>)]
    CreateCollection(CreateCollectionRequest),
    #[rpc(response = RpcResult<BlobStatusResponse>)]
    BlobStatus(BlobStatusRequest),

    #[bidi_streaming(update = BatchUpdate, response = BatchCreateResponse)]
    BatchCreate(BatchCreateRequest),
    BatchUpdate(BatchUpdate),
    #[bidi_streaming(update = BatchAddStreamUpdate, response = BatchAddStreamResponse)]
    BatchAddStream(BatchAddStreamRequest),
    BatchAddStreamUpdate(BatchAddStreamUpdate),
    #[server_streaming(response = BatchAddPathResponse)]
    BatchAddPath(BatchAddPathRequest),
    #[rpc(response = RpcResult<()>)]
    BatchCreateTempTag(BatchCreateTempTagRequest),
}

#[allow(missing_docs)]
#[derive(strum::Display, Debug, Serialize, Deserialize)]
#[enum_conversions(super::Response)]
pub enum Response {
    ReadAt(RpcResult<ReadAtResponse>),
    AddStream(AddStreamResponse),
    AddPath(AddPathResponse),
    List(RpcResult<BlobInfo>),
    ListIncomplete(RpcResult<IncompleteBlobInfo>),
    Download(DownloadResponse),
    Fsck(ConsistencyCheckProgress),
    Export(ExportResponse),
    Validate(ValidateProgress),
    CreateCollection(RpcResult<CreateCollectionResponse>),
    BlobStatus(RpcResult<BlobStatusResponse>),
    BatchCreate(BatchCreateResponse),
    BatchAddStream(BatchAddStreamResponse),
    BatchAddPath(BatchAddPathResponse),
}

/// A request to the node to provide the data at the given path
///
/// Will produce a stream of [`AddProgressEvent`] messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddPathRequest {
    /// The path to the data to provide.
    ///
    /// This should be an absolute path valid for the file system on which
    /// the node runs. Usually the cli will run on the same machine as the
    /// node, so this should be an absolute path on the cli machine.
    pub path: PathBuf,
    /// True if the provider can assume that the data will not change, so it
    /// can be shared in place.
    pub in_place: bool,
    /// Tag to tag the data with.
    pub tag: SetTagOption,
    /// Whether to wrap the added data in a collection
    pub wrap: WrapOption,
}

/// Wrapper around [`AddProgressEvent`].
#[derive(Debug, Serialize, Deserialize, derive_more::Into)]
pub struct AddPathResponse(pub AddProgressEvent);

/// Progress response for [`BlobDownloadRequest`]
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From, derive_more::Into)]
pub struct DownloadResponse(pub DownloadProgressEvent);

/// A request to the node to download and share the data specified by the hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequest {
    /// The hash of the blob to export.
    pub hash: Hash,
    /// The filepath to where the data should be saved
    ///
    /// This should be an absolute path valid for the file system on which
    /// the node runs.
    pub path: PathBuf,
    /// Set to [`ExportFormat::Collection`] if the `hash` refers to a [`Collection`] and you want
    /// to export all children of the collection into individual files.
    pub format: ExportFormat,
    /// The mode of exporting.
    ///
    /// The default is [`ExportMode::Copy`]. See [`ExportMode`] for details.
    pub mode: ExportMode,
}

/// Progress response for [`ExportRequest`]
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From, derive_more::Into)]
pub struct ExportResponse(pub ExportProgress);

/// A request to the node to validate the integrity of all provided data
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyCheckRequest {
    /// repair the store by dropping inconsistent blobs
    pub repair: bool,
}

/// A request to the node to validate the integrity of all provided data
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateRequest {
    /// repair the store by downgrading blobs from complete to partial
    pub repair: bool,
}

/// List all blobs, including collections
#[derive(Debug, Serialize, Deserialize)]
pub struct ListRequest;

/// List all blobs, including collections
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIncompleteRequest;

/// Get the bytes for a hash
#[derive(Serialize, Deserialize, Debug)]
pub struct ReadAtRequest {
    /// Hash to get bytes for
    pub hash: Hash,
    /// Offset to start reading at
    pub offset: u64,
    /// Length of the data to get
    pub len: ReadAtLen,
}

/// Response to [`ReadAtRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub enum ReadAtResponse {
    /// The entry header.
    Entry {
        /// The size of the blob
        size: BaoBlobSize,
        /// Whether the blob is complete
        is_complete: bool,
    },
    /// Chunks of entry data.
    Data {
        /// The data chunk
        chunk: Bytes,
    },
}

/// Write a blob from a byte stream
#[derive(Serialize, Deserialize, Debug)]
pub struct AddStreamRequest {
    /// Tag to tag the data with.
    pub tag: SetTagOption,
}

/// Write a blob from a byte stream
#[derive(Serialize, Deserialize, Debug)]
pub enum AddStreamUpdate {
    /// A chunk of stream data
    Chunk(Bytes),
    /// Abort the request due to an error on the client side
    Abort,
}

/// Wrapper around [`AddProgressEvent`].
#[derive(Debug, Serialize, Deserialize, derive_more::Into)]
pub struct AddStreamResponse(pub AddProgressEvent);

/// Delete a blob
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Name of the tag
    pub hash: Hash,
}

/// Create a collection.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCollectionRequest {
    /// The collection
    pub collection: Collection,
    /// Tag option.
    pub tag: SetTagOption,
    /// Tags that should be deleted after creation.
    pub tags_to_delete: Vec<Tag>,
}

/// A response to a create collection request
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCollectionResponse {
    /// The resulting hash.
    pub hash: Hash,
    /// The resulting tag.
    pub tag: Tag,
}

/// Request to get the status of a blob
#[derive(Debug, Serialize, Deserialize)]
pub struct BlobStatusRequest {
    /// The hash of the blob
    pub hash: Hash,
}

/// The response to a status request
#[derive(Debug, Serialize, Deserialize, derive_more::From, derive_more::Into)]
pub struct BlobStatusResponse(pub BlobStatus);

/// Request to create a new scope for temp tags
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCreateRequest;

/// Update to a temp tag scope
#[derive(Debug, Serialize, Deserialize)]
pub enum BatchUpdate {
    /// Drop of a remote temp tag
    Drop(HashAndFormat),
    /// Message to check that the connection is still alive
    Ping,
}

/// Response to a temp tag scope request
#[derive(Debug, Serialize, Deserialize)]
pub enum BatchCreateResponse {
    /// We got the id of the scope
    Id(BatchId),
}

/// Create a temp tag with a given hash and format
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchCreateTempTagRequest {
    /// Content to protect
    pub content: HashAndFormat,
    /// Batch to create the temp tag in
    pub batch: BatchId,
}

/// Write a blob from a byte stream
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchAddStreamRequest {
    /// What format to use for the blob
    pub format: BlobFormat,
    /// Batch to create the temp tag in
    pub batch: BatchId,
}

/// Write a blob from a byte stream
#[derive(Serialize, Deserialize, Debug)]
pub enum BatchAddStreamUpdate {
    /// A chunk of stream data
    Chunk(Bytes),
    /// Abort the request due to an error on the client side
    Abort,
}

/// Wrapper around [`AddProgressEvent`].
#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize)]
pub enum BatchAddStreamResponse {
    Abort(RpcError),
    OutboardProgress { offset: u64 },
    Result { hash: Hash },
}

/// Write a blob from a byte stream
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchAddPathRequest {
    /// The path to the data to provide.
    pub path: PathBuf,
    /// Add the data in place
    pub import_mode: ImportMode,
    /// What format to use for the blob
    pub format: BlobFormat,
    /// Batch to create the temp tag in
    pub batch: BatchId,
}

/// Response to a batch add path request
#[derive(Serialize, Deserialize, Debug)]
pub struct BatchAddPathResponse(pub BatchAddPathProgressEvent);

/// A request to the node to download and share the data specified by the hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobDownloadRequest {
    /// This mandatory field contains the hash of the data to download and share.
    pub hash: Hash,
    /// If the format is [`BlobFormat::HashSeq`], all children are downloaded and shared as
    /// well.
    pub format: BlobFormat,
    /// This mandatory field specifies the nodes to download the data from.
    ///
    /// If set to more than a single node, they will all be tried. If `mode` is set to
    /// [`DownloadMode::Direct`], they will be tried sequentially until a download succeeds.
    /// If `mode` is set to [`DownloadMode::Queued`], the nodes may be dialed in parallel,
    /// if the concurrency limits permit.
    pub nodes: Vec<NodeAddr>,
    /// Optional tag to tag the data with.
    pub tag: SetTagOption,
    /// Whether to directly start the download or add it to the download queue.
    pub mode: DownloadMode,
}

/// Progress updates for the add operation.
#[derive(Debug, Serialize, Deserialize)]
pub enum AddProgressEvent {
    /// An item was found with name `name`, from now on referred to via `id`
    Found {
        /// A new unique id for this entry.
        id: u64,
        /// The name of the entry.
        name: String,
        /// The size of the entry in bytes.
        size: u64,
    },
    /// We got progress ingesting item `id`.
    Progress {
        /// The unique id of the entry.
        id: u64,
        /// The offset of the progress, in bytes.
        offset: u64,
    },
    /// We are done with `id`, and the hash is `hash`.
    Done {
        /// The unique id of the entry.
        id: u64,
        /// The hash of the entry.
        hash: Hash,
    },
    /// We are done with the whole operation.
    AllDone {
        /// The hash of the created data.
        hash: Hash,
        /// The format of the added data.
        format: BlobFormat,
        /// The tag of the added data.
        tag: Tag,
    },
    /// We got an error and need to abort.
    ///
    /// This will be the last message in the stream.
    Abort(serde_error::Error),
}

/// Progress updates for the batch add operation.
#[derive(Debug, Serialize, Deserialize)]
pub enum BatchAddPathProgressEvent {
    /// An item was found with the given size
    Found {
        /// The size of the entry in bytes.
        size: u64,
    },
    /// We got progress ingesting the item.
    Progress {
        /// The offset of the progress, in bytes.
        offset: u64,
    },
    /// We are done, and the hash is `hash`.
    Done {
        /// The hash of the entry.
        hash: Hash,
    },
    /// We got an error and need to abort.
    ///
    /// This will be the last message in the stream.
    Abort(serde_error::Error),
}
