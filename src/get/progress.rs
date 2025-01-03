//! Types for get progress state management.

use std::{collections::HashMap, num::NonZeroU64};

use serde::{Deserialize, Serialize};
use tracing::warn;

use super::Stats;
use crate::{protocol::RangeSpec, store::BaoBlobSize, Hash};

/// The identifier for progress events.
pub type ProgressId = u64;

/// Accumulated progress state of a transfer.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TransferState {
    /// The root blob of this transfer (may be a hash seq),
    pub root: BlobState,
    /// Whether we are connected to a node
    pub connected: bool,
    /// Children if the root blob is a hash seq, empty for raw blobs
    pub children: HashMap<NonZeroU64, BlobState>,
    /// Child being transferred at the moment.
    pub current: Option<BlobId>,
    /// Progress ids for individual blobs.
    pub progress_id_to_blob: HashMap<ProgressId, BlobId>,
}

impl TransferState {
    /// Create a new, empty transfer state.
    pub fn new(root_hash: Hash) -> Self {
        Self {
            root: BlobState::new(root_hash),
            connected: false,
            children: Default::default(),
            current: None,
            progress_id_to_blob: Default::default(),
        }
    }
}

/// State of a single blob in transfer
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct BlobState {
    /// The hash of this blob.
    pub hash: Hash,
    /// The size of this blob. Only known if the blob is partially present locally, or after having
    /// received the size from the remote.
    pub size: Option<BaoBlobSize>,
    /// The current state of the blob transfer.
    pub progress: BlobProgressEvent,
    /// Ranges already available locally at the time of starting the transfer.
    pub local_ranges: Option<RangeSpec>,
    /// Number of children (only applies to hashseqs, None for raw blobs).
    pub child_count: Option<u64>,
}

/// Progress state for a single blob
#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum BlobProgressEvent {
    /// Download is pending
    #[default]
    Pending,
    /// Download is in progress
    Progressing(u64),
    /// Download has finished
    Done,
}

impl BlobState {
    /// Create a new [`BlobState`].
    pub fn new(hash: Hash) -> Self {
        Self {
            hash,
            size: None,
            local_ranges: None,
            child_count: None,
            progress: BlobProgressEvent::default(),
        }
    }
}

impl TransferState {
    /// Get state of the root blob of this transfer.
    pub fn root(&self) -> &BlobState {
        &self.root
    }

    /// Get a blob state by its [`BlobId`] in this transfer.
    pub fn get_blob(&self, blob_id: &BlobId) -> Option<&BlobState> {
        match blob_id {
            BlobId::Root => Some(&self.root),
            BlobId::Child(id) => self.children.get(id),
        }
    }

    /// Get the blob state currently being transferred.
    pub fn get_current(&self) -> Option<&BlobState> {
        self.current.as_ref().and_then(|id| self.get_blob(id))
    }

    fn get_or_insert_blob(&mut self, blob_id: BlobId, hash: Hash) -> &mut BlobState {
        match blob_id {
            BlobId::Root => &mut self.root,
            BlobId::Child(id) => self
                .children
                .entry(id)
                .or_insert_with(|| BlobState::new(hash)),
        }
    }
    fn get_blob_mut(&mut self, blob_id: &BlobId) -> Option<&mut BlobState> {
        match blob_id {
            BlobId::Root => Some(&mut self.root),
            BlobId::Child(id) => self.children.get_mut(id),
        }
    }

    fn get_by_progress_id(&mut self, progress_id: ProgressId) -> Option<&mut BlobState> {
        let blob_id = *self.progress_id_to_blob.get(&progress_id)?;
        self.get_blob_mut(&blob_id)
    }

    /// Update the state with a new [`DownloadProgressEvent`] for this transfer.
    pub fn on_progress(&mut self, event: DownloadProgressEvent) {
        match event {
            DownloadProgressEvent::InitialState(s) => {
                *self = s;
            }
            DownloadProgressEvent::FoundLocal {
                child,
                hash,
                size,
                valid_ranges,
            } => {
                let blob = self.get_or_insert_blob(child, hash);
                blob.size = Some(size);
                blob.local_ranges = Some(valid_ranges);
            }
            DownloadProgressEvent::Connected => self.connected = true,
            DownloadProgressEvent::Found {
                id: progress_id,
                child: blob_id,
                hash,
                size,
            } => {
                let blob = self.get_or_insert_blob(blob_id, hash);
                blob.size = match blob.size {
                    // If we don't have a verified size for this blob yet: Use the size as reported
                    // by the remote.
                    None | Some(BaoBlobSize::Unverified(_)) => Some(BaoBlobSize::Unverified(size)),
                    // Otherwise, keep the existing verified size.
                    value @ Some(BaoBlobSize::Verified(_)) => value,
                };
                blob.progress = BlobProgressEvent::Progressing(0);
                self.progress_id_to_blob.insert(progress_id, blob_id);
                self.current = Some(blob_id);
            }
            DownloadProgressEvent::FoundHashSeq { hash, children } => {
                if hash == self.root.hash {
                    self.root.child_count = Some(children);
                } else {
                    // I think it is an invariant of the protocol that `FoundHashSeq` is only
                    // triggered for the root hash.
                    warn!("Received `FoundHashSeq` event for a hash which is not the download's root hash.")
                }
            }
            DownloadProgressEvent::Progress { id, offset } => {
                if let Some(blob) = self.get_by_progress_id(id) {
                    blob.progress = BlobProgressEvent::Progressing(offset);
                } else {
                    warn!(%id, "Received `Progress` event for unknown progress id.")
                }
            }
            DownloadProgressEvent::Done { id } => {
                if let Some(blob) = self.get_by_progress_id(id) {
                    blob.progress = BlobProgressEvent::Done;
                    self.progress_id_to_blob.remove(&id);
                } else {
                    warn!(%id, "Received `Done` event for unknown progress id.")
                }
            }
            DownloadProgressEvent::AllDone(_) | DownloadProgressEvent::Abort(_) => {}
        }
    }
}

/// Progress updates for the get operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DownloadProgressEvent {
    /// Initial state if subscribing to a running or queued transfer.
    InitialState(TransferState),
    /// Data was found locally.
    FoundLocal {
        /// child offset
        child: BlobId,
        /// The hash of the entry.
        hash: Hash,
        /// The size of the entry in bytes.
        size: BaoBlobSize,
        /// The ranges that are available locally.
        valid_ranges: RangeSpec,
    },
    /// A new connection was established.
    Connected,
    /// An item was found with hash `hash`, from now on referred to via `id`.
    Found {
        /// A new unique progress id for this entry.
        id: u64,
        /// Identifier for this blob within this download.
        ///
        /// Will always be [`BlobId::Root`] unless a hashseq is downloaded, in which case this
        /// allows to identify the children by their offset in the hashseq.
        child: BlobId,
        /// The hash of the entry.
        hash: Hash,
        /// The size of the entry in bytes.
        size: u64,
    },
    /// An item was found with hash `hash`, from now on referred to via `id`.
    FoundHashSeq {
        /// The name of the entry.
        hash: Hash,
        /// Number of children in the collection, if known.
        children: u64,
    },
    /// We got progress ingesting item `id`.
    Progress {
        /// The unique id of the entry.
        id: u64,
        /// The offset of the progress, in bytes.
        offset: u64,
    },
    /// We are done with `id`.
    Done {
        /// The unique id of the entry.
        id: u64,
    },
    /// All operations finished.
    ///
    /// This will be the last message in the stream.
    AllDone(Stats),
    /// We got an error and need to abort.
    ///
    /// This will be the last message in the stream.
    Abort(serde_error::Error),
}

/// The id of a blob in a transfer
#[derive(
    Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, std::hash::Hash, Serialize, Deserialize,
)]
pub enum BlobId {
    /// The root blob (child id 0)
    Root,
    /// A child blob (child id > 0)
    Child(NonZeroU64),
}

impl BlobId {
    pub(crate) fn from_offset(id: u64) -> Self {
        NonZeroU64::new(id).map(Self::Child).unwrap_or(Self::Root)
    }
}

impl From<BlobId> for u64 {
    fn from(value: BlobId) -> Self {
        match value {
            BlobId::Root => 0,
            BlobId::Child(id) => id.into(),
        }
    }
}