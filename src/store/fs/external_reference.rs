//! Explicit validation and repair for file-store external references.
//!
//! Validation is read-only and store-specific. Repair and quarantine rerun
//! validation while holding the per-hash transition lock, then update metadata
//! with a paths/size/outboard compare-and-swap. External files are owned by the
//! caller and are never moved or deleted by this module.

use std::{
    fmt,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

use bao_tree::{
    blake3,
    io::{outboard::PreOrderOutboard, sync::encode_ranges_validated},
    BaoTree, ChunkRanges,
};
use bytes::Bytes;

use super::{
    bao_file::{BaoFileStorage, BlobStorageFailure},
    entry_state::{DataLocation, EntryState, OutboardLocation},
    meta::ExternalReferenceReconcileOutcome,
    options::Options,
    EmParams, HashContext, HashSpecificCommand, SyncEntityApi,
};
use crate::{
    api::proto::HashSpecific,
    store::{
        util::{FixedSize, MemOrFile},
        IROH_BLOCK_SIZE,
    },
    Hash,
};

/// Storage shape for an entry that is not backed by external paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NonExternalStorageKind {
    Inline,
    Owned,
    Partial,
}

/// Result of validating one recorded external path.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceCandidateStatus {
    Valid,
    Missing,
    Invalid(BlobStorageFailure),
}

impl ExternalReferenceCandidateStatus {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

/// Validation result for one external path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalReferenceCandidate {
    pub path: PathBuf,
    pub status: ExternalReferenceCandidateStatus,
}

/// Validation result for the store-owned outboard used by external data.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceOutboardStatus {
    Valid,
    NotChecked,
    Broken(BlobStorageFailure),
}

impl ExternalReferenceOutboardStatus {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

/// Full-content validation for one external blob metadata snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalReferenceValidation {
    pub hash: Hash,
    pub expected_size: u64,
    pub candidates: Vec<ExternalReferenceCandidate>,
    pub outboard: ExternalReferenceOutboardStatus,
    expected_paths: Vec<PathBuf>,
    expected_outboard: OutboardLocation<()>,
}

impl ExternalReferenceValidation {
    pub fn valid_paths(&self) -> impl Iterator<Item = &Path> {
        self.candidates.iter().filter_map(|candidate| {
            candidate
                .status
                .is_valid()
                .then_some(candidate.path.as_path())
        })
    }

    pub fn is_healthy(&self) -> bool {
        self.outboard.is_valid()
            && self
                .candidates
                .iter()
                .any(|candidate| candidate.status.is_valid())
            && self
                .candidates
                .iter()
                .all(|candidate| candidate.status.is_valid())
    }
}

/// Read-only result for one hash.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceInspection {
    Missing,
    NotExternal(NonExternalStorageKind),
    External(ExternalReferenceValidation),
}

/// Why explicit repair removed the external metadata entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceRemovalReason {
    NoValidCandidate,
    BrokenOutboard,
}

/// Durable action performed after validation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceMaintenanceAction {
    NoChange,
    Repaired {
        retained_paths: Vec<PathBuf>,
        quarantined_paths: Vec<PathBuf>,
    },
    Removed {
        quarantined_paths: Vec<PathBuf>,
        reason: ExternalReferenceRemovalReason,
    },
    Quarantined {
        paths: Vec<PathBuf>,
    },
    AlreadyAbsent,
    SkippedConcurrentChange,
}

/// Validation snapshot and the action taken from that snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalReferenceMaintenanceReport {
    pub inspection: ExternalReferenceInspection,
    pub action: ExternalReferenceMaintenanceAction,
}

/// Failure to execute external-reference validation or maintenance.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ExternalReferenceMaintenanceError {
    StoreUnavailable,
    ResponseDropped,
    EntityBusy,
    EntityDead,
    Metadata(BlobStorageFailure),
    ValidationWorker(String),
}

impl fmt::Display for ExternalReferenceMaintenanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreUnavailable => f.write_str("file-store actor is unavailable"),
            Self::ResponseDropped => f.write_str("external-reference response was dropped"),
            Self::EntityBusy => f.write_str("blob entity is busy"),
            Self::EntityDead => f.write_str("blob entity is unavailable"),
            Self::Metadata(cause) => write!(f, "external-reference metadata failed: {cause}"),
            Self::ValidationWorker(cause) => {
                write!(f, "external-reference validation worker failed: {cause}")
            }
        }
    }
}

impl std::error::Error for ExternalReferenceMaintenanceError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExternalReferenceMaintenanceMode {
    Repair,
    Quarantine,
}

#[derive(Debug)]
pub(super) struct ValidateExternalReferenceRequest {
    pub hash: Hash,
    pub tx: tokio::sync::oneshot::Sender<
        Result<ExternalReferenceInspection, ExternalReferenceMaintenanceError>,
    >,
}

#[derive(Debug)]
pub(super) struct MaintainExternalReferenceRequest {
    pub hash: Hash,
    pub mode: ExternalReferenceMaintenanceMode,
    pub tx: tokio::sync::oneshot::Sender<
        Result<ExternalReferenceMaintenanceReport, ExternalReferenceMaintenanceError>,
    >,
}

impl HashSpecific for ValidateExternalReferenceRequest {
    fn hash(&self) -> Hash {
        self.hash
    }
}

impl HashSpecificCommand for ValidateExternalReferenceRequest {
    async fn handle(self, ctx: HashContext) {
        let result = inspect_external_reference(&ctx).await;
        let _ = self.tx.send(result);
    }

    async fn on_error(self, arg: super::entity_manager::SpawnArg<EmParams>) {
        let _ = self.tx.send(Err(entity_error(arg)));
    }
}

impl HashSpecific for MaintainExternalReferenceRequest {
    fn hash(&self) -> Hash {
        self.hash
    }
}

impl HashSpecificCommand for MaintainExternalReferenceRequest {
    async fn handle(self, ctx: HashContext) {
        let result = maintain_external_reference(&ctx, self.mode).await;
        let _ = self.tx.send(result);
    }

    async fn on_error(self, arg: super::entity_manager::SpawnArg<EmParams>) {
        let _ = self.tx.send(Err(entity_error(arg)));
    }
}

fn entity_error(
    arg: super::entity_manager::SpawnArg<EmParams>,
) -> ExternalReferenceMaintenanceError {
    match arg {
        super::entity_manager::SpawnArg::Busy => ExternalReferenceMaintenanceError::EntityBusy,
        super::entity_manager::SpawnArg::Dead => ExternalReferenceMaintenanceError::EntityDead,
        super::entity_manager::SpawnArg::Active(_) => unreachable!(),
    }
}

async fn inspect_external_reference(
    ctx: &HashContext,
) -> Result<ExternalReferenceInspection, ExternalReferenceMaintenanceError> {
    let state = ctx.global.db.get(ctx.id).await.map_err(|cause| {
        ExternalReferenceMaintenanceError::Metadata(BlobStorageFailure::metadata(
            "load external-reference metadata",
            &cause,
        ))
    })?;
    let hash = ctx.id;
    let options = ctx.global.options.clone();
    tokio::task::spawn_blocking(move || inspect_state(hash, state, &options))
        .await
        .map_err(|cause| ExternalReferenceMaintenanceError::ValidationWorker(cause.to_string()))
}

fn inspect_state(
    hash: Hash,
    state: Option<EntryState<Bytes>>,
    options: &Options,
) -> ExternalReferenceInspection {
    let Some(state) = state else {
        return ExternalReferenceInspection::Missing;
    };
    let EntryState::Complete {
        data_location,
        outboard_location,
    } = state
    else {
        return ExternalReferenceInspection::NotExternal(NonExternalStorageKind::Partial);
    };
    let DataLocation::External(paths, expected_size) = data_location else {
        let kind = match data_location {
            DataLocation::Inline(_) => NonExternalStorageKind::Inline,
            DataLocation::Owned(_) => NonExternalStorageKind::Owned,
            DataLocation::External(_, _) => unreachable!(),
        };
        return ExternalReferenceInspection::NotExternal(kind);
    };

    let candidates = paths
        .iter()
        .map(|path| ExternalReferenceCandidate {
            path: path.clone(),
            status: validate_candidate(path, expected_size, hash),
        })
        .collect::<Vec<_>>();
    let first_valid = candidates.iter().find_map(|candidate| {
        candidate
            .status
            .is_valid()
            .then_some(candidate.path.as_path())
    });
    let outboard = first_valid.map_or(ExternalReferenceOutboardStatus::NotChecked, |path| {
        validate_outboard(path, expected_size, hash, &outboard_location, options)
    });
    ExternalReferenceInspection::External(ExternalReferenceValidation {
        hash,
        expected_size,
        candidates,
        outboard,
        expected_paths: paths,
        expected_outboard: outboard_location.discard_extra_data(),
    })
}

fn validate_candidate(
    path: &Path,
    expected_size: u64,
    expected_hash: Hash,
) -> ExternalReferenceCandidateStatus {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(cause) if cause.kind() == io::ErrorKind::NotFound => {
            return ExternalReferenceCandidateStatus::Missing;
        }
        Err(cause) => {
            return ExternalReferenceCandidateStatus::Invalid(BlobStorageFailure::external(
                "open external validation candidate",
                &cause,
            ));
        }
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(cause) => {
            return ExternalReferenceCandidateStatus::Invalid(BlobStorageFailure::external(
                "inspect external validation candidate",
                &cause,
            ));
        }
    };
    if !metadata.is_file() || metadata.len() != expected_size {
        let cause = io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "external candidate {} is not a regular {expected_size}-byte file",
                path.display()
            ),
        );
        return ExternalReferenceCandidateStatus::Invalid(BlobStorageFailure::external(
            "inspect external validation candidate",
            &cause,
        ));
    }

    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                hasher.update(&buffer[..read]);
            }
            Err(cause) => {
                return ExternalReferenceCandidateStatus::Invalid(BlobStorageFailure::external(
                    "hash external validation candidate",
                    &cause,
                ));
            }
        }
    }
    if Hash::from(hasher.finalize()) == expected_hash {
        ExternalReferenceCandidateStatus::Valid
    } else {
        let cause = io::Error::new(
            io::ErrorKind::InvalidData,
            format!("external candidate {} hash mismatch", path.display()),
        );
        ExternalReferenceCandidateStatus::Invalid(BlobStorageFailure::external(
            "hash external validation candidate",
            &cause,
        ))
    }
}

fn validate_outboard(
    path: &Path,
    size: u64,
    hash: Hash,
    location: &OutboardLocation<Bytes>,
    options: &Options,
) -> ExternalReferenceOutboardStatus {
    let data = match File::open(path) {
        Ok(file) => FixedSize::new(file, size),
        Err(cause) => {
            return ExternalReferenceOutboardStatus::Broken(BlobStorageFailure::external(
                "reopen external candidate for outboard validation",
                &cause,
            ));
        }
    };
    let outboard: MemOrFile<Bytes, File> = match location {
        OutboardLocation::Inline(bytes) => MemOrFile::Mem(bytes.clone()),
        OutboardLocation::Owned => match File::open(options.path.outboard_path(&hash)) {
            Ok(file) => MemOrFile::File(file),
            Err(cause) => {
                return ExternalReferenceOutboardStatus::Broken(
                    BlobStorageFailure::owned_outboard(
                        "open owned outboard for external validation",
                        &cause,
                    ),
                );
            }
        },
        OutboardLocation::NotNeeded => MemOrFile::empty(),
    };
    let outboard = PreOrderOutboard {
        tree: BaoTree::new(size, IROH_BLOCK_SIZE),
        root: hash.into(),
        data: outboard,
    };
    let ranges = ChunkRanges::all();
    match encode_ranges_validated(data, &outboard, &ranges, io::sink()) {
        Ok(()) => ExternalReferenceOutboardStatus::Valid,
        Err(cause) => {
            let cause = io::Error::new(io::ErrorKind::InvalidData, cause.to_string());
            ExternalReferenceOutboardStatus::Broken(BlobStorageFailure::external(
                "validate external data against stored outboard",
                &cause,
            ))
        }
    }
}

async fn maintain_external_reference(
    ctx: &HashContext,
    mode: ExternalReferenceMaintenanceMode,
) -> Result<ExternalReferenceMaintenanceReport, ExternalReferenceMaintenanceError> {
    let _transition = ctx.state.transition().await;
    let inspection = inspect_external_reference(ctx).await?;
    let ExternalReferenceInspection::External(validation) = &inspection else {
        return Ok(ExternalReferenceMaintenanceReport {
            inspection,
            action: ExternalReferenceMaintenanceAction::NoChange,
        });
    };

    let all_paths = validation.expected_paths.clone();
    let valid_paths = validation
        .valid_paths()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    let invalid_paths = all_paths
        .iter()
        .filter(|path| !valid_paths.contains(path))
        .cloned()
        .collect::<Vec<_>>();
    let retained_paths = match mode {
        ExternalReferenceMaintenanceMode::Repair if validation.outboard.is_valid() => {
            valid_paths.clone()
        }
        ExternalReferenceMaintenanceMode::Repair | ExternalReferenceMaintenanceMode::Quarantine => {
            Vec::new()
        }
    };

    if mode == ExternalReferenceMaintenanceMode::Repair
        && invalid_paths.is_empty()
        && validation.outboard.is_valid()
    {
        return Ok(ExternalReferenceMaintenanceReport {
            inspection,
            action: ExternalReferenceMaintenanceAction::NoChange,
        });
    }

    let outcome = ctx
        .global
        .db
        .reconcile_external_reference(
            validation.hash,
            validation.expected_paths.clone(),
            validation.expected_size,
            Some(validation.expected_outboard.clone()),
            retained_paths.clone(),
        )
        .await
        .map_err(|cause| {
            ExternalReferenceMaintenanceError::Metadata(BlobStorageFailure::metadata(
                "repair external-reference metadata",
                &cause,
            ))
        })?;

    let action = match outcome {
        ExternalReferenceReconcileOutcome::Retained => {
            ExternalReferenceMaintenanceAction::Repaired {
                retained_paths,
                quarantined_paths: invalid_paths,
            }
        }
        ExternalReferenceReconcileOutcome::Removed => match mode {
            ExternalReferenceMaintenanceMode::Quarantine => {
                ExternalReferenceMaintenanceAction::Quarantined { paths: all_paths }
            }
            ExternalReferenceMaintenanceMode::Repair => {
                let reason = match &validation.outboard {
                    ExternalReferenceOutboardStatus::Broken(_) => {
                        ExternalReferenceRemovalReason::BrokenOutboard
                    }
                    ExternalReferenceOutboardStatus::Valid
                    | ExternalReferenceOutboardStatus::NotChecked => {
                        ExternalReferenceRemovalReason::NoValidCandidate
                    }
                };
                ExternalReferenceMaintenanceAction::Removed {
                    quarantined_paths: all_paths,
                    reason,
                }
            }
        },
        ExternalReferenceReconcileOutcome::AlreadyAbsent => {
            ExternalReferenceMaintenanceAction::AlreadyAbsent
        }
        ExternalReferenceReconcileOutcome::Changed => {
            ExternalReferenceMaintenanceAction::SkippedConcurrentChange
        }
    };
    if matches!(
        action,
        ExternalReferenceMaintenanceAction::Repaired { .. }
            | ExternalReferenceMaintenanceAction::Removed { .. }
            | ExternalReferenceMaintenanceAction::Quarantined { .. }
    ) {
        ctx.state.send_replace(BaoFileStorage::Initial);
        ctx.load().await;
    }
    Ok(ExternalReferenceMaintenanceReport { inspection, action })
}
