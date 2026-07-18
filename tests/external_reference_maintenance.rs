#![cfg(feature = "fs-store")]

use std::fs;

use iroh_blobs::{
    api::blobs::{ExportMode, ExportOptions},
    store::fs::{
        BlobStorageFailureKind, ExternalReferenceCandidateStatus, ExternalReferenceInspection,
        ExternalReferenceMaintenanceAction, ExternalReferenceOutboardStatus,
        ExternalReferenceRemovalReason, FsStore, NonExternalStorageKind,
    },
    Hash, HashAndFormat,
};
use testresult::TestResult;

#[tokio::test]
async fn validate_is_read_only_and_repair_detaches_same_length_corruption() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("store");
    let external = temp.path().join("external.bin");
    let data = vec![b'A'; 1024 * 1024];
    let hash = Hash::new(&data);

    {
        let store = FsStore::load(&root).await?;
        store.add_bytes(data.clone()).await?;
        store
            .tags()
            .set("retained", HashAndFormat::raw(hash))
            .await?;
        store
            .export_with_opts(ExportOptions {
                hash,
                target: external.clone(),
                mode: ExportMode::TryReference,
            })
            .await?;
        store.sync_db().await?;
        store.shutdown().await?;
    }

    fs::write(&external, vec![b'X'; data.len()])?;
    let store = FsStore::load(&root).await?;
    let inspection = store.validate_external_reference(hash).await?;
    let ExternalReferenceInspection::External(validation) = &inspection else {
        panic!("exported reference must remain external");
    };
    assert!(matches!(
        validation.candidates.as_slice(),
        [candidate]
            if matches!(
                &candidate.status,
                ExternalReferenceCandidateStatus::Invalid(cause)
                    if cause.kind == BlobStorageFailureKind::ExternalDataInvalid
            )
    ));
    assert_eq!(
        store.status(hash).await?,
        iroh_blobs::api::blobs::BlobStatus::Complete {
            size: data.len() as u64
        },
        "validation must be read-only"
    );

    let report = store.repair_external_reference(hash).await?;
    assert!(matches!(
        report.action,
        ExternalReferenceMaintenanceAction::Removed {
            reason: ExternalReferenceRemovalReason::NoValidCandidate,
            ..
        }
    ));
    assert_eq!(
        store.status(hash).await?,
        iroh_blobs::api::blobs::BlobStatus::NotFound
    );
    assert_eq!(fs::read(&external)?, vec![b'X'; data.len()]);
    assert_eq!(
        store
            .tags()
            .get("retained")
            .await?
            .expect("repair must preserve persistent tags")
            .hash,
        hash
    );
    let recovered = store.add_bytes(data).await?;
    assert_eq!(recovered.hash, hash);
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn repair_retains_verified_candidate_and_quarantines_only_invalid_path() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("store");
    let corrupt = temp.path().join("corrupt.bin");
    let healthy = temp.path().join("healthy.bin");
    let data = vec![b'B'; 1024 * 1024];
    let hash = Hash::new(&data);

    {
        let store = FsStore::load(&root).await?;
        store.add_bytes(data.clone()).await?;
        for target in [&corrupt, &healthy] {
            store
                .export_with_opts(ExportOptions {
                    hash,
                    target: target.clone(),
                    mode: ExportMode::TryReference,
                })
                .await?;
        }
        store.sync_db().await?;
        store.shutdown().await?;
    }

    fs::write(&corrupt, vec![b'Y'; data.len()])?;
    let store = FsStore::load(&root).await?;
    let report = store.repair_external_reference(hash).await?;
    match report.action {
        ExternalReferenceMaintenanceAction::Repaired {
            retained_paths,
            quarantined_paths,
        } => {
            assert_eq!(retained_paths, vec![healthy.clone()]);
            assert_eq!(quarantined_paths, vec![corrupt.clone()]);
        }
        action => panic!("expected retained-candidate repair, got {action:?}"),
    }
    assert_eq!(store.get_bytes(hash).await?, data);
    store.shutdown().await?;

    let store = FsStore::load(&root).await?;
    let ExternalReferenceInspection::External(validation) =
        store.validate_external_reference(hash).await?
    else {
        panic!("repaired metadata must retain the healthy external path");
    };
    assert_eq!(
        validation.valid_paths().collect::<Vec<_>>(),
        vec![healthy.as_path()]
    );
    assert!(validation.is_healthy());
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn broken_owned_outboard_removes_entry_without_touching_external_file() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("store");
    let external = temp.path().join("external.bin");
    let data = vec![b'C'; 8 * 1024 * 1024];
    let hash = Hash::new(&data);

    {
        let store = FsStore::load(&root).await?;
        store.add_bytes(data.clone()).await?;
        store
            .export_with_opts(ExportOptions {
                hash,
                target: external.clone(),
                mode: ExportMode::TryReference,
            })
            .await?;
        store.sync_db().await?;
        store.shutdown().await?;
    }

    let outboard = root.join("data").join(format!("{}.obao4", hash.to_hex()));
    fs::remove_file(outboard)?;
    let store = FsStore::load(&root).await?;
    let ExternalReferenceInspection::External(validation) =
        store.validate_external_reference(hash).await?
    else {
        panic!("entry must remain external before explicit repair");
    };
    assert!(matches!(
        validation.outboard,
        ExternalReferenceOutboardStatus::Broken(ref cause)
            if cause.kind == BlobStorageFailureKind::OwnedOutboardMissing
    ));

    let report = store.repair_external_reference(hash).await?;
    assert!(matches!(
        report.action,
        ExternalReferenceMaintenanceAction::Removed {
            reason: ExternalReferenceRemovalReason::BrokenOutboard,
            ..
        }
    ));
    assert_eq!(fs::read(&external)?, data);
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn explicit_quarantine_preserves_healthy_user_file_and_tags() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("store");
    let external = temp.path().join("external.bin");
    let data = vec![b'D'; 1024 * 1024];
    let hash = Hash::new(&data);
    let store = FsStore::load(&root).await?;
    store.add_bytes(data.clone()).await?;
    store
        .tags()
        .set("retained", HashAndFormat::raw(hash))
        .await?;
    store
        .export_with_opts(ExportOptions {
            hash,
            target: external.clone(),
            mode: ExportMode::TryReference,
        })
        .await?;

    let report = store.quarantine_external_reference(hash).await?;
    assert_eq!(
        report.action,
        ExternalReferenceMaintenanceAction::Quarantined {
            paths: vec![external.clone()]
        }
    );
    assert_eq!(fs::read(&external)?, data);
    assert!(store.tags().get("retained").await?.is_some());
    assert_eq!(
        store.status(hash).await?,
        iroh_blobs::api::blobs::BlobStatus::NotFound
    );
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn validation_distinguishes_missing_and_inline_entries() -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let store = FsStore::load(temp.path().join("store")).await?;
    let missing = Hash::new(b"not stored");
    assert_eq!(
        store.validate_external_reference(missing).await?,
        ExternalReferenceInspection::Missing
    );

    let inline = store.add_bytes(b"inline".to_vec()).await?;
    assert_eq!(
        store.validate_external_reference(inline.hash).await?,
        ExternalReferenceInspection::NotExternal(NonExternalStorageKind::Inline)
    );
    store.shutdown().await?;
    Ok(())
}
