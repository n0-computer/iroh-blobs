use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use iroh_blobs::{
    api::{blobs::AddBytesOptions, Store},
    store::{gc_run_once, gc_run_once_with_late_protection, mem::MemStore, GcRunOutcome},
    BlobFormat, Hash,
};
use testresult::TestResult;
use tokio::{sync::oneshot, time::Duration};

#[tokio::test]
async fn memory_store_import_committed_after_mark_survives_sweep() -> TestResult<()> {
    let store = MemStore::new();
    assert_import_after_mark_survives(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn filesystem_store_import_committed_after_mark_survives_sweep() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_import_after_mark_survives(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_late_protection_aborts_before_memory_store_sweep() -> TestResult<()> {
    let store = MemStore::new();
    assert_failed_protection_aborts_sweep(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn failed_late_protection_aborts_before_filesystem_store_sweep() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_failed_protection_aborts_sweep(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn named_root_created_after_mark_survives_memory_store_sweep() -> TestResult<()> {
    let store = MemStore::new();
    assert_named_root_created_after_mark_survives(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn named_root_created_after_mark_survives_filesystem_store_sweep() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_named_root_created_after_mark_survives(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn temporary_root_created_after_mark_survives_memory_store_sweep() -> TestResult<()> {
    let store = MemStore::new();
    assert_temporary_root_created_after_mark_survives(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn temporary_root_created_after_mark_survives_filesystem_store_sweep() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_temporary_root_created_after_mark_survives(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn malformed_hash_sequence_aborts_memory_store_before_sweep() -> TestResult<()> {
    let store = MemStore::new();
    assert_malformed_hash_sequence_aborts_before_sweep(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn malformed_hash_sequence_aborts_filesystem_store_before_sweep() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_malformed_hash_sequence_aborts_before_sweep(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_memory_store_gc_cycle_is_rejected_and_recoverable() -> TestResult<()> {
    let store = MemStore::new();
    assert_concurrent_gc_cycle_is_rejected_and_recoverable(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn concurrent_filesystem_store_gc_cycle_is_rejected_and_recoverable() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_concurrent_gc_cycle_is_rejected_and_recoverable(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_memory_store_gc_cycle_releases_its_protection_epoch() -> TestResult<()> {
    let store = MemStore::new();
    assert_cancelled_gc_cycle_releases_its_protection_epoch(store.as_ref()).await
}

#[cfg(feature = "fs-store")]
#[tokio::test]
async fn cancelled_filesystem_store_gc_cycle_releases_its_protection_epoch() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = iroh_blobs::store::fs::FsStore::load(testdir.path().join("store")).await?;
    assert_cancelled_gc_cycle_releases_its_protection_epoch(store.as_ref()).await?;
    store.shutdown().await?;
    Ok(())
}

async fn assert_import_after_mark_survives(store: &Store) -> TestResult<()> {
    let existing_tag = store
        .blobs()
        .add_slice(b"existing durable reference")
        .temp_tag()
        .await?;
    let existing = existing_tag.hash();
    drop(existing_tag);

    let (start_import_tx, start_import_rx) = oneshot::channel();
    let (import_done_tx, import_done_rx) = oneshot::channel();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        start_import_rx
            .await
            .expect("late protection future must trigger the writer");
        let tag = writer_store
            .blobs()
            .add_slice(b"reference committed during gc")
            .temp_tag()
            .await
            .expect("concurrent import must succeed");
        drop(tag);
        import_done_tx
            .send(())
            .expect("late protection future must remain alive");
    });

    let load_late_protection = move || async move {
        start_import_tx.send(()).expect("writer must remain alive");
        import_done_rx.await.expect("writer must finish import");
        Ok::<_, &'static str>(HashSet::from([existing]))
    };
    let mut live = HashSet::new();
    let outcome = gc_run_once_with_late_protection(store, &mut live, load_late_protection).await?;
    writer.await?;

    assert!(matches!(outcome, GcRunOutcome::Completed));
    assert!(store.has(existing).await?);
    assert!(
        store
            .has(Hash::new(b"reference committed during gc"))
            .await?,
        "an import completed after mark must remain protected through sweep"
    );
    Ok(())
}

async fn assert_concurrent_gc_cycle_is_rejected_and_recoverable(store: &Store) -> TestResult<()> {
    let (mark_complete_tx, mark_complete_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first_store = store.clone();
    let first_cycle = tokio::spawn(async move {
        let mut live = HashSet::new();
        gc_run_once_with_late_protection(&first_store, &mut live, || async move {
            mark_complete_tx
                .send(())
                .expect("test must wait for the mark barrier");
            release_rx.await.expect("test must release the first cycle");
            Ok::<_, &'static str>(HashSet::new())
        })
        .await
    });
    mark_complete_rx.await?;

    let mut concurrent_live = HashSet::new();
    let concurrent = gc_run_once(store, &mut concurrent_live).await;
    assert!(
        concurrent.is_err(),
        "a second active cycle must be rejected"
    );

    release_tx
        .send(())
        .expect("first cycle must still be waiting");
    assert!(matches!(first_cycle.await??, GcRunOutcome::Completed));

    let mut recovery_live = HashSet::new();
    gc_run_once(store, &mut recovery_live).await?;
    Ok(())
}

async fn assert_cancelled_gc_cycle_releases_its_protection_epoch(store: &Store) -> TestResult<()> {
    let (mark_complete_tx, mark_complete_rx) = oneshot::channel();
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let cancelled_store = store.clone();
    let cancelled_cycle = tokio::spawn(async move {
        let mut live = HashSet::new();
        gc_run_once_with_late_protection(&cancelled_store, &mut live, || async move {
            mark_complete_tx
                .send(())
                .expect("test must wait for the mark barrier");
            let _ = release_rx.await;
            Ok::<_, &'static str>(HashSet::new())
        })
        .await
    });
    mark_complete_rx.await?;
    cancelled_cycle.abort();
    let cancellation = cancelled_cycle
        .await
        .expect_err("aborted garbage-collection task must report cancellation");
    assert!(cancellation.is_cancelled());

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut live = HashSet::new();
            match gc_run_once(store, &mut live).await {
                Ok(()) => return Ok::<_, iroh_blobs::api::Error>(()),
                Err(error)
                    if error
                        .to_string()
                        .contains("another garbage-collection cycle is already active") =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    Ok(())
}

async fn assert_failed_protection_aborts_sweep(store: &Store) -> TestResult<()> {
    let tag = store
        .blobs()
        .add_slice(b"unreferenced but not safe to sweep")
        .temp_tag()
        .await?;
    let hash = tag.hash();
    drop(tag);

    let mut live = HashSet::new();
    let outcome = gc_run_once_with_late_protection(store, &mut live, || async {
        Err::<HashSet<Hash>, _>("protection source unavailable")
    })
    .await?;

    assert_eq!(
        outcome,
        GcRunOutcome::Aborted("protection source unavailable")
    );
    assert!(
        store.has(hash).await?,
        "an aborted cycle must not sweep blobs"
    );

    gc_run_once(store, &mut live).await?;
    assert!(
        !store.has(hash).await?,
        "the control cycle proves the blob was otherwise collectable"
    );
    Ok(())
}

async fn assert_named_root_created_after_mark_survives(store: &Store) -> TestResult<()> {
    let tag = store
        .blobs()
        .add_slice(b"named root created after mark")
        .temp_tag()
        .await?;
    let value = tag.hash_and_format();
    drop(tag);

    let mut live = HashSet::new();
    let outcome = gc_run_once_with_late_protection(store, &mut live, || async {
        store.tags().set("late-named-root", value).await?;
        Ok::<_, anyhow::Error>(HashSet::new())
    })
    .await?;

    assert!(matches!(outcome, GcRunOutcome::Completed));
    assert!(store.has(value.hash).await?);
    assert_eq!(
        store
            .tags()
            .get("late-named-root")
            .await?
            .expect("named root must exist")
            .hash,
        value.hash
    );
    Ok(())
}

async fn assert_temporary_root_created_after_mark_survives(store: &Store) -> TestResult<()> {
    let tag = store
        .blobs()
        .add_slice(b"temporary root created after mark")
        .temp_tag()
        .await?;
    let value = tag.hash_and_format();
    drop(tag);
    let held_tag = Arc::new(Mutex::new(None));
    let held_tag_during_sweep = held_tag.clone();

    let mut live = HashSet::new();
    let outcome = gc_run_once_with_late_protection(store, &mut live, || async {
        let tag = store.tags().temp_tag(value).await?;
        *held_tag_during_sweep
            .lock()
            .expect("temporary root holder mutex poisoned") = Some(tag);
        Ok::<_, anyhow::Error>(HashSet::new())
    })
    .await?;

    assert!(matches!(outcome, GcRunOutcome::Completed));
    assert!(store.has(value.hash).await?);
    drop(
        held_tag
            .lock()
            .expect("temporary root holder mutex poisoned")
            .take(),
    );
    Ok(())
}

async fn assert_malformed_hash_sequence_aborts_before_sweep(store: &Store) -> TestResult<()> {
    let malformed = store
        .blobs()
        .add_bytes_with_opts(AddBytesOptions {
            data: b"not a canonical hash sequence".as_slice().into(),
            format: BlobFormat::HashSeq,
        })
        .temp_tag()
        .await?;
    let malformed_value = malformed.hash_and_format();
    store
        .tags()
        .set("malformed-hash-sequence", malformed_value)
        .await?;
    drop(malformed);

    let collectable = store
        .blobs()
        .add_slice(b"must survive a failed mark")
        .temp_tag()
        .await?;
    let collectable_hash = collectable.hash();
    drop(collectable);

    let mut live = HashSet::new();
    let result = gc_run_once(store, &mut live).await;

    assert!(result.is_err(), "invalid root traversal must fail the mark");
    assert!(
        store.has(collectable_hash).await?,
        "a failed mark must prevent every sweep deletion"
    );

    store.tags().delete("malformed-hash-sequence").await?;
    let mut recovery_live = HashSet::new();
    gc_run_once(store, &mut recovery_live).await?;
    assert!(
        !store.has(collectable_hash).await?,
        "a failed mark must still release its protection cycle"
    );
    Ok(())
}
