use std::{
    error::Error,
    future::{poll_fn, Future},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use anyhow::Result;
use bytes::Bytes;
use n0_future::StreamExt;
use tokio::sync::oneshot;

use crate::{
    api::{blobs::AddProgressItem, Store},
    store::{
        fs::{options::Options, BlobAvailabilityProbeError, FsStore, ShutdownError},
        GcConfig, ProtectOutcome,
    },
    Hash,
};

const DEADLINE: Duration = Duration::from_secs(10);

struct SignalOnDrop(Option<oneshot::Sender<()>>);

impl Drop for SignalOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(
        poll_fn(|context| Poll::Ready(future.as_mut().poll(context)))
            .await
            .is_pending()
    );
}

fn shutdown_failure(error: &irpc::Error) -> &ShutdownError {
    let irpc::Error::OneshotRecv { source, .. } = error else {
        panic!("expected the retained shutdown error, got {error:?}")
    };
    let irpc::channel::oneshot::RecvError::Io { source, .. } = source else {
        panic!("expected the shutdown error carrier, got {source:?}")
    };
    source
        .get_ref()
        .and_then(|source| source.downcast_ref())
        .expect("the original shutdown failure must remain available")
}

fn panic_in_source_chain(mut error: &(dyn Error + 'static)) -> bool {
    loop {
        if let Some(join) = error.downcast_ref::<tokio::task::JoinError>() {
            return join.is_panic();
        }
        let Some(source) = error.source() else {
            return false;
        };
        error = source;
    }
}

#[tokio::test]
async fn availability_probe_failure_is_not_reported_as_broken_storage() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FsStore::load(directory.path()).await?;
    store.shutdown().await?;
    assert_eq!(
        store
            .availability(Hash::new(b"unavailable actor"))
            .await
            .expect_err("closed actor channel must fail the probe"),
        BlobAvailabilityProbeError::StoreUnavailable
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_releases_database_while_store_and_api_clones_remain() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FsStore::load(directory.path()).await?;
    let clone = store.clone();
    let client = Store::clone(&store);
    let tag = store
        .add_bytes(b"durable across store shutdown".as_slice())
        .await?;
    let hash = tag.hash;
    assert!(redb::Database::create(directory.path().join("blobs.db")).is_err());

    let (first, second) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(store.shutdown(), clone.shutdown())
    })
    .await?;
    first?;
    second?;
    assert!(client.add_bytes(b"closed".as_slice()).await.is_err());
    assert!(store.dump().await.is_err());
    let reopened = FsStore::load(directory.path()).await?;
    assert_eq!(
        reopened.get_bytes(hash).await?.as_ref(),
        b"durable across store shutdown"
    );
    store.shutdown().await?;
    clone.shutdown().await?;
    assert_eq!(
        reopened.get_bytes(hash).await?.as_ref(),
        b"durable across store shutdown"
    );
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_drains_active_import_before_closing_the_database() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FsStore::load(directory.path()).await?;
    let chunk = Bytes::from(vec![42; 128 * 1024]);
    let (chunks, mut receiver) = tokio::sync::mpsc::channel(4);
    chunks.send(Ok(chunk.clone())).await?;
    chunks.send(Ok(chunk.clone())).await?;
    let data = n0_future::stream::poll_fn(move |context| receiver.poll_recv(context));
    let mut progress = Box::pin(store.blobs().add_stream(data).await.stream().await);
    assert!(matches!(
        tokio::time::timeout(DEADLINE, progress.next()).await?,
        Some(AddProgressItem::CopyProgress(_))
    ));

    // The import is active, but cannot finish until its input is closed. Its
    // FinishImport and entity-persistence messages must outlive the API fence.
    let mut close = Box::pin(store.shutdown());
    assert_pending(close.as_mut()).await;
    assert!(store.dump().await.is_err());
    chunks.send(Ok(chunk)).await?;
    drop(chunks);
    let (closed, tag) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(close, async {
            while let Some(item) = progress.next().await {
                match item {
                    AddProgressItem::Done(tag) => return Ok(tag),
                    AddProgressItem::Error(error) => return Err(anyhow::Error::new(error)),
                    _ => {}
                }
            }
            anyhow::bail!("import ended without a durable completion")
        })
    })
    .await?;
    closed?;
    let tag = tag?;
    let reopened = FsStore::load(directory.path()).await?;
    assert_eq!(
        reopened.get_bytes(tag.hash()).await?.as_ref(),
        vec![42; 3 * 128 * 1024]
    );
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn converting_to_the_generic_store_keeps_the_actual_owner_alive() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FsStore::load(directory.path()).await?;
    let client: Store = store.into();
    let clone = client.clone();
    drop(client);
    let tag = clone.add_bytes(b"generic client".as_slice()).await?;
    let hash = tag.hash;
    tokio::time::timeout(DEADLINE, clone.shutdown()).await??;
    assert!(clone.add_bytes(b"after shutdown".as_slice()).await.is_err());
    let reopened = FsStore::load(directory.path()).await?;
    assert_eq!(reopened.get_bytes(hash).await?.as_ref(), b"generic client");
    drop(clone);
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_every_external_client_releases_gc_without_an_owner_cycle() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (dropped_tx, mut dropped_rx) = oneshot::channel();
    let capture = Arc::new(SignalOnDrop(Some(dropped_tx)));
    let mut options = Options::new(directory.path());
    options.gc = Some(GcConfig {
        interval: Duration::from_secs(3600),
        add_protected: Some(Arc::new(move |_| {
            let capture = capture.clone();
            Box::pin(async move {
                let _capture = capture;
                ProtectOutcome::Continue
            })
        })),
    });
    let store = FsStore::load_with_opts(directory.path().join("blobs.db"), options).await?;
    let clone = store.clone();
    let client = Store::clone(&store);
    drop(store);
    drop(clone);
    assert!(matches!(
        dropped_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(client);
    tokio::time::timeout(DEADLINE, dropped_rx).await??;
    Ok(())
}

#[tokio::test]
async fn cancelled_and_concurrent_shutdowns_keep_the_actual_runtime_join() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (worker_started_tx, worker_started_rx) = oneshot::channel();
    let (gc_dropped_tx, gc_dropped_rx) = oneshot::channel();
    let (worker_dropped_tx, mut worker_dropped_rx) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let callback = Mutex::new(Some((
        worker_started_tx,
        gc_dropped_tx,
        worker_dropped_tx,
        blocked,
    )));
    let mut options = Options::new(directory.path());
    options.gc = Some(GcConfig {
        interval: Duration::from_millis(1),
        add_protected: Some(Arc::new(move |_| {
            let (started, gc_dropped, worker_dropped, blocked) =
                callback.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _gc = SignalOnDrop(Some(gc_dropped));
                // Deliberately detach a real blocking worker from the GC future.
                // Only joining the dedicated runtime proves this work has ended.
                let _worker = tokio::task::spawn_blocking(move || {
                    let _worker = SignalOnDrop(Some(worker_dropped));
                    let _ = started.send(());
                    let _ = blocked.recv();
                });
                std::future::pending::<()>().await;
                ProtectOutcome::Continue
            })
        })),
    });
    let store = FsStore::load_with_opts(directory.path().join("blobs.db"), options).await?;
    tokio::time::timeout(DEADLINE, worker_started_rx).await??;
    let client = Store::clone(&store);
    let mut first = Box::pin(store.shutdown());
    assert_pending(first.as_mut()).await;
    tokio::time::timeout(DEADLINE, gc_dropped_rx).await??;
    assert!(client
        .add_bytes(b"rejected during close".as_slice())
        .await
        .is_err());
    assert!(store.dump().await.is_err());
    tokio::time::timeout(
        DEADLINE,
        poll_fn(|context| {
            assert!(
                first.as_mut().poll(context).is_pending(),
                "runtime worker is still blocked"
            );
            if store.shutdown_actors_finished() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await?;
    // Both actor handles are complete. This poll reaches the blocking runtime
    // drop job, which cannot finish until the worker is explicitly released.
    assert_pending(first.as_mut()).await;
    drop(first);
    let clone = store.clone();
    let mut second = Box::pin(store.shutdown());
    let mut third = Box::pin(clone.shutdown());
    assert_pending(second.as_mut()).await;
    assert_pending(third.as_mut()).await;
    drop(second);
    assert_pending(third.as_mut()).await;
    release.send(())?;
    tokio::time::timeout(DEADLINE, third).await??;
    worker_dropped_rx
        .try_recv()
        .expect("runtime join must follow blocking-worker drop");
    store.shutdown().await?;
    let reopened = FsStore::load(directory.path()).await?;
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn gc_panic_is_preserved_after_all_resources_are_joined() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (panicked_tx, panicked_rx) = oneshot::channel();
    let panicked = Mutex::new(Some(panicked_tx));
    let mut options = Options::new(directory.path());
    options.gc = Some(GcConfig {
        interval: Duration::from_millis(1),
        add_protected: Some(Arc::new(move |_| {
            let panicked = panicked.lock().unwrap().take().unwrap();
            Box::pin(async move {
                let _during_unwind = SignalOnDrop(Some(panicked));
                panic!("injected GC failure");
            })
        })),
    });
    let store = FsStore::load_with_opts(directory.path().join("blobs.db"), options).await?;
    tokio::time::timeout(DEADLINE, panicked_rx).await??;
    let first = tokio::time::timeout(DEADLINE, store.shutdown())
        .await?
        .unwrap_err();
    let failure = shutdown_failure(&first);
    assert!(failure
        .causes()
        .iter()
        .any(|error| panic_in_source_chain(error)));
    let second = store.shutdown().await.unwrap_err();
    assert_eq!(
        failure.causes().as_ptr(),
        shutdown_failure(&second).causes().as_ptr()
    );
    assert!(store.shutdown_actors_finished());
    let reopened = FsStore::load(directory.path()).await?;
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn failed_shutdown_rpc_still_joins_and_keeps_the_same_failure() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = FsStore::load(directory.path()).await?;
    let client = Store::clone(&store);
    tokio::time::timeout(DEADLINE, client.shutdown()).await??;
    let first = tokio::time::timeout(DEADLINE, store.shutdown())
        .await?
        .unwrap_err();
    let failure = shutdown_failure(&first);
    assert!(failure
        .causes()
        .iter()
        .any(|error| error.to_string().contains("shutdown request")));
    let second = store.shutdown().await.unwrap_err();
    assert_eq!(
        failure.causes().as_ptr(),
        shutdown_failure(&second).causes().as_ptr()
    );
    assert!(store.shutdown_actors_finished());
    let reopened = FsStore::load(directory.path()).await?;
    reopened.shutdown().await?;
    Ok(())
}
