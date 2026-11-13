use std::{collections::HashSet, pin::Pin, sync::Arc};

use bao_tree::ChunkRanges;
use genawaiter::sync::{Co, Gen};
use n0_future::{time::Duration, Stream, StreamExt};
use tokio::sync::mpsc::WeakSender;
use tracing::{debug, error, info, warn};

use crate::{
    api::{proto::Command, Store},
    Hash, HashAndFormat,
};

/// An event related to GC
#[derive(Debug)]
pub enum GcMarkEvent {
    /// A custom event (info)
    CustomDebug(String),
    /// A custom non critical error
    CustomWarning(String, Option<crate::api::Error>),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// An event related to GC
#[derive(Debug)]
pub enum GcSweepEvent {
    /// A custom event (debug)
    CustomDebug(String),
    /// A custom non critical error
    #[allow(dead_code)]
    CustomWarning(String, Option<crate::api::Error>),
    /// An unrecoverable error during GC
    Error(crate::api::Error),
}

/// Compute the set of live hashes
pub(super) async fn gc_mark_task(
    store: &Store,
    live: &mut HashSet<Hash>,
    co: &Co<GcMarkEvent>,
) -> crate::api::Result<()> {
    macro_rules! trace {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomDebug(format!($($arg)*))).await;
        };
    }
    macro_rules! warn {
        ($($arg:tt)*) => {
            co.yield_(GcMarkEvent::CustomWarning(format!($($arg)*), None)).await;
        };
    }
    // Every root is live, and HashSeq roots also have their children traversed.
    // Traversal does not depend on `live`, which may already contain a root
    // because of a Raw tag on the same hash or the protect callback.
    let mut seqs = HashSet::new();
    let mut add_root = |HashAndFormat { hash, format }: HashAndFormat| {
        live.insert(hash);
        if !format.is_raw() {
            seqs.insert(hash);
        }
    };
    // Read named roots first, then current AND retained temp roots. A root
    // continuously protected during a handover is either in the named
    // snapshot or had a temp tag before that snapshot, which the second
    // listing retains even if it has since been dropped. Repeated handovers
    // are safe too. These roots must be traversed: protecting only a HashSeq
    // root at delete time would still allow its children to be swept.
    trace!("traversing tags");
    let mut tags = store.tags().list().await?;
    while let Some(tag) = tags.next().await {
        let info = tag?;
        trace!("adding root {:?} {:?}", info.name, info.hash_and_format());
        add_root(info.hash_and_format());
    }
    trace!("traversing temp roots");
    let mut tts = store.tags().list_temp_tags_for_gc().await?;
    while let Some(tt) = tts.next().await {
        let tt = tt?;
        trace!("adding temp root {:?}", tt);
        add_root(tt);
    }
    for hash in seqs {
        let mut stream = store.export_bao(hash, ChunkRanges::all()).hashes();
        while let Some(hash) = stream.next().await {
            match hash {
                Ok(hash) => {
                    live.insert(hash);
                }
                Err(e) => {
                    warn!("error while traversing hashseq: {e:?}");
                }
            }
        }
    }
    trace!("gc mark done. found {} live blobs", live.len());
    Ok(())
}

async fn gc_sweep_task(
    store: &Store,
    candidates: &[Hash],
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> crate::api::Result<()> {
    let mut count = 0;
    let mut batch = Vec::new();
    for &hash in candidates {
        if !live.contains(&hash) {
            batch.push(hash);
            count += 1;
        }
        if batch.len() >= 100 {
            store.blobs().delete(batch.clone()).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.blobs().delete(batch).await?;
    }
    store.sync_db().await?;
    co.yield_(GcSweepEvent::CustomDebug(format!(
        "requested deletion of {count} blobs"
    )))
    .await;
    Ok(())
}

fn gc_mark<'a>(
    store: &'a Store,
    live: &'a mut HashSet<Hash>,
) -> impl Stream<Item = GcMarkEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_mark_task(store, live, &co).await {
            co.yield_(GcMarkEvent::Error(e)).await;
        }
    })
}

fn gc_sweep<'a>(
    store: &'a Store,
    candidates: &'a [Hash],
    live: &'a HashSet<Hash>,
) -> impl Stream<Item = GcSweepEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_sweep_task(store, candidates, live, &co).await {
            co.yield_(GcSweepEvent::Error(e)).await;
        }
    })
}

/// Configuration for garbage collection.
///
/// To protect blobs during long-running writes without pausing the GC
/// schedule, use [`crate::api::blobs::Batch::temp_tag`].
#[derive(derive_more::Debug, Clone)]
pub struct GcConfig {
    /// Interval in which to run garbage collection.
    pub interval: Duration,
    /// Optional callback to manually add protected blobs.
    ///
    /// The callback is called after marking, before sweeping. It gets a `&mut HashSet<Hash>`
    /// and returns a future that returns [`ProtectOutcome`]. All hashes that are added to the
    /// [`HashSet`] will be protected from garbage collection during this run.
    ///
    /// In normal operation, return [`ProtectOutcome::Continue`] from the callback. If you return
    /// [`ProtectOutcome::Abort`], the garbage collection run will be aborted.Use this if your
    /// source of hashes to protect returned an error, and thus garbage collection should be skipped
    /// completely to not unintentionally delete blobs that should be protected.
    #[debug("ProtectCallback")]
    pub add_protected: Option<ProtectCb>,
}

/// Returned from [`ProtectCb`].
///
/// See [`GcConfig::add_protected] for details.
#[derive(Debug)]
pub enum ProtectOutcome {
    /// Continue with the garbage collection run.
    Continue,
    /// Abort the garbage collection run.
    Abort,
}

/// The type of the garbage collection callback.
///
/// See [`GcConfig::add_protected] for details.
pub type ProtectCb = Arc<
    dyn for<'a> Fn(
            &'a mut HashSet<Hash>,
        )
            -> Pin<Box<dyn std::future::Future<Output = ProtectOutcome> + Send + Sync + 'a>>
        + Send
        + Sync
        + 'static,
>;

/// Runs a single mark-and-sweep garbage collection cycle.
///
/// Candidates are collected before marking. Blobs absent from that listing
/// cannot be swept this round, including children downloaded after their
/// protected, incomplete HashSeq root was traversed. Available HashSeq chunks
/// are still marked, so incomplete roots do not prevent collection.
///
/// The optional `protect` callback ([`GcConfig::add_protected`]) is invoked after
/// marking, immediately before sweep, to add externally protected hashes to `live`.
/// Existing candidates still require protection: the candidate snapshot does not
/// protect a reused blob whose reference is only revealed after marking.
pub async fn gc_run_once(
    store: &Store,
    live: &mut HashSet<Hash>,
    protect: Option<&ProtectCb>,
) -> crate::api::Result<()> {
    debug!(externally_protected = live.len(), "gc: start");
    // Until this is dropped at the end of the sweep, the store keeps content
    // that has or had a temp tag during the run, see `GcRunRequest`.
    let _run = store.gc_run().await?;
    // Both fs and mem list from a snapshot. Finish collecting it before any
    // root traversal, and abort on a listing error rather than sweeping a
    // partial result. Do not list again during sweep: a growing HashSeq can
    // reveal children that did not exist when we marked it.
    let candidates = store.blobs().list().hashes().await?;
    {
        let mut stream = gc_mark(store, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcMarkEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcMarkEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcMarkEvent::Error(err) => {
                    error!("error during gc mark: {:?}", err);
                    return Err(err);
                }
            }
        }
    }
    // Capture the external protect set after the mark phase, just before the sweep.
    if let Some(protect) = protect {
        match (protect)(live).await {
            ProtectOutcome::Continue => {}
            ProtectOutcome::Abort => {
                info!("abort gc run: protect callback indicated abort");
                return Ok(());
            }
        }
    }
    debug!(total_protected = live.len(), "gc: sweep");
    {
        let mut stream = gc_sweep(store, &candidates, live);
        while let Some(ev) = stream.next().await {
            match ev {
                GcSweepEvent::CustomDebug(msg) => {
                    debug!("{}", msg);
                }
                GcSweepEvent::CustomWarning(msg, err) => {
                    warn!("{}: {:?}", msg, err);
                }
                GcSweepEvent::Error(err) => {
                    error!("error during gc sweep: {:?}", err);
                    return Err(err);
                }
            }
        }
    }
    debug!("gc: done");

    Ok(())
}

/// Runs garbage collection in a loop until the store is gone.
///
/// This holds only a weak sender, so it does not keep the store alive. Once all
/// handles to the store are dropped, the loop ends.
pub async fn run_gc(store: WeakSender<Command>, config: GcConfig) {
    debug!("gc enabled with interval {:?}", config.interval);
    let mut live = HashSet::new();
    loop {
        live.clear();
        n0_future::time::sleep(config.interval).await;
        let Some(sender) = store.upgrade() else {
            debug!("gc: store dropped, stopping");
            break;
        };
        let store = Store::from_client(sender);
        if let Err(e) = gc_run_once(&store, &mut live, config.add_protected.as_ref()).await {
            error!("error during gc run: {e}");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self};

    use bao_tree::io::EncodeError;
    use n0_tracing_test::traced_test;
    use range_collections::RangeSet2;
    use testresult::TestResult;

    use super::*;
    use crate::{
        api::{blobs::AddBytesOptions, ExportBaoError, RequestError, Store},
        hashseq::HashSeq,
        BlobFormat,
    };

    async fn gc_smoke(store: &Store) -> TestResult<()> {
        let blobs = store.blobs();
        let at = blobs.add_slice("a").temp_tag().await?;
        let bt = blobs.add_slice("b").temp_tag().await?;
        let ct = blobs.add_slice("c").temp_tag().await?;
        let dt = blobs.add_slice("d").temp_tag().await?;
        let et = blobs.add_slice("e").temp_tag().await?;
        let ft = blobs.add_slice("f").temp_tag().await?;
        let gt = blobs.add_slice("g").temp_tag().await?;
        let ht = blobs.add_slice("h").with_named_tag("h").await?;
        let a = at.hash();
        let b = bt.hash();
        let c = ct.hash();
        let d = dt.hash();
        let e = et.hash();
        let f = ft.hash();
        let g = gt.hash();
        let h = ht.hash;
        store.tags().set("c", ct.hash_and_format()).await?;
        let dehs = [d, e].into_iter().collect::<HashSeq>();
        let hehs = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: dehs.into(),
                format: BlobFormat::HashSeq,
            })
            .await?;
        let fghs = [f, g].into_iter().collect::<HashSeq>();
        let fghs = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: fghs.into(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        store.tags().set("fg", fghs.hash_and_format()).await?;
        drop(fghs);
        drop(bt);
        store.tags().delete("h").await?;
        let mut live = HashSet::new();
        gc_run_once(store, &mut live, None).await?;
        // a is protected because we keep the temp tag
        assert!(live.contains(&a));
        assert!(store.has(a).await?);
        // b is not protected because we drop the temp tag
        assert!(!live.contains(&b));
        assert!(!store.has(b).await?);
        // c is protected because we set an explicit tag
        assert!(live.contains(&c));
        assert!(store.has(c).await?);
        // d and e are protected because they are part of a hashseq protected by a temp tag
        assert!(live.contains(&d));
        assert!(store.has(d).await?);
        assert!(live.contains(&e));
        assert!(store.has(e).await?);
        // f and g are protected because they are part of a hashseq protected by a tag
        assert!(live.contains(&f));
        assert!(store.has(f).await?);
        assert!(live.contains(&g));
        assert!(store.has(g).await?);
        // h is not protected because we deleted the tag before gc ran
        assert!(!live.contains(&h));
        assert!(!store.has(h).await?);
        drop(at);
        drop(hehs);
        Ok(())
    }

    /// A HashSeq root that also carries a Raw tag must still have its children traversed.
    /// `roots` iteration order is random, so run GC repeatedly.
    async fn gc_dual_tagged_root_keeps_children(store: &Store) -> TestResult<()> {
        let blobs = store.blobs();
        let x = blobs.add_slice("child-x").temp_tag().await?;
        let y = blobs.add_slice("child-y").temp_tag().await?;
        let (xh, yh) = (x.hash(), y.hash());
        let xyhs = [xh, yh].into_iter().collect::<HashSeq>();
        let root = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: xyhs.into(),
                format: BlobFormat::HashSeq,
            })
            .await?;
        let rh = root.hash;
        store.tags().set("raw", HashAndFormat::raw(rh)).await?;
        store
            .tags()
            .set("hashseq", HashAndFormat::hash_seq(rh))
            .await?;
        drop(x);
        drop(y);
        for round in 0..20 {
            let mut live = HashSet::new();
            gc_run_once(store, &mut live, None).await?;
            assert!(
                live.contains(&xh) && live.contains(&yh),
                "round {round}: children not marked live"
            );
            assert!(
                store.has(xh).await? && store.has(yh).await?,
                "round {round}: a child was swept"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn gc_dual_tagged_root_keeps_children_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        gc_dual_tagged_root_keeps_children(&store).await?;
        Ok(())
    }

    /// A HashSeq root that is already live before the mark, as after the
    /// protect callback, must still have its children traversed.
    async fn gc_protected_root_keeps_children(store: &Store) -> TestResult<()> {
        let blobs = store.blobs();
        let x = blobs.add_slice("child-x").temp_tag().await?;
        let y = blobs.add_slice("child-y").temp_tag().await?;
        let (xh, yh) = (x.hash(), y.hash());
        let xyhs = [xh, yh].into_iter().collect::<HashSeq>();
        let root = blobs
            .add_bytes_with_opts(AddBytesOptions {
                data: xyhs.into(),
                format: BlobFormat::HashSeq,
            })
            .await?;
        let rh = root.hash;
        store
            .tags()
            .set("hashseq", HashAndFormat::hash_seq(rh))
            .await?;
        drop(x);
        drop(y);
        let mut live = HashSet::from([rh]);
        gc_run_once(store, &mut live, None).await?;
        assert!(
            live.contains(&xh) && live.contains(&yh),
            "children not marked live"
        );
        assert!(
            store.has(xh).await? && store.has(yh).await?,
            "a child was swept"
        );
        Ok(())
    }

    #[tokio::test]
    async fn gc_protected_root_keeps_children_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        gc_protected_root_keeps_children(&store).await?;
        Ok(())
    }

    /// New blobs are deferred to the next candidate snapshot, even without a
    /// tag. Existing candidates still need current or retained temp protection.
    async fn gc_import_after_mark_survives(store: &Store) -> TestResult<()> {
        use bao_tree::ChunkNum;

        use crate::{api::blobs::BlobStatus, store::util::tests::create_n0_bao};
        let kept = store.add_slice(vec![0u8; 100000]).temp_tag().await?;
        let dropped = store.add_slice(vec![2u8; 100000]).temp_tag().await?;
        let dropped_hash = dropped.hash();
        let old = store.add_slice("old untagged blob").temp_tag().await?;
        let old_hash = old.hash();
        drop(old);
        store.sync_db().await?;
        let run = store.gc_run().await?;
        let candidates = store.blobs().list().hashes().await?;
        assert!(candidates.contains(&kept.hash()));
        assert!(candidates.contains(&dropped_hash));
        assert!(candidates.contains(&old_hash));
        // Model a mark that missed these roots. Temp-tag retention protects
        // candidates even when a tag is dropped before sweep.
        let live = HashSet::new();
        drop(dropped);
        // Written after candidate collection, without any tag.
        let data = vec![1u8; 100000];
        let ranges = ChunkRanges::from(..ChunkNum(16));
        let (untagged, bao) = create_n0_bao(&data, &ranges)?;
        store.import_bao_bytes(untagged, ranges, bao).await?;
        let mut stream = gc_sweep(store, &candidates, &live);
        while let Some(ev) = stream.next().await {
            if let GcSweepEvent::Error(err) = ev {
                return Err(err.into());
            }
        }
        assert!(store.has(kept.hash()).await?, "a temp tag protects");
        assert!(
            store.has(dropped_hash).await?,
            "a dropped tag protects this round"
        );
        assert!(
            !store.has(old_hash).await?,
            "old unprotected candidates are swept"
        );
        assert!(matches!(
            store.status(untagged).await?,
            BlobStatus::Partial { .. }
        ));
        drop(stream);
        drop(run);

        // Deferral lasts only this round; there is no retained write-protection set.
        gc_run_once(store, &mut HashSet::new(), None).await?;
        assert_eq!(store.status(untagged).await?, BlobStatus::NotFound);
        assert!(store.has(kept.hash()).await?);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn gc_import_after_mark_survives_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        gc_import_after_mark_survives(&store).await
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    #[traced_test]
    async fn gc_import_after_mark_survives_fs() -> TestResult {
        let testdir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(testdir.path().join("db")).await?;
        gc_import_after_mark_survives(&store).await
    }

    /// A protected HashSeq gains another chunk and a newly downloaded child
    /// after marking. The final chunk never arrives: GC must still collect
    /// unrelated old garbage, without deleting the new child.
    async fn gc_growing_hashseq_keeps_new_child(store: &Store) -> TestResult {
        use bao_tree::ChunkNum;
        use tokio::sync::{mpsc, oneshot};

        use crate::store::util::tests::create_n0_bao;

        let first = store.add_slice("first child").temp_tag().await?;
        let first_hash = first.hash();
        drop(first);
        let garbage = store.add_slice("old garbage").temp_tag().await?;
        let garbage_hash = garbage.hash();
        drop(garbage);
        let child_data = vec![42u8; 100_000];
        let child_hash = Hash::new(&child_data);
        // Each 16 KiB block contains 512 child hashes.
        let root_data: bytes::Bytes = std::iter::repeat_n(first_hash, 512)
            .chain(std::iter::repeat_n(child_hash, 512))
            .chain(std::iter::repeat_n(Hash::new("never downloaded"), 512))
            .collect::<HashSeq>()
            .into();
        let first_range = ChunkRanges::from(..ChunkNum(16));
        let (root, first_bao) = create_n0_bao(&root_data, &first_range)?;
        let _root_tag = store.tags().temp_tag(HashAndFormat::hash_seq(root)).await?;
        store.import_bao_bytes(root, first_range, first_bao).await?;
        store.sync_db().await?;

        let (trigger_tx, mut trigger_rx) = mpsc::channel::<oneshot::Sender<()>>(1);
        let writer_store = store.clone();
        let writer = tokio::spawn(async move {
            let ack = trigger_rx.recv().await.expect("GC callback must run");
            let next_range = ChunkRanges::from(ChunkNum(16)..ChunkNum(32));
            let (_, next_bao) = create_n0_bao(&root_data, &next_range)?;
            writer_store
                .import_bao_bytes(root, next_range, next_bao)
                .await?;
            // Match a network download: no import temp tag for this child.
            let all = ChunkRanges::all();
            let (_, child_bao) = create_n0_bao(&child_data, &all)?;
            writer_store
                .import_bao_bytes(child_hash, all, child_bao)
                .await?;
            writer_store.sync_db().await?;
            ack.send(()).unwrap();
            TestResult::Ok(())
        });
        let cb: ProtectCb = Arc::new(move |_| {
            let trigger_tx = trigger_tx.clone();
            Box::pin(async move {
                let (tx, rx) = oneshot::channel();
                trigger_tx.send(tx).await.unwrap();
                rx.await.unwrap();
                ProtectOutcome::Continue
            })
        });
        let mut live = HashSet::new();
        gc_run_once(store, &mut live, Some(&cb)).await?;
        writer.await??;
        assert!(live.contains(&first_hash));
        assert!(
            !live.contains(&child_hash),
            "the child arrived after marking"
        );
        assert_eq!(
            store.get_bytes(child_hash).await?.as_ref(),
            vec![42u8; 100_000]
        );
        assert!(
            !store.has(garbage_hash).await?,
            "incomplete roots must not stop GC"
        );

        // The next mark sees the new child, although the root remains incomplete.
        let garbage = store.add_slice("more old garbage").temp_tag().await?;
        let garbage_hash = garbage.hash();
        drop(garbage);
        store.sync_db().await?;
        live.clear();
        gc_run_once(store, &mut live, None).await?;
        assert!(live.contains(&child_hash));
        assert!(store.has(child_hash).await?);
        assert!(!store.has(garbage_hash).await?);
        Ok(())
    }

    #[tokio::test]
    async fn gc_growing_hashseq_keeps_new_child_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        gc_growing_hashseq_keeps_new_child(&store).await
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_growing_hashseq_keeps_new_child_fs() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(dir.path()).await?;
        gc_growing_hashseq_keeps_new_child(&store).await?;
        store.shutdown().await?;
        Ok(())
    }

    /// Returns a store that forwards every request to `inner`, and runs
    /// `handover` once the first tag listing of a gc run, named or temp, has
    /// gone to `inner`. The handover thus lands between the two listings.
    ///
    /// `handover` must make a request to `inner` before it changes any temp
    /// tag, so that the listing is answered first.
    fn store_with_handover<F>(inner: &Store, handover: F) -> Store
    where
        F: FnOnce() -> n0_future::boxed::BoxFuture<()> + Send + 'static,
    {
        use crate::api::proto::Command;
        let inner = inner.client().as_local().expect("a local store");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(32);
        tokio::spawn(async move {
            let mut handover = Some(handover);
            while let Some(cmd) = rx.recv().await {
                let listing = matches!(cmd, Command::ListTags(_) | Command::ListTempTags(_));
                if inner.send_raw(cmd).await.is_err() {
                    break;
                }
                if let Some(handover) = handover.take_if(|_| listing) {
                    handover().await;
                }
            }
        });
        Store::from_client(tx)
    }

    #[tokio::test]
    async fn gc_aborts_on_incomplete_candidate_listing() -> TestResult {
        use crate::api::proto::{Command, ListBlobsItem};

        for close_early in [false, true] {
            let store = crate::store::mem::MemStore::new();
            let tag = store
                .add_slice("must survive a failed snapshot")
                .temp_tag()
                .await?;
            let hash = tag.hash();
            drop(tag);
            let inner = store.client().as_local().expect("a local store");
            let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(32);
            let responder = tokio::spawn(async move {
                while let Some(cmd) = rx.recv().await {
                    if let Command::ListBlobs(cmd) = cmd {
                        cmd.tx.send(ListBlobsItem::Hash(hash)).await.unwrap();
                        if !close_early {
                            cmd.tx
                                .send(ListBlobsItem::Error(crate::api::Error::io(
                                    io::ErrorKind::Other,
                                    "candidate snapshot failed",
                                )))
                                .await
                                .unwrap();
                        }
                        // No Done: GC must not sweep the partial candidate list.
                    } else if inner.send_raw(cmd).await.is_err() {
                        break;
                    }
                }
            });
            let proxy = Store::from_client(tx);
            assert!(gc_run_once(&proxy, &mut HashSet::new(), None)
                .await
                .is_err());
            assert!(
                store.has(hash).await?,
                "GC swept after a failed candidate listing"
            );
            drop(proxy);
            responder.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn gc_aborts_on_temp_tag_listing_error() -> TestResult {
        gc_aborts_on_incomplete_temp_tag_listing(false).await
    }

    #[tokio::test]
    async fn gc_aborts_on_temp_tag_listing_closed_early() -> TestResult {
        gc_aborts_on_incomplete_temp_tag_listing(true).await
    }

    async fn gc_aborts_on_incomplete_temp_tag_listing(close_early: bool) -> TestResult {
        use crate::api::proto::Command;

        let store = crate::store::mem::MemStore::new();
        let tag = store
            .add_slice("must survive a failed mark")
            .temp_tag()
            .await?;
        let hash = tag.hash();
        drop(tag);
        let inner = store.client().as_local().expect("a local store");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(32);
        let responder = tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let Command::ListTempTags(cmd) = cmd {
                    if close_early {
                        continue;
                    }
                    cmd.tx
                        .send(crate::api::proto::ListTempTagsItem::Error(
                            crate::api::Error::io(
                                io::ErrorKind::Unsupported,
                                "temp tag listing unsupported",
                            ),
                        ))
                        .await
                        .unwrap();
                } else if inner.send_raw(cmd).await.is_err() {
                    break;
                }
            }
        });
        let proxy = Store::from_client(tx);
        assert!(gc_run_once(&proxy, &mut HashSet::new(), None)
            .await
            .is_err());
        assert!(store.has(hash).await?, "GC swept after a failed mark");
        drop(proxy);
        responder.await?;
        Ok(())
    }

    /// A blob whose temp tag is replaced by a named tag during the mark, as
    /// `with_named_tag` does, survives the run (#247).
    #[tokio::test]
    #[traced_test]
    async fn gc_temp_to_named_handover() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        let tt = store.add_slice("handover").temp_tag().await?;
        let hash = tt.hash();
        let inner = (*store).clone();
        let proxy = store_with_handover(&store, move || {
            Box::pin(async move {
                inner
                    .tags()
                    .set("named", tt.hash_and_format())
                    .await
                    .unwrap();
                drop(tt);
            })
        });
        gc_run_once(&proxy, &mut HashSet::new(), None).await?;
        assert!(store.has(hash).await?, "the handed over blob was swept");
        Ok(())
    }

    /// A blob whose named tag is replaced by a temp tag during the mark
    /// survives the run.
    #[tokio::test]
    #[traced_test]
    async fn gc_named_to_temp_handover() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        let tt = store.add_slice("handover").temp_tag().await?;
        let content = tt.hash_and_format();
        store.tags().set("named", content).await?;
        drop(tt);
        // The handover lands between the two root listings.
        let inner = (*store).clone();
        let held = std::sync::Arc::new(std::sync::Mutex::new(None));
        let hold = held.clone();
        let proxy = store_with_handover(&store, move || {
            Box::pin(async move {
                let tt = inner.tags().temp_tag(content).await.unwrap();
                assert_eq!(inner.tags().delete("named").await.unwrap(), 1);
                *hold.lock().unwrap() = Some(tt);
            })
        });
        gc_run_once(&proxy, &mut HashSet::new(), None).await?;
        assert!(held.lock().unwrap().is_some(), "the handover ran");
        assert!(
            store.has(content.hash).await?,
            "the handed over blob was swept"
        );
        Ok(())
    }

    /// Exercise both directions, including repeated moves between snapshots.
    async fn gc_hashseq_handover(store: &Store, initially_named: bool) -> TestResult {
        let child = store.add_slice("handover child").temp_tag().await?;
        let child_hash = child.hash();
        let root = store
            .add_bytes_with_opts(AddBytesOptions {
                data: [child_hash].into_iter().collect::<HashSeq>().into(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        let content = root.hash_and_format();
        let mut root = Some(root);
        if initially_named {
            store.tags().set("handover", content).await?;
            drop(root.take());
        }
        drop(child);
        let garbage = store.add_slice("unrelated garbage").temp_tag().await?;
        let garbage_hash = garbage.hash();
        drop(garbage);

        let inner = store.clone();
        let held = Arc::new(std::sync::Mutex::new(None));
        let hold = held.clone();
        let proxy = store_with_handover(store, move || {
            Box::pin(async move {
                for _ in 0..3 {
                    if initially_named {
                        let tt = inner.tags().temp_tag(content).await.unwrap();
                        inner.tags().delete("handover").await.unwrap();
                        inner.tags().set("handover", content).await.unwrap();
                        drop(tt);
                    } else {
                        inner.tags().set("handover", content).await.unwrap();
                        drop(root.take());
                        root = Some(inner.tags().temp_tag(content).await.unwrap());
                        inner.tags().delete("handover").await.unwrap();
                    }
                }
                if initially_named {
                    let tt = inner.tags().temp_tag(content).await.unwrap();
                    inner.tags().delete("handover").await.unwrap();
                    *hold.lock().unwrap() = Some(tt);
                } else {
                    inner.tags().set("handover", content).await.unwrap();
                    drop(root);
                }
            })
        });
        gc_run_once(&proxy, &mut HashSet::new(), None).await?;
        assert!(store.has(content.hash).await?, "root was swept");
        assert!(store.has(child_hash).await?, "HashSeq child was swept");
        assert!(
            !store.has(garbage_hash).await?,
            "unrelated garbage must be collected"
        );
        if !initially_named {
            assert!(
                store.tags().list_temp_tags().await?.next().await.is_none(),
                "ordinary listings must not expose retained roots"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn gc_hashseq_handover_mem() -> TestResult {
        for initially_named in [false, true] {
            let store = crate::store::mem::MemStore::new();
            gc_hashseq_handover(&store, initially_named).await?;
        }
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_hashseq_handover_fs() -> TestResult {
        for initially_named in [false, true] {
            let dir = tempfile::tempdir()?;
            let store = crate::store::fs::FsStore::load(dir.path()).await?;
            gc_hashseq_handover(&store, initially_named).await?;
            store.shutdown().await?;
        }
        Ok(())
    }

    #[cfg(feature = "fs-store")]
    async fn gc_file_delete(path: &std::path::Path, store: &Store) -> TestResult<()> {
        use bao_tree::ChunkNum;

        use crate::store::{fs::options::PathOptions, util::tests::create_n0_bao};
        let mut live = HashSet::new();
        let options = PathOptions::new(&path.join("db"));
        // create a large complete file and check that the data and outboard files are deleted by gc
        {
            let a = store
                .blobs()
                .add_slice(vec![0u8; 8000000])
                .temp_tag()
                .await?;
            let ah = a.hash();
            let data_path = options.data_path(&ah);
            let outboard_path = options.outboard_path(&ah);
            assert!(data_path.exists());
            assert!(outboard_path.exists());
            assert!(store.has(ah).await?);
            drop(a);
            gc_run_once(store, &mut live, None).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
        }
        live.clear();
        // create a large partial file and check that the data and outboard file as well as
        // the sizes and bitfield files are deleted by gc
        {
            let data = vec![1u8; 8000000];
            let ranges = ChunkRanges::from(..ChunkNum(19));
            let (bh, b_bao) = create_n0_bao(&data, &ranges)?;
            store.import_bao_bytes(bh, ranges, b_bao).await?;
            let data_path = options.data_path(&bh);
            let outboard_path = options.outboard_path(&bh);
            let sizes_path = options.sizes_path(&bh);
            let bitfield_path = options.bitfield_path(&bh);
            store.wait_idle().await?;
            assert!(data_path.exists());
            assert!(outboard_path.exists());
            assert!(sizes_path.exists());
            assert!(bitfield_path.exists());
            gc_run_once(store, &mut live, None).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
            assert!(!sizes_path.exists());
            assert!(!bitfield_path.exists());
        }
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    #[traced_test]
    async fn gc_smoke_fs() -> TestResult {
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_smoke(&store).await?;
        gc_file_delete(testdir.path(), &store).await?;
        Ok(())
    }

    fn long_interval_gc() -> GcConfig {
        GcConfig {
            interval: Duration::from_secs(3600),
            add_protected: None,
        }
    }

    /// GC must not keep a mem store alive once all its handles are dropped.
    #[tokio::test]
    #[traced_test]
    async fn gc_does_not_keep_mem_store_alive() -> TestResult {
        let store = crate::store::mem::MemStore::new_with_opts(crate::store::mem::Options {
            gc_config: Some(long_interval_gc()),
        });
        let hash = store.add_slice(b"data").await?.hash;
        let mut updates = store.observe(hash).stream().await?;
        updates.next().await;
        drop(store);
        // the store's actor stops, which ends the observe stream
        let next = tokio::time::timeout(Duration::from_secs(5), updates.next()).await?;
        assert!(next.is_none());
        Ok(())
    }

    /// GC must not keep an fs store alive once all its handles are dropped.
    #[tokio::test]
    #[cfg(feature = "fs-store")]
    #[traced_test]
    async fn gc_does_not_keep_fs_store_alive() -> TestResult {
        use crate::store::fs::{options::Options, FsStore};
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options {
            gc: Some(long_interval_gc()),
            ..Options::new(&db_dir)
        };
        let db_path = db_dir.join("blobs.db");
        let store = FsStore::load_with_opts(db_path.clone(), options.clone()).await?;
        drop(store);
        // the store releases its database once it is gone, so it can be opened again
        for _ in 0..50 {
            if let Ok(store) = FsStore::load_with_opts(db_path.clone(), options.clone()).await {
                store.shutdown().await?;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the store is still open");
    }

    #[tokio::test]
    #[traced_test]
    async fn gc_smoke_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        gc_smoke(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    #[traced_test]
    async fn gc_check_deletion_fs() -> TestResult {
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_check_deletion(&store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn gc_check_deletion_mem() -> TestResult {
        let store = crate::store::mem::MemStore::default();
        gc_check_deletion(&store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn gc_protect_race_mem() -> TestResult {
        let store = crate::store::mem::MemStore::default();
        gc_protect_race(&store).await
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    #[traced_test]
    async fn gc_protect_race_fs() -> TestResult {
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_protect_race(&store).await
    }

    /// A blob whose reference is committed during a GC run is not swept.
    ///
    /// Models an iroh-docs-style consumer: committed references are reported via the
    /// `add_protected` callback, and an import temp tag is held across each commit. The callback
    /// imports a blob and drops its temp tag without reporting it, modeling a reference committed
    /// just after the protect snapshot. The blob must survive: the gc run tracks its
    /// dropped temp tag until the sweep finishes.
    async fn gc_protect_race(store: &Store) -> TestResult {
        use tokio::sync::{mpsc, oneshot};

        // A committed reference, protected only via the callback; its import temp tag is dropped.
        let existing = {
            let tt = store.blobs().add_slice("existing").temp_tag().await?;
            let hash = tt.hash();
            drop(tt);
            hash
        };

        // The racy import runs in a separate task: a `ProtectCb` future must be `Sync`, but the
        // import future is not (the real iroh-docs callback only awaits channels for this reason).
        let (trigger_tx, mut trigger_rx) = mpsc::channel::<oneshot::Sender<Hash>>(1);
        let store_writer = store.clone();
        let writer = tokio::spawn(async move {
            while let Some(ack) = trigger_rx.recv().await {
                let tt = store_writer
                    .blobs()
                    .add_slice("racy")
                    .temp_tag()
                    .await
                    .expect("import failed");
                let hash = tt.hash();
                drop(tt); // temp tag dropped after the (modeled) commit
                let _ = ack.send(hash);
            }
        });

        let cb: ProtectCb = Arc::new(move |live: &mut HashSet<Hash>| {
            let trigger_tx = trigger_tx.clone();
            Box::pin(async move {
                // Snapshot of committed references at callback time.
                live.insert(existing);
                // Trigger the racy write and wait for it to complete. Its hash is intentionally
                // NOT added to `live`, modeling a reference committed just after this snapshot.
                let (ack_tx, ack_rx) = oneshot::channel();
                trigger_tx.send(ack_tx).await.expect("writer gone");
                ack_rx.await.expect("writer gone");
                ProtectOutcome::Continue
            })
        });

        let mut live = HashSet::new();
        gc_run_once(store, &mut live, Some(&cb)).await?;

        // The racy blob is content-addressed, so its hash is deterministic.
        let racy = Hash::new("racy");
        assert!(
            store.has(existing).await?,
            "committed reference reported by the callback must survive"
        );
        assert!(
            store.has(racy).await?,
            "blob imported during the GC run must survive through its retained temp tag"
        );
        drop(writer);
        Ok(())
    }

    async fn gc_check_deletion(store: &Store) -> TestResult {
        let temp_tag = store.add_bytes(b"foo".to_vec()).temp_tag().await?;
        let hash = temp_tag.hash();
        assert_eq!(store.get_bytes(hash).await?.as_ref(), b"foo");
        drop(temp_tag);
        let mut live = HashSet::new();
        gc_run_once(store, &mut live, None).await?;

        // check that `get_bytes` returns an error.
        let res = store.get_bytes(hash).await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(ExportBaoError::ExportBaoInner {
                source: EncodeError::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export_ranges` returns an error.
        let res = store
            .export_ranges(hash, RangeSet2::all())
            .concatenate()
            .await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export_bao` returns an error.
        let res = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await;
        assert!(res.is_err());
        println!("export_bao res {res:?}");
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));

        // check that `export` returns an error.
        let target = tempfile::NamedTempFile::new()?;
        let path = target.path();
        let res = store.export(hash, path).await;
        assert!(res.is_err());
        assert!(matches!(
            res,
            Err(RequestError::Inner{
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ));
        Ok(())
    }
}
