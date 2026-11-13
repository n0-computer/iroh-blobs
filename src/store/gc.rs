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
    roots: &[HashAndFormat],
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
    // Entry freezes and tag suspension cover this exact root snapshot until
    // the GC guard is dropped. Never re-list roots during the session.
    for &root in roots {
        add_root(root);
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
    run: &crate::api::blobs::GcRun,
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> crate::api::Result<()> {
    let mut count = 0;
    let mut batch = Vec::new();
    for &hash in &run.snapshot.candidates {
        if !live.contains(&hash) {
            batch.push(hash);
            count += 1;
        }
        if batch.len() >= 100 {
            run.delete(store, batch.clone()).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        run.delete(store, batch).await?;
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
    roots: &'a [HashAndFormat],
) -> impl Stream<Item = GcMarkEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_mark_task(store, live, roots, &co).await {
            co.yield_(GcMarkEvent::Error(e)).await;
        }
    })
}

fn gc_sweep<'a>(
    store: &'a Store,
    run: &'a crate::api::blobs::GcRun,
    live: &'a HashSet<Hash>,
) -> impl Stream<Item = GcSweepEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_sweep_task(store, run, live, &co).await {
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
    /// and returns a future that returns [`ProtectOutcome`]. It must not await tag
    /// mutations, imports that publish a temp tag, or writes to protected HashSeqs:
    /// those are suspended during GC.
    /// All hashes that are added to the
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
/// Entering GC captures candidates and roots, freezes HashSeq data, and defers
/// all tag mutations (including imports that publish a temp tag). Available
/// chunks of incomplete roots are traversed without waiting for missing data.
/// Blobs absent from the candidate snapshot cannot be swept this round.
///
/// The optional `protect` callback ([`GcConfig::add_protected`]) runs after
/// marking, before sweep. It must only add hashes to `live`, and must not wait
/// for a tag mutation or a write to a frozen HashSeq. External writers must
/// acquire store protection before publishing references, and establish/check
/// content after acquisition if it was previously unprotected.
///
/// Completion, errors and cancellation release the session. The store waits
/// for accepted sweep deletes before resuming deferred tag publication, and
/// rejects requests arriving after their session ends.
pub async fn gc_run_once(
    store: &Store,
    live: &mut HashSet<Hash>,
    protect: Option<&ProtectCb>,
) -> crate::api::Result<()> {
    debug!(externally_protected = live.len(), "gc: start");
    // Enter returns only after tag mutations are suspended and every captured
    // HashSeq root is frozen. Dropping the guard exits on success/error/cancel.
    let run = store.gc_run().await?;
    {
        let mut stream = gc_mark(store, live, &run.snapshot.roots);
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
        let mut stream = gc_sweep(store, &run, live);
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
        let candidates = &run.snapshot.candidates;
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
        let mut stream = gc_sweep(store, &run, &live);
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

    async fn assert_pending<T>(task: &mut tokio::task::JoinHandle<T>) {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), task)
                .await
                .is_err(),
            "operation completed while GC held the freeze"
        );
    }

    /// A partial root remains readable but cannot gain references; a complete
    /// root cannot be deleted/recreated. Unrelated writes/deletes still proceed.
    async fn gc_freezes_hashseqs(store: &Store) -> TestResult {
        use crate::{api::blobs::DeleteOptions, store::util::tests::create_n0_bao};
        use bao_tree::ChunkNum;
        let child = store.add_slice("known child").temp_tag().await?;
        let hash = child.hash();
        let late = store.add_slice("unpublished child").temp_tag().await?;
        let late_hash = late.hash();
        drop(late);
        let data: bytes::Bytes = std::iter::repeat_n(hash, 512)
            .chain(std::iter::repeat_n(late_hash, 512))
            .chain(std::iter::repeat_n(Hash::new("unavailable"), 512))
            .collect::<HashSeq>()
            .into();
        let first = ChunkRanges::from(..ChunkNum(16));
        let (root, bao) = create_n0_bao(&data, &first)?;
        let root_tag = store.tags().temp_tag(HashAndFormat::hash_seq(root)).await?;
        store
            .import_bao_bytes(root, first.clone(), bao.clone())
            .await?;
        let complete_data: bytes::Bytes = [hash].into_iter().collect::<HashSeq>().into();
        let complete = store
            .add_bytes_with_opts(AddBytesOptions {
                data: complete_data.clone(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        let complete_hash = complete.hash();
        let garbage = store.add_slice("garbage").temp_tag().await?;
        let garbage_hash = garbage.hash();
        drop(garbage);
        store.sync_db().await?;

        let run = store.gc_run().await?;
        assert!(run.delete(store, vec![complete_hash]).await.is_err());
        assert!(run.snapshot.candidates.contains(&late_hash));
        assert!(run.snapshot.roots.contains(&root_tag.hash_and_format()));
        let writer_store = store.clone();
        let mut writer = tokio::spawn(async move {
            let range = ChunkRanges::from(ChunkNum(16)..ChunkNum(32));
            let (_, bao) = create_n0_bao(&data, &range)?;
            writer_store.import_bao_bytes(root, range, bao).await?;
            TestResult::Ok(())
        });
        let delete_store = store.clone();
        let mut delete = tokio::spawn(async move {
            delete_store
                .blobs()
                .delete_with_opts(DeleteOptions {
                    hashes: vec![complete_hash],
                    force: true,
                    gc_id: None,
                })
                .await
        });
        assert_pending(&mut writer).await;
        assert_pending(&mut delete).await;
        assert_eq!(store.export_bao(root, first).bao_to_vec().await?, bao);
        assert_eq!(store.get_bytes(complete_hash).await?, complete_data);
        let mut observed = store.observe(root).stream().await?;
        assert!(observed.next().await.is_some());

        // A raw download needs no tag operation and is outside the candidates.
        let new_data = b"new raw download";
        let (new_hash, new_bao) = create_n0_bao(new_data, &ChunkRanges::all())?;
        store
            .import_bao_bytes(new_hash, ChunkRanges::all(), new_bao)
            .await?;
        let mut live = HashSet::new();
        let mut mark = gc_mark(store, &mut live, &run.snapshot.roots);
        while let Some(event) = mark.next().await {
            if let GcMarkEvent::Error(err) = event {
                return Err(err.into());
            }
        }
        drop(mark);
        assert!(live.contains(&hash));
        assert!(!live.contains(&late_hash));
        let mut sweep = gc_sweep(store, &run, &live);
        while let Some(event) = sweep.next().await {
            if let GcSweepEvent::Error(err) = event {
                return Err(err.into());
            }
        }
        drop(sweep);
        assert!(!store.has(garbage_hash).await?);
        assert!(store.has(new_hash).await?);
        assert_pending(&mut writer).await;
        assert_pending(&mut delete).await;
        drop(run);
        writer.await??;
        delete.await??;
        assert!(!store.has(complete_hash).await?);
        // The previously unknown reference is only published after sweep.
        let mut hashes = store
            .export_bao(root, ChunkRanges::from(..ChunkNum(32)))
            .hashes();
        let mut seen = HashSet::new();
        while let Some(hash) = hashes.next().await {
            seen.insert(hash?);
        }
        assert!(seen.contains(&late_hash));
        drop(hashes);
        drop(observed);
        drop(root_tag);
        drop(complete);
        drop(child);
        Ok(())
    }

    async fn gc_suspends_tag_mutations(store: &Store) -> TestResult {
        let a = store.add_slice("a").temp_tag().await?;
        let b = store.add_slice("b").temp_tag().await?;
        let ah = a.hash();
        let bh = b.hash();
        for name in ["set", "rename", "delete"] {
            store.tags().set(name, ah).await?;
        }
        let run = store.gc_run().await?;
        let mut changes = Vec::new();
        for kind in 0..5 {
            let store = store.clone();
            changes.push(tokio::spawn(async move {
                match kind {
                    0 => store.tags().set("set", bh).await?,
                    1 => {
                        store.tags().create(bh).await?;
                    }
                    2 => store.tags().rename("rename", "renamed").await?,
                    3 => {
                        store.tags().delete("delete").await?;
                    }
                    4 => {
                        store
                            .tags()
                            .set("hashseq", HashAndFormat::hash_seq(bh))
                            .await?
                    }
                    _ => unreachable!(),
                }
                TestResult::Ok(())
            }));
        }
        let mut temps = Vec::new();
        for value in [HashAndFormat::raw(bh), HashAndFormat::hash_seq(bh)] {
            let store = store.clone();
            temps.push(tokio::spawn(
                async move { store.tags().temp_tag(value).await },
            ));
        }
        let import_store = store.clone();
        let mut import =
            tokio::spawn(async move { import_store.add_slice("new import").temp_tag().await });
        for task in &mut changes {
            assert_pending(task).await;
        }
        for task in &mut temps {
            assert_pending(task).await;
        }
        assert_pending(&mut import).await;
        // Reads work, and replacements/deletes have not taken effect.
        let mut tags = store.tags().list().await?;
        let mut before = Vec::new();
        while let Some(tag) = tags.next().await {
            before.push(tag?);
        }
        assert_eq!(before.len(), 3);
        assert!(before.iter().all(|tag| tag.hash == ah));
        drop(tags);
        // The root captured at entry remains protected even when its tag drops.
        drop(a);
        assert!(run.snapshot.roots.contains(&HashAndFormat::raw(ah)));
        drop(run);
        for task in changes {
            task.await??;
        }
        let mut held = Vec::new();
        for task in temps {
            held.push(task.await??);
        }
        held.push(import.await??);
        let mut tags = store.tags().list().await?;
        let mut names = std::collections::BTreeSet::new();
        while let Some(tag) = tags.next().await {
            names.insert(tag?.name);
        }
        assert!(names.contains(&crate::api::Tag::from("renamed")));
        assert!(!names.contains(&crate::api::Tag::from("delete")));
        Ok(())
    }

    /// Cancel a client while waiting for the entry reply, and while holding a
    /// session. Both cases must release freezes and let later sessions enter.
    async fn gc_cancellation_releases_session(store: &Store) -> TestResult {
        let root = store
            .add_bytes_with_opts(AddBytesOptions {
                data: [Hash::new("child")].into_iter().collect::<HashSeq>().into(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        for _ in 0..5 {
            let (updates, reply) = store
                .client()
                .client_streaming(crate::api::proto::GcRunRequest, 1)
                .await?;
            drop(reply);
            drop(updates);
            let run = store.gc_run().await?;
            let next_store = store.clone();
            let mut next = tokio::spawn(async move { next_store.gc_run().await });
            assert_pending(&mut next).await;
            drop(run);
            drop(next.await??);
        }
        store
            .tags()
            .set("after cancellation", root.hash_and_format())
            .await?;
        Ok(())
    }

    /// A delete delayed in transit must be rejected if its GC session ended,
    /// including when another session has already started.
    async fn gc_cancel_during_delete(store: &Store) -> TestResult {
        use tokio::sync::oneshot;
        let garbage = store
            .add_slice("cancelled-sweep candidate")
            .temp_tag()
            .await?;
        let hash = garbage.hash();
        drop(garbage);
        store.sync_db().await?;
        let inner = store.client().as_local().expect("local store");
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Command>(32);
        let (queued, queued_rx) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let responder = tokio::spawn(async move {
            let mut pause = Some((queued, released));
            while let Some(cmd) = rx.recv().await {
                if matches!(cmd, Command::DeleteBlobs(_)) {
                    if let Some((queued, released)) = pause.take() {
                        queued.send(()).unwrap();
                        released.await.unwrap();
                    }
                }
                if inner.send_raw(cmd).await.is_err() {
                    break;
                }
            }
        });
        let proxy = Store::from_client(tx);
        let gc_store = proxy.clone();
        let gc =
            tokio::spawn(async move { gc_run_once(&gc_store, &mut HashSet::new(), None).await });
        queued_rx.await?;
        gc.abort();
        assert!(gc.await.unwrap_err().is_cancelled());
        let tag_store = store.clone();
        let tag = tokio::spawn(async move { tag_store.tags().temp_tag(hash).await });
        let _tag = tag.await??;
        let next = store.gc_run().await?;
        release.send(()).unwrap();
        drop(proxy);
        responder.await?;
        assert!(store.has(hash).await?, "a stale sweep deleted the blob");
        drop(next);
        Ok(())
    }

    async fn gc_shutdown_releases_session(store: &Store) -> TestResult {
        let root = store
            .add_bytes_with_opts(AddBytesOptions {
                data: [Hash::new("shutdown child")]
                    .into_iter()
                    .collect::<HashSeq>()
                    .into(),
                format: BlobFormat::HashSeq,
            })
            .temp_tag()
            .await?;
        let run = store.gc_run().await?;
        let tag_store = store.clone();
        let hash = root.hash();
        let mut tag = tokio::spawn(async move { tag_store.tags().temp_tag(hash).await });
        assert_pending(&mut tag).await;
        store.shutdown().await?;
        assert!(tag.await?.is_err(), "shutdown must reject deferred tags");
        drop(run);
        drop(root);
        Ok(())
    }

    #[tokio::test]
    async fn gc_session_mem() -> TestResult {
        let store = crate::store::mem::MemStore::new();
        tokio::time::timeout(Duration::from_secs(15), async {
            gc_freezes_hashseqs(&store).await?;
            gc_suspends_tag_mutations(&store).await?;
            gc_cancellation_releases_session(&store).await?;
            gc_cancel_during_delete(&store).await?;
            gc_shutdown_releases_session(&store).await
        })
        .await??;
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_session_fs() -> TestResult {
        let dir = tempfile::tempdir()?;
        let store = crate::store::fs::FsStore::load(dir.path()).await?;
        tokio::time::timeout(Duration::from_secs(15), async {
            gc_freezes_hashseqs(&store).await?;
            gc_suspends_tag_mutations(&store).await?;
            gc_cancellation_releases_session(&store).await?;
            gc_cancel_during_delete(&store).await?;
            gc_shutdown_releases_session(&store).await
        })
        .await??;
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

    /// The callback reports external references while concurrent tag publication
    /// waits. Its snapshot and the store snapshot together protect old content.
    async fn gc_protect_race(store: &Store) -> TestResult {
        use tokio::sync::{mpsc, oneshot};
        let tt = store.add_slice("external").temp_tag().await?;
        let hash = tt.hash();
        drop(tt);
        let (entered_tx, mut entered_rx) = mpsc::channel::<oneshot::Sender<()>>(1);
        let cb: ProtectCb = Arc::new(move |live| {
            live.insert(hash);
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                let (release, released) = oneshot::channel();
                entered_tx.send(release).await.unwrap();
                released.await.unwrap();
                ProtectOutcome::Continue
            })
        });
        let gc_store = store.clone();
        let gc =
            tokio::spawn(
                async move { gc_run_once(&gc_store, &mut HashSet::new(), Some(&cb)).await },
            );
        let release = entered_rx.recv().await.unwrap();
        let writer_store = store.clone();
        let mut writer = tokio::spawn(async move { writer_store.tags().temp_tag(hash).await });
        assert_pending(&mut writer).await;
        assert!(store.has(hash).await?);
        release.send(()).unwrap();
        gc.await??;
        let _tag = writer.await??;
        assert!(store.has(hash).await?);
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
