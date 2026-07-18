use std::{collections::HashSet, convert::Infallible, future::Future, pin::Pin, sync::Arc};

use bao_tree::ChunkRanges;
use genawaiter::sync::{Co, Gen};
use n0_future::{time::Duration, Stream, StreamExt};
use tracing::{debug, error, info, warn};

use crate::{
    api::{
        proto::{FinishGcProtectionOutcome, GcProtectionCycleId},
        Store,
    },
    Hash, HashAndFormat,
};

/// An event related to GC
#[derive(Debug)]
pub enum GcMarkEvent {
    /// A custom event (info)
    CustomDebug(String),
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
    let mut roots = HashSet::new();
    trace!("traversing tags");
    let mut tags = store.tags().list().await?;
    while let Some(tag) = tags.next().await {
        let info = tag?;
        trace!("adding root {:?} {:?}", info.name, info.hash_and_format());
        roots.insert(info.hash_and_format());
    }
    trace!("traversing temp roots");
    let mut tts = store.tags().list_temp_tags().await?;
    while let Some(tt) = tts.next().await {
        trace!("adding temp root {:?}", tt);
        roots.insert(tt);
    }
    for HashAndFormat { hash, format } in roots {
        // we need to do this for all formats except raw
        if live.insert(hash) && !format.is_raw() {
            let mut stream = store.export_bao(hash, ChunkRanges::all()).hashes();
            while let Some(hash) = stream.next().await {
                match hash {
                    Ok(hash) => {
                        live.insert(hash);
                    }
                    Err(error) => return Err(crate::api::Error::other(error)),
                }
            }
        }
    }
    trace!("gc mark done. found {} live blobs", live.len());
    Ok(())
}

async fn gc_sweep_task(
    store: &Store,
    live: &HashSet<Hash>,
    co: &Co<GcSweepEvent>,
) -> crate::api::Result<()> {
    let mut blobs = store.blobs().list().stream().await?;
    let mut count = 0;
    let mut batch = Vec::new();
    while let Some(hash) = blobs.next().await {
        let hash = hash?;
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
    co.yield_(GcSweepEvent::CustomDebug(format!("deleted {count} blobs")))
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
    live: &'a HashSet<Hash>,
) -> impl Stream<Item = GcSweepEvent> + 'a {
    Gen::new(|co| async move {
        if let Err(e) = gc_sweep_task(store, live, &co).await {
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
    /// The callback is called after built-in tags and temporary tags have been marked and
    /// immediately before sweep. It gets an empty `&mut HashSet<Hash>` and returns a future that
    /// returns [`ProtectOutcome`]. All hashes added to the set are protected during this run.
    ///
    /// In normal operation, return [`ProtectOutcome::Continue`] from the callback. If you return
    /// [`ProtectOutcome::Abort`], the garbage collection run will be aborted.Use this if your
    /// source of hashes to protect returned an error, and thus garbage collection should be skipped
    /// completely to avoid unintentionally deleting blobs that should be protected.
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

/// Outcome of one garbage-collection cycle with a late protection source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcRunOutcome<E> {
    /// Marking, late protection discovery, and sweeping all completed.
    Completed,
    /// Protection discovery failed, so sweeping was not attempted.
    Aborted(E),
}

/// Store-owned protection state for exactly one active GC cycle.
#[derive(Debug, Default)]
pub(crate) struct GcProtectionSet {
    active_cycle: Option<GcProtectionCycleId>,
    last_cycle: u64,
    hashes: HashSet<Hash>,
}

impl GcProtectionSet {
    pub fn start(&mut self) -> crate::api::Result<GcProtectionCycleId> {
        if self.active_cycle.is_some() {
            return Err(crate::api::Error::other(
                "another garbage-collection cycle is already active",
            ));
        }
        self.last_cycle = self
            .last_cycle
            .checked_add(1)
            .ok_or_else(|| crate::api::Error::other("garbage-collection cycle id exhausted"))?;
        let cycle = GcProtectionCycleId::new(self.last_cycle);
        self.hashes.clear();
        self.active_cycle = Some(cycle);
        Ok(cycle)
    }

    pub fn finish(&mut self, cycle: GcProtectionCycleId) -> FinishGcProtectionOutcome {
        if self.active_cycle != Some(cycle) {
            return FinishGcProtectionOutcome::Stale;
        }
        self.active_cycle = None;
        self.hashes.clear();
        FinishGcProtectionOutcome::Finished
    }

    pub fn protect(&mut self, hash: Hash) {
        if self.active_cycle.is_some() {
            self.hashes.insert(hash);
        }
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.active_cycle.is_some() && self.hashes.contains(hash)
    }
}

struct GcProtectionGuard {
    store: Store,
    cycle: Option<GcProtectionCycleId>,
}

impl GcProtectionGuard {
    async fn start(store: &Store) -> crate::api::Result<Self> {
        let cycle = store.start_gc_protection().await?;
        Ok(Self {
            store: store.clone(),
            cycle: Some(cycle),
        })
    }

    async fn finish(mut self) -> crate::api::Result<()> {
        let cycle = self.cycle.expect("active guard always owns a cycle");
        let outcome = self.store.finish_gc_protection(cycle).await?;
        self.cycle = None;
        if matches!(outcome, FinishGcProtectionOutcome::Stale) {
            warn!(?cycle, "gc protection cycle was already released");
        }
        Ok(())
    }
}

impl Drop for GcProtectionGuard {
    fn drop(&mut self) {
        let Some(cycle) = self.cycle.take() else {
            return;
        };
        let store = self.store.clone();
        n0_future::task::spawn(async move {
            match store.finish_gc_protection(cycle).await {
                Ok(FinishGcProtectionOutcome::Finished | FinishGcProtectionOutcome::Stale) => {}
                Err(error) => {
                    error!(
                        ?cycle,
                        "failed to release cancelled gc protection cycle: {error}"
                    )
                }
            }
        });
    }
}

/// The type of the garbage collection callback.
///
/// See [`GcConfig::add_protected] for details.
pub type ProtectCb = Arc<
    dyn for<'a> Fn(
            &'a mut HashSet<Hash>,
        )
            -> Pin<Box<dyn std::future::Future<Output = ProtectOutcome> + Send + 'a>>
        + Send
        + Sync
        + 'static,
>;

/// Runs one garbage-collection cycle without an external protection source.
///
/// Call [`gc_run_once_with_late_protection`] when another durable index owns
/// references that are not represented by blob tags.
pub async fn gc_run_once(store: &Store, live: &mut HashSet<Hash>) -> crate::api::Result<()> {
    match gc_run_once_with_late_protection(store, live, || {
        std::future::ready(Ok::<HashSet<Hash>, Infallible>(HashSet::new()))
    })
    .await?
    {
        GcRunOutcome::Completed => Ok(()),
        GcRunOutcome::Aborted(never) => match never {},
    }
}

/// Runs one mark-and-sweep cycle and discovers external roots immediately
/// before sweeping.
///
/// `late_protection` is intentionally a future factory rather than an eagerly
/// created future: it is not invoked until the store's protection cycle has
/// started and
/// tags and temporary tags have been marked. This closes the snapshot window in
/// which a consumer could commit a reference, drop its import temp tag, and have
/// the referenced blob swept. Returning `Err` aborts the cycle before sweep.
pub async fn gc_run_once_with_late_protection<L, F, E>(
    store: &Store,
    live: &mut HashSet<Hash>,
    load_late_protection: L,
) -> crate::api::Result<GcRunOutcome<E>>
where
    L: FnOnce() -> F,
    F: Future<Output = Result<HashSet<Hash>, E>>,
{
    debug!(externally_protected = live.len(), "gc: start");
    let protection = GcProtectionGuard::start(store).await?;
    let cycle_result = async {
        {
            let mut stream = gc_mark(store, live);
            while let Some(ev) = stream.next().await {
                match ev {
                    GcMarkEvent::CustomDebug(msg) => {
                        debug!("{}", msg);
                    }
                    GcMarkEvent::Error(err) => {
                        error!("error during gc mark: {:?}", err);
                        return Err(err);
                    }
                }
            }
        }
        let externally_protected = match load_late_protection().await {
            Ok(externally_protected) => externally_protected,
            Err(error) => {
                info!("abort gc run: late protection discovery failed");
                return Ok(GcRunOutcome::Aborted(error));
            }
        };
        live.extend(externally_protected);
        debug!(total_protected = live.len(), "gc: sweep");
        let mut stream = gc_sweep(store, live);
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
        debug!("gc: done");
        Ok(GcRunOutcome::Completed)
    }
    .await;

    let finish_result = protection.finish().await;
    match (cycle_result, finish_result) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(finish)) => Err(finish),
        (Err(primary), Err(finish)) => {
            error!("failed to release gc protection after cycle error: {finish}");
            Err(primary)
        }
    }
}

pub async fn run_gc(store: Store, config: GcConfig) {
    debug!("gc enabled with interval {:?}", config.interval);
    let mut live = HashSet::new();
    loop {
        live.clear();
        n0_future::time::sleep(config.interval).await;
        let outcome = if let Some(ref cb) = config.add_protected {
            let load_late_protection = || async {
                let mut externally_protected = HashSet::new();
                match (cb)(&mut externally_protected).await {
                    ProtectOutcome::Continue => Ok(externally_protected),
                    ProtectOutcome::Abort => Err(()),
                }
            };
            gc_run_once_with_late_protection(&store, &mut live, load_late_protection).await
        } else {
            gc_run_once(&store, &mut live)
                .await
                .map(|()| GcRunOutcome::Completed)
        };
        match outcome {
            Ok(GcRunOutcome::Completed) => {}
            Ok(GcRunOutcome::Aborted(())) => {
                info!("abort gc run: protect callback indicated abort");
            }
            Err(error) => {
                error!("error during gc run: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self};

    use bao_tree::io::EncodeError;
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
        gc_run_once(store, &mut live).await?;
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
            gc_run_once(store, &mut live).await?;
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
            gc_run_once(store, &mut live).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
            assert!(!sizes_path.exists());
            assert!(!bitfield_path.exists());
        }
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_smoke_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_smoke(&store).await?;
        gc_file_delete(testdir.path(), &store).await?;
        Ok(())
    }

    #[tokio::test]
    async fn gc_smoke_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::new();
        gc_smoke(&store).await?;
        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn gc_check_deletion_fs() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db");
        let store = crate::store::fs::FsStore::load(&db_path).await?;
        gc_check_deletion(&store).await
    }

    #[tokio::test]
    async fn gc_check_deletion_mem() -> TestResult {
        tracing_subscriber::fmt::try_init().ok();
        let store = crate::store::mem::MemStore::default();
        gc_check_deletion(&store).await
    }

    async fn gc_check_deletion(store: &Store) -> TestResult {
        let temp_tag = store.add_bytes(b"foo".to_vec()).temp_tag().await?;
        let hash = temp_tag.hash();
        assert_eq!(store.get_bytes(hash).await?.as_ref(), b"foo");
        drop(temp_tag);
        let mut live = HashSet::new();
        gc_run_once(store, &mut live).await?;

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
