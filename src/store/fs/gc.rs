use std::collections::HashSet;

use bao_tree::ChunkRanges;
use genawaiter::sync::{Co, Gen};
use n0_future::{Stream, StreamExt};
use tracing::{debug, error, warn};

use crate::{api::Store, Hash, HashAndFormat};

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
                    Err(e) => {
                        warn!("error while traversing hashseq: {e:?}");
                    }
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

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub interval: std::time::Duration,
}

pub async fn gc_run_once(store: &Store, live: &mut HashSet<Hash>) -> crate::api::Result<()> {
    {
        live.clear();
        store.clear_protected().await?;
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
    {
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
    }

    Ok(())
}

pub async fn run_gc(store: Store, config: GcConfig) {
    let mut live = HashSet::new();
    loop {
        tokio::time::sleep(config.interval).await;
        if let Err(e) = gc_run_once(&store, &mut live).await {
            error!("error during gc run: {e}");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bao_tree::ChunkNum;
    use testresult::TestResult;

    use super::*;
    use crate::{
        api::{blobs::AddBytesOptions, Store},
        hashseq::HashSeq,
        store::fs::{options::PathOptions, tests::create_n0_bao},
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
        let a = *at.hash();
        let b = *bt.hash();
        let c = *ct.hash();
        let d = *dt.hash();
        let e = *et.hash();
        let f = *ft.hash();
        let g = *gt.hash();
        store.tags().set("c", *ct.hash_and_format()).await?;
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
        store.tags().set("fg", *fghs.hash_and_format()).await?;
        drop(fghs);
        drop(bt);
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
        drop(at);
        drop(hehs);
        Ok(())
    }

    async fn gc_file_delete(path: &Path, store: &Store) -> TestResult<()> {
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
            let data_path = options.data_path(ah);
            let outboard_path = options.outboard_path(ah);
            assert!(data_path.exists());
            assert!(outboard_path.exists());
            assert!(store.has(*ah).await?);
            drop(a);
            gc_run_once(store, &mut live).await?;
            assert!(!data_path.exists());
            assert!(!outboard_path.exists());
        }
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
}
