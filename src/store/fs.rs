//! # File based blob store.
//!
//! A file based blob store needs a writeable directory to work with.
//!
//! # Design
//!
//! The metadata actor owns the metadata, and each entity owns the files of its
//! hash. They talk only by messages. An entity's in-memory state is a cache of
//! its entry, valid as long as the entity is the only writer of that entry.
//! Entries only grow, which is why a write to a complete entry can be a no-op.
//! A delete is the one change that does not grow an entry, so it goes through
//! the entity too.
//!
//! # Actors
//!
//! - **Main actor**: handles user commands, runs the entity manager, holds the
//!   temp tags, forwards tag, status, listing and lifecycle commands to the
//!   metadata actor, fans deletes out to the entities, and routes unreferenced
//!   parts.
//! - **Entity actors**, one per hash that is currently being worked on: run the
//!   per-hash tasks (import, export, observe, persist, delete). While an entity
//!   exists, it is the only one that writes and deletes the files of its hash.
//! - **Metadata actor**: owns the redb database with the entry of each hash,
//!   inlined data and outboards of small blobs, and the tags. It groups queued
//!   commands into read or write transactions. It never touches data files,
//!   except for recovery when the store opens, before any entity exists.
//!
//! All senders to the metadata actor share one channel, so it sees commands in
//! the order the sends completed.
//!
//! # Metadata protocol
//!
//! An entity talks to the metadata actor through `EntryMeta`, scoped to its
//! hash:
//!
//! | Method | Waits | Meaning |
//! |---|---|---|
//! | `get()` | for the reply | get the entry state |
//! | `set(state)` | until applied | replace the entry state |
//! | `update(state)` | no | merge into the entry state |
//! | `update_await(state)` | until applied | merge into the entry state |
//! | `delete()` | until committed | ensure the entry is absent |
//!
//! "Applied" means visible to later commands, not durable. Durability comes with
//! the commit of the batch, and commits are durable (redb's default
//! `Durability::Immediate`). Only `delete()` is answered after the commit, since
//! its entity unlinks the files right after.
//!
//! After each commit, the metadata actor sends `UnreferencedParts` to the main
//! actor for each hash that is not deleted and no longer references some of its
//! files: the files it referenced before or during the batch and no longer does.
//! The channel is unbounded, so the metadata actor never waits on the main
//! actor, which waits on it.
//!
//! # Entities
//!
//! An entity's state is `Initial`, `Loading` or `Deleting` while it is in
//! transition, and otherwise a loaded `Settled` entry: `NonExisting`,
//! `Partial`, `Complete` or `Invalid`. A settled entry is `ReadWrite`, or
//! `ReadOnly` while a gc session freezes it.
//!
//! An entity's tasks are futures polled by one actor, so they only interleave at
//! `.await` points. Every change of an entity's state or files runs after
//! `wait_writable`, which waits while the state is `Loading`, `Deleting` or
//! `ReadOnly`, with no `.await` between the check and the change. Readers use
//! `wait_settled`, which only waits for `Loading` and `Deleting`. Both wait on
//! the entity's state `watch`.
//!
//! # Unreferenced parts
//!
//! A committed state no longer referencing a file does not make it garbage: the
//! entity may use it again, since its state is never behind its entry. So the
//! main actor hands the parts to the hash's entity, into its unbounded inbox,
//! also while it shuts down, since its shutdown may write the bitfield. If
//! there is no entity, one is woken for this, which keeps all file IO in
//! entities. The entity checks each part against its own state, and only
//! unlinks those it does not use.
//!
//! A notice is only as recent as its commit. By the time it arrives, the entry
//! may have been deleted and created again, with new files, and its entity
//! unloaded. So an entity that is not loaded, like one woken for the notice,
//! loads before it checks the parts.
//!
//! At shutdown, the main actor hands the parts of the last batch to the
//! entities before they shut down. Parts that arrive after that stay, as after
//! a crash.
//!
//! # Deletes
//!
//! The main actor fans `DeleteBlobs` out to the entity of each hash, creating it
//! if needed, also for inline entries. That orders a delete with
//! concurrent requests even if no entity existed when it arrived. A delete that
//! is not forced skips content with a current or retained temp tag; that check
//! happens in the main actor, before the fan-out. The entity:
//!
//! 1. Loads, and notes the files its state references: those of its entry, and
//!    those it wrote that the entry does not reference yet. Then it starts a
//!    new generation of the entry, which ends observers, and switches to
//!    `Deleting`. The cached state is dropped: readers see no entry, and
//!    changes wait from here on.
//! 2. Sends `delete` to the metadata actor. Deletes that arrive together still
//!    land in one transaction.
//! 3. Waits for the answer, which comes once the delete is committed, even if
//!    there was no entry. Waiting keeps the entity alive, so it cannot unload
//!    and come back with new files that the unlink would take.
//! 4. Unlinks the files it noted, so an inline entry costs no unlink, and
//!    switches to `NonExisting`. Waiting writes continue on an empty entry: a running
//!    download keeps going with the ranges that arrive from then on.
//!
//! The main actor answers `DeleteBlobs` once every entity is done, after its
//! unlink attempts. Unlink failures are logged. Shutdown waits for running
//! deletes before it shuts down the entities, since an entity in shutdown takes
//! no new tasks.
//!
//! A metadata error aborts the whole store. The main actor cancels the workers
//! and releases pending callers. A delete in a failed batch gets no answer, and
//! fails instead of unlinking. There is no per-entity rollback, and the abort does not
//! persist entity state or clear the unclean-run marker.
//!
//! # GC sessions
//!
//! `GcRun` is a bidi streaming request that enters an exclusive gc session. The
//! store first sends a snapshot of candidates and roots. GC then sends its sweep
//! deletes on the session's update channel, and gets one reply per delete, in
//! order. Closing the update channel ends the session. A second `GcRun` waits
//! until the current session ends.
//!
//! While a session lasts, the main actor holds back all tag changes: setting,
//! creating, deleting and renaming tags, creating temp tags, and import
//! completion, which creates a temp tag. It queues them with their reply
//! channels and keeps handling reads and other blob writes. Temp tag drops and
//! scope ends cannot wait; `TempTags` keeps their protection until the session
//! ends. A batch scope that ended meanwhile is not recreated: its queued tag and
//! import requests are rejected.
//!
//! To build the snapshot, the main actor sends `GcSnapshot` to the metadata actor,
//! a top-level command, so tag changes queued before it are committed. It takes
//! all blobs as candidates, and the named tags plus current and retained temp
//! tags as roots. Then it freezes each HashSeq root through its entity, and
//! sends the snapshot only once every freeze is acknowledged. It keeps running
//! meanwhile, handling entity restarts and unreferenced parts. The mem store takes the same
//! view and freezes its handles in place.
//!
//! A freeze turns `ReadWrite` into `ReadOnly` with the same settled entry: reads
//! go on, while writes, replacing imports and deletes wait. Complete and
//! invalid entries are frozen too, since a delete would turn them into an entry
//! that can grow again. A freeze task keeps the entity alive. All freezes of a
//! session wait on one release channel; ending the session drops it, and each
//! freeze task turns its entry back into `ReadWrite` itself. A freeze waits for an earlier one to end, so freezes never
//! overlap. If a freeze task is dropped at shutdown instead, `persist` looks
//! through the freeze.
//!
//! GC traverses exactly the snapshot's roots, calls the protect callback, and
//! sweeps only the snapshot's candidates. Incomplete HashSeqs contribute the
//! children in their available chunks, and missing chunks never hold up a run.
//! Their writes stay frozen until the sweep ends, so no new reference can appear
//! between traversal and deletion. Blobs added after the snapshot are not
//! candidates, so a run never sweeps them.
//!
//! Sweep deletes travel on the session's own channel, so none can arrive after
//! its session ended. The session task finishes each delete before it reads the
//! next update, and the session ends only after that, even if the GC client or
//! its connection disappears. So a held-back tag change can never overtake a
//! sweep delete. Sweeping a frozen root is rejected, since it would wait on the
//! sweep's own freeze. A plain `DeleteBlobs` is independent of the session: it
//! can be sent at any time, and during a session it skips protected content and
//! waits on frozen roots.
//!
//! Shutdown stops accepting work, rejects held-back tag changes, and cancels a
//! running session without waiting for its client. It lets deletes that were
//! already sent to entities finish before it closes the metadata actor.
//!
//! The protect callback runs in the GC task while the session is held. It must
//! only add protected hashes. It must not await, directly or through another
//! task, anything the session holds back: tag changes, temp tag creation, import
//! completion, and writes to or deletes of frozen HashSeq roots. Those wait for
//! the session to end, which waits for the callback, so the GC run and
//! everything queued behind it would deadlock. External systems must protect
//! content in the store before they publish references to it. If they protect
//! content that was unprotected before, they must check it is still there
//! afterwards: a tag does not bring back data that GC already deleted.
//!
//! # Tasks
//!
//! Tasks do not return a result. They are responsible for sending an error
//! to the requester if possible. Otherwise, just dropping the sender will
//! also fail the receiver, but without a descriptive error message.
//!
//! Tasks are usually implemented as an impl fn that does return a result,
//! and a wrapper (named `..._task`) that just forwards the error, if any.
//!
//! That way you can use `?` syntax in the task implementation. The impl fns
//! are also easier to test.
//!
//! # Context
//!
//! The main actor holds a TaskContext that is needed for almost all tasks,
//! such as the config and a way to interact with the database.
//!
//! For tasks that are specific to a hash, a HashContext combines the task
//! context with a slot from the table of the main actor that can be used
//! to obtain an unique handle for the hash.
//!
//! # Runtime
//!
//! The fs store owns and manages its own tokio runtime. Almost all store
//! operations are small synchronous IO operations: file reads and writes, and
//! redb transactions. Running them on a dedicated runtime lets the store
//! perform them directly inside its tasks. The alternative, wrapping every
//! tiny operation in `spawn_blocking`, would be slow and error-prone. It also
//! means that blocking IO in the store never stalls the caller's runtime, and
//! the store works no matter which runtime flavor the caller uses.
//!
//! Dropping the store shuts down the runtime and closes the database in the
//! background, without waiting for either to finish. The database stays locked
//! until it is closed, so loading the store again right after dropping it will
//! usually fail. Also, some parts of the persistent state won't make it to
//! disk, so operations involving large partial blobs will have a large initial
//! delay on the next startup.
//!
//! It is also not guaranteed that all write operations will make it to disk.
//! The on-disk store will be in a consistent state, but might miss some writes
//! in the last seconds before shutdown.
//!
//! To avoid this, await [`crate::api::Store::shutdown`] before dropping the
//! store. It returns once the database is closed and ephemeral state is saved
//! to disk, so the store can be loaded again right away.
//!
//! Note that if you use the store inside a [`iroh::protocol::Router`] and shut
//! down the router using [`iroh::protocol::Router::shutdown`], the store will be
//! safely shut down as well. Any store refs you are holding will be inoperable
//! after this.
//!
//! # Limitations
//!
//! - A crash between a commit and the unlink that follows it leaves files that
//!   nothing references.
//! - After `export_path` with `TryReference` copies across file systems, the
//!   entity still uses the owned data file, so its check keeps the file when
//!   it comes back unreferenced, and it leaks.
//! - A same-file-system `TryReference` export renames the file before it
//!   commits the external location. If that commit fails, the metadata can
//!   still refer to the old path.
#[cfg(test)]
use std::sync::{atomic::AtomicU64, Arc};
use std::{
    fmt::Debug,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
};

use n0_error::{Result, StdResultExt};
use tracing::trace;

use crate::{
    api::{proto, ApiClient},
    util::channel::oneshot,
};
mod entity;
use entity::FsEntity;
#[cfg(test)]
mod lazyfs_tests;
#[cfg(test)]
pub(crate) mod mem_backend;
pub mod options;
mod redb;
use crate::store::engine::{
    actor::{self, InternalCommand},
    entity::Backend,
    metadata::{self, MetaChannels, MetaTask},
    options::StoreOptions,
};
use options::{BatchOptions, FsConfig, Options};

use crate::api::Store;

/// The fs store's backend: entities with their files in a data directory, and a redb metadata store.
#[derive(Debug)]
struct FsBackend {
    /// What the entities need: paths and sync mode.
    config: FsConfig,
    /// Where the redb database is.
    db_path: PathBuf,
    /// How the redb store batches its transactions.
    batch: BatchOptions,
}

impl FsBackend {
    fn new(db_path: PathBuf, options: &Options) -> Self {
        Self {
            config: FsConfig::from(options),
            db_path,
            batch: options.batch.clone(),
        }
    }
}

impl Backend for FsBackend {
    type Entity = FsEntity;

    fn start_meta(&self, channels: MetaChannels) -> Result<impl MetaTask> {
        redb::start(&self.db_path, &self.config, &self.batch, channels)
    }
}

impl FsStore {
    /// Load or create a new store.
    pub async fn load(root: impl AsRef<Path>) -> Result<Self> {
        let path = root.as_ref();
        let db_path = path.join("blobs.db");
        let options = Options::new(path);
        Self::load_with_opts(db_path, options).await
    }

    /// Load or create a new store with custom options, returning an additional sender for file store specific commands.
    pub async fn load_with_opts(db_path: PathBuf, options: Options) -> Result<FsStore> {
        let backend = FsBackend::new(db_path, &options);
        let data_path = &backend.config.path.data_path;
        trace!("creating data directory: {}", data_path.display());
        fs::create_dir_all(data_path)?;
        Self::from_parts(StoreOptions::from(&options), backend).await
    }

    /// Starts a store from its options and a backend.
    ///
    /// The backend starts the metadata store on the store's runtime, and is
    /// shared by all entities as their configuration.
    async fn from_parts<B: Backend>(options: StoreOptions, backend: B) -> Result<FsStore> {
        let started = actor::start(options, backend).await?;
        Ok(FsStore {
            #[cfg(test)]
            threads: started.threads,
            ..FsStore::new(started.commands, started.internal)
        })
    }
}

/// A file based store.
///
/// A store can be created using [`load`](FsStore::load) or [`load_with_opts`](FsStore::load_with_opts).
/// Load will use the default options and create the required directories, while load_with_opts allows
/// you to customize the options and the location of the database. Both variants will create the database
/// if it does not exist, and load an existing database if one is found at the configured location.
///
/// In addition to implementing the [`Store`](`crate::api::Store`) API via [`Deref`](`std::ops::Deref`),
/// there are a few additional methods that are specific to file based stores, such as [`dump`](FsStore::dump).
#[derive(Debug, Clone)]
pub struct FsStore {
    sender: ApiClient,
    db: tokio::sync::mpsc::Sender<InternalCommand>,
    #[cfg(test)]
    threads: Arc<AtomicU64>,
}

impl From<FsStore> for Store {
    fn from(value: FsStore) -> Self {
        Store::from_client(value.sender)
    }
}

impl Deref for FsStore {
    type Target = Store;

    fn deref(&self) -> &Self::Target {
        Store::ref_from_sender(&self.sender)
    }
}

impl AsRef<Store> for FsStore {
    fn as_ref(&self) -> &Store {
        self.deref()
    }
}

impl FsStore {
    fn new(
        sender: irpc::LocalSender<proto::Request>,
        db: tokio::sync::mpsc::Sender<InternalCommand>,
    ) -> Self {
        Self {
            sender: sender.into(),
            db,
            #[cfg(test)]
            threads: Default::default(),
        }
    }

    pub async fn dump(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.db
            .send(
                metadata::proto::Dump {
                    tx,
                    span: tracing::Span::current(),
                }
                .into(),
            )
            .await
            .anyerr()?;
        rx.await.anyerr()??;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use core::panic;
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use bao_tree::{io::round_up_to_chunks_groups, ChunkNum, ChunkRanges};

    use crate::protocol::ChunkRangesExt;
    use bytes::Bytes;
    use n0_future::{stream, Stream, StreamExt};
    use n0_tracing_test::traced_test;
    use testresult::TestResult;
    use walkdir::WalkDir;

    use super::*;
    use std::sync::atomic::Ordering;

    use bao_tree::io::sync::ReadAt;

    use tokio::task::JoinSet;

    use crate::{
        api::blobs::{AddPathOptions, Bitfield, ImportMode},
        store::{
            util::{read_checksummed, tests::create_n0_bao, SliceInfoExt, Tag},
            IROH_BLOCK_SIZE,
        },
        BlobFormat,
    };
    use n0_future::io;

    use crate::{
        api,
        store::engine::{
            actor::{Actor, ClearScope},
            entity::EntityApi,
            entity_manager::SpawnArg,
        },
        Hash, HashAndFormat,
    };

    /// The fs backend for a store built by hand, with its data directory created.
    fn fs_backend(db_path: PathBuf, options: &Options) -> io::Result<FsBackend> {
        fs::create_dir_all(&options.path.data_path)?;
        Ok(FsBackend::new(db_path, options))
    }

    /// Interesting sizes for testing.
    pub const INTERESTING_SIZES: [usize; 8] = [
        0,               // annoying corner case - always present, handled by the api
        1,               // less than 1 chunk, data inline, outboard not needed
        1024,            // exactly 1 chunk, data inline, outboard not needed
        1024 * 16 - 1,   // less than 1 chunk group, data inline, outboard not needed
        1024 * 16,       // exactly 1 chunk group, data inline, outboard not needed
        1024 * 16 + 1,   // data file, outboard inline (just 1 hash pair)
        1024 * 1024,     // data file, outboard inline (many hash pairs)
        1024 * 1024 * 8, // data file, outboard file
    ];

    /// A write to an entity that a read loaded sees a delete that happened in between.
    ///
    /// The observer keeps the entity loaded for reading while the blob is
    /// deleted. If the write took the entity's old state, the import would find
    /// the blob complete and do nothing, and the blob would be gone.
    #[tokio::test]
    #[traced_test]
    async fn write_after_read_sees_a_delete() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let store = FsStore::load(testdir.path().join("db")).await?;
        let data = test_data(100_000);
        let hash = store.add_bytes(data.clone()).await?.hash;
        let bao = store
            .export_bao(hash, ChunkRanges::all())
            .bao_to_vec()
            .await?;
        let mut observer = std::pin::pin!(store.observe(hash).stream().await?);
        let first = observer.next().await.expect("a first bitfield");
        assert!(first.is_complete());

        store
            .blobs()
            .delete_with_opts(api::blobs::DeleteOptions {
                hashes: vec![hash],
                force: true,
            })
            .await?;
        assert!(matches!(
            store.status(hash).await?,
            api::blobs::BlobStatus::NotFound
        ));

        store
            .import_bao_bytes(hash, ChunkRanges::all(), bao)
            .await?;
        assert!(matches!(
            store.status(hash).await?,
            api::blobs::BlobStatus::Complete { size: 100_000 }
        ));
        assert_eq!(store.get_bytes(hash).await?.as_ref(), data.as_ref());
        // The observer never sees the blob shrink, its stream ends instead.
        let rest =
            tokio::time::timeout(Duration::from_secs(5), observer.collect::<Vec<_>>()).await?;
        assert!(rest.iter().all(|bitfield| bitfield.is_complete()));
        store.shutdown().await?;
        Ok(())
    }

    /// Disconnecting a GC client must not resume tags ahead of an accepted
    /// delete. Hold its entity frozen independently to make that race explicit.
    #[tokio::test]
    async fn gc_cancel_waits_for_accepted_delete() -> TestResult<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let dir = tempfile::tempdir()?;
            let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(32);
            let (fs_tx, fs_rx) = tokio::sync::mpsc::channel(32);
            let options = Options::new(dir.path());
            let mut actor = Actor::<FsBackend>::new(
                cmd_rx,
                fs_rx,
                fs_tx,
                Arc::new(StoreOptions::from(&options)),
                fs_backend(dir.path().join("blobs.db"), &options)?,
            )
            .await?;
            let mut deletes = actor.deletes.subscribe();
            let hash = Hash::new("held deletion");
            let (ack, frozen) = tokio::sync::oneshot::channel();
            let (release, release_rx) = tokio::sync::watch::channel(());
            if let Some(task) = actor
                .handles
                .spawn(hash, move |arg| async move {
                    if let SpawnArg::Active(ctx) = arg {
                        ctx.state.freeze(&ctx.global, ack, release_rx).await;
                    }
                })
                .await
            {
                actor.spawn(task);
            }
            let actor_task = tokio::spawn(actor.run());
            frozen.await?;
            let store = Store::from_client(cmd_tx);
            // A raw client, to send a delete and disconnect without its reply.
            let (updates, mut events) = store
                .client()
                .bidi_streaming(proto::GcRunRequest, 1, 1)
                .await?;
            let Some(proto::GcEvent::Snapshot(Ok(_))) = events.recv().await? else {
                panic!("expected the gc snapshot");
            };
            updates.send(proto::GcRunUpdate::Delete(vec![hash])).await?;
            while *deletes.borrow_and_update() == 0 {
                deletes.changed().await?;
            }
            drop(updates);
            drop(events);
            let tag_store = store.clone();
            let mut tag = tokio::spawn(async move { tag_store.tags().temp_tag(hash).await });
            assert!(tokio::time::timeout(Duration::from_millis(30), &mut tag)
                .await
                .is_err());
            drop(release);
            let _tag = tag.await??;
            assert!(!store.has(hash).await?);
            store.shutdown().await?;
            actor_task.await?;
            TestResult::Ok(())
        })
        .await??;
        Ok(())
    }

    /// A last task can finish after its follow-up queue was polled empty.
    #[tokio::test]
    async fn test_idle_after_last_task_followup() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(32);
        let (fs_tx, fs_rx) = tokio::sync::mpsc::channel(32);
        let options = Options::new(dir.path());
        let mut actor = Actor::<FsBackend>::new(
            cmd_rx,
            fs_rx,
            fs_tx.clone(),
            Arc::new(StoreOptions::from(&options)),
            fs_backend(dir.path().join("blobs.db"), &options)?,
        )
        .await?;
        let (idle_tx, idle_rx) = irpc::channel::oneshot::channel();
        actor.idle_waiters.push(idle_tx);
        let (scope, _) = actor.temp_tags.create_scope();
        actor.spawn(async move {
            fs_tx.send(ClearScope { scope }.into()).await.unwrap();
        });
        // Start at the race's outcome: the last task is joined, but its
        // follow-up is queued. Processing it does not spawn another task.
        actor.tasks.join_next().await.unwrap()?;
        assert!(!actor.fs_cmd_rx.is_empty());
        let task = tokio::spawn(actor.run());
        tokio::time::timeout(Duration::from_secs(5), idle_rx).await??;
        drop(cmd_tx);
        task.await?;
        Ok(())
    }

    pub fn round_up_request(size: u64, ranges: &ChunkRanges) -> ChunkRanges {
        let last_chunk = ChunkNum::chunks(size);
        let data_range = ChunkRanges::from(..last_chunk);
        let ranges = if !data_range.intersects(ranges) && !ranges.is_empty() {
            if last_chunk == 0 {
                ChunkRanges::all()
            } else {
                ChunkRanges::from(last_chunk - 1..)
            }
        } else {
            ranges.clone()
        };
        round_up_to_chunks_groups(ranges, IROH_BLOCK_SIZE)
    }

    fn create_n0_bao_full(
        data: &[u8],
        ranges: &ChunkRanges,
    ) -> n0_error::Result<(Hash, ChunkRanges, Vec<u8>)> {
        let ranges = round_up_request(data.len() as u64, ranges);
        let (hash, encoded) = create_n0_bao(data, &ranges)?;
        Ok((hash, ranges, encoded))
    }

    #[tokio::test]
    #[traced_test]
    async fn test_observe() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options).await?;
        let sizes = INTERESTING_SIZES;
        for size in sizes {
            let data = test_data(size);
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            let obs = store.observe(hash);
            let task = n0_future::task::spawn(async move {
                obs.await_completion().await?;
                api::Result::Ok(())
            });
            store.import_bao_bytes(hash, ranges, bao).await?;
            task.await??;
        }
        Ok(())
    }

    /// Generate test data for size n.
    ///
    /// We don't really care about the content, since we assume blake3 works.
    /// The only thing it should not be is all zeros, since that is what you
    /// will get for a gap.
    pub fn test_data(n: usize) -> Bytes {
        let mut res = Vec::with_capacity(n);
        // Using uppercase A-Z (65-90), 26 possible characters
        for i in 0..n {
            // Change character every 1024 bytes
            let block_num = i / 1024;
            // Map to uppercase A-Z range (65-90)
            let ascii_val = 65 + (block_num % 26) as u8;
            res.push(ascii_val);
        }
        Bytes::from(res)
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    #[traced_test]
    async fn test_import_byte_stream() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = Hash::new(&expected);
            let stream = bytes_to_stream(expected.clone(), 1023);
            let obs = store.observe(expected_hash);
            let tt = store.add_stream(stream).await.temp_tag().await?;
            assert_eq!(expected_hash, tt.hash());
            // we must at some point see completion, otherwise the test will hang
            obs.await_completion().await?;
            let actual = store.get_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
        }
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    #[traced_test]
    async fn test_import_bytes_simple() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(&db_dir).await?;
        let sizes = INTERESTING_SIZES;
        trace!("{}", Options::new(&db_dir).is_inlined_data(16385));
        for size in sizes {
            let expected = test_data(size);
            let expected_hash = Hash::new(&expected);
            let obs = store.observe(expected_hash);
            let tt = store.add_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, tt.hash);
            // we must at some point see completion, otherwise the test will hang
            obs.await_completion().await?;
            let actual = store.get_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
        }
        store.shutdown().await?;
        dump_dir_full(db_dir)?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    #[traced_test]
    async fn test_roundtrip_bytes_small() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES
            .into_iter()
            .filter(|x| *x != 0 && *x <= IROH_BLOCK_SIZE.bytes())
        {
            let expected = test_data(size);
            let expected_hash = Hash::new(&expected);
            // An active observer keeps the handle alive. Otherwise it is persisted
            // when idle, and the export reloads the data as a new instance.
            let mut obs = std::pin::pin!(store.observe(expected_hash).stream().await?);
            obs.next().await;
            let tt = store.add_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, tt.hash);
            let actual = store.get_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual);
            assert_eq!(
                &expected.addr(),
                &actual.addr(),
                "address mismatch for size {size}"
            );
        }
        store.shutdown().await?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    #[traced_test]
    async fn test_import_path() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = Hash::new(&expected);
            let path = testdir.path().join(format!("in-{size}"));
            fs::write(&path, &expected)?;
            let obs = store.observe(expected_hash);
            let tt = store.add_path(&path).await?;
            assert_eq!(expected_hash, tt.hash);
            // we must at some point see completion, otherwise the test will hang
            obs.await_completion().await?;
            let actual = store.get_bytes(expected_hash).await?;
            // check that the data is there
            assert_eq!(&expected, &actual, "size={size}");
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    // import data via import_bytes, check that we can observe it and that it is complete
    #[tokio::test]
    #[traced_test]
    async fn test_export_path() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        for size in INTERESTING_SIZES {
            let expected = test_data(size);
            let expected_hash = Hash::new(&expected);
            let tt = store.add_bytes(expected.clone()).await?;
            assert_eq!(expected_hash, tt.hash);
            let out_path = testdir.path().join(format!("out-{size}"));
            store.export(expected_hash, &out_path).await?;
            let actual = fs::read(&out_path)?;
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_ranges() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            let data = test_data(100000);
            let ranges = ChunkRanges::chunks(16..32);
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            store
                .import_bao_bytes(hash, ranges.clone(), bao.clone())
                .await?;
            let bitfield = store.observe(hash).await?;
            assert_eq!(bitfield.ranges, ranges);
            assert_eq!(bitfield.size(), data.len() as u64);
            let export = store.export_bao(hash, ranges).bao_to_vec().await?;
            assert_eq!(export, bao);
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_minimal() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = [1];
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = vec![0u8; size];
                let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
                let data = Bytes::from(encoded);
                store
                    .import_bao_bytes(hash, ChunkRanges::all(), data)
                    .await?;
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_simple() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = [1048576];
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = vec![0u8; size];
                let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
                let data = Bytes::from(encoded);
                trace!("importing size={}", size);
                store
                    .import_bao_bytes(hash, ChunkRanges::all(), data)
                    .await?;
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_persistence_full() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = vec![0u8; size];
                let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
                let data = Bytes::from(encoded);
                store
                    .import_bao_bytes(hash, ChunkRanges::all(), data)
                    .await?;
            }
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let expected = vec![0u8; size];
                let hash = Hash::new(&expected);
                let actual = store
                    .export_bao(hash, ChunkRanges::all())
                    .data_to_vec()
                    .await?;
                assert_eq!(&expected, &actual);
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_persistence_just_size() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        let just_size = ChunkRanges::last_chunk();
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, expected) = create_n0_bao_full(&data, &just_size)?;
                let actual = match store.export_bao(hash, ranges).bao_to_vec().await {
                    Ok(actual) => actual,
                    Err(cause) => panic!("failed to export size={size}: {cause}"),
                };
                assert_eq!(&expected, &actual);
            }
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_persistence_two_stages() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        let just_size = ChunkRanges::last_chunk();
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let remaining = ChunkRanges::all() - round_up_request(size as u64, &just_size);
                if remaining.is_empty() {
                    continue;
                }
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &remaining)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        // check if the data is complete
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, expected) = create_n0_bao_full(&data, &ChunkRanges::all())?;
                let actual = match store.export_bao(hash, ranges).bao_to_vec().await {
                    Ok(actual) => actual,
                    Err(cause) => panic!("failed to export size={size}: {cause}"),
                };
                assert_eq!(&expected, &actual);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        Ok(())
    }

    fn just_size() -> ChunkRanges {
        ChunkRanges::last_chunk()
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_persistence_observe() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        let just_size = just_size();
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load(&db_dir).await?;
            for size in sizes {
                let expected_ranges = round_up_request(size as u64, &just_size);
                let data = test_data(size);
                let hash = Hash::new(&data);
                let bitfield = store.observe(hash).await?;
                assert_eq!(bitfield.ranges, expected_ranges);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_persistence_recover() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let just_size = just_size();
        // stage 1, import just the last full chunk group to get a validated size
        {
            let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
            for size in sizes {
                let data = test_data(size);
                let (hash, ranges, encoded) = create_n0_bao_full(&data, &just_size)?;
                let data = Bytes::from(encoded);
                if let Err(cause) = store.import_bao_bytes(hash, ranges, data).await {
                    panic!("failed to import size={size}: {cause}");
                }
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        delete_rec(testdir.path(), "bitfield")?;
        dump_dir_full(testdir.path())?;
        // stage 2, import the rest
        {
            let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
            for size in sizes {
                let expected_ranges = round_up_request(size as u64, &just_size);
                let data = test_data(size);
                let hash = Hash::new(&data);
                let bitfield = store.observe(hash).await?;
                assert_eq!(bitfield.ranges, expected_ranges, "size={size}");
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        Ok(())
    }

    /// Observing an entry whose data file is missing must end the stream
    /// instead of panicking.
    #[tokio::test]
    #[traced_test]
    async fn test_observe_invalid_missing_data_file() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let data = test_data(1024 * 16 + 1);
        let hash = Hash::new(&data);
        {
            let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
            store.add_bytes(data.clone()).await?;
            store.shutdown().await?;
        }
        std::fs::remove_file(options.path.data_path(&hash))?;
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options).await?;
        assert!(store.observe(hash).await.is_err());
        // the entity for the hash must still be alive, so re-adding works
        store.add_bytes(data).await?;
        store.shutdown().await?;
        Ok(())
    }

    /// Importing into an entry whose data file is missing must fail.
    #[tokio::test]
    #[traced_test]
    async fn test_import_bao_invalid_missing_data_file() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let data = test_data(1024 * 16 + 1);
        let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
        {
            let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
            store.add_bytes(data).await?;
            store.shutdown().await?;
        }
        std::fs::remove_file(options.path.data_path(&hash))?;
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options).await?;
        let res = store
            .import_bao_bytes(hash, ChunkRanges::all(), encoded)
            .await;
        assert!(res.is_err());
        store.shutdown().await?;
        Ok(())
    }

    /// An observer must see the stream end when a write makes the entry invalid.
    #[cfg(unix)]
    #[tokio::test]
    #[traced_test]
    async fn test_observe_ends_when_write_invalidates() -> TestResult<()> {
        use std::os::unix::fs::PermissionsExt;

        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options).await?;
        // just over the inline limit, so the write has to create the data file
        let data = test_data(1024 * 16 + 1);
        let (hash, encoded) = create_n0_bao(&data, &ChunkRanges::all())?;
        let mut stream = Box::pin(store.observe(hash).stream().await?);
        assert!(stream.next().await.is_some());
        // make it impossible to create the data file
        let data_dir = db_dir.join("data");
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o555))?;
        if std::fs::File::create(data_dir.join("probe")).is_ok() {
            // running as root, permissions are not enforced
            return Ok(());
        }
        let res = store
            .import_bao_bytes(hash, ChunkRanges::all(), encoded)
            .await;
        let next = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await;
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755))?;
        assert!(res.is_err());
        assert!(matches!(next, Ok(None)));
        Ok(())
    }

    /// Re-adding content in place from a new path must work after the old external file was deleted.
    #[tokio::test]
    #[traced_test]
    async fn test_reference_new_path_after_external_file_deleted() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_path = testdir.path().join("db").join("blobs.db");
        let options = Options::new(&testdir.path().join("db"));
        let data = test_data(1024 * 64);
        let hash = Hash::new(&data);
        // the old path sorts before the new one, so it is the first one in the db entry
        let old = testdir.path().join("a.bin");
        let new = testdir.path().join("b.bin");
        std::fs::write(&old, &data)?;
        std::fs::write(&new, &data)?;
        let reference = |path: &Path| AddPathOptions {
            path: path.to_owned(),
            mode: ImportMode::TryReference,
            format: BlobFormat::Raw,
        };
        let store = FsStore::load_with_opts(db_path.clone(), options.clone()).await?;
        store.add_path_with_opts(reference(&old)).await?;
        store.shutdown().await?;
        std::fs::remove_file(&old)?;
        let store = FsStore::load_with_opts(db_path.clone(), options.clone()).await?;
        store.add_path_with_opts(reference(&new)).await?;
        assert_eq!(store.get_bytes(hash).await?, data);
        store.shutdown().await?;
        let store = FsStore::load_with_opts(db_path, options).await?;
        assert_eq!(store.get_bytes(hash).await?, data);
        store.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_import_bytes_persistence_full() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let sizes = INTERESTING_SIZES;
        let db_dir = testdir.path().join("db");
        {
            let store = FsStore::load(&db_dir).await?;
            let mut tts = Vec::new();
            for size in sizes {
                let data = test_data(size);
                let data = data;
                tts.push(store.add_bytes(data.clone()).await?);
            }
            store.dump().await?;
            store.shutdown().await?;
        }
        {
            let store = FsStore::load(&db_dir).await?;
            store.dump().await?;
            for size in sizes {
                let expected = test_data(size);
                let hash = Hash::new(&expected);
                let Ok(actual) = store
                    .export_bao(hash, ChunkRanges::all())
                    .data_to_vec()
                    .await
                else {
                    panic!("failed to export size={size}");
                };
                assert_eq!(&expected, &actual, "size={size}");
            }
            store.shutdown().await?;
        }
        Ok(())
    }

    async fn test_batch(store: &Store) -> TestResult<()> {
        let batch = store.blobs().batch().await?;
        let tt1 = batch.temp_tag(Hash::new("foo")).await?;
        let tt2 = batch.add_slice("boo").await?;
        let tts = store
            .tags()
            .list_temp_tags()
            .await?
            .map(|item| item.unwrap())
            .collect::<HashSet<_>>()
            .await;
        assert!(tts.contains(&tt1.hash_and_format()));
        assert!(tts.contains(&tt2.hash_and_format()));
        drop(batch);
        store.sync_db().await?;
        store.wait_idle().await?;
        let tts = store
            .tags()
            .list_temp_tags()
            .await?
            .map(|item| item.unwrap())
            .collect::<HashSet<_>>()
            .await;
        // temp tag went out of scope, so it does not work anymore
        assert!(!tts.contains(&tt1.hash_and_format()));
        assert!(!tts.contains(&tt2.hash_and_format()));
        drop(tt1);
        drop(tt2);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_batch_fs() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        test_batch(&store).await
    }

    #[tokio::test]
    #[traced_test]
    async fn smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let store = FsStore::load(db_dir).await?;
        let haf = HashAndFormat::raw(Hash::from([0u8; 32]));
        store.tags().set(Tag::from("test"), haf).await?;
        store.tags().set(Tag::from("boo"), haf).await?;
        store.tags().set(Tag::from("bar"), haf).await?;
        let sizes = INTERESTING_SIZES;
        let mut hashes = Vec::new();
        let mut data_by_hash = HashMap::new();
        let mut bao_by_hash = HashMap::new();
        for size in sizes {
            let data = vec![0u8; size];
            let data = Bytes::from(data);
            let tt = store.add_bytes(data.clone()).temp_tag().await?;
            data_by_hash.insert(tt.hash(), data);
            hashes.push(tt);
        }
        store.sync_db().await?;
        for tt in &hashes {
            let hash = tt.hash();
            let path = testdir.path().join(format!("{hash}.txt"));
            store.export(hash, path).await?;
        }
        for tt in &hashes {
            let hash = tt.hash();
            let data = store
                .export_bao(hash, ChunkRanges::all())
                .data_to_vec()
                .await
                .unwrap();
            assert_eq!(data, data_by_hash[&hash].to_vec());
            let bao = store
                .export_bao(hash, ChunkRanges::all())
                .bao_to_vec()
                .await
                .unwrap();
            bao_by_hash.insert(hash, bao);
        }
        store.dump().await?;

        for size in sizes {
            let data = test_data(size);
            let ranges = ChunkRanges::all();
            let (hash, bao) = create_n0_bao(&data, &ranges)?;
            store.import_bao_bytes(hash, ranges, bao).await?;
        }

        for (_hash, _bao_tree) in bao_by_hash {
            // let mut reader = Cursor::new(bao_tree);
            // let size = reader.read_u64_le().await?;
            // let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
            // let ranges = ChunkRanges::all();
            // let mut decoder = DecodeResponseIter::new(hash, tree, reader, &ranges);
            // while let Some(item) = decoder.next() {
            //     let item = item?;
            // }
            // store.import_bao_bytes(hash, ChunkRanges::all(), bao_tree.into()).await?;
        }
        Ok(())
    }

    /// An import that does not complete must not leave its temp files behind.
    #[tokio::test]
    #[traced_test]
    async fn test_incomplete_import_removes_temp_files() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
        let temp_files =
            || -> io::Result<usize> { Ok(std::fs::read_dir(&options.path.temp_path)?.count()) };
        let size = 1024 * 1024 * 4;
        // cancelled after the first progress item
        let mut stream = store.add_bytes(vec![1u8; size]).stream().await;
        stream.next().await;
        drop(stream);
        store.wait_idle().await?;
        assert_eq!(temp_files()?, 0, "cancelled add_bytes");
        // cancelled after the first progress item
        let path = testdir.path().join("source");
        std::fs::write(&path, vec![2u8; size])?;
        let mut stream = store.add_path(&path).stream().await;
        stream.next().await;
        drop(stream);
        store.wait_idle().await?;
        assert_eq!(temp_files()?, 0, "cancelled add_path");
        // the source fails after some data
        let source = stream::iter([
            Ok(Bytes::from(vec![3u8; size])),
            Err(io::Error::other("source failed")),
        ]);
        assert!(store.add_stream(source).await.temp_tag().await.is_err());
        store.wait_idle().await?;
        assert_eq!(temp_files()?, 0, "failed add_stream");
        Ok(())
    }

    /// Loading a store removes temp files left over by a previous run, and nothing else.
    #[tokio::test]
    #[traced_test]
    async fn test_load_removes_leftover_temp_files() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let db_path = db_dir.join("blobs.db");
        let store = FsStore::load_with_opts(db_path.clone(), options.clone()).await?;
        let data = vec![1u8; 1024 * 1024];
        let hash = store.add_slice(&data).await?.hash;
        store.shutdown().await?;
        // as left by a process that was killed during an import
        let leftover = options.path.temp_file_name();
        std::fs::write(&leftover, b"leftover")?;
        let store = FsStore::load_with_opts(db_path, options.clone()).await?;
        assert!(!leftover.exists());
        assert_eq!(std::fs::read_dir(&options.path.temp_path)?.count(), 0);
        assert_eq!(store.get_bytes(hash).await?.as_ref(), data.as_slice());
        store.shutdown().await?;
        Ok(())
    }

    /// When a partial entry completes, the files the complete entry does not use are
    /// removed, e.g. the data or outboard file when it gets inlined.
    #[tokio::test]
    #[traced_test]
    async fn test_complete_removes_unused_files() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
        let path = &options.path;
        // 8 KiB gets inlined data, 1 MiB its own data file and an inlined outboard
        for (size, by_import) in [
            (8 * 1024, false),
            (8 * 1024 + 1, true),
            (1024 * 1024, false),
            (1024 * 1024 + 1, true),
        ] {
            let data = test_data(size);
            let half = ChunkRanges::from(..ChunkNum(4));
            let (hash, bao) = create_n0_bao(&data, &half)?;
            store.import_bao_bytes(hash, half, bao).await?;
            store.wait_idle().await?;
            assert!(path.data_path(&hash).exists());
            assert!(path.outboard_path(&hash).exists());
            if by_import {
                // an import of the whole blob over the partial entry
                drop(store.add_bytes(data.clone()).temp_tag().await?);
            } else {
                // the rest of the blob through the partial entry
                let rest = ChunkRanges::from(ChunkNum(4)..);
                let (_, bao) = create_n0_bao(&data, &rest)?;
                store.import_bao_bytes(hash, rest, bao).await?;
            }
            // The files go after the commit, as unreferenced parts the entity deletes.
            store.sync_db().await?;
            store.wait_idle().await?;
            let inlined = size <= options.inline.max_data_inlined as usize;
            assert_eq!(path.data_path(&hash).exists(), !inlined, "size={size}");
            assert!(!path.outboard_path(&hash).exists(), "by_import={by_import}");
            assert!(!path.sizes_path(&hash).exists(), "by_import={by_import}");
            assert!(!path.bitfield_path(&hash).exists(), "by_import={by_import}");
            assert_eq!(store.get_bytes(hash).await?, data);
        }
        store.shutdown().await?;
        Ok(())
    }

    /// Deleting an observed entry ends the observer, and its files are gone once the delete returns.
    #[tokio::test]
    #[traced_test]
    async fn test_delete_ends_observers_and_removes_files() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let db_dir = testdir.path().join("db");
        let options = Options::new(&db_dir);
        let store = FsStore::load_with_opts(db_dir.join("blobs.db"), options.clone()).await?;
        let path = &options.path;
        let data = test_data(1024 * 1024);
        let half = ChunkRanges::from(..ChunkNum(4));
        let (hash, bao) = create_n0_bao(&data, &half)?;
        store.import_bao_bytes(hash, half, bao).await?;
        store.wait_idle().await?;
        let files = [
            path.data_path(&hash),
            path.outboard_path(&hash),
            path.sizes_path(&hash),
        ];
        assert!(files.iter().all(|f| f.exists()));
        let mut obs = Box::pin(store.observe(hash).stream().await?);
        obs.next().await.expect("initial bitfield");
        store
            .blobs()
            .delete_with_opts(crate::api::proto::BlobDeleteRequest {
                hashes: vec![hash],
                force: true,
            })
            .await?;
        assert!(
            files.iter().all(|f| !f.exists()),
            "the delete returns after the files are gone"
        );
        assert!(!path.bitfield_path(&hash).exists());
        let rest = tokio::time::timeout(Duration::from_secs(5), obs.collect::<Vec<_>>()).await?;
        assert!(rest.is_empty(), "the observer ends instead of seeing less");
        store.shutdown().await?;
        Ok(())
    }

    /// Unreferenced parts that arrive after the entry was recreated and its entity unloaded do not delete the new files.
    #[tokio::test]
    #[traced_test]
    async fn test_stale_unreferenced_parts_keep_recreated_files() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(32);
        let (fs_tx, fs_rx) = tokio::sync::mpsc::channel(32);
        let mut options = Options::new(dir.path());
        options.inline.max_data_inlined = 64 * 1024;
        let options = Arc::new(options);
        let mut actor = Actor::<FsBackend>::new(
            cmd_rx,
            fs_rx,
            fs_tx,
            Arc::new(StoreOptions::from(&*options)),
            fs_backend(dir.path().join("blobs.db"), &options)?,
        )
        .await?;
        // Hold back the notices; metadata and entities run normally.
        let (deliver, delayed) = tokio::sync::mpsc::unbounded_channel();
        let mut notices = std::mem::replace(&mut actor.unreferenced, delayed);
        let actor_task = tokio::spawn(actor.run());
        let store = Store::from_client(cmd_tx);
        let path = &options.path;
        // Two chunk groups: partial on disk, inlined once complete. Whole
        // groups, so the partial entry can be exported.
        let data = test_data(32 * 1024);
        let half = ChunkRanges::from(..ChunkNum(16));
        let rest = ChunkRanges::from(ChunkNum(16)..);
        let (hash, half_bao) = create_n0_bao(&data, &half)?;
        let (_, rest_bao) = create_n0_bao(&data, &rest)?;
        store
            .import_bao_bytes(hash, half.clone(), half_bao.clone())
            .await?;
        store.sync_db().await?;
        store.import_bao_bytes(hash, rest, rest_bao).await?;
        store.sync_db().await?;
        // Partial to complete and inline: the data file is no longer referenced.
        let notice = notices.recv().await.expect("notice");
        assert_eq!(notice.0, hash);
        assert!(notice.1 .0.contains(&metadata::parts::BaoFilePart::Data));
        store
            .blobs()
            .delete_with_opts(crate::api::proto::BlobDeleteRequest {
                hashes: vec![hash],
                force: true,
            })
            .await?;
        // Partial again, with a new data file, and no entity in memory.
        store
            .import_bao_bytes(hash, half.clone(), half_bao.clone())
            .await?;
        store.sync_db().await?;
        store.wait_idle().await?;
        assert!(path.data_path(&hash).exists());
        // The notice from before the delete arrives now.
        deliver.send(notice)?;
        store.wait_idle().await?;
        assert!(
            path.data_path(&hash).exists(),
            "a stale notice deleted the data of the recreated entry"
        );
        let exported = store.export_bao(hash, half).bao_to_vec().await?;
        assert_eq!(exported, half_bao);
        store.shutdown().await?;
        actor_task.await?;
        Ok(())
    }

    /// Deleting an entry whose entity is not loaded removes the files the entry referenced.
    #[tokio::test]
    #[traced_test]
    async fn test_delete_unloaded_entry_removes_files() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let options = Options::new(dir.path());
        let store = FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()).await?;
        let path = &options.path;
        // 1 MiB: an owned data file and an inlined outboard
        let data = test_data(1024 * 1024);
        let hash = store.add_bytes(data).await?.hash;
        store.sync_db().await?;
        store.wait_idle().await?;
        assert!(path.data_path(&hash).exists());
        store
            .blobs()
            .delete_with_opts(crate::api::proto::BlobDeleteRequest {
                hashes: vec![hash],
                force: true,
            })
            .await?;
        assert!(!path.data_path(&hash).exists());
        store.shutdown().await?;
        Ok(())
    }

    /// No metadata row is still a completed delete, including observer teardown.
    #[tokio::test]
    async fn test_delete_absent_entry_completes_and_can_be_reimported() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let options = Options::new(dir.path());
        let store = FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()).await?;
        let data = test_data(100_000);
        let hash = Hash::new(&data);
        let mut observer = Box::pin(store.observe(hash).stream().await?);
        assert!(observer.next().await.is_some());
        tokio::time::timeout(
            Duration::from_secs(5),
            store.blobs().delete(vec![hash, hash]),
        )
        .await??;
        assert!(
            tokio::time::timeout(Duration::from_secs(5), observer.next())
                .await?
                .is_none()
        );
        assert_eq!(store.add_bytes(data.clone()).await?.hash, hash);
        assert_eq!(store.get_bytes(hash).await?, data);
        store.shutdown().await?;
        Ok(())
    }

    /// Requests that reach the store after its shutdown must not panic its tasks,
    /// e.g. on an entry that the shutdown made invalid.
    #[tokio::test]
    #[traced_test]
    async fn test_requests_after_shutdown_do_not_panic() -> TestResult<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static PANICS: AtomicUsize = AtomicUsize::new(0);
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.to_string().contains("should not be used") {
                PANICS.fetch_add(1, Ordering::SeqCst);
            }
            prev(info);
        }));
        let testdir = tempfile::tempdir()?;
        let store = FsStore::load(testdir.path().join("db")).await?;
        let complete = store.add_bytes(test_data(1024 * 1024)).await?.hash;
        let unknown = Hash::new(b"unknown");
        store.observe(complete).await?;
        store.shutdown().await?;
        for hash in [complete, unknown] {
            let timeout = Duration::from_secs(5);
            for _ in 0..2 {
                assert!(tokio::time::timeout(
                    timeout,
                    store.blobs().delete_with_opts(api::blobs::DeleteOptions {
                        hashes: vec![hash],
                        force: true,
                    })
                )
                .await?
                .is_err());
            }
            tokio::time::timeout(timeout, store.observe(hash))
                .await
                .ok();
            tokio::time::timeout(
                timeout,
                store.export_bao(hash, ChunkRanges::all()).bao_to_vec(),
            )
            .await
            .ok();
        }
        assert_eq!(PANICS.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// A failed load must return the error instead of hanging in the drop of the store runtime.
    #[tokio::test]
    #[traced_test]
    async fn test_load_root_is_file() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let root = testdir.path().join("file");
        std::fs::write(&root, b"not a directory")?;
        let res = tokio::time::timeout(Duration::from_secs(10), FsStore::load(&root)).await?;
        assert!(res.is_err());
        Ok(())
    }

    /// A failed load must return the error instead of hanging in the drop of the store runtime.
    #[tokio::test]
    #[traced_test]
    async fn test_load_already_open() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let root = testdir.path().join("db");
        let _store = FsStore::load(&root).await?;
        let res = tokio::time::timeout(Duration::from_secs(10), FsStore::load(&root)).await?;
        assert!(res.is_err());
        Ok(())
    }

    /// Dropping stores concurrently must not deadlock in the drop of the store runtime.
    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn test_load_drop_concurrent() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let mut tasks = JoinSet::new();
        for i in 0..4 {
            let root = testdir.path().join(format!("db{i}"));
            tasks.spawn(async move {
                for j in 0..20 {
                    let store = FsStore::load(root.join(format!("{j}"))).await?;
                    store.add_bytes(vec![0u8; 100]).await?;
                    let threads = store.threads.clone();
                    drop(store);
                    while threads.load(Ordering::SeqCst) > 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
                n0_error::Ok(())
            });
        }
        let all = tasks.join_all();
        for res in tokio::time::timeout(Duration::from_secs(30), all).await? {
            res?;
        }
        Ok(())
    }

    pub fn delete_rec(root_dir: impl AsRef<Path>, extension: &str) -> Result<(), std::io::Error> {
        // Remove leading dot if present, so we have just the extension
        let ext = extension.trim_start_matches('.').to_lowercase();

        for entry in WalkDir::new(root_dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            if path.is_file() {
                if let Some(file_ext) = path.extension() {
                    if file_ext.to_string_lossy().to_lowercase() == ext {
                        fs::remove_file(path)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn dump_dir(path: impl AsRef<Path>) -> io::Result<()> {
        let mut entries: Vec<_> = WalkDir::new(&path)
            .into_iter()
            .filter_map(Result::ok) // Skip errors
            .collect();

        // Sort by path (name at each depth)
        entries.sort_by(|a, b| a.path().cmp(b.path()));

        for entry in entries {
            let depth = entry.depth();
            let indent = "  ".repeat(depth); // Two spaces per level
            let name = entry.file_name().to_string_lossy();
            let size = entry.metadata()?.len(); // Size in bytes

            if entry.file_type().is_file() {
                println!("{indent}{name} ({size} bytes)");
            } else if entry.file_type().is_dir() {
                println!("{indent}{name}/");
            }
        }
        Ok(())
    }

    pub fn dump_dir_full(path: impl AsRef<Path>) -> io::Result<()> {
        let mut entries: Vec<_> = WalkDir::new(&path)
            .into_iter()
            .filter_map(Result::ok) // Skip errors
            .collect();

        // Sort by path (name at each depth)
        entries.sort_by(|a, b| a.path().cmp(b.path()));

        for entry in entries {
            let depth = entry.depth();
            let indent = "  ".repeat(depth);
            let name = entry.file_name().to_string_lossy();

            if entry.file_type().is_dir() {
                println!("{indent}{name}/");
            } else if entry.file_type().is_file() {
                let size = entry.metadata()?.len();
                println!("{indent}{name} ({size} bytes)");

                // Dump depending on file type
                let path = entry.path();
                if name.ends_with(".data") {
                    print!("{indent}  ");
                    dump_file(path, 1024 * 16)?;
                } else if name.ends_with(".obao4") {
                    print!("{indent}  ");
                    dump_file(path, 64)?;
                } else if name.ends_with(".sizes4") {
                    print!("{indent}  ");
                    dump_file(path, 8)?;
                } else if name.ends_with(".bitfield") {
                    match read_checksummed::<Bitfield>(path) {
                        Ok(bitfield) => {
                            println!("{indent}  bitfield: {bitfield:?}");
                        }
                        Err(cause) => {
                            println!("{indent}  bitfield: error: {cause}");
                        }
                    }
                } else {
                    continue; // Skip content dump for other files
                };
            }
        }
        Ok(())
    }

    pub fn dump_file<P: AsRef<Path>>(path: P, chunk_size: u64) -> io::Result<()> {
        let bits = file_bits(path, chunk_size)?;
        println!("{}", print_bitfield_ansi(bits));
        Ok(())
    }

    pub fn file_bits(path: impl AsRef<Path>, chunk_size: u64) -> io::Result<Vec<bool>> {
        let file = fs::File::open(&path)?;
        let file_size = file.metadata()?.len();
        let mut buffer = vec![0u8; chunk_size as usize];
        let mut bits = Vec::new();

        let mut offset = 0u64;
        while offset < file_size {
            let remaining = file_size - offset;
            let current_chunk_size = chunk_size.min(remaining);

            let chunk = &mut buffer[..current_chunk_size as usize];
            file.read_exact_at(offset, chunk)?;

            let has_non_zero = chunk.iter().any(|&byte| byte != 0);
            bits.push(has_non_zero);

            offset += current_chunk_size;
        }

        Ok(bits)
    }

    #[allow(dead_code)]
    fn print_bitfield(bits: impl IntoIterator<Item = bool>) -> String {
        bits.into_iter()
            .map(|bit| if bit { '#' } else { '_' })
            .collect()
    }

    fn print_bitfield_ansi(bits: impl IntoIterator<Item = bool>) -> String {
        let mut result = String::new();
        let mut iter = bits.into_iter();

        while let Some(b1) = iter.next() {
            let b2 = iter.next();

            // ANSI color codes
            let white_fg = "\x1b[97m"; // bright white foreground
            let reset = "\x1b[0m"; // reset all attributes
            let gray_bg = "\x1b[100m"; // bright black (gray) background
            let black_bg = "\x1b[40m"; // black background

            let colored_char = match (b1, b2) {
                (true, Some(true)) => format!("{}{}{}", white_fg, '█', reset), // 11 - solid white on default background
                (true, Some(false)) => format!("{}{}{}{}", gray_bg, white_fg, '▌', reset), // 10 - left half white on gray background
                (false, Some(true)) => format!("{}{}{}{}", gray_bg, white_fg, '▐', reset), // 01 - right half white on gray background
                (false, Some(false)) => format!("{}{}{}{}", gray_bg, white_fg, ' ', reset), // 00 - space with gray background
                (true, None) => format!("{}{}{}{}", black_bg, white_fg, '▌', reset), // 1 (pad 0) - left half white on black background
                (false, None) => format!("{}{}{}{}", black_bg, white_fg, ' ', reset), // 0 (pad 0) - space with black background
            };

            result.push_str(&colored_char);
        }

        // Ensure we end with a reset code to prevent color bleeding
        result.push_str("\x1b[0m");
        result
    }

    fn bytes_to_stream(
        bytes: Bytes,
        chunk_size: usize,
    ) -> impl Stream<Item = io::Result<Bytes>> + 'static {
        assert!(chunk_size > 0, "Chunk size must be greater than 0");
        stream::unfold((bytes, 0), move |(bytes, offset)| async move {
            if offset >= bytes.len() {
                None
            } else {
                let chunk_len = chunk_size.min(bytes.len() - offset);
                let chunk = bytes.slice(offset..offset + chunk_len);
                Some((Ok(chunk), (bytes, offset + chunk_len)))
            }
        })
    }
}
