//! The generic store actor: routing by hash, temp tags, gc sessions, imports, and shutdown.
//!
//! It runs the entities of a [`Backend`] on the entity manager and talks to
//! the backend's metadata store through the metadata protocol. See the fs
//! store's module docs for how the parts work together.
use std::{
    collections::{HashSet, VecDeque},
    fmt::{self, Debug},
    fs,
    future::Future,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bao_tree::io::mixed::EncodedItem;
use n0_error::{Result, StdResultExt};
use n0_future::{io, stream, StreamExt};
use nested_enum_utils::enum_conversions;
use tokio::task::{JoinError, JoinSet};
use tracing::{error, trace, warn, Instrument};

use super::{
    entity::{Backend, EmParams, EntityApi, EntityCtx, EntityGlobal},
    entity_manager::{self, EntityManagerState, SpawnArg},
    import::{import_byte_stream, import_bytes, import_path, ImportEntryMsg},
    metadata::{self, parts::UnreferencedParts, MetaChannels},
    options::StoreOptions,
};
use crate::{
    api::{
        self,
        blobs::{AddProgressItem, ExportProgressItem},
        proto::{
            self, BatchMsg, BatchResponse, Command, CreateTempTagMsg, ExportBaoMsg, ExportPathMsg,
            ExportRangesItem, ExportRangesMsg, HashSpecific, ImportBaoMsg, ObserveMsg, Scope,
        },
    },
    store::{
        gc::run_gc,
        util::{drain, ValueOrPoisioned},
    },
    util::temp_tag::{TagDrop, TempTag, TempTagScope, TempTags},
    Hash, HashAndFormat,
};

#[derive(Debug)]
#[enum_conversions()]
pub(crate) enum InternalCommand {
    FinishImport(ImportEntryMsg),
    ClearScope(ClearScope),
    EndGcRun(EndGcRun),
    GcDelete(GcDelete),
}

/// A gc run ended, see [`proto::GcRunRequest`].
///
/// Sessions are exclusive, so this needs no session id.
#[derive(Debug)]
pub(crate) struct EndGcRun;

/// A sweep delete from the session task of the current gc run.
#[derive(Debug)]
pub(crate) struct GcDelete {
    hashes: Vec<Hash>,
    tx: tokio::sync::oneshot::Sender<api::Result<()>>,
}

#[derive(Debug)]
pub(crate) struct ClearScope {
    pub scope: Scope,
}

impl InternalCommand {
    pub fn parent_span(&self) -> tracing::Span {
        match self {
            Self::ClearScope(_) => tracing::Span::current(),
            Self::EndGcRun(_) | Self::GcDelete(_) => tracing::Span::current(),
            Self::FinishImport(cmd) => cmd.span.clone(),
        }
    }
}

/// Context needed by most tasks
#[derive(Debug)]
pub(crate) struct TaskContext {
    // The store's own options: inlining, the temp directory, gc.
    pub options: Arc<StoreOptions>,
    // Metadata database, basically a mpsc sender with some extra functionality.
    pub db: metadata::Db,
    // Handle to send internal commands
    pub internal_cmd_tx: tokio::sync::mpsc::Sender<InternalCommand>,
}

impl TaskContext {
    pub async fn clear_scope(&self, scope: Scope) {
        self.internal_cmd_tx
            .send(ClearScope { scope }.into())
            .await
            .ok();
    }
}

/// Waits for the deletes of one request, and reports the last error.
async fn collect_deletes(
    results: Vec<tokio::sync::oneshot::Receiver<io::Result<()>>>,
) -> api::Result<()> {
    let mut res = Ok(());
    for rx in results {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(cause)) => res = Err(api::Error::from(cause)),
            Err(_) => res = Err(api::Error::other("delete task dropped")),
        }
    }
    res
}

/// Handles the sweep deletes of a gc session until the client closes it.
///
/// Each delete is awaited before the next update is read, so when this returns,
/// every delete the session received is done. Shutdown only cancels between
/// deletes.
async fn gc_session(
    tx: &irpc::channel::mpsc::Sender<proto::GcEvent>,
    rx: &mut irpc::channel::mpsc::Receiver<proto::GcRunUpdate>,
    mut cancelled: tokio::sync::oneshot::Receiver<()>,
    internal: &tokio::sync::mpsc::Sender<InternalCommand>,
) {
    loop {
        let update = tokio::select! {
            update = rx.recv() => update,
            _ = &mut cancelled => return,
        };
        let Ok(Some(proto::GcRunUpdate::Delete(hashes))) = update else {
            return;
        };
        let (res_tx, res_rx) = tokio::sync::oneshot::channel();
        let cmd = GcDelete { hashes, tx: res_tx };
        if internal.send(cmd.into()).await.is_err() {
            return;
        }
        let res = res_rx
            .await
            .unwrap_or_else(|_| Err(api::Error::other("gc delete dropped")));
        tokio::select! {
            result = tx.send(proto::GcEvent::Deleted(res)) => {
                if result.is_err() { return; }
            }
            _ = &mut cancelled => return,
        }
    }
}

#[derive(Debug)]
pub(crate) struct GcSession {
    frozen: HashSet<Hash>,
    // Only held: dropping it ends the freezes.
    _release: tokio::sync::watch::Sender<()>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Debug)]
pub(crate) enum Deferred {
    Command(Command),
    Import(ImportEntryMsg),
}

#[derive(Debug)]
pub(crate) struct Actor<B: Backend> {
    // Context that can be cheaply shared with tasks.
    pub(crate) context: Arc<TaskContext>,
    // Receiver for incoming user commands.
    pub(crate) cmd_rx: tokio::sync::mpsc::Receiver<Command>,
    // Receiver for incoming file store specific commands.
    pub(crate) fs_cmd_rx: tokio::sync::mpsc::Receiver<InternalCommand>,
    // Parts committed metadata batches stopped referencing, from the database actor.
    pub(crate) unreferenced: tokio::sync::mpsc::UnboundedReceiver<(Hash, UnreferencedParts)>,
    // A failed metadata task must abort the workers, including waiting deletes.
    pub(crate) metadata_task: Option<tokio::task::JoinHandle<Result<(), metadata::Failure>>>,
    // How many deletes are running, so shutdown can let them finish.
    pub(crate) deletes: Arc<tokio::sync::watch::Sender<usize>>,
    // Tasks for import and export operations.
    pub(crate) tasks: JoinSet<()>,
    // Entity manager that handles concurrency for entities.
    pub(crate) handles: EntityManagerState<EmParams<B>>,
    // temp tags
    pub(crate) temp_tags: TempTags,
    pub(crate) gc: Option<GcSession>,
    pub(crate) deferred: VecDeque<Deferred>,
    // waiters for idle state.
    pub(crate) idle_waiters: Vec<irpc::channel::oneshot::Sender<()>>,
    // our private tokio runtime. It has to live somewhere.
    pub(crate) _rt: RtWrapper,
}

impl<B: Backend> Actor<B> {
    fn db(&self) -> &metadata::Db {
        &self.context.db
    }

    fn context(&self) -> Arc<TaskContext> {
        self.context.clone()
    }

    pub(crate) fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        let span = tracing::Span::current();
        self.tasks.spawn(fut.instrument(span));
    }

    fn log_task_result(res: Result<(), JoinError>) {
        match res {
            Ok(_) => {}
            Err(e) => {
                error!("task failed: {e}");
            }
        }
    }

    /// Abort workers without persisting their cached state or unlinking files.
    /// On a transaction error, `result` retains the database lock until all
    /// workers have stopped, preventing a reopened store from racing them.
    async fn abort_on_metadata_failure(
        result: Result<Result<(), metadata::Failure>, JoinError>,
        tasks: &mut JoinSet<()>,
    ) -> bool {
        match &result {
            Ok(Ok(())) => return false,
            Ok(Err(failure)) => error!("metadata failed; aborting store: {:#}", failure.error),
            Err(cause) => error!("metadata task stopped; aborting store: {cause}"),
        }
        tasks.shutdown().await;
        drop(result);
        true
    }

    async fn create_temp_tag(&mut self, cmd: CreateTempTagMsg) {
        let CreateTempTagMsg { tx, inner, .. } = cmd;
        let Some(mut tt) = self.temp_tags.create(inner.scope, inner.value) else {
            // This RPC has no application-error response; closing its reply
            // reports a request error to the caller.
            return;
        };
        if tx.is_rpc() {
            tt.leak();
        }
        tx.send(tt).await.ok();
    }

    async fn handle_command(&mut self, cmd: Command) {
        if self.gc.is_some() && (cmd.mutates_tags() || matches!(cmd, Command::GcRun(_))) {
            self.deferred.push_back(Deferred::Command(cmd));
            return;
        }
        let span = cmd.span().clone();
        let _entered = span.enter();
        debug_assert_eq!(cmd.validate(), Ok(()), "protocol violation in {cmd:?}");
        match cmd {
            Command::SyncDb(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::WaitIdle(cmd) => {
                trace!("{cmd:?}");
                self.idle_waiters.push(cmd.tx);
            }
            Command::Shutdown(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::CreateTag(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::SetTag(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::ListTags(cmd) => {
                trace!("{cmd:?}");
                let request = cmd.inner.clone();
                match self.db().list_tags(cmd.span.clone(), request).await {
                    Ok(tags) => self.spawn(drain(
                        tags,
                        cmd.tx,
                        proto::ListTagsItem::Tag,
                        proto::ListTagsItem::Error,
                        proto::ListTagsItem::Done,
                    )),
                    Err(cause) => {
                        self.spawn(async move {
                            cmd.tx
                                .send(proto::ListTagsItem::Error(api::Error::other(cause)))
                                .await
                                .ok();
                        });
                    }
                }
            }
            Command::DeleteTags(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::RenameTag(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::GcRun(cmd) => self.enter_gc(cmd).await,
            Command::BlobStatus(cmd) => {
                trace!("{cmd:?}");
                self.db().send(cmd.into()).await.ok();
            }
            Command::DeleteBlobs(cmd) => {
                trace!("{cmd:?}");
                self.delete_blobs(cmd).await;
            }
            Command::ListBlobs(cmd) => {
                trace!("{cmd:?}");
                match self.db().list_blobs(cmd.span.clone()).await {
                    Ok(hashes) => self.spawn(drain(
                        hashes,
                        cmd.tx,
                        proto::ListBlobsItem::Hash,
                        proto::ListBlobsItem::Error,
                        proto::ListBlobsItem::Done,
                    )),
                    Err(cause) => {
                        self.spawn(async move {
                            cmd.tx
                                .send(proto::ListBlobsItem::Error(api::Error::other(cause)))
                                .await
                                .ok();
                        });
                    }
                }
            }
            Command::Batch(cmd) => {
                trace!("{cmd:?}");
                let (id, scope) = self.temp_tags.create_scope();
                self.spawn(handle_batch(cmd, id, scope, self.context()));
            }
            Command::CreateTempTag(cmd) => {
                trace!("{cmd:?}");
                self.create_temp_tag(cmd).await;
            }
            Command::ListTempTags(cmd) => {
                trace!("{cmd:?}");
                let tts = self.temp_tags.list().into_iter().map(Ok);
                self.spawn(drain(
                    stream::iter(tts),
                    cmd.tx,
                    proto::ListTempTagsItem::Tag,
                    proto::ListTempTagsItem::Error,
                    proto::ListTempTagsItem::Done,
                ));
            }
            Command::ImportBytes(cmd) => {
                trace!("{cmd:?}");
                self.spawn(import_bytes(cmd, self.context()));
            }
            Command::ImportByteStream(cmd) => {
                trace!("{cmd:?}");
                self.spawn(import_byte_stream(cmd, self.context()));
            }
            Command::ImportPath(cmd) => {
                trace!("{cmd:?}");
                self.spawn(import_path(cmd, self.context()));
            }
            Command::ExportPath(cmd) if cmd.hash == Hash::EMPTY => {
                self.spawn(crate::store::util::export_empty(cmd));
            }
            Command::ExportPath(cmd) => {
                trace!("{cmd:?}");
                cmd.spawn(&mut self.handles, &mut self.tasks).await;
            }
            Command::ExportBao(cmd) => {
                trace!("{cmd:?}");
                cmd.spawn(&mut self.handles, &mut self.tasks).await;
            }
            Command::ExportRanges(cmd) => {
                trace!("{cmd:?}");
                cmd.spawn(&mut self.handles, &mut self.tasks).await;
            }
            Command::ImportBao(cmd) => {
                trace!("{cmd:?}");
                cmd.spawn(&mut self.handles, &mut self.tasks).await;
            }
            Command::Observe(cmd) => {
                trace!("{cmd:?}");
                cmd.spawn(&mut self.handles, &mut self.tasks).await;
            }
        }
    }

    async fn handle_fs_command(&mut self, cmd: InternalCommand) {
        let span = cmd.parent_span();
        let _entered = span.enter();
        match cmd {
            InternalCommand::EndGcRun(_) => self.exit_gc(),
            InternalCommand::GcDelete(cmd) => self.gc_delete(cmd).await,
            InternalCommand::ClearScope(cmd) => {
                trace!("{cmd:?}");
                self.temp_tags.end_scope(cmd.scope);
            }
            InternalCommand::FinishImport(cmd) => {
                // Finishing an import both modifies an entity and creates a temp tag.
                if self.gc.is_some() {
                    self.deferred.push_back(Deferred::Import(cmd));
                    return;
                }
                trace!("{cmd:?}");
                let value = HashAndFormat {
                    hash: cmd.hash,
                    format: cmd.format,
                };
                let Some(tt) = self.temp_tags.create(cmd.scope, value) else {
                    cmd.tx
                        .send(AddProgressItem::Error(io::Error::other(
                            "batch scope has ended",
                        )))
                        .await
                        .ok();
                    return;
                };
                if cmd.hash == Hash::EMPTY {
                    // the empty blob is always present, so there is nothing to store or protect
                    cmd.tx.send(AddProgressItem::Done(tt)).await.ok();
                    return;
                }
                (tt, cmd).spawn(&mut self.handles, &mut self.tasks).await;
            }
        }
    }

    /// Collects the gc candidates, and the named tags plus the current and
    /// retained temp tags as roots.
    async fn gc_snapshot(&mut self, span: tracing::Span) -> api::Result<proto::GcSnapshot> {
        let (candidates, tags) = self.db().gc_snapshot(span).await?;
        let candidates = candidates.try_collect::<_, _, Vec<_>>().await?;
        let mut roots = tags.try_collect::<_, _, HashSet<_>>().await?;
        roots.extend(self.temp_tags.list_for_gc());
        Ok(proto::GcSnapshot {
            candidates,
            roots: roots.into_iter().collect(),
        })
    }

    async fn enter_gc(&mut self, cmd: proto::GcRunMsg) {
        self.temp_tags.start_gc_run();
        // GcSnapshot is a top-level metadata command: earlier tag mutations have
        // committed before this returns. Main has not dispatched any later ones.
        let snapshot = self.gc_snapshot(cmd.span.clone()).await;
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.temp_tags.end_gc_run();
                cmd.tx.send(proto::GcEvent::Snapshot(Err(err))).await.ok();
                return;
            }
        };
        let frozen = snapshot.hashseqs();
        let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
        let mut acks = Vec::new();
        let (release, _) = tokio::sync::watch::channel(());
        for &hash in &frozen {
            let (ack, rx) = tokio::sync::oneshot::channel();
            acks.push(rx);
            let release_rx = release.subscribe();
            if let Some(task) = self
                .handles
                .spawn(hash, move |arg| async move {
                    if let SpawnArg::Active(ctx) = arg {
                        ctx.state.freeze(&ctx.global, ack, release_rx).await;
                    }
                })
                .await
            {
                self.spawn(task);
            }
        }
        self.gc = Some(GcSession {
            frozen,
            _release: release,
            cancel: Some(cancel),
        });
        let internal = self.context.internal_cmd_tx.clone();
        self.spawn(async move {
            let proto::GcRunMsg { tx, mut rx, .. } = cmd;
            // Reply only once every root is frozen. Exiting early is still safe:
            // a freeze that lands after its release was dropped ends at once.
            let mut result = Ok(snapshot);
            for ack in acks {
                if ack.await.is_err() {
                    result = Err(api::Error::other("entity stopped while freezing"));
                }
            }
            let ok = result.is_ok();
            let delivered = tokio::select! {
                result = tx.send(proto::GcEvent::Snapshot(result)) => result.is_ok(),
                _ = &mut cancelled => false,
            };
            if delivered && ok {
                gc_session(&tx, &mut rx, cancelled, &internal).await;
            }
            // Every delete this session received is done, so it may end.
            internal.send(EndGcRun.into()).await.ok();
        });
    }

    fn exit_gc(&mut self) {
        // Dropping the session drops the release sender, which ends the freezes.
        self.gc = None;
        self.temp_tags.end_gc_run();
    }

    /// Runs a sweep delete of the current gc session.
    async fn gc_delete(&mut self, cmd: GcDelete) {
        let GcDelete { hashes, tx } = cmd;
        let Some(gc) = &self.gc else {
            // The session task sends this, and the session ends after it.
            tx.send(Err(api::Error::other("no gc session"))).ok();
            return;
        };
        // A sweep must never wait on its own frozen roots.
        if hashes.iter().any(|hash| gc.frozen.contains(hash)) {
            tx.send(Err(api::Error::other("cannot sweep a frozen GC root")))
                .ok();
            return;
        }
        let results = self.spawn_deletes(hashes, false).await;
        self.spawn(async move {
            tx.send(collect_deletes(results).await).ok();
        });
    }

    pub(crate) async fn run(mut self) {
        let mut closing = false;
        let mut shutdown = None;
        loop {
            if self.gc.is_none() {
                if closing {
                    break;
                }
                if let Some(cmd) = self.deferred.pop_front() {
                    match cmd {
                        Deferred::Command(cmd) => self.handle_command(cmd).await,
                        Deferred::Import(cmd) => self.handle_fs_command(cmd.into()).await,
                    }
                    continue;
                }
            }

            tokio::select! {
                result = async { self.metadata_task.as_mut().unwrap().await },
                    if self.metadata_task.is_some() => {
                    self.metadata_task.take();
                    if Self::abort_on_metadata_failure(result, &mut self.tasks).await {
                        return;
                    }
                }
                task = self.handles.tick() => {
                    if let Some(task) = task {
                        self.spawn(task);
                    }
                }
                Some(cmd) = self.fs_cmd_rx.recv() => {
                    self.handle_fs_command(cmd).await;
                }
                Some((hash, parts)) = self.unreferenced.recv() => {
                    self.route_unreferenced(hash, parts).await;
                }
                cmd = self.cmd_rx.recv(), if !closing => {
                    let Some(cmd) = cmd else {
                        closing = true;
                        if let Some(gc) = self.gc.as_mut() {
                            if let Some(cancel) = gc.cancel.take() { cancel.send(()).ok(); }
                        }
                        continue;
                    };
                    if let Command::Shutdown(cmd) = cmd {
                        // Stop admitting work before releasing a GC freeze. In
                        // particular, deferred tags must not overtake a sweep
                        // delete whose caller still holds the session guard.
                        closing = true;
                        shutdown = Some(cmd);
                        self.cmd_rx.close();
                        while self.cmd_rx.try_recv().is_ok() {}
                        self.deferred.clear();
                        if let Some(gc) = self.gc.as_mut() {
                            if let Some(cancel) = gc.cancel.take() { cancel.send(()).ok(); }
                        }
                    } else {
                        self.handle_command(cmd).await;
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Self::log_task_result(res);
                }
            }
            // Check after joining: a task may have queued work since select!
            // polled the receivers. With no tasks left, no follow-ups can arrive.
            // Queued API requests are new work and do not prevent idle.
            if !self.idle_waiters.is_empty()
                && self.tasks.is_empty()
                && self.fs_cmd_rx.is_empty()
                && self.unreferenced.is_empty()
                && !self.handles.has_pending()
            {
                for tx in self.idle_waiters.drain(..) {
                    tx.send(()).await.ok();
                }
            }
        }
        // A delete waits for the metadata actor to answer once it is
        // committed, and then unlinks the files, so let running deletes
        // finish before the entities and the metadata actor shut down.
        let mut deletes = self.deletes.subscribe();
        while *deletes.borrow_and_update() > 0 {
            tokio::select! {
                result = async { self.metadata_task.as_mut().unwrap().await },
                    if self.metadata_task.is_some() => {
                    self.metadata_task.take();
                    if Self::abort_on_metadata_failure(result, &mut self.tasks).await {
                        return;
                    }
                }
                Some((hash, parts)) = self.unreferenced.recv() => {
                    self.route_unreferenced(hash, parts).await;
                }
                _ = deletes.changed() => {}
                task = self.handles.tick() => {
                    if let Some(task) = task {
                        self.spawn(task);
                    }
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    Self::log_task_result(res);
                }
            }
        }
        if let Some(cmd) = shutdown {
            self.db().send(cmd.into()).await.ok();
            // Keep the runtime alive until metadata has delivered the shutdown
            // reply, even when there are no entity tasks left to drain.
            if let Some(task) = self.metadata_task.take() {
                if Self::abort_on_metadata_failure(task.await, &mut self.tasks).await {
                    return;
                }
            }
        }
        // The metadata actor committed its last batch. Hand what that left to
        // do to the entities while they still take tasks, since only they
        // touch files.
        while let Ok((hash, parts)) = self.unreferenced.try_recv() {
            self.route_unreferenced(hash, parts).await;
        }
        self.handles.shutdown().await;
        while !self.tasks.is_empty() {
            tokio::select! {
                result = async { self.metadata_task.as_mut().unwrap().await },
                    if self.metadata_task.is_some() => {
                    self.metadata_task.take();
                    if Self::abort_on_metadata_failure(result, &mut self.tasks).await {
                        return;
                    }
                }
                Some(res) = self.tasks.join_next() => Self::log_task_result(res),
            }
        }
        // Parts that still arrive now stay, as after a crash: nothing
        // references them, and only entities touch files.
    }

    /// Hands unreferenced parts to the hash's entity, waking it if there is none.
    ///
    /// All file IO happens in entities. A woken entity loads before it checks the parts.
    async fn route_unreferenced(&mut self, hash: Hash, parts: UnreferencedParts) {
        trace!("{} {parts:?}", hash.fmt_short());
        // Queued even while the entity is shutting down. The entity checks
        // the parts against its own state before deleting.
        let task = self
            .handles
            .spawn(hash, move |arg| async move {
                match arg {
                    SpawnArg::Active(ctx) => {
                        ctx.state.delete_unreferenced(&ctx.global, parts).await
                    }
                    SpawnArg::Dead => {
                        warn!("no entity for {}, its files stay", hash);
                    }
                }
            })
            .await;
        if let Some(task) = task {
            self.spawn(task);
        }
    }

    /// Deletes through each hash's entity, and answers once all are done.
    async fn delete_blobs(&mut self, cmd: proto::DeleteBlobsMsg) {
        let proto::DeleteBlobsMsg {
            inner: proto::BlobDeleteRequest { hashes, force },
            tx,
            ..
        } = cmd;
        let results = self.spawn_deletes(hashes, force).await;
        self.spawn(async move {
            tx.send(collect_deletes(results).await).await.ok();
        });
    }

    /// Starts a delete through the entity of each hash that is not protected.
    async fn spawn_deletes(
        &mut self,
        hashes: Vec<Hash>,
        force: bool,
    ) -> Vec<tokio::sync::oneshot::Receiver<io::Result<()>>> {
        let mut results = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let (res_tx, res_rx) = tokio::sync::oneshot::channel();
            results.push(res_rx);
            // Only this actor creates temp tags, so none can appear between
            // this check and the delete reaching the entity.
            if !force && self.temp_tags.protects(hash) {
                trace!("delete {hash}: skip, protected by a temp tag");
                res_tx.send(Ok(())).ok();
                continue;
            }
            let running = RunningDelete::new(&self.deletes);
            let task = self
                .handles
                .spawn(hash, move |arg| async move {
                    let _running = running;
                    let res = match arg {
                        SpawnArg::Active(ctx) => ctx.state.delete(&ctx.global).await,
                        SpawnArg::Dead => Err(io::Error::other("no entity to delete through")),
                    };
                    res_tx.send(res).ok();
                })
                .await;
            if let Some(task) = task {
                self.spawn(task);
            }
        }
        results
    }

    /// Creates the actor and spawns the database actor.
    ///
    /// This must only be called in the context of the store runtime, since the
    /// database actor is spawned on the current runtime and does blocking IO.
    pub(crate) async fn new(
        cmd_rx: tokio::sync::mpsc::Receiver<Command>,
        fs_commands_rx: tokio::sync::mpsc::Receiver<InternalCommand>,
        fs_commands_tx: tokio::sync::mpsc::Sender<InternalCommand>,
        options: Arc<StoreOptions>,
        backend: B,
    ) -> Result<Self> {
        trace!("creating temp directory: {}", options.temp_path.display());
        fs::create_dir_all(&options.temp_path)?;
        let (db_send, requests) = tokio::sync::mpsc::channel(100);
        let (unreferenced_send, unreferenced) = tokio::sync::mpsc::unbounded_channel();
        let metadata = backend.start_meta(MetaChannels {
            requests,
            unreferenced: unreferenced_send,
        })?;
        // The metadata store holds its lock now, so no other store is importing
        // into the temp directory. Anything in it is left over from imports that
        // did not complete, for example because the process was killed.
        remove_all_files(&options.temp_path);
        let slot_context = Arc::new(TaskContext {
            options: options.clone(),
            db: metadata::Db::new(db_send),
            internal_cmd_tx: fs_commands_tx,
        });
        let metadata_task = Some(tokio::spawn(metadata));
        let global = Arc::new(EntityGlobal {
            store: slot_context.clone(),
            backend,
        });
        Ok(Self {
            context: slot_context,
            cmd_rx,
            fs_cmd_rx: fs_commands_rx,
            unreferenced,
            metadata_task,
            deletes: Arc::new(tokio::sync::watch::Sender::new(0)),
            tasks: JoinSet::new(),
            handles: EntityManagerState::new(global, 1024, 32, 2),
            temp_tags: Default::default(),
            gc: None,
            deferred: VecDeque::new(),
            idle_waiters: Vec::new(),
            _rt: RtWrapper(None),
        })
    }
}

/// Counts a running delete for as long as it lives, see `Actor::deletes`.
///
/// Counted by drop, so a delete whose entity never ran it is not waited for.
struct RunningDelete(Arc<tokio::sync::watch::Sender<usize>>);

impl RunningDelete {
    fn new(deletes: &Arc<tokio::sync::watch::Sender<usize>>) -> Self {
        deletes.send_modify(|n| *n += 1);
        Self(deletes.clone())
    }
}

impl Drop for RunningDelete {
    fn drop(&mut self) {
        self.0.send_modify(|n| *n -= 1);
    }
}

/// Remove all files directly in `dir`, logging but otherwise ignoring failures.
///
/// Subdirectories and their contents are left alone.
fn remove_all_files(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(cause) => {
            warn!("failed to read directory {}: {cause}", dir.display());
            return;
        }
    };
    for path in entries.flatten().map(|entry| entry.path()) {
        if !path.is_file() {
            continue;
        }
        if let Err(cause) = fs::remove_file(&path) {
            warn!("failed to remove file {}: {cause}", path.display());
        }
    }
}

trait HashSpecificCommand: HashSpecific + Send + 'static {
    /// Handle the command on success by spawning a task into the per-hash context.
    fn handle<B: Backend>(self, ctx: EntityCtx<B>) -> impl Future<Output = ()> + Send + 'static;

    /// Opportunity to send an error if the entity is dead (e.g. after a panic).
    fn on_error(self) -> impl Future<Output = ()> + Send + 'static;

    async fn spawn<B: Backend>(
        self,
        manager: &mut entity_manager::EntityManagerState<EmParams<B>>,
        tasks: &mut JoinSet<()>,
    ) where
        Self: Sized,
    {
        let span = tracing::Span::current();
        let task = manager
            .spawn(self.hash(), |arg| {
                async move {
                    match arg {
                        SpawnArg::Active(state) => {
                            self.handle(state).await;
                        }
                        SpawnArg::Dead => {
                            self.on_error().await;
                        }
                    }
                }
                .instrument(span)
            })
            .await;
        if let Some(task) = task {
            tasks.spawn(task);
        }
    }
}

impl HashSpecificCommand for ObserveMsg {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        ctx.state.observe(&ctx.global, self).await
    }
    async fn on_error(self) {}
}
impl HashSpecificCommand for ExportPathMsg {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        ctx.state.export_path(&ctx.global, self).await
    }
    async fn on_error(self) {
        let err = err_entity_dead();
        self.tx
            .send(ExportProgressItem::Error(api::Error::from(err)))
            .await
            .ok();
    }
}
impl HashSpecificCommand for ExportBaoMsg {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        ctx.state.export_bao(&ctx.global, self).await
    }
    async fn on_error(self) {
        let err = err_entity_dead();
        self.tx
            .send(EncodedItem::Error(bao_tree::io::EncodeError::Io(err)))
            .await
            .ok();
    }
}
impl HashSpecificCommand for ExportRangesMsg {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        ctx.state.export_ranges(&ctx.global, self).await
    }
    async fn on_error(self) {
        let err = err_entity_dead();
        self.tx
            .send(ExportRangesItem::Error(api::Error::from(err)))
            .await
            .ok();
    }
}
impl HashSpecificCommand for ImportBaoMsg {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        ctx.state.import_bao(&ctx.global, self).await
    }
    async fn on_error(self) {
        let err = err_entity_dead();
        self.tx.send(Err(api::Error::from(err))).await.ok();
    }
}
impl HashSpecific for (TempTag, ImportEntryMsg) {
    fn hash(&self) -> Hash {
        self.1.hash()
    }
}
impl HashSpecificCommand for (TempTag, ImportEntryMsg) {
    async fn handle<B: Backend>(self, ctx: EntityCtx<B>) {
        let (tt, cmd) = self;
        ctx.state.finish_import(&ctx.global, cmd, tt).await
    }
    async fn on_error(self) {
        let err = err_entity_dead();
        self.1.tx.send(AddProgressItem::Error(err)).await.ok();
    }
}

fn err_entity_dead() -> io::Error {
    io::Error::other("entity is dead")
}

pub(crate) struct RtWrapper(pub(crate) Option<tokio::runtime::Runtime>);

impl From<tokio::runtime::Runtime> for RtWrapper {
    fn from(rt: tokio::runtime::Runtime) -> Self {
        Self(Some(rt))
    }
}

impl fmt::Debug for RtWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        ValueOrPoisioned(self.0.as_ref()).fmt(f)
    }
}

impl Deref for RtWrapper {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Drop for RtWrapper {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            trace!("shutting down tokio runtime");
            rt.shutdown_background();
        }
    }
}

async fn handle_batch(cmd: BatchMsg, id: Scope, scope: Arc<TempTagScope>, ctx: Arc<TaskContext>) {
    if let Err(cause) = handle_batch_impl(cmd, id, &scope).await {
        error!("batch failed: {cause}");
    }
    ctx.clear_scope(id).await;
}

async fn handle_batch_impl(cmd: BatchMsg, id: Scope, scope: &Arc<TempTagScope>) -> api::Result<()> {
    let BatchMsg { tx, mut rx, .. } = cmd;
    trace!("created scope {}", id);
    tx.send(id).await?;
    while let Some(msg) = rx.recv().await? {
        match msg {
            BatchResponse::Drop(msg) => scope.on_drop(&msg),
            BatchResponse::Ping => {}
        }
    }
    Ok(())
}

/// What a started store is driven through.
pub(crate) struct Started {
    /// Requests of the store api.
    pub commands: irpc::LocalSender<proto::Request>,
    /// The live threads of the store runtime, for tests.
    #[cfg(test)]
    pub threads: Arc<AtomicU64>,
}

/// Starts a store from its options and a backend, on a runtime of its own.
///
/// The backend starts the metadata store on that runtime, and is shared by
/// all entities as their configuration.
pub(crate) async fn start<B: Backend>(options: StoreOptions, backend: B) -> Result<Started> {
    static THREAD_NR: AtomicU64 = AtomicU64::new(0);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .thread_name_fn(|| {
            format!(
                "iroh-blob-store-{}",
                THREAD_NR.fetch_add(1, Ordering::Relaxed)
            )
        })
        .enable_time();
    // Counts the live threads of the store runtime, so that
    // `test_load_drop_concurrent` can check that they exit after the store
    // is dropped. `Runtime::metrics` only has this with `tokio_unstable`.
    #[cfg(test)]
    let threads = Arc::new(AtomicU64::new(0));
    #[cfg(test)]
    {
        let start = threads.clone();
        let stop = threads.clone();
        builder
            .on_thread_start(move || {
                start.fetch_add(1, Ordering::SeqCst);
            })
            .on_thread_stop(move || {
                stop.fetch_sub(1, Ordering::SeqCst);
            });
    }
    let rt = RtWrapper::from(builder.build()?);
    let handle = rt.handle().clone();
    let (commands_tx, commands_rx) = tokio::sync::mpsc::channel(100);
    let gc_sender = commands_tx.downgrade();
    let (fs_commands_tx, fs_commands_rx) = tokio::sync::mpsc::channel(100);
    let gc_config = options.gc.clone();
    let mut actor = handle
        .spawn(Actor::<B>::new(
            commands_rx,
            fs_commands_rx,
            fs_commands_tx.clone(),
            Arc::new(options),
            backend,
        ))
        .await
        .anyerr()??;
    actor._rt = rt;
    handle.spawn(actor.run());
    if let Some(config) = gc_config {
        handle.spawn(run_gc(gc_sender, config));
    }
    Ok(Started {
        commands: commands_tx.into(),
        #[cfg(test)]
        threads,
    })
}
