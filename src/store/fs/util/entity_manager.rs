#![allow(dead_code)]
use std::{fmt::Debug, future::Future, hash::Hash};

use n0_future::{future, FuturesUnordered};
use tokio::sync::{mpsc, oneshot};

/// Trait to reset an entity state in place.
///
/// In many cases this is just assigning the default value, but e.g. for an
/// `Arc<Mutex<T>>` resetting to the default value means an allocation, whereas
/// reset can be done without.
pub trait Reset: Default {
    /// Reset the state to its default value.
    fn reset(&mut self);

    /// A ref count to ensure that the state is unique when shutting down.
    ///
    /// You are not allowed to clone the state out of a task, even though that
    /// is possible.
    fn ref_count(&self) -> usize;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownCause {
    /// The entity is shutting down gracefully because the entity is idle.
    Idle,
    /// The entity is shutting down because the entity manager is shutting down.
    Soft,
    /// The entity is shutting down because the sender was dropped.
    Drop,
}

/// Parameters for the entity manager system.
pub trait Params: Send + Sync + 'static {
    /// Entity id type.
    ///
    /// This does not require Copy to allow for more complex types, such as `String`,
    /// but you have to make sure that ids are small and cheap to clone, since they are
    /// used as keys in maps.
    type EntityId: Debug + Hash + Eq + Clone + Send + Sync + 'static;
    /// Global state type.
    ///
    /// This is passed into all entity actors. It also needs to be cheap handle.
    /// If you don't need it, just set it to `()`.
    type GlobalState: Debug + Clone + Send + Sync + 'static;
    /// Entity state type.
    ///
    /// This is the actual distinct per-entity state. This needs to implement
    /// `Default` and a matching `Reset`. It also needs to implement `Clone`
    /// since we unfortunately need to pass an owned copy of the state to the
    /// callback - otherwise we run into some rust lifetime limitations
    /// <https://github.com/rust-lang/rust/issues/100013>.
    ///
    /// Frequently this is an `Arc<Mutex<T>>` or similar. Note that per entity
    /// access is concurrent but not parallel, so you can use a more efficient
    /// synchronization primitive like [`AtomicRefCell`](https://crates.io/crates/atomic_refcell) if you want to.
    type EntityState: Default + Debug + Reset + Clone + Send + Sync + 'static;
    /// Function being called when an entity actor is shutting down.
    fn on_shutdown(
        state: entity_actor::State<Self>,
        cause: ShutdownCause,
    ) -> impl Future<Output = ()> + Send + 'static
    where
        Self: Sized;
}

/// Sent to the main actor and then delegated to the entity actor to spawn a new task.
pub(crate) struct Spawn<P: Params> {
    id: P::EntityId,
    f: Box<dyn FnOnce(SpawnArg<P>) -> future::Boxed<()> + Send>,
}

pub(crate) struct EntityShutdown;

/// Argument for the `EntityManager::spawn` function.
pub enum SpawnArg<P: Params> {
    /// The entity is active, and we were able to spawn a task.
    Active(ActiveEntityState<P>),
    /// The entity is busy and cannot spawn a new task.
    Busy,
    /// The entity is dead.
    Dead,
}

/// Sent from the entity actor to the main actor to notify that it is shutting down.
///
/// With this message the entity actor gives back the receiver for its command channel,
/// so it can be reusd either immediately if commands come in during shutdown, or later
/// if the entity actor is reused for a different entity.
struct Shutdown<P: Params> {
    id: P::EntityId,
    receiver: mpsc::Receiver<entity_actor::Command<P>>,
}

struct ShutdownAll {
    tx: oneshot::Sender<()>,
}

/// Sent from the main actor to the entity actor to notify that it has completed shutdown.
///
/// With this message the entity actor sends back the remaining state. The tasks set
/// at this point must be empty, as the entity actor has already completed all tasks.
struct ShutdownComplete<P: Params> {
    state: ActiveEntityState<P>,
    tasks: FuturesUnordered<future::Boxed<()>>,
}

mod entity_actor {
    #![allow(dead_code)]
    use n0_future::{future, FuturesUnordered, StreamExt};
    use tokio::sync::mpsc;

    use super::{
        EntityShutdown, Params, Reset, Shutdown, ShutdownCause, ShutdownComplete, Spawn, SpawnArg,
    };

    /// State of an active entity.
    #[derive(Debug)]
    pub struct State<P: Params> {
        /// The entity id.
        pub id: P::EntityId,
        /// A copy of the global state.
        pub global: P::GlobalState,
        /// The per-entity state which might have internal mutability.
        pub state: P::EntityState,
    }

    impl<P: Params> Clone for State<P> {
        fn clone(&self) -> Self {
            Self {
                id: self.id.clone(),
                global: self.global.clone(),
                state: self.state.clone(),
            }
        }
    }

    pub enum Command<P: Params> {
        Spawn(Spawn<P>),
        EntityShutdown(EntityShutdown),
    }

    impl<P: Params> From<EntityShutdown> for Command<P> {
        fn from(_: EntityShutdown) -> Self {
            Self::EntityShutdown(EntityShutdown)
        }
    }

    #[derive(Debug)]
    pub struct Actor<P: Params> {
        pub recv: mpsc::Receiver<Command<P>>,
        pub main: mpsc::Sender<super::main_actor::InternalCommand<P>>,
        pub state: State<P>,
        pub tasks: FuturesUnordered<future::Boxed<()>>,
    }

    impl<P: Params> Actor<P> {
        pub async fn run(mut self) {
            loop {
                tokio::select! {
                    command = self.recv.recv() => {
                        let Some(command) = command else {
                            // Channel closed, this means that the main actor is shutting down.
                            self.drop_shutdown_state().await;
                            break;
                        };
                        match command {
                            Command::Spawn(spawn) => {
                                let task = (spawn.f)(SpawnArg::Active(self.state.clone()));
                                self.tasks.push(task);
                            }
                            Command::EntityShutdown(_) => {
                                self.soft_shutdown_state().await;
                                break;
                            }
                        }
                    }
                    Some(_) = self.tasks.next(), if !self.tasks.is_empty() => {}
                }
                if self.tasks.is_empty() && self.recv.is_empty() {
                    // No more tasks and no more commands, we can recycle the actor.
                    self.recycle_state().await;
                    break; // Exit the loop, actor is done.
                }
            }
        }

        /// drop shutdown state.
        ///
        /// All senders for our receive channel were dropped, so we shut down without waiting for any tasks to complete.
        async fn drop_shutdown_state(self) {
            let Self { state, .. } = self;
            P::on_shutdown(state, ShutdownCause::Drop).await;
        }

        /// Soft shutdown state.
        ///
        /// We have received an explicit shutdown command, so we wait for all tasks to complete and then call the shutdown function.
        async fn soft_shutdown_state(mut self) {
            while (self.tasks.next().await).is_some() {}
            P::on_shutdown(self.state.clone(), ShutdownCause::Soft).await;
        }

        async fn recycle_state(self) {
            // we can't check if recv is empty here, since new messages might come in while we are in recycle_state.
            assert!(
                self.tasks.is_empty(),
                "Tasks must be empty before recycling"
            );
            // notify main actor that we are starting to shut down.
            // if the main actor is shutting down, this could fail, but we don't care.
            self.main
                .send(
                    Shutdown {
                        id: self.state.id.clone(),
                        receiver: self.recv,
                    }
                    .into(),
                )
                .await
                .ok();
            assert_eq!(self.state.state.ref_count(), 1);
            P::on_shutdown(self.state.clone(), ShutdownCause::Idle).await;
            // Notify the main actor that we have completed shutdown.
            // here we also give back the rest of ourselves so the main actor can recycle us.
            self.main
                .send(
                    ShutdownComplete {
                        state: self.state,
                        tasks: self.tasks,
                    }
                    .into(),
                )
                .await
                .ok();
        }

        /// Recycle the actor for reuse by setting its state to default.
        ///
        /// This also checks several invariants:
        /// - There must be no pending messages in the receive channel.
        /// - The sender must have a strong count of 1, meaning no other references exist
        /// - The tasks set must be empty, meaning no tasks are running.
        /// - The global state must match the scope provided.
        /// - The state must be unique to the actor, meaning no other references exist.
        pub fn recycle(&mut self) {
            assert!(
                self.recv.is_empty(),
                "Cannot recycle actor with pending messages"
            );
            assert!(
                self.recv.sender_strong_count() == 1,
                "There must be only one sender left"
            );
            assert!(
                self.tasks.is_empty(),
                "Tasks must be empty before recycling"
            );
            self.state.state.reset();
        }
    }
}
pub use entity_actor::State as ActiveEntityState;
pub use main_actor::ActorState as EntityManagerState;

mod main_actor {
    #![allow(dead_code)]
    use std::{collections::HashMap, future::Future};

    use n0_future::{future, FuturesUnordered};
    use tokio::{sync::mpsc, task::JoinSet};
    use tracing::{error, warn};

    use super::{
        entity_actor, EntityShutdown, Params, Reset, Shutdown, ShutdownAll, ShutdownComplete,
        Spawn, SpawnArg,
    };

    pub(super) enum Command<P: Params> {
        Spawn(Spawn<P>),
        ShutdownAll(ShutdownAll),
    }

    impl<P: Params> From<ShutdownAll> for Command<P> {
        fn from(shutdown_all: ShutdownAll) -> Self {
            Self::ShutdownAll(shutdown_all)
        }
    }

    pub(super) enum InternalCommand<P: Params> {
        ShutdownComplete(ShutdownComplete<P>),
        Shutdown(Shutdown<P>),
    }

    impl<P: Params> From<Shutdown<P>> for InternalCommand<P> {
        fn from(shutdown: Shutdown<P>) -> Self {
            Self::Shutdown(shutdown)
        }
    }

    impl<P: Params> From<ShutdownComplete<P>> for InternalCommand<P> {
        fn from(shutdown_complete: ShutdownComplete<P>) -> Self {
            Self::ShutdownComplete(shutdown_complete)
        }
    }

    #[derive(Debug)]
    pub enum EntityHandle<P: Params> {
        /// A running entity actor.
        Live {
            send: mpsc::Sender<entity_actor::Command<P>>,
        },
        ShuttingDown {
            send: mpsc::Sender<entity_actor::Command<P>>,
            recv: mpsc::Receiver<entity_actor::Command<P>>,
        },
    }

    impl<P: Params> EntityHandle<P> {
        pub fn send(&self) -> &mpsc::Sender<entity_actor::Command<P>> {
            match self {
                EntityHandle::Live { send } => send,
                EntityHandle::ShuttingDown { send, .. } => send,
            }
        }
    }

    /// State machine for an entity actor manager.
    ///
    /// This is if you don't want a separate manager actor, but want to inline the entity
    /// actor management into your main actor.
    #[derive(Debug)]
    pub struct ActorState<P: Params> {
        /// Channel to receive internal commands from the entity actors.
        /// This channel will never be closed since we also hold a sender to it.
        internal_recv: mpsc::Receiver<InternalCommand<P>>,
        /// Channel to send internal commands to ourselves, to hand out to entity actors.
        internal_send: mpsc::Sender<InternalCommand<P>>,
        /// Map of live entity actors.
        live: HashMap<P::EntityId, EntityHandle<P>>,
        /// Global state shared across all entity actors.
        state: P::GlobalState,
        /// Pool of inactive entity actors to reuse.
        pool: Vec<(
            mpsc::Sender<entity_actor::Command<P>>,
            entity_actor::Actor<P>,
        )>,
        /// Maximum size of the inbox of an entity actor.
        entity_inbox_size: usize,
        /// Initial capacity of the futures set for entity actors.
        entity_futures_initial_capacity: usize,
    }

    impl<P: Params> ActorState<P> {
        pub fn new(
            state: P::GlobalState,
            pool_capacity: usize,
            entity_inbox_size: usize,
            entity_response_inbox_size: usize,
            entity_futures_initial_capacity: usize,
        ) -> Self {
            let (internal_send, internal_recv) = mpsc::channel(entity_response_inbox_size);
            Self {
                internal_recv,
                internal_send,
                live: HashMap::new(),
                state,
                pool: Vec::with_capacity(pool_capacity),
                entity_inbox_size,
                entity_futures_initial_capacity,
            }
        }

        #[must_use = "this function may return a future that must be spawned by the caller"]
        /// Friendly version of `spawn_boxed` that does the boxing
        pub async fn spawn<F, Fut>(
            &mut self,
            id: P::EntityId,
            f: F,
        ) -> Option<impl Future<Output = ()> + Send + 'static>
        where
            F: FnOnce(SpawnArg<P>) -> Fut + Send + 'static,
            Fut: Future<Output = ()> + Send + 'static,
        {
            self.spawn_boxed(
                id,
                Box::new(|x| {
                    Box::pin(async move {
                        f(x).await;
                    })
                }),
            )
            .await
        }

        #[must_use = "this function may return a future that must be spawned by the caller"]
        pub async fn spawn_boxed(
            &mut self,
            id: P::EntityId,
            f: Box<dyn FnOnce(SpawnArg<P>) -> future::Boxed<()> + Send>,
        ) -> Option<impl Future<Output = ()> + Send + 'static> {
            let (entity_handle, task) = self.get_or_create(id.clone());
            let sender = entity_handle.send();
            if let Err(e) =
                sender.try_send(entity_actor::Command::Spawn(Spawn { id: id.clone(), f }))
            {
                match e {
                    mpsc::error::TrySendError::Full(cmd) => {
                        let entity_actor::Command::Spawn(spawn) = cmd else {
                            panic!()
                        };
                        warn!(
                            "Entity actor inbox is full, cannot send command to entity actor {:?}.",
                            id
                        );
                        // we await in the select here, but I think this is fine, since the actor is busy.
                        // maybe slowing things down a bit is helpful.
                        (spawn.f)(SpawnArg::Busy).await;
                    }
                    mpsc::error::TrySendError::Closed(cmd) => {
                        let entity_actor::Command::Spawn(spawn) = cmd else {
                            panic!()
                        };
                        error!(
                            "Entity actor inbox is closed, cannot send command to entity actor {:?}.",
                            id
                        );
                        // give the caller a chance to react to this bad news.
                        // at this point we are in trouble anyway, so awaiting is going to be the least of our problems.
                        (spawn.f)(SpawnArg::Dead).await;
                    }
                }
            };
            task
        }

        /// This function needs to be polled by the owner of the actor state to advance the
        /// entity manager state machine. If it returns a future, that future must be spawned
        /// by the caller.
        #[must_use = "this function may return a future that must be spawned by the caller"]
        pub async fn tick(&mut self) -> Option<impl Future<Output = ()> + Send + 'static> {
            if let Some(cmd) = self.internal_recv.recv().await {
                match cmd {
                    InternalCommand::Shutdown(Shutdown { id, receiver }) => {
                        let Some(entity_handle) = self.live.remove(&id) else {
                            error!("Received shutdown command for unknown entity actor {id:?}");
                            return None;
                        };
                        let EntityHandle::Live { send } = entity_handle else {
                            error!(
                                "Received shutdown command for entity actor {id:?} that is already shutting down"
                            );
                            return None;
                        };
                        self.live.insert(
                            id.clone(),
                            EntityHandle::ShuttingDown {
                                send,
                                recv: receiver,
                            },
                        );
                    }
                    InternalCommand::ShutdownComplete(ShutdownComplete { state, tasks }) => {
                        let id = state.id.clone();
                        let Some(entity_handle) = self.live.remove(&id) else {
                            error!(
                                "Received shutdown complete command for unknown entity actor {id:?}"
                            );
                            return None;
                        };
                        let EntityHandle::ShuttingDown { send, recv } = entity_handle else {
                            error!(
                                "Received shutdown complete command for entity actor {id:?} that is not shutting down"
                            );
                            return None;
                        };
                        // re-assemble the actor from the parts
                        let mut actor = entity_actor::Actor {
                            main: self.internal_send.clone(),
                            recv,
                            state,
                            tasks,
                        };
                        if actor.recv.is_empty() {
                            // No commands during shutdown, we can recycle the actor.
                            self.recycle(send, actor);
                        } else {
                            actor.state.state.reset();
                            self.live.insert(id.clone(), EntityHandle::Live { send });
                            return Some(actor.run());
                        }
                    }
                }
            }
            None
        }

        /// Send a shutdown command to all live entity actors.
        pub async fn shutdown(self) {
            for handle in self.live.values() {
                handle.send().send(EntityShutdown {}.into()).await.ok();
            }
        }

        /// Get or create an entity actor for the given id.
        ///
        /// If this function returns a future, it must be spawned by the caller.
        fn get_or_create(
            &mut self,
            id: P::EntityId,
        ) -> (
            &mut EntityHandle<P>,
            Option<impl Future<Output = ()> + Send + 'static>,
        ) {
            let mut task = None;
            let handle = self.live.entry(id.clone()).or_insert_with(|| {
                if let Some((send, mut actor)) = self.pool.pop() {
                    // Get an actor from the pool of inactive actors and initialize it.
                    actor.state.id = id.clone();
                    actor.state.global = self.state.clone();
                    // strictly speaking this is not needed, since we reset the state when adding the actor to the pool.
                    actor.state.state.reset();
                    task = Some(actor.run());
                    EntityHandle::Live { send }
                } else {
                    // Create a new entity actor and inbox.
                    let (send, recv) = mpsc::channel(self.entity_inbox_size);
                    let state: entity_actor::State<P> = entity_actor::State {
                        id: id.clone(),
                        global: self.state.clone(),
                        state: Default::default(),
                    };
                    let actor = entity_actor::Actor {
                        main: self.internal_send.clone(),
                        recv,
                        state,
                        tasks: FuturesUnordered::with_capacity(
                            self.entity_futures_initial_capacity,
                        ),
                    };
                    task = Some(actor.run());
                    EntityHandle::Live { send }
                }
            });
            (handle, task)
        }

        fn recycle(
            &mut self,
            sender: mpsc::Sender<entity_actor::Command<P>>,
            mut actor: entity_actor::Actor<P>,
        ) {
            assert!(sender.strong_count() == 1);
            // todo: check that sender and receiver are the same channel. tokio does not have an api for this, unfortunately.
            // reset the actor in any case, just to check the invariants.
            actor.recycle();
            // Recycle the actor for later use.
            if self.pool.len() < self.pool.capacity() {
                self.pool.push((sender, actor));
            }
        }
    }

    pub struct Actor<P: Params> {
        /// Channel to receive commands from the outside world.
        /// If this channel is closed, it means we need to shut down in a hurry.
        recv: mpsc::Receiver<Command<P>>,
        /// Tasks that are currently running.
        tasks: JoinSet<()>,
        /// Internal state of the actor
        state: ActorState<P>,
    }

    impl<P: Params> Actor<P> {
        pub fn new(
            state: P::GlobalState,
            recv: tokio::sync::mpsc::Receiver<Command<P>>,
            pool_capacity: usize,
            entity_inbox_size: usize,
            entity_response_inbox_size: usize,
            entity_futures_initial_capacity: usize,
        ) -> Self {
            Self {
                recv,
                tasks: JoinSet::new(),
                state: ActorState::new(
                    state,
                    pool_capacity,
                    entity_inbox_size,
                    entity_response_inbox_size,
                    entity_futures_initial_capacity,
                ),
            }
        }

        pub async fn run(mut self) {
            enum SelectOutcome<A, B, C> {
                Command(A),
                Tick(B),
                TaskDone(C),
            }
            loop {
                let res = tokio::select! {
                    x = self.recv.recv() => SelectOutcome::Command(x),
                    x = self.state.tick() => SelectOutcome::Tick(x),
                    Some(task) = self.tasks.join_next(), if !self.tasks.is_empty() => SelectOutcome::TaskDone(task),
                };
                match res {
                    SelectOutcome::Command(cmd) => {
                        let Some(cmd) = cmd else {
                            // Channel closed, this means that the main actor is shutting down.
                            self.hard_shutdown().await;
                            break;
                        };
                        match cmd {
                            Command::Spawn(spawn) => {
                                if let Some(task) = self.state.spawn_boxed(spawn.id, spawn.f).await
                                {
                                    self.tasks.spawn(task);
                                }
                            }
                            Command::ShutdownAll(arg) => {
                                self.soft_shutdown().await;
                                arg.tx.send(()).ok();
                                break;
                            }
                        }
                        // Handle incoming command
                    }
                    SelectOutcome::Tick(future) => {
                        if let Some(task) = future {
                            self.tasks.spawn(task);
                        }
                    }
                    SelectOutcome::TaskDone(result) => {
                        // Handle completed task
                        if let Err(e) = result {
                            error!("Task failed: {e:?}");
                        }
                    }
                }
            }
        }

        async fn soft_shutdown(self) {
            let Self {
                mut tasks, state, ..
            } = self;
            state.shutdown().await;
            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    eprintln!("Task failed during shutdown: {e:?}");
                }
            }
        }

        async fn hard_shutdown(self) {
            let Self {
                mut tasks, state, ..
            } = self;
            // this is needed so calls to internal_send in idle shutdown fail fast.
            // otherwise we would have to drain the channel, but we don't care about the messages at
            // this point.
            drop(state);
            while let Some(res) = tasks.join_next().await {
                if let Err(e) = res {
                    eprintln!("Task failed during shutdown: {e:?}");
                }
            }
        }
    }
}

/// A manager for entities identified by an entity id.
///
/// The manager provides parallelism between entities, but just concurrency within a single entity.
/// This is useful if the entity wraps an external resource such as a file that does not benefit
/// from parallelism.
///
/// The entity manager internally uses a main actor and per-entity actors. Per entity actors
/// and their inbox queues are recycled when they become idle, to save allocations.
///
/// You can mostly ignore these implementation details, except when you want to customize the
/// queue sizes in the [`Options`] struct.
///
/// The main entry point is the [`EntityManager::spawn`] function.
///
/// Dropping the `EntityManager` will shut down the entity actors without waiting for their
/// tasks to complete. For a more gentle shutdown, use the [`EntityManager::shutdown`] function
/// that does wait for tasks to complete.
#[derive(Debug, Clone)]
pub struct EntityManager<P: Params>(mpsc::Sender<main_actor::Command<P>>);

#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Maximum number of inactive entity actors that are being pooled for reuse.
    pub pool_capacity: usize,
    /// Size of the inbox for the manager actor.
    pub inbox_size: usize,
    /// Size of the inbox for entity actors.
    pub entity_inbox_size: usize,
    /// Size of the inbox for entity actor responses to the manager actor.
    pub entity_response_inbox_size: usize,
    /// Initial capacity of the futures set for entity actors.
    ///
    /// Set this to the expected average concurrency level of your entities.
    pub entity_futures_initial_capacity: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            pool_capacity: 10,
            inbox_size: 10,
            entity_inbox_size: 10,
            entity_response_inbox_size: 100,
            entity_futures_initial_capacity: 16,
        }
    }
}

impl<P: Params> EntityManager<P> {
    pub fn new(state: P::GlobalState, options: Options) -> Self {
        let (send, recv) = mpsc::channel(options.inbox_size);
        let actor = main_actor::Actor::new(
            state,
            recv,
            options.pool_capacity,
            options.entity_inbox_size,
            options.entity_response_inbox_size,
            options.entity_futures_initial_capacity,
        );
        tokio::spawn(actor.run());
        Self(send)
    }

    /// Spawn a new task on the entity actor with the given id.
    ///
    /// Unless the world is ending - e.g. tokio runtime is shutting down - the passed function
    /// is guaranteed to be called. However, there is no guarantee that the entity actor is
    /// alive and responsive. See [`SpawnArg`] for details.
    ///
    /// Multiple callbacks for the same entity will be executed sequentially. There is no
    /// parallelism within a single entity. So you can use synchronization primitives that
    /// assume unique access in P::EntityState. And even if you do use multithreaded synchronization
    /// primitives, they will never be contended.
    ///
    /// The future returned by `f` will be executed concurrently with other tasks, but again
    /// there will be no real parallelism within a single entity actor.
    pub async fn spawn<F, Fut>(&self, id: P::EntityId, f: F) -> Result<(), &'static str>
    where
        F: FnOnce(SpawnArg<P>) -> Fut + Send + 'static,
        Fut: future::Future<Output = ()> + Send + 'static,
    {
        let spawn = Spawn {
            id,
            f: Box::new(|arg| {
                Box::pin(async move {
                    f(arg).await;
                })
            }),
        };
        self.0
            .send(main_actor::Command::Spawn(spawn))
            .await
            .map_err(|_| "Failed to send spawn command")
    }

    pub async fn shutdown(&self) -> std::result::Result<(), &'static str> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ShutdownAll { tx }.into())
            .await
            .map_err(|_| "Failed to send shutdown command")?;
        rx.await
            .map_err(|_| "Failed to receive shutdown confirmation")
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the entity manager.
    //!
    //! We implement a simple database for u128 counters, identified by u64 ids,
    //! with both an in-memory and a file-based implementation.
    //!
    //! The database does internal consistency checks, to ensure that each
    //! entity is only ever accessed by a single tokio task at a time, and to
    //! ensure that wakeup and shutdown events are interleaved.
    //!
    //! We also check that the database behaves correctly by comparing with an
    //! in-memory implementation.
    //!
    //! Database operations are done in parallel, so the fact that we are using
    //! AtomicRefCell provides another test - if there was parallel write access
    //! to a single entity due to a bug, it would panic.
    use std::collections::HashMap;

    use n0_future::{BufferedStreamExt, StreamExt};
    use testresult::TestResult;

    use super::*;

    // a simple database for u128 counters, identified by u64 ids.
    trait CounterDb {
        async fn add(&self, id: u64, value: u128) -> Result<(), &'static str>;
        async fn get(&self, id: u64) -> Result<u128, &'static str>;
        async fn shutdown(&self) -> Result<(), &'static str>;
        async fn check_consistency(&self, values: HashMap<u64, u128>);
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Event {
        Wakeup,
        Shutdown,
    }

    mod mem {
        //! The in-memory database uses a HashMap in the global state to store
        //! the values of the counters. Loading means reading from the global
        //! state into the entity state, and persisting means writing to the
        //! global state from the entity state.
        use std::{
            collections::{HashMap, HashSet},
            sync::{
                atomic::{AtomicUsize, Ordering},
                Arc, Mutex,
            },
            time::Instant,
        };

        use atomic_refcell::AtomicRefCell;

        use super::*;

        #[derive(Debug, Default)]
        struct Inner {
            value: Option<u128>,
            tasks: HashSet<tokio::task::Id>,
        }

        #[derive(Debug, Clone, Default)]
        struct State(Arc<AtomicRefCell<Inner>>);

        impl Reset for State {
            fn reset(&mut self) {
                *self.0.borrow_mut() = Default::default();
            }

            fn ref_count(&self) -> usize {
                Arc::strong_count(&self.0)
            }
        }

        #[derive(Debug, Default)]
        struct Global {
            // the "database" of entity values
            data: HashMap<u64, u128>,
            // log of awake and shutdown events
            log: HashMap<u64, Vec<(Event, Instant)>>,
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = Arc<Mutex<Global>>;
            type EntityState = State;
            async fn on_shutdown(entity: entity_actor::State<Self>, _cause: ShutdownCause) {
                let state = entity.state.0.borrow();
                let mut global = entity.global.lock().unwrap();
                assert_eq!(state.tasks.len(), 1);
                // persist the state
                if let Some(value) = state.value {
                    global.data.insert(entity.id, value);
                }
                // log the shutdown event
                global
                    .log
                    .entry(entity.id)
                    .or_default()
                    .push((Event::Shutdown, Instant::now()));
            }
        }

        pub struct MemDb {
            m: EntityManager<Counters>,
            global: Arc<Mutex<Global>>,
        }

        impl entity_actor::State<Counters> {
            async fn with_value(&self, f: impl FnOnce(&mut u128)) -> Result<(), &'static str> {
                let mut state = self.state.0.borrow_mut();
                // lazily load the data from the database
                if state.value.is_none() {
                    let mut global = self.global.lock().unwrap();
                    state.value = Some(global.data.get(&self.id).copied().unwrap_or_default());
                    // log the wakeup event
                    global
                        .log
                        .entry(self.id)
                        .or_default()
                        .push((Event::Wakeup, Instant::now()));
                }
                // insert the task id into the tasks set to check that access is always
                // from the same tokio task (not necessarily the same thread).
                state.tasks.insert(tokio::task::id());
                // do the actual work
                let r = state.value.as_mut().unwrap();
                f(r);
                Ok(())
            }
        }

        impl MemDb {
            pub fn new() -> Self {
                let global = Arc::new(Mutex::new(Global::default()));
                Self {
                    global: global.clone(),
                    m: EntityManager::<Counters>::new(global, Options::default()),
                }
            }
        }

        impl super::CounterDb for MemDb {
            async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            SpawnArg::Active(state) => {
                                state
                                    .with_value(|v| *v = v.wrapping_add(value))
                                    .await
                                    .unwrap();
                            }
                            SpawnArg::Busy => println!("Entity actor is busy"),
                            SpawnArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await
            }

            async fn get(&self, id: u64) -> Result<u128, &'static str> {
                let (tx, rx) = oneshot::channel();
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            SpawnArg::Active(state) => {
                                state
                                    .with_value(|v| {
                                        tx.send(*v)
                                            .unwrap_or_else(|_| println!("Failed to send value"))
                                    })
                                    .await
                                    .unwrap();
                            }
                            SpawnArg::Busy => println!("Entity actor is busy"),
                            SpawnArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await?;
                rx.await.map_err(|_| "Failed to receive value")
            }

            async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }

            async fn check_consistency(&self, values: HashMap<u64, u128>) {
                let global = self.global.lock().unwrap();
                assert_eq!(global.data, values, "Data mismatch");
                for id in values.keys() {
                    let log = global.log.get(id).unwrap();
                    if log.len() % 2 != 0 {
                        println!("{:#?}", log);
                        panic!("Log for entity {id} must contain an even number of events");
                    }
                    for (i, (event, _)) in log.iter().enumerate() {
                        assert_eq!(
                            *event,
                            if i % 2 == 0 {
                                Event::Wakeup
                            } else {
                                Event::Shutdown
                            },
                            "Unexpected event type"
                        );
                    }
                }
            }
        }

        /// If a task is so busy that it can't drain it's inbox in time, we will
        /// get a SpawnArg::Busy instead of access to the actual state.
        ///
        /// This will only happen if the system is seriously overloaded, since
        /// the entity actor just spawns tasks for each message. So here we
        /// simulate it by just not spawning the task as we are supposed to.
        #[tokio::test]
        async fn test_busy() -> TestResult<()> {
            let mut state = EntityManagerState::<Counters>::new(
                Arc::new(Mutex::new(Global::default())),
                1024,
                8,
                8,
                2,
            );
            let active = Arc::new(AtomicUsize::new(0));
            let busy = Arc::new(AtomicUsize::new(0));
            let inc = || {
                let active = active.clone();
                let busy = busy.clone();
                |arg: SpawnArg<Counters>| async move {
                    match arg {
                        SpawnArg::Active(_) => {
                            active.fetch_add(1, Ordering::SeqCst);
                        }
                        SpawnArg::Busy => {
                            busy.fetch_add(1, Ordering::SeqCst);
                        }
                        SpawnArg::Dead => {
                            println!("Entity actor is dead");
                        }
                    }
                }
            };
            let fut1 = state.spawn(1, inc()).await;
            assert!(fut1.is_some(), "First spawn should give us a task to spawn");
            for _ in 0..9 {
                let fut = state.spawn(1, inc()).await;
                assert!(
                    fut.is_none(),
                    "Subsequent spawns should assume first task has been spawned"
                );
            }
            assert_eq!(
                active.load(Ordering::SeqCst),
                0,
                "Active should have never been called, since we did not spawn the task!"
            );
            assert_eq!(busy.load(Ordering::SeqCst), 2, "Busy should have been called two times, since we sent 10 msgs to a queue with capacity 8, and nobody is draining it");
            Ok(())
        }

        /// If there is a panic in any of the fns that run on an entity actor,
        /// the entire entity becomes dead. This can not be recovered from, and
        /// trying to spawn a new task on the dead entity actor will result in
        /// a SpawnArg::Dead.
        #[tokio::test]
        async fn test_dead() -> TestResult<()> {
            let manager = EntityManager::<Counters>::new(
                Arc::new(Mutex::new(Global::default())),
                Options::default(),
            );
            let (tx, rx) = oneshot::channel();
            let killer = |arg: SpawnArg<Counters>| async move {
                if let SpawnArg::Active(_) = arg {
                    tx.send(()).ok();
                    panic!("Panic to kill the task");
                }
            };
            // spawn a task that kills the entity actor
            manager.spawn(1, killer).await?;
            rx.await.expect("Failed to receive kill confirmation");
            let (tx, rx) = oneshot::channel();
            let counter = |arg: SpawnArg<Counters>| async move {
                if let SpawnArg::Dead = arg {
                    tx.send(()).ok();
                }
            };
            // // spawn another task on the - now dead - entity actor
            manager.spawn(1, counter).await?;
            rx.await.expect("Failed to receive dead confirmation");
            Ok(())
        }
    }

    mod fs {
        //! The fs db uses one file per counter, stored as a 16-byte big-endian u128.
        use std::{
            collections::HashSet,
            path::{Path, PathBuf},
            sync::{Arc, Mutex},
            time::Instant,
        };

        use atomic_refcell::AtomicRefCell;

        use super::*;

        #[derive(Debug, Clone, Default)]
        struct State {
            value: Option<u128>,
            tasks: HashSet<tokio::task::Id>,
        }

        #[derive(Debug)]
        struct Global {
            path: PathBuf,
            log: HashMap<u64, Vec<(Event, Instant)>>,
        }

        #[derive(Debug, Clone, Default)]
        struct EntityState(Arc<AtomicRefCell<State>>);

        impl Reset for EntityState {
            fn reset(&mut self) {
                *self.0.borrow_mut() = Default::default();
            }

            fn ref_count(&self) -> usize {
                1
            }
        }

        fn get_path(root: impl AsRef<Path>, id: u64) -> PathBuf {
            root.as_ref().join(hex::encode(id.to_be_bytes()))
        }

        impl entity_actor::State<Counters> {
            async fn with_value(&self, f: impl FnOnce(&mut u128)) -> Result<(), &'static str> {
                let Ok(mut r) = self.state.0.try_borrow_mut() else {
                    panic!("failed to borrow state mutably");
                };
                if r.value.is_none() {
                    let mut global = self.global.lock().unwrap();
                    global
                        .log
                        .entry(self.id)
                        .or_default()
                        .push((Event::Wakeup, Instant::now()));
                    let path = get_path(&global.path, self.id);
                    // note: if we were to use async IO, we would need to make sure not to hold the
                    // lock guard over an await point. The entity manager makes sure that all fns
                    // are run on the same tokio task, but there is still concurrency, which
                    // a mutable borrow of the state does not allow.
                    let value = match std::fs::read(path) {
                        Ok(value) => value,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // If the file does not exist, we initialize it to 0.
                            vec![0; 16]
                        }
                        Err(_) => return Err("Failed to read disk state"),
                    };
                    let value = u128::from_be_bytes(
                        value.try_into().map_err(|_| "Invalid disk state format")?,
                    );
                    r.value = Some(value);
                }
                let Some(value) = r.value.as_mut() else {
                    panic!("State must be Memory at this point");
                };
                f(value);
                Ok(())
            }
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = Arc<Mutex<Global>>;
            type EntityState = EntityState;
            async fn on_shutdown(state: entity_actor::State<Self>, _cause: ShutdownCause) {
                let r = state.state.0.borrow();
                let mut global = state.global.lock().unwrap();
                if let Some(value) = r.value {
                    let path = get_path(&global.path, state.id);
                    let value_bytes = value.to_be_bytes();
                    std::fs::write(&path, value_bytes).expect("Failed to write disk state");
                }
                global
                    .log
                    .entry(state.id)
                    .or_default()
                    .push((Event::Shutdown, Instant::now()));
            }
        }

        pub struct FsDb {
            global: Arc<Mutex<Global>>,
            m: EntityManager<Counters>,
        }

        impl FsDb {
            pub fn new(path: impl AsRef<Path>) -> Self {
                let global = Global {
                    path: path.as_ref().to_owned(),
                    log: HashMap::new(),
                };
                let global = Arc::new(Mutex::new(global));
                Self {
                    global: global.clone(),
                    m: EntityManager::<Counters>::new(global, Options::default()),
                }
            }
        }

        impl super::CounterDb for FsDb {
            async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            SpawnArg::Active(state) => {
                                println!(
                                    "Adding value {} to entity actor with id {:?}",
                                    value, state.id
                                );
                                state
                                    .with_value(|v| *v = v.wrapping_add(value))
                                    .await
                                    .unwrap();
                            }
                            SpawnArg::Busy => println!("Entity actor is busy"),
                            SpawnArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await
            }

            async fn get(&self, id: u64) -> Result<u128, &'static str> {
                let (tx, rx) = oneshot::channel();
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            SpawnArg::Active(state) => {
                                state
                                    .with_value(|v| {
                                        tx.send(*v)
                                            .unwrap_or_else(|_| println!("Failed to send value"))
                                    })
                                    .await
                                    .unwrap();
                            }
                            SpawnArg::Busy => println!("Entity actor is busy"),
                            SpawnArg::Dead => println!("Entity actor is dead"),
                        }
                    })
                    .await?;
                rx.await.map_err(|_| "Failed to receive value in get")
            }

            async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }

            async fn check_consistency(&self, values: HashMap<u64, u128>) {
                let global = self.global.lock().unwrap();
                for (id, value) in &values {
                    let path = get_path(&global.path, *id);
                    let disk_value = match std::fs::read(path) {
                        Ok(data) => u128::from_be_bytes(data.try_into().unwrap()),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                        Err(_) => panic!("Failed to read disk state for id {id}"),
                    };
                    assert_eq!(disk_value, *value, "Disk value mismatch for id {id}");
                }
                for id in values.keys() {
                    let log = global.log.get(id).unwrap();
                    assert!(
                        log.len() % 2 == 0,
                        "Log must contain alternating wakeup and shutdown events"
                    );
                    for (i, (event, _)) in log.iter().enumerate() {
                        assert_eq!(
                            *event,
                            if i % 2 == 0 {
                                Event::Wakeup
                            } else {
                                Event::Shutdown
                            },
                            "Unexpected event type"
                        );
                    }
                }
            }
        }
    }

    async fn test_random(
        db: impl CounterDb,
        entries: &[(u64, u128)],
    ) -> testresult::TestResult<()> {
        // compute the expected values
        let mut reference = HashMap::new();
        for (id, value) in entries {
            let v: &mut u128 = reference.entry(*id).or_default();
            *v = v.wrapping_add(*value);
        }
        // do the same computation using the database, and some concurrency
        // and parallelism (we will get parallelism if we are using a multi-threaded runtime).
        let mut errors = Vec::new();
        n0_future::stream::iter(entries)
            .map(|(id, value)| db.add(*id, *value))
            .buffered_unordered(16)
            .for_each(|result| {
                if let Err(e) = result {
                    errors.push(e);
                }
            })
            .await;
        assert!(errors.is_empty(), "Failed to add some entries: {errors:?}");
        // check that the db contains the expected values
        let ids = reference.keys().copied().collect::<Vec<_>>();
        for id in &ids {
            let res = db.get(*id).await?;
            assert_eq!(res, reference.get(id).copied().unwrap_or_default());
        }
        db.shutdown().await?;
        // check that the db is consistent with the reference
        db.check_consistency(reference).await;
        Ok(())
    }

    #[test_strategy::proptest]
    fn test_counters_manager_proptest_mem(entries: Vec<(u64, u128)>) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .build()
            .expect("Failed to create tokio runtime");
        rt.block_on(async move {
            let db = mem::MemDb::new();
            test_random(db, &entries).await
        })
        .expect("Test failed");
    }

    #[test]
    fn counter_manager_case_1() {
        let entries: Vec<(u64, u128)> = vec![
            (
                18388365709074834514,
                194444835943844072850826645853183303049,
            ),
            (960158425716839792, 70550702232084465071825242386427394095),
            (3186873658602636868, 148188033569534503124640136521127325068),
            (
                15841980008467069742,
                151174981689849463642221795624442563542,
            ),
            (
                13023338767385866119,
                193238684289181333005049821157294047398,
            ),
            (16098863205515154997, 80978418781577370901253652214289107284),
            (
                15494567123826916198,
                155627092752672738120697133860680193469,
            ),
            (5977791771239323572, 123472069275349355161383651481886216632),
            (
                13052414105389926722,
                131961256616178196923794330816338585046,
            ),
            (3265887016396946394, 310496101037658730275516655380984358257),
            (6225591563791670942, 286677436984807113590349457123804595045),
            (7719654970290070575, 261360661842631502732458498326060575764),
            (16346812062252468634, 20400716043115856216887689562958795320),
            (14895994802127672851, 65044937087750427440151725121831729492),
            (
                13867149684135690908,
                251366416673854856023601420456388852782,
            ),
            (13198762185366773363, 16049497253389531856433290782978461866),
            (15662018632505153162, 80545892061421306362192535611401208653),
            (
                10904741085833535475,
                317619745773233937506743252527124191472,
            ),
            (9659369546555131509, 320745667028802630937237317954626404036),
            (9587720266200277507, 53717910536324979289331387490265657851),
            (4520474546171085081, 167298150261458948825809210629580417056),
            (16317689018231234032, 56904692996624675090815630807576954277),
            (1145020483821164407, 62208025549090848844487296478186250150),
            (12275840731881721168, 91306991379847803048964981673487745749),
            (4420042860659089062, 273964008846252634637821526029408158951),
            (6122064752200521014, 318781380881508640367600316004018858292),
            (
                18331791613663867073,
                244797279528173449608649865877521898679,
            ),
            (
                12536131143448721128,
                118936654309826194076792135199593582814,
            ),
            (
                10871411965855657987,
                217312065385902682099626586116718936639,
            ),
            (
                10961019520920546084,
                194969590260788153365214695605968172735,
            ),
            (465341043682902302, 313210447970957224597900277640449786169),
            (1930531913153049005, 191839680217418490007545636458005832918),
            (17610861271281749389, 2158112867473280423584106075388203148),
            (7152124269848448741, 136954811775819110432364454487364326065),
            (1568989979285654655, 20222353298755466913838704346056341244),
            (7965349575514397114, 289787046147370952383585129455974538147),
            (4190853641474855654, 252661458083167852070830937806141596795),
            (15998503947870046493, 64060435197012338872019450356958606700),
            (13026276258592007494, 19335529647117235114474075201115024719),
            (8441510215020513967, 280188261773078012965811787866560404361),
            (
                10888572289604438016,
                107801275926977255088182396292810389978,
            ),
            (8028792684930056950, 279265463046144709574860584412295388880),
            (
                16285632686692032678,
                173211331899082901582219216760818084141,
            ),
            (4252535566180812551, 26342932911822612391116163156475293693),
            (
                13804628128928961483,
                284154599974337978388128510231757620390,
            ),
            (8546188891118395704, 10518169822941605468924627069322563371),
            (15103555168237341487, 65242721972208308766038820838106150898),
            (
                10178269157159463595,
                192339869053417790279388173311883453342,
            ),
            (4581403777792509197, 14776646444861423261575214657678462007),
            (
                16380082972821908840,
                171941200656585174471984020218050726276,
            ),
            (1264309960451701386, 134493939272359117017871860428297927223),
            (
                14979589892199220502,
                111589155031018603387685907617392369538,
            ),
            (17242367228189069279, 72930121541745954198212835929591900203),
            (8121456326821108294, 66979609476744620710556910365293284684),
            (2007258142204162697, 251687000354783335205647262458691353893),
            (8737832108509452949, 220588935218057557435066308484665836550),
            (1633859723143576509, 136307380230378890472597645690064879661),
            (3631531089628868939, 117193189302995348875537780371743885092),
            (14116484475174217773, 54047184398246577478804256478007223219),
            (3958875956249588345, 245230852280065944767864702369745470693),
            (
                13540877093555590901,
                315784456112577256036745768678791135182,
            ),
            (
                16806612096618910759,
                249546776531613765982448526314333635625,
            ),
            (787606879345898885, 106585693463551369746522944067316950687),
            (4600372411498679193, 212195607965427065442095429056450131388),
            (
                15549710434033886583,
                123117317137788876226523772665571872625,
            ),
            (18351596054746942123, 45300934307303895571243742711664551642),
            (4289295596982939716, 57179935864702088014701622708597340262),
            (
                14236340878952223471,
                276513355195514407125503145808612698776,
            ),
            (
                16793945091236936234,
                225504874419954047826410211270129372771,
            ),
            (6083434966673351288, 132608700674916922118081114178409918884),
            (8650928456447960764, 224830460140536261415627615111659295798),
            (544098001581244681, 23519946291190733604063394180239868939),
            (6677173915608753908, 172822575779164355980943891119053877737),
            (9904411679706436478, 305002806492672706404515108669905493060),
            (3748913669028317014, 198508895764401149420178353122470259248),
            (18429857739493813163, 52742680723713620788331716379318053621),
            (
                16160446355791350386,
                138122750568611844010694432995073272608,
            ),
            (6546381586950511182, 211244543160814081913465334374927296227),
            (2141568463796935601, 208339082504024808729178477691685775704),
            (1557540402694871065, 123820668511543909640550604769940320362),
            (4806376387020260938, 242198889246468758028422097703476517279),
            (10572989355971956281, 10538572317690180019027649199957487191),
            (4419528985467303189, 241717029481312211533043931115392103679),
            (6026123969772770806, 107638168362307674120726354668199681855),
            (10446707069162381062, 8055541720197104310969619867721024782),
            (5775673932884639633, 78808623784784204136453081044300463180),
            (8334877772954168794, 247863931114156722743273061049957344581),
            (3268873687286354543, 295921729987362586985932430782633333103),
            (3966164288731105812, 168025259063973884714536065035065597041),
            (
                11203489126702094930,
                202722625156888049503317726338728149715,
            ),
            (15630106992179578877, 35009551135670394358942249653897229827),
            (8273535691491394488, 109976597742540116974717569314107701591),
            (3805901298951698996, 235306340537276700227769214923480868076),
            (2117742626710589122, 327739111844587050291636958827184679871),
            (
                15269331250753017373,
                319720502694359881891904231590616597459,
            ),
            (6441260656891689190, 21330291223001965237969254632976565895),
            (7256786238996167073, 176897044696772200117671096478294494714),
            (12590622778405228523, 79144381831870004849360854116364256296),
        ];
        let rt = tokio::runtime::Builder::new_multi_thread()
            .build()
            .expect("Failed to create tokio runtime");
        rt.block_on(async move {
            let db = mem::MemDb::new();
            test_random(db, &entries).await
        })
        .expect("Test failed");
    }

    #[test_strategy::proptest]
    fn test_counters_manager_proptest_fs(entries: Vec<(u64, u128)>) {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .build()
            .expect("Failed to create tokio runtime");
        rt.block_on(async move {
            let db = fs::FsDb::new(dir.path());
            test_random(db, &entries).await
        })
        .expect("Test failed");
    }
}
