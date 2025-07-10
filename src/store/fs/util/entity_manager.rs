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
            println!("Calling on_shutdown {}", self.state.state.ref_count());
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

        /// Friendly version of `spawn_boxed` that does the boxing
        #[must_use = "this function may return a future that must be spawned by the caller"]
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
                            eprintln!("Task failed: {e:?}");
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

    use super::*;

    mod mem {
        use std::sync::Arc;

        use atomic_refcell::AtomicRefCell;

        use super::*;

        #[derive(Debug, Clone, Default)]
        struct State(Arc<AtomicRefCell<u128>>);

        impl Reset for State {
            fn reset(&mut self) {
                *self.0.borrow_mut() = Default::default();
            }

            fn ref_count(&self) -> usize {
                Arc::strong_count(&self.0)
            }
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = ();
            type EntityState = State;
            async fn on_shutdown(_state: entity_actor::State<Self>, _cause: ShutdownCause) {}
        }

        struct CountersManager {
            m: EntityManager<Counters>,
        }

        impl entity_actor::State<Counters> {
            async fn with_value(&self, f: impl FnOnce(&mut u128)) -> Result<(), &'static str> {
                let mut r = self.state.0.borrow_mut();
                f(&mut r);
                Ok(())
            }
        }

        impl CountersManager {
            pub fn new() -> Self {
                Self {
                    m: EntityManager::<Counters>::new((), Options::default()),
                }
            }

            pub async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
                self.m
                    .spawn(id, move |arg| async move {
                        match arg {
                            SpawnArg::Active(state) => {
                                // println!(
                                //     "Adding value {} to entity actor with id {:?}",
                                //     value, state.id
                                // );
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

            pub async fn get(&self, id: u64) -> Result<u128, &'static str> {
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

            pub async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }
        }

        #[tokio::test]
        async fn bench_entity_manager_mem() -> testresult::TestResult<()> {
            let counter_manager = CountersManager::new();
            for i in 0..100 {
                counter_manager.add(i, i as u128).await?;
            }
            counter_manager.shutdown().await?;
            Ok(())
        }
    }

    mod persistent {
        use std::{
            path::{Path, PathBuf},
            sync::Arc,
            time::Duration,
        };

        use atomic_refcell::AtomicRefCell;

        use super::*;

        #[derive(Debug, Clone, Default)]
        enum State {
            #[default]
            Disk,
            Memory(u128),
        }

        #[derive(Debug, Clone, Default)]
        struct EntityState(Arc<AtomicRefCell<State>>);

        impl Reset for EntityState {
            fn reset(&mut self) {
                *self.0.borrow_mut() = State::Disk;
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
                let mut r = self.state.0.borrow_mut();
                if let State::Disk = &*r {
                    let path = get_path(&*self.global, self.id);
                    let value = match tokio::fs::read(path).await {
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
                    *r = State::Memory(value);
                }
                let State::Memory(value) = &mut *r else {
                    panic!("State must be Memory at this point");
                };
                f(value);
                Ok(())
            }
        }

        struct Counters;
        impl Params for Counters {
            type EntityId = u64;
            type GlobalState = Arc<PathBuf>;
            type EntityState = EntityState;
            async fn on_shutdown(state: entity_actor::State<Self>, cause: ShutdownCause) {
                let r = state.state.0.borrow();
                if let State::Memory(value) = &*r {
                    let path = get_path(&*state.global, state.id);
                    println!(
                        "{} persisting value {} to {}, cause {:?}",
                        state.id,
                        value,
                        path.display(),
                        cause
                    );
                    let value_bytes = value.to_be_bytes();
                    tokio::fs::write(&path, &value_bytes)
                        .await
                        .expect("Failed to write disk state");
                    println!(
                        "{} persisted value {} to {}",
                        state.id,
                        value,
                        path.display()
                    );
                }
            }
        }

        struct CountersManager {
            m: EntityManager<Counters>,
        }

        impl CountersManager {
            pub fn new(path: impl AsRef<Path>) -> Self {
                let state = Arc::new(path.as_ref().to_owned());
                Self {
                    m: EntityManager::<Counters>::new(state, Options::default()),
                }
            }

            pub async fn add(&self, id: u64, value: u128) -> Result<(), &'static str> {
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

            pub async fn get(&self, id: u64) -> Result<u128, &'static str> {
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

            pub async fn shutdown(&self) -> Result<(), &'static str> {
                self.m.shutdown().await
            }
        }

        #[tokio::test]
        async fn bench_entity_manager_fs() -> testresult::TestResult<()> {
            let dir = tempfile::tempdir()?;
            let counter_manager = CountersManager::new(dir.path());
            for i in 0..100 {
                counter_manager.add(i, i as u128).await?;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            counter_manager.shutdown().await?;
            Ok(())
        }
    }
}
