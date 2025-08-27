//! A simple iroh connection pool
//!
//! Entry point is [`ConnectionPool`]. You create a connection pool for a specific
//! ALPN and [`Options`]. Then the pool will manage connections for you.
//!
//! Access to connections is via the [`ConnectionPool::get_or_connect`] method, which
//! gives you access to a connection via a [`ConnectionRef`] if possible.
//!
//! It is important that you keep the [`ConnectionRef`] alive while you are using
//! the connection.
use std::{
    collections::{HashMap, VecDeque},
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use iroh::{endpoint::ConnectError, Endpoint, NodeId};
use n0_future::{
    future::{self},
    FuturesUnordered, MaybeFuture, Stream, StreamExt,
};
use snafu::Snafu;
use tokio::sync::{
    mpsc::{self, error::SendError as TokioSendError},
    oneshot, Notify,
};
use tokio_util::time::FutureExt as TimeFutureExt;
use tracing::{debug, error, info, trace};

/// Configuration options for the connection pool
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_connections: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(1),
            max_connections: 1024,
        }
    }
}

/// A reference to a connection that is owned by a connection pool.
#[derive(Debug)]
pub struct ConnectionRef {
    connection: iroh::endpoint::Connection,
    _permit: OneConnection,
}

impl Deref for ConnectionRef {
    type Target = iroh::endpoint::Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl ConnectionRef {
    fn new(connection: iroh::endpoint::Connection, counter: OneConnection) -> Self {
        Self {
            connection,
            _permit: counter,
        }
    }
}

/// Error when a connection can not be acquired
///
/// This includes the normal iroh connection errors as well as pool specific
/// errors such as timeouts and connection limits.
#[derive(Debug, Clone, Snafu)]
#[snafu(module)]
pub enum PoolConnectError {
    /// Connection pool is shut down
    Shutdown,
    /// Timeout during connect
    Timeout,
    /// Too many connections
    TooManyConnections,
    /// Error during connect
    ConnectError { source: Arc<ConnectError> },
}

impl From<ConnectError> for PoolConnectError {
    fn from(e: ConnectError) -> Self {
        PoolConnectError::ConnectError {
            source: Arc::new(e),
        }
    }
}

/// Error when calling a fn on the [`ConnectionPool`].
///
/// The only thing that can go wrong is that the connection pool is shut down.
#[derive(Debug, Snafu)]
#[snafu(module)]
pub enum ConnectionPoolError {
    /// The connection pool has been shut down
    Shutdown,
}

enum ActorMessage {
    RequestRef(RequestRef),
    ConnectionIdle { id: NodeId },
    ConnectionShutdown { id: NodeId },
}

struct RequestRef {
    id: NodeId,
    tx: oneshot::Sender<Result<ConnectionRef, PoolConnectError>>,
}

struct Context {
    options: Options,
    endpoint: Endpoint,
    owner: ConnectionPool,
    alpn: Vec<u8>,
}

impl Context {
    async fn run_connection_actor(
        self: Arc<Self>,
        node_id: NodeId,
        mut rx: mpsc::Receiver<RequestRef>,
    ) {
        let context = self;

        // Connect to the node
        let state = context
            .endpoint
            .connect(node_id, &context.alpn)
            .timeout(context.options.connect_timeout)
            .await
            .map_err(|_| PoolConnectError::Timeout)
            .and_then(|r| r.map_err(PoolConnectError::from));
        let conn_close = match &state {
            Ok(conn) => {
                let conn = conn.clone();
                MaybeFuture::Some(async move { conn.closed().await })
            }
            Err(e) => {
                debug!(%node_id, "Failed to connect {e:?}, requesting shutdown");
                tokio::time::sleep(Duration::from_secs(1)).await;
                if context.owner.close(node_id).await.is_err() {
                    return;
                }
                MaybeFuture::None
            }
        };

        let counter = ConnectionCounter::new();
        let idle_timer = MaybeFuture::default();
        let idle_stream = counter.clone().idle_stream();

        tokio::pin!(idle_timer, idle_stream, conn_close);

        loop {
            tokio::select! {
                biased;

                // Handle new work
                handler = rx.recv() => {
                    match handler {
                        Some(RequestRef { id, tx }) => {
                            assert!(id == node_id, "Not for me!");
                            match &state {
                                Ok(state) => {
                                    let res = ConnectionRef::new(state.clone(), counter.get_one());
                                    info!(%node_id, "Handing out ConnectionRef {}", counter.current());

                                    // clear the idle timer
                                    idle_timer.as_mut().set_none();
                                    tx.send(Ok(res)).ok();
                                }
                                Err(cause) => {
                                    tx.send(Err(cause.clone())).ok();
                                }
                            }
                        }
                        None => {
                            // Channel closed - exit
                            break;
                        }
                    }
                }

                _ = &mut conn_close => {
                    // connection was closed by somebody, notify owner that we should be removed
                    context.owner.close(node_id).await.ok();
                }

                _ = idle_stream.next() => {
                    if !counter.is_idle() {
                        continue;
                    };
                    // notify the pool that we are idle.
                    trace!(%node_id, "Idle");
                    if context.owner.idle(node_id).await.is_err() {
                        // If we can't notify the pool, we are shutting down
                        break;
                    }
                    // set the idle timer
                    idle_timer.as_mut().set_future(tokio::time::sleep(context.options.idle_timeout));
                }

                // Idle timeout - request shutdown
                _ = &mut idle_timer => {
                    trace!(%node_id, "Idle timer expired, requesting shutdown");
                    context.owner.close(node_id).await.ok();
                    // Don't break here - wait for main actor to close our channel
                }
            }
        }

        if let Ok(connection) = state {
            let reason = if counter.is_idle() { b"idle" } else { b"drop" };
            connection.close(0u32.into(), reason);
        }

        trace!(%node_id, "Connection actor shutting down");
    }
}

struct Actor {
    rx: mpsc::Receiver<ActorMessage>,
    connections: HashMap<NodeId, mpsc::Sender<RequestRef>>,
    context: Arc<Context>,
    // idle set (most recent last)
    // todo: use a better data structure if this becomes a performance issue
    idle: VecDeque<NodeId>,
    // per connection tasks
    tasks: FuturesUnordered<future::Boxed<()>>,
}

impl Actor {
    pub fn new(
        endpoint: Endpoint,
        alpn: &[u8],
        options: Options,
    ) -> (Self, mpsc::Sender<ActorMessage>) {
        let (tx, rx) = mpsc::channel(100);
        (
            Self {
                rx,
                connections: HashMap::new(),
                idle: VecDeque::new(),
                context: Arc::new(Context {
                    options,
                    alpn: alpn.to_vec(),
                    endpoint,
                    owner: ConnectionPool { tx: tx.clone() },
                }),
                tasks: FuturesUnordered::new(),
            },
            tx,
        )
    }

    fn add_idle(&mut self, id: NodeId) {
        self.remove_idle(id);
        self.idle.push_back(id);
    }

    fn remove_idle(&mut self, id: NodeId) {
        self.idle.retain(|&x| x != id);
    }

    fn pop_oldest_idle(&mut self) -> Option<NodeId> {
        self.idle.pop_front()
    }

    fn remove_connection(&mut self, id: NodeId) {
        self.connections.remove(&id);
        self.remove_idle(id);
    }

    async fn handle_msg(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::RequestRef(mut msg) => {
                let id = msg.id;
                self.remove_idle(id);
                // Try to send to existing connection actor
                if let Some(conn_tx) = self.connections.get(&id) {
                    if let Err(TokioSendError(e)) = conn_tx.send(msg).await {
                        msg = e;
                    } else {
                        return;
                    }
                    // Connection actor died, remove it
                    self.remove_connection(id);
                }

                // No connection actor or it died - check limits
                if self.connections.len() >= self.context.options.max_connections {
                    if let Some(idle) = self.pop_oldest_idle() {
                        // remove the oldest idle connection to make room for one more
                        trace!("removing oldest idle connection {}", idle);
                        self.connections.remove(&idle);
                    } else {
                        msg.tx.send(Err(PoolConnectError::TooManyConnections)).ok();
                        return;
                    }
                }
                let (conn_tx, conn_rx) = mpsc::channel(100);
                self.connections.insert(id, conn_tx.clone());

                let context = self.context.clone();

                self.tasks
                    .push(Box::pin(context.run_connection_actor(id, conn_rx)));

                // Send the handler to the new actor
                if conn_tx.send(msg).await.is_err() {
                    error!(%id, "Failed to send handler to new connection actor");
                    self.connections.remove(&id);
                }
            }
            ActorMessage::ConnectionIdle { id } => {
                self.add_idle(id);
                trace!(%id, "connection idle");
            }
            ActorMessage::ConnectionShutdown { id } => {
                // Remove the connection from our map - this closes the channel
                self.remove_connection(id);
                trace!(%id, "removed connection");
            }
        }
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;

                msg = self.rx.recv() => {
                    if let Some(msg) = msg {
                        self.handle_msg(msg).await;
                    } else {
                        break;
                    }
                }

                _ = self.tasks.next(), if !self.tasks.is_empty() => {}
            }
        }
    }
}

/// A connection pool
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    tx: mpsc::Sender<ActorMessage>,
}

impl ConnectionPool {
    pub fn new(endpoint: Endpoint, alpn: &[u8], options: Options) -> Self {
        let (actor, tx) = Actor::new(endpoint, alpn, options);

        // Spawn the main actor
        tokio::spawn(actor.run());

        Self { tx }
    }

    /// Returns either a fresh connection or a reference to an existing one.
    ///
    /// This is guaranteed to return after approximately [Options::connect_timeout]
    /// with either an error or a connection.
    pub async fn get_or_connect(
        &self,
        id: NodeId,
    ) -> std::result::Result<ConnectionRef, PoolConnectError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::RequestRef(RequestRef { id, tx }))
            .await
            .map_err(|_| PoolConnectError::Shutdown)?;
        rx.await.map_err(|_| PoolConnectError::Shutdown)?
    }

    /// Close an existing connection, if it exists
    ///
    /// This will finish pending tasks and close the connection. New tasks will
    /// get a new connection if they are submitted after this call
    pub async fn close(&self, id: NodeId) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionShutdown { id })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)?;
        Ok(())
    }

    /// Notify the connection pool that a connection is idle.
    ///
    /// Should only be called from connection handlers.
    pub(crate) async fn idle(&self, id: NodeId) -> std::result::Result<(), ConnectionPoolError> {
        self.tx
            .send(ActorMessage::ConnectionIdle { id })
            .await
            .map_err(|_| ConnectionPoolError::Shutdown)?;
        Ok(())
    }
}

#[derive(Debug)]
struct ConnectionCounterInner {
    count: AtomicUsize,
    notify: Notify,
}

#[derive(Debug, Clone)]
struct ConnectionCounter {
    inner: Arc<ConnectionCounterInner>,
}

impl ConnectionCounter {
    fn new() -> Self {
        Self {
            inner: Arc::new(ConnectionCounterInner {
                count: Default::default(),
                notify: Notify::new(),
            }),
        }
    }

    fn current(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    /// Increase the connection count and return a guard for the new connection
    fn get_one(&self) -> OneConnection {
        self.inner.count.fetch_add(1, Ordering::SeqCst);
        OneConnection {
            inner: self.inner.clone(),
        }
    }

    fn is_idle(&self) -> bool {
        self.inner.count.load(Ordering::SeqCst) == 0
    }

    /// Infinite stream that yields when the connection is briefly idle.
    ///
    /// Note that you still have to check if the connection is still idle when
    /// you get the notification.
    ///
    /// Also note that this stream is triggered on [OneConnection::drop], so it
    /// won't trigger initially even though a [ConnectionCounter] starts up as
    /// idle.
    fn idle_stream(self) -> impl Stream<Item = ()> {
        n0_future::stream::unfold(self, |c| async move {
            c.inner.notify.notified().await;
            Some(((), c))
        })
    }
}

/// Guard for one connection
#[derive(Debug)]
struct OneConnection {
    inner: Arc<ConnectionCounterInner>,
}

impl Drop for OneConnection {
    fn drop(&mut self) {
        if self.inner.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.notify.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use iroh::{
        discovery::static_provider::StaticProvider,
        endpoint::Connection,
        protocol::{AcceptError, ProtocolHandler, Router},
        NodeAddr, NodeId, SecretKey, Watcher,
    };
    use n0_future::{stream, BufferedStreamExt, StreamExt};
    use n0_snafu::ResultExt;
    use testresult::TestResult;
    use tracing::trace;

    use super::{ConnectionPool, Options, PoolConnectError};

    const ECHO_ALPN: &[u8] = b"echo";

    #[derive(Debug, Clone)]
    struct Echo;

    impl ProtocolHandler for Echo {
        async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
            let conn_id = connection.stable_id();
            let id = connection.remote_node_id().map_err(AcceptError::from_err)?;
            trace!(%id, %conn_id, "Accepting echo connection");
            loop {
                match connection.accept_bi().await {
                    Ok((mut send, mut recv)) => {
                        trace!(%id, %conn_id, "Accepted echo request");
                        tokio::io::copy(&mut recv, &mut send).await?;
                        send.finish().map_err(AcceptError::from_err)?;
                    }
                    Err(e) => {
                        trace!(%id, %conn_id, "Failed to accept echo request {e}");
                        break;
                    }
                }
            }
            Ok(())
        }
    }

    async fn echo_client(conn: &Connection, text: &[u8]) -> n0_snafu::Result<Vec<u8>> {
        let conn_id = conn.stable_id();
        let id = conn.remote_node_id().e()?;
        trace!(%id, %conn_id, "Sending echo request");
        let (mut send, mut recv) = conn.open_bi().await.e()?;
        send.write_all(text).await.e()?;
        send.finish().e()?;
        let response = recv.read_to_end(1000).await.e()?;
        trace!(%id, %conn_id, "Received echo response");
        Ok(response)
    }

    async fn echo_server() -> TestResult<(NodeAddr, Router)> {
        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![ECHO_ALPN.to_vec()])
            .bind()
            .await?;
        let addr = endpoint.node_addr().initialized().await;
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(ECHO_ALPN, Echo)
            .spawn();

        Ok((addr, router))
    }

    async fn echo_servers(n: usize) -> TestResult<Vec<(NodeAddr, Router)>> {
        stream::iter(0..n)
            .map(|_| echo_server())
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }

    fn test_options() -> Options {
        Options {
            idle_timeout: Duration::from_millis(100),
            connect_timeout: Duration::from_secs(2),
            max_connections: 32,
            on_connect: None,
        }
    }

    struct EchoClient {
        pool: ConnectionPool,
    }

    impl EchoClient {
        async fn echo(
            &self,
            id: NodeId,
            text: Vec<u8>,
        ) -> Result<Result<(usize, Vec<u8>), n0_snafu::Error>, PoolConnectError> {
            let conn = self.pool.get_or_connect(id).await?;
            let id = conn.stable_id();
            match echo_client(&conn, &text).await {
                Ok(res) => Ok(Ok((id, res))),
                Err(e) => Ok(Err(e)),
            }
        }
    }

    #[tokio::test]
    async fn connection_pool_errors() -> TestResult<()> {
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .ok();
        // set up static discovery for all addrs
        let discovery = StaticProvider::new();
        let endpoint = iroh::Endpoint::builder()
            .discovery(discovery.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(endpoint, ECHO_ALPN, test_options());
        let client = EchoClient { pool };
        {
            let non_existing = SecretKey::from_bytes(&[0; 32]).public();
            let res = client.echo(non_existing, b"Hello, world!".to_vec()).await;
            // trying to connect to a non-existing id will fail with ConnectError
            // because we don't have any information about the node
            assert!(matches!(res, Err(PoolConnectError::ConnectError { .. })));
        }
        {
            let non_listening = SecretKey::from_bytes(&[0; 32]).public();
            // make up fake node info
            discovery.add_node_info(NodeAddr {
                node_id: non_listening,
                relay_url: None,
                direct_addresses: vec!["127.0.0.1:12121".parse().unwrap()]
                    .into_iter()
                    .collect(),
            });
            // trying to connect to an id for which we have info, but the other
            // end is not listening, will lead to a timeout.
            let res = client.echo(non_listening, b"Hello, world!".to_vec()).await;
            assert!(matches!(res, Err(PoolConnectError::Timeout)));
        }
        Ok(())
    }

    #[tokio::test]
    async fn connection_pool_smoke() -> TestResult<()> {
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .ok();
        let n = 32;
        let nodes = echo_servers(n).await?;
        let ids = nodes
            .iter()
            .map(|(addr, _)| addr.node_id)
            .collect::<Vec<_>>();
        // set up static discovery for all addrs
        let discovery = StaticProvider::from_node_info(nodes.iter().map(|(addr, _)| addr.clone()));
        // build a client endpoint that can resolve all the node ids
        let endpoint = iroh::Endpoint::builder()
            .discovery(discovery.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(endpoint.clone(), ECHO_ALPN, test_options());
        let client = EchoClient { pool };
        let mut connection_ids = BTreeMap::new();
        let msg = b"Hello, world!".to_vec();
        for id in &ids {
            let (cid1, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            let (cid2, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            assert_eq!(cid1, cid2);
            connection_ids.insert(id, cid1);
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
        for id in &ids {
            let cid1 = *connection_ids.get(id).expect("Connection ID not found");
            let (cid2, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
            assert_ne!(cid1, cid2);
        }
        Ok(())
    }

    /// Tests that idle connections are being reclaimed to make room if we hit the
    /// maximum connection limit.
    #[tokio::test]
    async fn connection_pool_idle() -> TestResult<()> {
        let filter = tracing_subscriber::EnvFilter::from_default_env();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .ok();
        let n = 32;
        let nodes = echo_servers(n).await?;
        let ids = nodes
            .iter()
            .map(|(addr, _)| addr.node_id)
            .collect::<Vec<_>>();
        // set up static discovery for all addrs
        let discovery = StaticProvider::from_node_info(nodes.iter().map(|(addr, _)| addr.clone()));
        // build a client endpoint that can resolve all the node ids
        let endpoint = iroh::Endpoint::builder()
            .discovery(discovery.clone())
            .bind()
            .await?;
        let pool = ConnectionPool::new(
            endpoint.clone(),
            ECHO_ALPN,
            Options {
                idle_timeout: Duration::from_secs(100),
                max_connections: 8,
                ..test_options()
            },
        );
        let client = EchoClient { pool };
        let msg = b"Hello, world!".to_vec();
        for id in &ids {
            let (_, res) = client.echo(*id, msg.clone()).await??;
            assert_eq!(res, msg);
        }
        Ok(())
    }
}
