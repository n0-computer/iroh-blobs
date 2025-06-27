//! API for downloads from multiple nodes.
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::{Future, IntoFuture},
    io,
    ops::Deref,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::bail;
use genawaiter::sync::Gen;
use iroh::{endpoint::Connection, Endpoint, NodeId};
use irpc::{channel::mpsc, rpc_requests};
use n0_future::{future, stream, BufferedStreamExt, Stream, StreamExt};
use rand::seq::SliceRandom;
use serde::{de::Error, Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::time::FutureExt;
use tracing::{info, instrument::Instrument, warn};

use super::{remote::GetConnection, Store};
use crate::{
    protocol::{GetManyRequest, GetRequest},
    util::sink::{Drain, IrpcSenderRefSink, Sink, TokioMpscSenderSink},
    BlobFormat, Hash, HashAndFormat,
};

#[derive(Debug, Clone)]
pub struct Downloader {
    client: irpc::Client<SwarmMsg, SwarmProtocol, DownloaderService>,
}

#[derive(Debug, Clone)]
pub struct DownloaderService;

impl irpc::Service for DownloaderService {}

#[rpc_requests(DownloaderService, message = SwarmMsg, alias = "Msg")]
#[derive(Debug, Serialize, Deserialize)]
enum SwarmProtocol {
    #[rpc(tx = mpsc::Sender<DownloadProgessItem>)]
    Download(DownloadRequest),
}

struct DownloaderActor {
    store: Store,
    pool: ConnectionPool,
    tasks: JoinSet<()>,
    running: HashSet<tokio::task::Id>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgessItem {
    #[serde(skip)]
    Error(anyhow::Error),
    TryProvider {
        id: NodeId,
        request: Arc<GetRequest>,
    },
    ProviderFailed {
        id: NodeId,
        request: Arc<GetRequest>,
    },
    PartComplete {
        request: Arc<GetRequest>,
    },
    Progress(u64),
    DownloadError,
}

impl DownloaderActor {
    fn new(store: Store, endpoint: Endpoint) -> Self {
        Self {
            store,
            pool: ConnectionPool::new(endpoint, crate::ALPN.to_vec()),
            tasks: JoinSet::new(),
            running: HashSet::new(),
        }
    }

    async fn run(mut self, mut rx: tokio::sync::mpsc::Receiver<SwarmMsg>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                SwarmMsg::Download(request) => {
                    self.spawn(handle_download(
                        self.store.clone(),
                        self.pool.clone(),
                        request,
                    ));
                }
            }
        }
    }

    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        let span = tracing::Span::current();
        let id = self.tasks.spawn(fut.instrument(span)).id();
        self.running.insert(id);
    }
}

async fn handle_download(store: Store, pool: ConnectionPool, msg: DownloadMsg) {
    let DownloadMsg { inner, mut tx, .. } = msg;
    if let Err(cause) = handle_download_impl(store, pool, inner, &mut tx).await {
        tx.send(DownloadProgessItem::Error(cause)).await.ok();
    }
}

async fn handle_download_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgessItem>,
) -> anyhow::Result<()> {
    match request.strategy {
        SplitStrategy::Split => handle_download_split_impl(store, pool, request, tx).await?,
        SplitStrategy::None => match request.request {
            FiniteRequest::Get(get) => {
                let sink = IrpcSenderRefSink(tx).with_map_err(io::Error::other);
                execute_get(&pool, Arc::new(get), &request.providers, &store, sink).await?;
            }
            FiniteRequest::GetMany(_) => {
                handle_download_split_impl(store, pool, request, tx).await?
            }
        },
    }
    Ok(())
}

async fn handle_download_split_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgessItem>,
) -> anyhow::Result<()> {
    let providers = request.providers;
    let requests = split_request(&request.request, &providers, &pool, &store, Drain).await?;
    let (progress_tx, progress_rx) = tokio::sync::mpsc::channel(32);
    let mut futs = stream::iter(requests.into_iter().enumerate())
        .map(|(id, request)| {
            let pool = pool.clone();
            let providers = providers.clone();
            let store = store.clone();
            let progress_tx = progress_tx.clone();
            async move {
                let hash = request.hash;
                let (tx, rx) = tokio::sync::mpsc::channel::<(usize, DownloadProgessItem)>(16);
                progress_tx.send(rx).await.ok();
                let sink = TokioMpscSenderSink(tx)
                    .with_map_err(io::Error::other)
                    .with_map(move |x| (id, x));
                let res = execute_get(&pool, Arc::new(request), &providers, &store, sink).await;
                (hash, res)
            }
        })
        .buffered_unordered(32);
    let mut progress_stream = {
        let mut offsets = HashMap::new();
        let mut total = 0;
        into_stream(progress_rx)
            .flat_map(into_stream)
            .map(move |(id, item)| match item {
                DownloadProgessItem::Progress(offset) => {
                    total += offset;
                    if let Some(prev) = offsets.insert(id, offset) {
                        total -= prev;
                    }
                    DownloadProgessItem::Progress(total)
                }
                x => x,
            })
    };
    loop {
        tokio::select! {
            Some(item) = progress_stream.next() => {
                tx.send(item).await?;
            },
            res = futs.next() => {
                match res {
                    Some((_hash, Ok(()))) => {
                    }
                    Some((_hash, Err(_e))) => {
                        tx.send(DownloadProgessItem::DownloadError).await?;
                    }
                    None => break,
                }
            }
            _ = tx.closed() => {
                // The sender has been closed, we should stop processing.
                break;
            }
        }
    }
    Ok(())
}

fn into_stream<T>(mut recv: tokio::sync::mpsc::Receiver<T>) -> impl Stream<Item = T> {
    Gen::new(|co| async move {
        while let Some(item) = recv.recv().await {
            co.yield_(item).await;
        }
    })
}

#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum FiniteRequest {
    Get(GetRequest),
    GetMany(GetManyRequest),
}

pub trait SupportedRequest {
    fn into_request(self) -> FiniteRequest;
}

impl<I: Into<Hash>, T: IntoIterator<Item = I>> SupportedRequest for T {
    fn into_request(self) -> FiniteRequest {
        let hashes = self.into_iter().map(Into::into).collect::<GetManyRequest>();
        FiniteRequest::GetMany(hashes)
    }
}

impl SupportedRequest for GetRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for GetManyRequest {
    fn into_request(self) -> FiniteRequest {
        self.into()
    }
}

impl SupportedRequest for Hash {
    fn into_request(self) -> FiniteRequest {
        GetRequest::blob(self).into()
    }
}

impl SupportedRequest for HashAndFormat {
    fn into_request(self) -> FiniteRequest {
        (match self.format {
            BlobFormat::Raw => GetRequest::blob(self.hash),
            BlobFormat::HashSeq => GetRequest::all(self.hash),
        })
        .into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddProviderRequest {
    pub hash: Hash,
    pub providers: Vec<NodeId>,
}

#[derive(Debug)]
pub struct DownloadRequest {
    pub request: FiniteRequest,
    pub providers: Arc<dyn ContentDiscovery>,
    pub strategy: SplitStrategy,
}

impl DownloadRequest {
    pub fn new(
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
        strategy: SplitStrategy,
    ) -> Self {
        Self {
            request: request.into_request(),
            providers: Arc::new(providers),
            strategy,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SplitStrategy {
    None,
    Split,
}

impl Serialize for DownloadRequest {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom(
            "cannot serialize DownloadRequest",
        ))
    }
}

// Implement Deserialize to always fail
impl<'de> Deserialize<'de> for DownloadRequest {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom("cannot deserialize DownloadRequest"))
    }
}

pub type DownloadOptions = DownloadRequest;

pub struct DownloadProgress {
    fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgessItem>>>,
}

impl DownloadProgress {
    fn new(fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgessItem>>>) -> Self {
        Self { fut }
    }

    pub async fn stream(self) -> irpc::Result<impl Stream<Item = DownloadProgessItem> + Unpin> {
        let rx = self.fut.await?;
        Ok(Box::pin(rx.into_stream().map(|item| match item {
            Ok(item) => item,
            Err(e) => DownloadProgessItem::Error(e.into()),
        })))
    }

    async fn complete(self) -> anyhow::Result<()> {
        let rx = self.fut.await?;
        let stream = rx.into_stream();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            match item? {
                DownloadProgessItem::Error(e) => Err(e)?,
                DownloadProgessItem::DownloadError => anyhow::bail!("Download error"),
                _ => {}
            }
        }
        Ok(())
    }
}

impl IntoFuture for DownloadProgress {
    type Output = anyhow::Result<()>;
    type IntoFuture = future::Boxed<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.complete())
    }
}

impl Downloader {
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<SwarmMsg>(32);
        let actor = DownloaderActor::new(store.clone(), endpoint.clone());
        tokio::spawn(actor.run(rx));
        Self { client: tx.into() }
    }

    pub fn download(
        &self,
        request: impl SupportedRequest,
        providers: impl ContentDiscovery,
    ) -> DownloadProgress {
        let request = request.into_request();
        let providers = Arc::new(providers);
        self.download_with_opts(DownloadOptions {
            request,
            providers,
            strategy: SplitStrategy::Split,
        })
    }

    pub fn download_with_opts(&self, options: DownloadOptions) -> DownloadProgress {
        let fut = self.client.server_streaming(options, 32);
        DownloadProgress::new(Box::pin(fut))
    }
}

/// Split a request into multiple requests that can be run in parallel.
async fn split_request<'a>(
    request: &'a FiniteRequest,
    providers: &Arc<dyn ContentDiscovery>,
    pool: &ConnectionPool,
    store: &Store,
    progress: impl Sink<DownloadProgessItem, Error = io::Error>,
) -> anyhow::Result<Box<dyn Iterator<Item = GetRequest> + Send + 'a>> {
    Ok(match request {
        FiniteRequest::Get(req) => {
            let Some(_first) = req.ranges.iter_infinite().next() else {
                return Ok(Box::new(std::iter::empty()));
            };
            let first = GetRequest::blob(req.hash);
            execute_get(pool, Arc::new(first), providers, store, progress).await?;
            let size = store.observe(req.hash).await?.size();
            anyhow::ensure!(size % 32 == 0, "Size is not a multiple of 32");
            let n = size / 32;
            Box::new(
                req.ranges
                    .iter_infinite()
                    .take(n as usize + 1)
                    .enumerate()
                    .filter_map(|(i, ranges)| {
                        if i != 0 && !ranges.is_empty() {
                            Some(
                                GetRequest::builder()
                                    .offset(i as u64, ranges.clone())
                                    .build(req.hash),
                            )
                        } else {
                            None
                        }
                    }),
            )
        }
        FiniteRequest::GetMany(req) => Box::new(
            req.hashes
                .iter()
                .enumerate()
                .map(|(i, hash)| GetRequest::blob_ranges(*hash, req.ranges[i as u64].clone())),
        ),
    })
}

#[derive(Debug)]
struct ConnectionPoolInner {
    alpn: Vec<u8>,
    endpoint: Endpoint,
    connections: Mutex<HashMap<NodeId, Arc<Mutex<SlotState>>>>,
    retry_delay: Duration,
    connect_timeout: Duration,
}

#[derive(Debug, Clone)]
struct ConnectionPool(Arc<ConnectionPoolInner>);

#[derive(Debug, Default)]
enum SlotState {
    #[default]
    Initial,
    Connected(Connection),
    AttemptFailed(SystemTime),
    #[allow(dead_code)]
    Evil(String),
}

impl ConnectionPool {
    fn new(endpoint: Endpoint, alpn: Vec<u8>) -> Self {
        Self(
            ConnectionPoolInner {
                endpoint,
                alpn,
                connections: Default::default(),
                retry_delay: Duration::from_secs(5),
                connect_timeout: Duration::from_secs(2),
            }
            .into(),
        )
    }

    pub fn alpn(&self) -> &[u8] {
        &self.0.alpn
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.0.endpoint
    }

    pub fn retry_delay(&self) -> Duration {
        self.0.retry_delay
    }

    fn dial(&self, id: NodeId) -> DialNode {
        DialNode {
            pool: self.clone(),
            id,
        }
    }

    #[allow(dead_code)]
    async fn mark_evil(&self, id: NodeId, reason: String) {
        let slot = self
            .0
            .connections
            .lock()
            .await
            .entry(id)
            .or_default()
            .clone();
        let mut t = slot.lock().await;
        *t = SlotState::Evil(reason)
    }

    #[allow(dead_code)]
    async fn mark_closed(&self, id: NodeId) {
        let slot = self
            .0
            .connections
            .lock()
            .await
            .entry(id)
            .or_default()
            .clone();
        let mut t = slot.lock().await;
        *t = SlotState::Initial
    }
}

/// Execute a get request sequentially for multiple providers.
///
/// It will try each provider in order
/// until it finds one that can fulfill the request. When trying a new provider,
/// it takes the progress from the previous providers into account, so e.g.
/// if the first provider had the first 10% of the data, it will only ask the next
/// provider for the remaining 90%.
///
/// This is fully sequential, so there will only be one request in flight at a time.
///
/// If the request is not complete after trying all providers, it will return an error.
/// If the provider stream never ends, it will try indefinitely.
async fn execute_get(
    pool: &ConnectionPool,
    request: Arc<GetRequest>,
    providers: &Arc<dyn ContentDiscovery>,
    store: &Store,
    mut progress: impl Sink<DownloadProgessItem, Error = io::Error>,
) -> anyhow::Result<()> {
    let remote = store.remote();
    let mut providers = providers.find_providers(request.content());
    while let Some(provider) = providers.next().await {
        progress
            .send(DownloadProgessItem::TryProvider {
                id: provider,
                request: request.clone(),
            })
            .await?;
        let mut conn = pool.dial(provider);
        let local = remote.local_for_request(request.clone()).await?;
        if local.is_complete() {
            return Ok(());
        }
        let local_bytes = local.local_bytes();
        let Ok(conn) = conn.connection().await else {
            progress
                .send(DownloadProgessItem::ProviderFailed {
                    id: provider,
                    request: request.clone(),
                })
                .await?;
            continue;
        };
        match remote
            .execute_get_sink(
                conn,
                local.missing(),
                (&mut progress).with_map(move |x| DownloadProgessItem::Progress(x + local_bytes)),
            )
            .await
        {
            Ok(_stats) => {
                progress
                    .send(DownloadProgessItem::PartComplete {
                        request: request.clone(),
                    })
                    .await?;
                return Ok(());
            }
            Err(_cause) => {
                progress
                    .send(DownloadProgessItem::ProviderFailed {
                        id: provider,
                        request: request.clone(),
                    })
                    .await?;
                continue;
            }
        }
    }
    bail!("Unable to download {}", request.hash);
}

#[derive(Debug, Clone)]
struct DialNode {
    pool: ConnectionPool,
    id: NodeId,
}

impl DialNode {
    async fn connection_impl(&self) -> anyhow::Result<Connection> {
        info!("Getting connection for node {}", self.id);
        let slot = self
            .pool
            .0
            .connections
            .lock()
            .await
            .entry(self.id)
            .or_default()
            .clone();
        info!("Dialing node {}", self.id);
        let mut guard = slot.lock().await;
        match guard.deref() {
            SlotState::Connected(conn) => {
                return Ok(conn.clone());
            }
            SlotState::AttemptFailed(time) => {
                let elapsed = time.elapsed().unwrap_or_default();
                if elapsed <= self.pool.retry_delay() {
                    bail!(
                        "Connection attempt failed {} seconds ago",
                        elapsed.as_secs_f64()
                    );
                }
            }
            SlotState::Evil(reason) => {
                bail!("Node is banned due to evil behavior: {reason}");
            }
            SlotState::Initial => {}
        }
        let res = self
            .pool
            .endpoint()
            .connect(self.id, self.pool.alpn())
            .timeout(self.pool.0.connect_timeout)
            .await;
        match res {
            Ok(Ok(conn)) => {
                info!("Connected to node {}", self.id);
                *guard = SlotState::Connected(conn.clone());
                Ok(conn)
            }
            Ok(Err(e)) => {
                warn!("Failed to connect to node {}: {}", self.id, e);
                *guard = SlotState::AttemptFailed(SystemTime::now());
                Err(e.into())
            }
            Err(e) => {
                warn!("Failed to connect to node {}: {}", self.id, e);
                *guard = SlotState::AttemptFailed(SystemTime::now());
                bail!("Failed to connect to node: {}", e);
            }
        }
    }
}

impl GetConnection for DialNode {
    fn connection(&mut self) -> impl Future<Output = Result<Connection, anyhow::Error>> + '_ {
        let this = self.clone();
        async move { this.connection_impl().await }
    }
}

/// Trait for pluggable content discovery strategies.
pub trait ContentDiscovery: Debug + Send + Sync + 'static {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<NodeId>;
}

impl<C, I> ContentDiscovery for C
where
    C: Debug + Clone + IntoIterator<Item = I> + Send + Sync + 'static,
    C::IntoIter: Send + Sync + 'static,
    I: Into<NodeId> + Send + Sync + 'static,
{
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<NodeId> {
        let providers = self.clone();
        n0_future::stream::iter(providers.into_iter().map(Into::into)).boxed()
    }
}

#[derive(derive_more::Debug)]
pub struct Shuffled {
    nodes: Vec<NodeId>,
}

impl Shuffled {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }
}

impl ContentDiscovery for Shuffled {
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<NodeId> {
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rand::thread_rng());
        n0_future::stream::iter(nodes).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use bao_tree::ChunkRanges;
    use iroh::Watcher;
    use n0_future::StreamExt;
    use testresult::TestResult;

    use crate::{
        api::{
            blobs::AddBytesOptions,
            downloader::{DownloadOptions, Downloader, Shuffled, SplitStrategy},
        },
        hashseq::HashSeq,
        protocol::{GetManyRequest, GetRequest},
        tests::node_test_setup_fs,
    };

    #[tokio::test]
    async fn downloader_get_many_smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let node1_addr = r1.endpoint().node_addr().initialized().await?;
        let node1_id = node1_addr.node_id;
        let node2_addr = r2.endpoint().node_addr().initialized().await?;
        let node2_id = node2_addr.node_id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetManyRequest::builder()
            .hash(tt1.hash, ChunkRanges::all())
            .hash(tt2.hash, ChunkRanges::all())
            .build();
        let mut progress = swarm
            .download(request, Shuffled::new(vec![node1_id, node2_id]))
            .stream()
            .await?;
        while let Some(item) = progress.next().await {
            println!("Got item: {item:?}");
        }
        assert_eq!(store3.get_bytes(tt1.hash).await?.deref(), b"hello world");
        assert_eq!(store3.get_bytes(tt2.hash).await?.deref(), b"hello world 2");
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_smoke() -> TestResult<()> {
        // tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().node_addr().initialized().await?;
        let node1_id = node1_addr.node_id;
        let node2_addr = r2.endpoint().node_addr().initialized().await?;
        let node2_id = node2_addr.node_id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetRequest::builder()
            .root(ChunkRanges::all())
            .next(ChunkRanges::all())
            .next(ChunkRanges::all())
            .build(root.hash);
        if true {
            let mut progress = swarm
                .download_with_opts(DownloadOptions::new(
                    request,
                    [node1_id, node2_id],
                    SplitStrategy::Split,
                ))
                .stream()
                .await?;
            while let Some(item) = progress.next().await {
                println!("Got item: {item:?}");
            }
        }
        if false {
            let conn = r3.endpoint().connect(node1_addr, crate::ALPN).await?;
            let remote = store3.remote();
            let _rh = remote
                .execute_get(
                    conn.clone(),
                    GetRequest::builder()
                        .root(ChunkRanges::all())
                        .build(root.hash),
                )
                .await?;
            let h1 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(0, ChunkRanges::all())
                    .build(root.hash),
            );
            let h2 = remote.execute_get(
                conn.clone(),
                GetRequest::builder()
                    .child(1, ChunkRanges::all())
                    .build(root.hash),
            );
            h1.await?;
            h2.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_all() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().node_addr().initialized().await?;
        let node1_id = node1_addr.node_id;
        let node2_addr = r2.endpoint().node_addr().initialized().await?;
        let node2_id = node2_addr.node_id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        r3.endpoint().add_node_addr(node1_addr.clone())?;
        r3.endpoint().add_node_addr(node2_addr.clone())?;
        let request = GetRequest::all(root.hash);
        let mut progress = swarm
            .download_with_opts(DownloadOptions::new(
                request,
                [node1_id, node2_id],
                SplitStrategy::Split,
            ))
            .stream()
            .await?;
        while let Some(item) = progress.next().await {
            println!("Got item: {item:?}");
        }
        Ok(())
    }
}
