//! API for downloads from multiple nodes.
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    future::{Future, IntoFuture},
    sync::Arc,
};

use genawaiter::sync::Gen;
use iroh::{Endpoint, EndpointId};
use irpc::{channel::mpsc, rpc_requests};
use n0_error::{anyerr, Result};
use n0_future::{future, stream, task::JoinSet, BufferedStreamExt, Stream, StreamExt};
use rand::seq::SliceRandom;
use serde::{de::Error, Deserialize, Serialize};
use tracing::instrument::Instrument;

use super::Store;
use crate::{
    protocol::{GetManyRequest, GetRequest},
    util::{
        connection_pool::ConnectionPool,
        sink::{Drain, IrpcSenderRefSink, Sink, TokioMpscSenderSink},
    },
    BlobFormat, Hash, HashAndFormat,
};

/// Downloads blobs from multiple remote nodes with automatic failover.
///
/// The `Downloader` is an actor that manages a pool of connections and
/// dispatches download requests. It tries each provider in the order returned
/// by the [`ContentDiscovery`] strategy, keeping track of what the previous
/// provider already delivered so the next one only needs to provide the
/// remainder.
///
/// Create one via [`crate::api::Store::downloader`] and keep it alive for the
/// lifetime of your download workload; do not create it ad hoc per-request
/// because it owns connection state.
#[derive(Debug, Clone)]
pub struct Downloader {
    client: irpc::Client<SwarmProtocol>,
}

#[rpc_requests(message = SwarmMsg, alias = "Msg", rpc_feature = "rpc")]
#[derive(Debug, Serialize, Deserialize)]
enum SwarmProtocol {
    #[rpc(tx = mpsc::Sender<DownloadProgressItem>)]
    Download(DownloadRequest),
}

struct DownloaderActor {
    store: Store,
    pool: ConnectionPool,
    tasks: JoinSet<()>,
    running: HashSet<n0_future::task::Id>,
}

/// Progress updates emitted during a download managed by [`Downloader`].
#[derive(Debug, Serialize, Deserialize)]
pub enum DownloadProgressItem {
    /// A non-recoverable error terminated the download.
    #[serde(skip)]
    Error(n0_error::AnyError),
    /// The downloader is about to try this provider for the given request.
    TryProvider {
        /// The provider being tried.
        id: EndpointId,
        /// The sub-request being attempted.
        request: Arc<GetRequest>,
    },
    /// The provider could not fulfill the request.
    ProviderFailed {
        /// The provider that failed.
        id: EndpointId,
        /// The request that was being attempted.
        request: Arc<GetRequest>,
    },
    /// A partial (or full) request was completed successfully.
    PartComplete {
        /// The completed request.
        request: Arc<GetRequest>,
    },
    /// Cumulative payload bytes downloaded so far.
    Progress(u64),
    /// The download failed and could not be completed from any provider.
    DownloadError,
}

impl DownloaderActor {
    fn new(store: Store, endpoint: Endpoint) -> Self {
        Self {
            store,
            pool: ConnectionPool::new(endpoint, crate::ALPN, Default::default()),
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
        tx.send(DownloadProgressItem::Error(cause)).await.ok();
    }
}

async fn handle_download_impl(
    store: Store,
    pool: ConnectionPool,
    request: DownloadRequest,
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
    match request.strategy {
        SplitStrategy::Split => handle_download_split_impl(store, pool, request, tx).await?,
        SplitStrategy::None => match request.request {
            FiniteRequest::Get(get) => {
                let sink = IrpcSenderRefSink(tx);
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
    tx: &mut mpsc::Sender<DownloadProgressItem>,
) -> Result<()> {
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
                let (tx, rx) = tokio::sync::mpsc::channel::<(usize, DownloadProgressItem)>(16);
                progress_tx.send(rx).await.ok();
                let sink = TokioMpscSenderSink(tx).with_map(move |x| (id, x));
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
                DownloadProgressItem::Progress(offset) => {
                    total += offset;
                    if let Some(prev) = offsets.insert(id, offset) {
                        total -= prev;
                    }
                    DownloadProgressItem::Progress(total)
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
                        tx.send(DownloadProgressItem::DownloadError).await?;
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

/// A concrete download request that specifies a finite set of content to fetch.
///
/// A `FiniteRequest` is either a single [`GetRequest`] (for structured
/// range-aware fetching) or a [`GetManyRequest`] (for fetching multiple
/// independent blobs in one go).
#[derive(Debug, Serialize, Deserialize, derive_more::From)]
pub enum FiniteRequest {
    /// Fetch a single blob or a structured subset of a blob tree.
    Get(GetRequest),
    /// Fetch multiple independent blobs, each with its own range set.
    GetMany(GetManyRequest),
}

/// Conversion into a [`FiniteRequest`].
///
/// This trait is implemented for the common request types (`GetRequest`,
/// `GetManyRequest`, `Hash`, `HashAndFormat`) as well as for any iterator
/// of things that convert to [`Hash`], so you can pass a `Vec<Hash>` directly
/// to [`Downloader::download`].
pub trait SupportedRequest {
    /// Converts this value into a [`FiniteRequest`].
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

/// A request to register additional providers for a hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddProviderRequest {
    /// The hash whose providers are being registered.
    pub hash: Hash,
    /// The set of endpoint IDs that are known to have this hash.
    pub providers: Vec<EndpointId>,
}

/// Options controlling a single download operation.
///
/// Use [`DownloadRequest::new`] to construct one, then pass it to
/// [`Downloader::download_with_opts`].
#[derive(Debug)]
pub struct DownloadRequest {
    /// The content to fetch.
    pub request: FiniteRequest,
    /// The strategy used to discover and order providers.
    pub providers: Arc<dyn ContentDiscovery>,
    /// Whether to fetch sub-blobs in parallel across providers.
    pub strategy: SplitStrategy,
}

impl DownloadRequest {
    /// Creates a new `DownloadRequest`.
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

/// Controls how multi-blob requests are dispatched across providers.
#[derive(Debug, Serialize, Deserialize)]
pub enum SplitStrategy {
    /// Send the entire request to each provider in sequence.
    ///
    /// Each provider must satisfy the whole request before the next is tried.
    /// Use this when the content is small or comes from a single authoritative source.
    None,
    /// Split the request into per-blob sub-requests and fetch them in parallel.
    ///
    /// Each sub-blob can be fetched from a different provider simultaneously,
    /// which reduces latency when providers each hold different parts of the data.
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

/// Alias for [`DownloadRequest`].
pub type DownloadOptions = DownloadRequest;

/// A handle to an in-progress download.
///
/// You can either await `DownloadProgress` directly to wait for completion (via
/// `IntoFuture`, which returns an error if the download fails), or call
/// [`DownloadProgress::stream`] to receive individual [`DownloadProgressItem`]
/// events as the download proceeds.
pub struct DownloadProgress {
    fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>,
}

impl DownloadProgress {
    fn new(fut: future::Boxed<irpc::Result<mpsc::Receiver<DownloadProgressItem>>>) -> Self {
        Self { fut }
    }

    /// Returns a stream of [`DownloadProgressItem`] events for this download.
    pub async fn stream(self) -> irpc::Result<impl Stream<Item = DownloadProgressItem> + Unpin> {
        let rx = self.fut.await?;
        Ok(Box::pin(rx.into_stream().map(|item| match item {
            Ok(item) => item,
            Err(e) => DownloadProgressItem::Error(e.into()),
        })))
    }

    async fn complete(self) -> Result<()> {
        let rx = self.fut.await?;
        let stream = rx.into_stream();
        tokio::pin!(stream);
        while let Some(item) = stream.next().await {
            match item? {
                DownloadProgressItem::Error(e) => Err(e)?,
                DownloadProgressItem::DownloadError => {
                    n0_error::bail_any!("Download error");
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl IntoFuture for DownloadProgress {
    type Output = Result<()>;
    type IntoFuture = future::Boxed<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.complete())
    }
}

impl Downloader {
    /// Creates a new `Downloader` backed by the given [`Store`] and [`Endpoint`].
    ///
    /// This spawns a background actor that manages the connection pool and
    /// dispatches download tasks. Keep the returned `Downloader` alive for the
    /// duration of your workload; it is cheap to clone.
    pub fn new(store: &Store, endpoint: &Endpoint) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<SwarmMsg>(32);
        let actor = DownloaderActor::new(store.clone(), endpoint.clone());
        n0_future::task::spawn(actor.run(rx));
        Self { client: tx.into() }
    }

    /// Downloads `request` from `providers` using the default [`SplitStrategy::None`].
    ///
    /// Returns a [`DownloadProgress`] handle. Await it to block until the
    /// download completes, or call `.stream()` to observe per-event progress.
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
            strategy: SplitStrategy::None,
        })
    }

    /// Downloads with full control over options.
    ///
    /// This is the most flexible entry point. All other download methods
    /// delegate to this one.
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
    progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<Box<dyn Iterator<Item = GetRequest> + Send + 'a>> {
    Ok(match request {
        FiniteRequest::Get(req) => {
            let Some(_first) = req.ranges.iter_infinite().next() else {
                return Ok(Box::new(std::iter::empty()));
            };
            let first = GetRequest::blob(req.hash);
            execute_get(pool, Arc::new(first), providers, store, progress).await?;
            let size = store.observe(req.hash).await?.size();
            n0_error::ensure_any!(size % 32 == 0, "Size is not a multiple of 32");
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
    mut progress: impl Sink<DownloadProgressItem, Error = irpc::channel::SendError>,
) -> Result<()> {
    let remote = store.remote();
    let mut providers = providers.find_providers(request.content());
    while let Some(provider) = providers.next().await {
        progress
            .send(DownloadProgressItem::TryProvider {
                id: provider,
                request: request.clone(),
            })
            .await?;
        let conn = pool.get_or_connect(provider);
        let local = remote.local_for_request(request.clone()).await?;
        if local.is_complete() {
            return Ok(());
        }
        let local_bytes = local.local_bytes();
        let Ok(conn) = conn.await else {
            progress
                .send(DownloadProgressItem::ProviderFailed {
                    id: provider,
                    request: request.clone(),
                })
                .await?;
            continue;
        };
        match remote
            .execute_get_sink(
                conn.clone(),
                local.missing(),
                (&mut progress).with_map(move |x| DownloadProgressItem::Progress(x + local_bytes)),
            )
            .await
        {
            Ok(_stats) => {
                progress
                    .send(DownloadProgressItem::PartComplete {
                        request: request.clone(),
                    })
                    .await?;
                return Ok(());
            }
            Err(_cause) => {
                progress
                    .send(DownloadProgressItem::ProviderFailed {
                        id: provider,
                        request: request.clone(),
                    })
                    .await?;
                continue;
            }
        }
    }
    Err(anyerr!("Unable to download {}", request.hash))
}

/// Trait for pluggable content discovery strategies.
///
/// Implement this trait to control how the [`Downloader`] finds the nodes that
/// hold a given piece of content. The downloader calls `find_providers` once
/// per request and iterates the returned stream, attempting each provider in
/// order until the download succeeds.
///
/// Any type that implements `Clone + IntoIterator<Item: Into<EndpointId>>`
/// already satisfies this trait, so passing a `Vec<EndpointId>` works without
/// any boilerplate.
pub trait ContentDiscovery: Debug + Send + Sync + 'static {
    /// Returns a stream of endpoint IDs that may hold `hash`.
    ///
    /// The stream may be infinite (e.g. a DHT lookup that keeps returning new
    /// peers). The downloader stops consuming it as soon as the download
    /// completes.
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<EndpointId>;
}

impl<C, I> ContentDiscovery for C
where
    C: Debug + Clone + IntoIterator<Item = I> + Send + Sync + 'static,
    C::IntoIter: Send + Sync + 'static,
    I: Into<EndpointId> + Send + Sync + 'static,
{
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let providers = self.clone();
        n0_future::stream::iter(providers.into_iter().map(Into::into)).boxed()
    }
}

/// A [`ContentDiscovery`] implementation that returns providers in random order.
///
/// Shuffling spreads load across a fixed set of peers when every peer holds
/// the full content. For partial data or DHT-style lookups, implement
/// [`ContentDiscovery`] directly instead.
#[derive(derive_more::Debug)]
pub struct Shuffled {
    nodes: Vec<EndpointId>,
}

impl Shuffled {
    /// Creates a new `Shuffled` provider set from a list of endpoint IDs.
    pub fn new(nodes: Vec<EndpointId>) -> Self {
        Self { nodes }
    }
}

impl ContentDiscovery for Shuffled {
    fn find_providers(&self, _: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rand::rng());
        n0_future::stream::iter(nodes).boxed()
    }
}

#[cfg(test)]
#[cfg(feature = "fs-store")]
mod tests {
    use std::ops::Deref;

    use bao_tree::ChunkRanges;
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
    #[ignore = "todo"]
    async fn downloader_get_many_smoke() -> TestResult<()> {
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice("hello world").await?;
        let tt2 = store2.add_slice("hello world 2").await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetManyRequest::builder()
            .hash(tt1.hash, ChunkRanges::all())
            .hash(tt2.hash, ChunkRanges::all())
            .build();
        let mut progress = swarm
            .download(request, Shuffled::new(vec![node1_id, node2_id]))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        assert_eq!(store3.get_bytes(tt1.hash).await?.deref(), b"hello world");
        assert_eq!(store3.get_bytes(tt2.hash).await?.deref(), b"hello world 2");
        Ok(())
    }

    #[tokio::test]
    async fn downloader_get_smoke() -> TestResult<()> {
        // tracing_subscriber::fmt::try_init().ok();
        let testdir = tempfile::tempdir()?;
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
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
            while progress.next().await.is_some() {}
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
        let (r1, store1, _, _) = node_test_setup_fs(testdir.path().join("a")).await?;
        let (r2, store2, _, _) = node_test_setup_fs(testdir.path().join("b")).await?;
        let (r3, store3, _, sp3) = node_test_setup_fs(testdir.path().join("c")).await?;
        let tt1 = store1.add_slice(vec![1; 10000000]).await?;
        let tt2 = store2.add_slice(vec![2; 10000000]).await?;
        let hs = [tt1.hash, tt2.hash].into_iter().collect::<HashSeq>();
        let root = store1
            .add_bytes_with_opts(AddBytesOptions {
                data: hs.clone().into(),
                format: crate::BlobFormat::HashSeq,
            })
            .await?;
        let node1_addr = r1.endpoint().addr();
        let node1_id = node1_addr.id;
        let node2_addr = r2.endpoint().addr();
        let node2_id = node2_addr.id;
        let swarm = Downloader::new(&store3, r3.endpoint());
        sp3.add_endpoint_info(node1_addr.clone());
        sp3.add_endpoint_info(node2_addr.clone());
        let request = GetRequest::all(root.hash);
        let mut progress = swarm
            .download_with_opts(DownloadOptions::new(
                request,
                [node1_id, node2_id],
                SplitStrategy::Split,
            ))
            .stream()
            .await?;
        while progress.next().await.is_some() {}
        Ok(())
    }
}
