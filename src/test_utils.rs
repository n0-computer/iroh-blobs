//! An iroh node that just has the blobs transport
use std::{path::Path, sync::Arc};

use iroh_net::{Endpoint, NodeAddr, NodeId};
use iroh_router::Router;
use tokio_util::task::AbortOnDropHandle;

use crate::{
    downloader::Downloader,
    net_protocol::Blobs,
    provider::{CustomEventSender, EventSender},
    rpc::{
        client::{blobs, tags},
        proto::RpcService,
    },
    util::local_pool::LocalPool,
};

type RpcClient = quic_rpc::RpcClient<RpcService>;

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node {
    router: iroh_router::Router,
    client: RpcClient,
    _local_pool: LocalPool,
    _rpc_task: AbortOnDropHandle<()>,
}

/// An iroh node builder
#[derive(Debug)]
pub struct Builder<S> {
    store: S,
    events: EventSender,
    endpoint: Option<iroh_net::endpoint::Builder>,
}

impl<S: crate::store::Store> Builder<S> {
    /// Sets the event sender
    pub fn blobs_events(self, events: impl CustomEventSender) -> Self {
        Self {
            events: events.into(),
            ..self
        }
    }

    /// Set an endpoint builder
    pub fn endpoint(self, endpoint: iroh_net::endpoint::Builder) -> Self {
        Self {
            endpoint: Some(endpoint),
            ..self
        }
    }

    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node> {
        let store = self.store;
        let events = self.events;
        let endpoint = self
            .endpoint
            .unwrap_or_else(|| Endpoint::builder().discovery_n0())
            .bind()
            .await?;
        let local_pool = LocalPool::single();
        let mut router = Router::builder(endpoint.clone());

        // Setup blobs
        let downloader =
            Downloader::new(store.clone(), endpoint.clone(), local_pool.handle().clone());
        let blobs = Arc::new(Blobs::new_with_events(
            store.clone(),
            local_pool.handle().clone(),
            events,
            downloader,
            endpoint.clone(),
        ));
        router = router.accept(crate::protocol::ALPN.to_vec(), blobs.clone());

        // Build the router
        let router = router.spawn().await?;

        // Setup RPC
        let (internal_rpc, controller) = quic_rpc::transport::flume::channel(32);
        let internal_rpc = quic_rpc::RpcServer::new(internal_rpc).boxed();
        let _rpc_task = internal_rpc
            .spawn_accept_loop(move |msg, chan| blobs.clone().handle_rpc_request(msg, chan));
        let client = quic_rpc::RpcClient::new(controller).boxed();
        Ok(Node {
            router,
            client,
            _rpc_task,
            _local_pool: local_pool,
        })
    }
}

impl Node {
    /// Creates a new node with memory storage
    pub fn memory() -> Builder<crate::store::mem::Store> {
        Builder {
            store: crate::store::mem::Store::new(),
            events: Default::default(),
            endpoint: None,
        }
    }

    /// Creates a new node with persistent storage
    pub async fn persistent(
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Builder<crate::store::fs::Store>> {
        Ok(Builder {
            store: crate::store::fs::Store::load(path).await?,
            events: Default::default(),
            endpoint: None,
        })
    }

    /// Returns the endpoint
    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    /// Returns the node id
    pub fn node_id(&self) -> NodeId {
        self.router.endpoint().node_id()
    }

    /// Returns the node address
    pub async fn node_addr(&self) -> anyhow::Result<NodeAddr> {
        self.router.endpoint().node_addr().await
    }

    /// Shuts down the node
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.router.shutdown().await
    }

    /// Returns an in-memory blobs client
    pub fn blobs(&self) -> blobs::Client {
        blobs::Client::new(self.client.clone())
    }

    /// Returns an in-memory tags client
    pub fn tags(&self) -> tags::Client {
        tags::Client::new(self.client.clone())
    }
}
