//! An iroh node that just has the blobs transport
use std::{path::Path, sync::Arc};

use iroh_net::{NodeAddr, NodeId};
use quic_rpc::client::BoxedServiceConnection;
use tokio_util::task::AbortOnDropHandle;

use crate::{
    provider::{CustomEventSender, EventSender},
    rpc::client::{blobs, tags},
    util::local_pool::LocalPool,
};

type RpcClient = quic_rpc::RpcClient<
    crate::rpc::proto::RpcService,
    BoxedServiceConnection<crate::rpc::proto::RpcService>,
    crate::rpc::proto::RpcService,
>;

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
}

impl<S: crate::store::Store> Builder<S> {
    /// Sets the event sender
    pub fn blobs_events(self, events: impl CustomEventSender) -> Self {
        Builder {
            store: self.store,
            events: events.into(),
        }
    }

    /// Spawns the node
    pub async fn spawn(self) -> anyhow::Result<Node> {
        let (client, router, rpc_task, _local_pool) = setup_router(self.store, self.events).await?;
        Ok(Node {
            router,
            client,
            _rpc_task: AbortOnDropHandle::new(rpc_task),
            _local_pool,
        })
    }
}

impl Node {
    /// Creates a new node with memory storage
    pub fn memory() -> Builder<crate::store::mem::Store> {
        Builder {
            store: crate::store::mem::Store::new(),
            events: Default::default(),
        }
    }

    /// Creates a new node with persistent storage
    pub async fn persistent(
        path: impl AsRef<Path>,
    ) -> anyhow::Result<Builder<crate::store::fs::Store>> {
        Ok(Builder {
            store: crate::store::fs::Store::load(path).await?,
            events: Default::default(),
        })
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

async fn setup_router<S: crate::store::Store>(
    store: S,
    events: EventSender,
) -> anyhow::Result<(
    RpcClient,
    iroh_router::Router,
    tokio::task::JoinHandle<()>,
    LocalPool,
)> {
    let endpoint = iroh_net::Endpoint::builder().bind().await?;
    let local_pool = LocalPool::single();
    let mut router = iroh_router::Router::builder(endpoint.clone());

    // Setup blobs
    let downloader = crate::downloader::Downloader::new(
        store.clone(),
        endpoint.clone(),
        local_pool.handle().clone(),
    );
    let blobs = Arc::new(crate::net_protocol::Blobs::new_with_events(
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
    let (internal_rpc, controller) =
        quic_rpc::transport::flume::service_connection::<crate::rpc::proto::RpcService>(32);
    let controller = quic_rpc::transport::boxed::Connection::new(controller);
    let internal_rpc = quic_rpc::transport::boxed::ServerEndpoint::new(internal_rpc);
    let internal_rpc = quic_rpc::RpcServer::new(internal_rpc);

    let rpc_server_task: tokio::task::JoinHandle<()> = tokio::task::spawn(async move {
        loop {
            let request = internal_rpc.accept().await;
            match request {
                Ok(accepting) => {
                    let blobs = blobs.clone();
                    tokio::task::spawn(async move {
                        let (msg, chan) = accepting.read_first().await.unwrap();
                        blobs.handle_rpc_request(msg, chan).await.unwrap();
                    });
                }
                Err(err) => {
                    tracing::warn!("rpc error: {:?}", err);
                }
            }
        }
    });

    let client = quic_rpc::RpcClient::new(controller);

    Ok((client, router, rpc_server_task, local_pool))
}
