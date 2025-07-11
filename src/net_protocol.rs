//! Adaptation of `iroh-blobs` as an [`iroh`] [`ProtocolHandler`].
//!
//! This is the easiest way to share data from a [`crate::api::Store`] over iroh connections.
//!
//! # Example
//!
//! ```rust
//! # async fn example() -> anyhow::Result<()> {
//! use iroh::{protocol::Router, Endpoint};
//! use iroh_blobs::{net_protocol::Blobs, store};
//!
//! // create a store
//! let store = store::fs::FsStore::load("blobs").await?;
//!
//! // add some data
//! let t = store.add_slice(b"hello world").await?;
//!
//! // create an iroh endpoint
//! let endpoint = Endpoint::builder().discovery_n0().bind().await?;
//!
//! // create a blobs protocol handler
//! let blobs = Blobs::new(&store, endpoint.clone(), None);
//!
//! // create a router and add the blobs protocol handler
//! let router = Router::builder(endpoint)
//!     .accept(iroh_blobs::ALPN, blobs.clone())
//!     .spawn();
//!
//! // this data is now globally available using the ticket
//! let ticket = blobs.ticket(t).await?;
//! println!("ticket: {}", ticket);
//!
//! // wait for control-c to exit
//! tokio::signal::ctrl_c().await?;
//! #   Ok(())
//! # }
//! ```

use std::{fmt::Debug, future::Future, sync::Arc};

use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    Endpoint, Watcher,
};
use tokio::sync::mpsc;
use tracing::error;

use crate::{
    api::Store,
    provider::{Event, EventSender},
    ticket::BlobTicket,
    HashAndFormat,
};

#[derive(Debug)]
pub(crate) struct BlobsInner {
    pub(crate) store: Store,
    pub(crate) endpoint: Endpoint,
    pub(crate) events: EventSender,
}

/// A protocol handler for the blobs protocol.
#[derive(Debug, Clone)]
pub struct BlobsProtocol {
    pub(crate) inner: Arc<BlobsInner>,
}

impl BlobsProtocol {
    pub fn new(store: &Store, endpoint: Endpoint, events: Option<mpsc::Sender<Event>>) -> Self {
        Self {
            inner: Arc::new(BlobsInner {
                store: store.clone(),
                endpoint,
                events: EventSender::new(events),
            }),
        }
    }

    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    /// Create a ticket for content on this node.
    ///
    /// Note that this does not check whether the content is partially or fully available. It is
    /// just a convenience method to create a ticket from content and the address of this node.
    pub async fn ticket(&self, content: impl Into<HashAndFormat>) -> anyhow::Result<BlobTicket> {
        let content = content.into();
        let addr = self.inner.endpoint.node_addr().initialized().await?;
        let ticket = BlobTicket::new(addr, content.hash, content.format);
        Ok(ticket)
    }
}

impl ProtocolHandler for BlobsProtocol {
    fn accept(
        &self,
        conn: Connection,
    ) -> impl Future<Output = std::result::Result<(), AcceptError>> + Send {
        let store = self.store().clone();
        let events = self.inner.events.clone();

        Box::pin(async move {
            crate::provider::handle_connection(conn, store, events).await;
            Ok(())
        })
    }

    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        let store = self.store().clone();
        Box::pin(async move {
            if let Err(cause) = store.shutdown().await {
                error!("error shutting down store: {:?}", cause);
            }
        })
    }
}
