//! A blob store that allows writes from a set of authorized clients.
mod common;
use std::{collections::HashSet, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use common::setup_logging;
use iroh::{protocol::Router, EndpointAddr, EndpointId};
use iroh_blobs::{
    api::Store,
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode,
    },
    store::{fs::FsStore, mem::MemStore},
    BlobsProtocol,
};
use iroh_tickets::endpoint::EndpointTicket;

use crate::common::get_or_generate_secret_key;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Args {
    /// Path for the blob store.
    path: Option<PathBuf>,
    #[clap(long("allow"))]
    /// Endpoints that are allowed to download content.
    allowed_endpoints: Vec<EndpointId>,
}

fn limit_by_node_id(allowed_nodes: HashSet<EndpointId>) -> EventSender {
    let mask = EventMask {
        // We want a request for each incoming connection so we can accept
        // or reject them. We don't need any other events.
        connected: ConnectMode::Intercept,
        // We explicitly allow all request types without any logging.
        push: RequestMode::None,
        get: RequestMode::None,
        get_many: RequestMode::None,
        ..EventMask::DEFAULT
    };
    let (tx, mut rx) = EventSender::channel(32, mask);
    n0_future::task::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::ClientConnected(msg) = msg {
                let res: std::result::Result<(), AbortReason> = match msg.endpoint_id {
                    Some(endpoint_id) if allowed_nodes.contains(&endpoint_id) => {
                        println!("Client connected: {endpoint_id}");
                        Ok(())
                    }
                    Some(endpoint_id) => {
                        println!("Client rejected: {endpoint_id}");
                        Err(AbortReason::Permission)
                    }
                    None => {
                        println!("Client rejected: no endpoint id");
                        Err(AbortReason::Permission)
                    }
                };
                msg.tx.send(res).await.ok();
            }
        }
    });
    tx
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();
    let args = Args::parse();
    let Args {
        path,
        allowed_endpoints,
    } = args;
    let allowed_endpoints = allowed_endpoints.into_iter().collect::<HashSet<_>>();
    let store: Store = if let Some(path) = path {
        let abs_path = std::path::absolute(path)?;
        (*FsStore::load(abs_path).await?).clone()
    } else {
        (*MemStore::new()).clone()
    };
    let events = limit_by_node_id(allowed_endpoints.clone());
    let (router, addr) = setup(store, events).await?;
    let ticket: EndpointTicket = addr.into();
    println!("Endpoint id: {}", router.endpoint().id());
    println!("Ticket: {}", ticket);
    for id in &allowed_endpoints {
        println!("Allowed endpoint: {id}");
    }
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

async fn setup(store: Store, events: EventSender) -> Result<(Router, EndpointAddr)> {
    let secret = get_or_generate_secret_key()?;
    let endpoint = iroh::Endpoint::builder().secret_key(secret).bind().await?;
    endpoint.online().await;
    let addr = endpoint.addr();
    let blobs = BlobsProtocol::new(&store, Some(events));
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();
    Ok((router, addr))
}
