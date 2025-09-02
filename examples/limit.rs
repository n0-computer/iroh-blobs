/// Example how to limit blob requests by hash and node id, and to add
/// restrictions on limited content.
mod common;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use clap::Parser;
use common::setup_logging;
use iroh::{NodeId, SecretKey, Watcher};
use iroh_blobs::{
    provider::events::{
        AbortReason, ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode,
        ThrottleMode,
    },
    store::mem::MemStore,
    ticket::BlobTicket,
    BlobsProtocol, Hash,
};
use rand::thread_rng;

use crate::common::get_or_generate_secret_key;

#[derive(Debug, Parser)]
#[command(version, about)]
pub enum Args {
    /// Limit requests by node id
    ByNodeId {
        /// Path for files to add
        paths: Vec<PathBuf>,
        #[clap(long("allow"))]
        /// Nodes that are allowed to download content.
        allowed_nodes: Vec<NodeId>,
        #[clap(long, default_value_t = 1)]
        secrets: usize,
    },
    /// Limit requests by hash, only first hash is allowed
    ByHash {
        /// Path for files to add
        paths: Vec<PathBuf>,
    },
    /// Throttle requests
    Throttle {
        /// Path for files to add
        paths: Vec<PathBuf>,
        #[clap(long, default_value = "100")]
        delay_ms: u64,
    },
    /// Limit maximum number of connections.
    MaxConnections {
        /// Path for files to add
        paths: Vec<PathBuf>,
        #[clap(long, default_value = "1")]
        max_connections: usize,
    },
    Get {
        /// Ticket for the blob to download
        ticket: BlobTicket,
    },
}

fn limit_by_node_id(allowed_nodes: HashSet<NodeId>) -> EventSender {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    n0_future::task::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::ClientConnected(msg) = msg {
                let node_id = msg.node_id;
                let res = if allowed_nodes.contains(&node_id) {
                    println!("Client connected: {node_id}");
                    Ok(())
                } else {
                    println!("Client rejected: {node_id}");
                    Err(AbortReason::Permission)
                };
                msg.tx.send(res).await.ok();
            }
        }
    });
    EventSender::new(
        tx,
        EventMask {
            connected: ConnectMode::Request,
            ..EventMask::DEFAULT
        },
    )
}

fn limit_by_hash(allowed_hashes: HashSet<Hash>) -> EventSender {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    n0_future::task::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::GetRequestReceived(msg) = msg {
                let res = if !msg.request.ranges.is_blob() {
                    println!("HashSeq request not allowed");
                    Err(AbortReason::Permission)
                } else if !allowed_hashes.contains(&msg.request.hash) {
                    println!("Request for hash {} not allowed", msg.request.hash);
                    Err(AbortReason::Permission)
                } else {
                    println!("Request for hash {} allowed", msg.request.hash);
                    Ok(())
                };
                msg.tx.send(res).await.ok();
            }
        }
    });
    EventSender::new(
        tx,
        EventMask {
            get: RequestMode::Request,
            ..EventMask::DEFAULT
        },
    )
}

fn throttle(delay_ms: u64) -> EventSender {
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    n0_future::task::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::Throttle(msg) = msg {
                n0_future::task::spawn(async move {
                    println!(
                        "Throttling {} {}, {}ms",
                        msg.connection_id, msg.request_id, delay_ms
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    msg.tx.send(Ok(())).await.ok();
                });
            }
        }
    });
    EventSender::new(
        tx,
        EventMask {
            throttle: ThrottleMode::Throttle,
            ..EventMask::DEFAULT
        },
    )
}

fn limit_max_connections(max_connections: usize) -> EventSender {
    #[derive(Default, Debug, Clone)]
    struct ConnectionCounter(Arc<(AtomicUsize, usize)>);

    impl ConnectionCounter {
        fn new(max: usize) -> Self {
            Self(Arc::new((Default::default(), max)))
        }

        fn inc(&self) -> Result<usize, usize> {
            let (c, max) = &*self.0;
            c.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n >= *max {
                    None
                } else {
                    Some(n + 1)
                }
            })
        }

        fn dec(&self) {
            let (c, _) = &*self.0;
            c.fetch_sub(1, Ordering::SeqCst);
        }
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    n0_future::task::spawn(async move {
        let requests = ConnectionCounter::new(max_connections);
        while let Some(msg) = rx.recv().await {
            if let ProviderMessage::GetRequestReceived(mut msg) = msg {
                let connection_id = msg.connection_id;
                let request_id = msg.request_id;
                let res = requests.inc();
                match res {
                    Ok(n) => {
                        println!("Accepting request {n}, id ({connection_id},{request_id})");
                        msg.tx.send(Ok(())).await.ok();
                    }
                    Err(_) => {
                        println!(
                            "Connection limit of {max_connections} exceeded, rejecting request"
                        );
                        msg.tx.send(Err(AbortReason::RateLimited)).await.ok();
                        continue;
                    }
                }
                let requests = requests.clone();
                n0_future::task::spawn(async move {
                    // just drain the per request events
                    //
                    // Note that we have requested updates for the request, now we also need to process them
                    // otherwise the request will be aborted!
                    while let Ok(Some(_)) = msg.rx.recv().await {}
                    println!("Stopping request, id ({connection_id},{request_id})");
                    requests.dec();
                });
            }
        }
    });
    EventSender::new(
        tx,
        EventMask {
            get: RequestMode::RequestLog,
            ..EventMask::DEFAULT
        },
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();
    let args = Args::parse();
    match args {
        Args::Get { ticket } => {
            let secret = get_or_generate_secret_key()?;
            let endpoint = iroh::Endpoint::builder()
                .secret_key(secret)
                .discovery_n0()
                .bind()
                .await?;
            let connection = endpoint
                .connect(ticket.node_addr().clone(), iroh_blobs::ALPN)
                .await?;
            let (data, stats) = iroh_blobs::get::request::get_blob(connection, ticket.hash())
                .bytes_and_stats()
                .await?;
            println!("Downloaded {} bytes", data.len());
            println!("Stats: {stats:?}");
        }
        Args::ByNodeId {
            paths,
            allowed_nodes,
            secrets,
        } => {
            let mut allowed_nodes = allowed_nodes.into_iter().collect::<HashSet<_>>();
            if secrets > 0 {
                println!("Generating {secrets} new secret keys for allowed nodes:");
                let mut rand = thread_rng();
                for _ in 0..secrets {
                    let secret = SecretKey::generate(&mut rand);
                    let public = secret.public();
                    allowed_nodes.insert(public);
                    println!("IROH_SECRET={}", hex::encode(secret.to_bytes()));
                }
            }
            let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
            let store = MemStore::new();
            let mut hashes = HashMap::new();
            for path in paths {
                let tag = store.add_path(&path).await?;
                hashes.insert(path, tag.hash);
            }
            let _ = endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let events = limit_by_node_id(allowed_nodes.clone());
            let blobs = BlobsProtocol::new(&store, endpoint.clone(), Some(events));
            let router = iroh::protocol::Router::builder(endpoint)
                .accept(iroh_blobs::ALPN, blobs)
                .spawn();
            println!("Node id: {}\n", router.endpoint().node_id());
            for id in &allowed_nodes {
                println!("Allowed node: {id}");
            }
            println!();
            for (path, hash) in &hashes {
                let ticket = BlobTicket::new(addr.clone(), *hash, iroh_blobs::BlobFormat::Raw);
                println!("{}: {ticket}", path.display());
            }
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        Args::ByHash { paths } => {
            let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
            let store = MemStore::new();
            let mut hashes = HashMap::new();
            let mut allowed_hashes = HashSet::new();
            for (i, path) in paths.into_iter().enumerate() {
                let tag = store.add_path(&path).await?;
                hashes.insert(path, tag.hash);
                if i == 0 {
                    allowed_hashes.insert(tag.hash);
                }
            }
            let _ = endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let events = limit_by_hash(allowed_hashes.clone());
            let blobs = BlobsProtocol::new(&store, endpoint.clone(), Some(events));
            let router = iroh::protocol::Router::builder(endpoint)
                .accept(iroh_blobs::ALPN, blobs)
                .spawn();
            for (i, (path, hash)) in hashes.iter().enumerate() {
                let ticket = BlobTicket::new(addr.clone(), *hash, iroh_blobs::BlobFormat::Raw);
                let permitted = if i == 0 { "" } else { "limited" };
                println!("{}: {ticket} ({permitted})", path.display());
            }
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        Args::Throttle { paths, delay_ms } => {
            let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
            let store = MemStore::new();
            let mut hashes = HashMap::new();
            for path in paths {
                let tag = store.add_path(&path).await?;
                hashes.insert(path, tag.hash);
            }
            let _ = endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let events = throttle(delay_ms);
            let blobs = BlobsProtocol::new(&store, endpoint.clone(), Some(events));
            let router = iroh::protocol::Router::builder(endpoint)
                .accept(iroh_blobs::ALPN, blobs)
                .spawn();
            for (path, hash) in hashes {
                let ticket = BlobTicket::new(addr.clone(), hash, iroh_blobs::BlobFormat::Raw);
                println!("{}: {ticket}", path.display());
            }
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
        Args::MaxConnections {
            paths,
            max_connections,
        } => {
            let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
            let store = MemStore::new();
            let mut hashes = HashMap::new();
            for path in paths {
                let tag = store.add_path(&path).await?;
                hashes.insert(path, tag.hash);
            }
            let _ = endpoint.home_relay().initialized().await;
            let addr = endpoint.node_addr().initialized().await;
            let events = limit_max_connections(max_connections);
            let blobs = BlobsProtocol::new(&store, endpoint.clone(), Some(events));
            let router = iroh::protocol::Router::builder(endpoint)
                .accept(iroh_blobs::ALPN, blobs)
                .spawn();
            for (path, hash) in hashes {
                let ticket = BlobTicket::new(addr.clone(), hash, iroh_blobs::BlobFormat::Raw);
                println!("{}: {ticket}", path.display());
            }
            tokio::signal::ctrl_c().await?;
            router.shutdown().await?;
        }
    }
    Ok(())
}
