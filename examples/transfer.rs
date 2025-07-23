use std::path::PathBuf;

use clap::Parser;
use iroh::{discovery::pkarr::PkarrResolver, protocol::Router, Endpoint};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket, BlobsProtocol};
mod common;
use common::setup_logging;
#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
pub enum Command {
    /// Send a file to the network
    Send {
        /// Path to the file to send
        file: PathBuf,
    },
    /// Receive a file from the network
    Receive {
        /// Ticket describing the content to fetch
        ticket: BlobTicket,
        /// Path to save the received file
        filename: PathBuf,
    },
}

async fn send(filename: PathBuf) -> anyhow::Result<()> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // We initialize an in-memory backing store for iroh-blobs
    let store = MemStore::new();
    // Then we initialize a struct that can accept blobs requests over iroh connections
    let blobs = BlobsProtocol::new(&store, endpoint.clone(), None);

    let abs_path = std::path::absolute(&filename)?;
    let tag = store.blobs().add_path(abs_path).await?;

    let node_id = endpoint.node_id();
    let ticket = BlobTicket::new(node_id.into(), tag.hash, tag.format);

    println!("File hashed. Fetch this file by running:");
    println!(
        "cargo run --example transfer -- receive {ticket} {}",
        filename.display()
    );

    // For sending files we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    tokio::signal::ctrl_c().await?;

    // Gracefully shut down the node
    println!("Shutting down.");
    router.shutdown().await?;
    Ok(())
}

async fn receive(ticket: BlobTicket, filename: PathBuf) -> anyhow::Result<()> {
    // Create a store to download blobs into
    let store = MemStore::new();

    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world.
    //
    // Since we just want to receive files, we don't need a stable node address
    // or to publish our discovery information.
    let endpoint = Endpoint::builder()
        .discovery(PkarrResolver::n0_dns())
        .bind()
        .await?;

    // For receiving files, we create a "downloader" that allows us to fetch files
    // from other nodes via iroh connections
    let downloader = store.downloader(&endpoint);

    println!("Starting download.");

    downloader
        .download(ticket.hash(), [ticket.node_addr().node_id])
        .await?;

    println!("Finished download.");
    println!("Copying to destination.");

    store.export(ticket.hash(), filename).await?;

    println!("Finished copying.");

    // Gracefully shut down the endpoint and the store
    println!("Shutting down.");
    endpoint.close().await;
    store.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();
    let cli = Cli::parse();

    match cli.command {
        Command::Send { file } => {
            send(file).await?;
        }
        Command::Receive { ticket, filename } => {
            receive(ticket, filename).await?;
        }
    }
    Ok(())
}
