//! Example that runs an iroh node with local node discovery and no relay server.
//!
//! You can think of this as a local version of [sendme](https://www.iroh.computer/sendme)
//! that only works for individual files.
//!
//! **This example is using a non-default feature of iroh, so you need to run it with the
//! examples feature enabled.**
//!
//! Run the follow command to run the "accept" side, that hosts the content:
//!  $ cargo run --example mdns-discovery --features examples -- accept [FILE_PATH]
//! Wait for output that looks like the following:
//!  $ cargo run --example mdns-discovery --features examples -- connect [NODE_ID] [HASH] -o [FILE_PATH]
//! Run that command on another machine in the same local network, replacing [FILE_PATH] to the path on which you want to save the transferred content.
use std::path::{Path, PathBuf};

use n0_error::{ensure, Result};
use clap::{Parser, Subcommand};
use iroh::{
    discovery::mdns::MdnsDiscovery, protocol::Router, Endpoint, PublicKey, RelayMode, SecretKey,
};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, Hash};

mod common;
use common::{get_or_generate_secret_key, setup_logging};

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum Commands {
    /// Launch an iroh node and provide the content at the given path
    Accept {
        /// path to the file you want to provide
        path: PathBuf,
    },
    /// Get the node_id and hash string from a node running accept in the local network
    /// Download the content from that node.
    Connect {
        /// Endpoint ID of a node on the local network
        endpoint_id: PublicKey,
        /// Hash of content you want to download from the node
        hash: Hash,
        /// save the content to a file
        #[clap(long, short)]
        out: Option<PathBuf>,
    },
}

async fn accept(path: &Path) -> Result<()> {
    if !path.is_file() {
        println!("Content must be a file.");
        return Ok(());
    }

    let key = get_or_generate_secret_key()?;

    println!("Starting iroh node with mdns discovery...");
    // create a new node
    let endpoint = Endpoint::empty_builder(RelayMode::Default)
        .secret_key(key)
        .discovery(MdnsDiscovery::builder())
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;
    let builder = Router::builder(endpoint.clone());
    let store = MemStore::new();
    let blobs = BlobsProtocol::new(&store, None);
    let builder = builder.accept(iroh_blobs::ALPN, blobs.clone());
    let node = builder.spawn();

    if !path.is_file() {
        println!("Content must be a file.");
        node.shutdown().await?;
        return Ok(());
    }
    let absolute = path.canonicalize()?;
    println!("Adding {} as {}...", path.display(), absolute.display());
    let tag = store.add_path(absolute).await?;
    println!("To fetch the blob:\n\tcargo run --example mdns-discovery --features examples -- connect {} {} -o [FILE_PATH]", node.endpoint().id(), tag.hash);
    tokio::signal::ctrl_c().await?;
    node.shutdown().await?;
    Ok(())
}

async fn connect(node_id: PublicKey, hash: Hash, out: Option<PathBuf>) -> Result<()> {
    let key = SecretKey::generate(&mut rand::rng());
    // todo: disable discovery publishing once https://github.com/n0-computer/iroh/issues/3401 is implemented
    let discovery = MdnsDiscovery::builder();

    println!("Starting iroh node with mdns discovery...");
    // create a new node
    let endpoint = Endpoint::empty_builder(RelayMode::Disabled)
        .secret_key(key)
        .discovery(discovery)
        .bind()
        .await?;
    let store = MemStore::new();

    println!("NodeID: {}", endpoint.id());
    let conn = endpoint.connect(node_id, iroh_blobs::ALPN).await?;
    let stats = store.remote().fetch(conn, hash).await?;
    println!(
        "Fetched {} bytes for hash {}",
        stats.payload_bytes_read, hash
    );
    if let Some(path) = out {
        let absolute = std::env::current_dir()?.join(&path);
        ensure!(!absolute.is_dir(), "output must not be a directory");
        println!(
            "exporting {hash} to {} -> {}",
            path.display(),
            absolute.display()
        );
        let size = store.export(hash, absolute).await?;
        println!("Exported {size} bytes");
    }

    endpoint.close().await;
    // Shutdown the store. This is not needed for the mem store, but would be
    // necessary for a persistent store to allow it to write any pending data to disk.
    store.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> n0_error::Result<()> {
    setup_logging();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Accept { path } => {
            accept(path).await?;
        }
        Commands::Connect {
            endpoint_id,
            hash,
            out,
        } => {
            connect(*endpoint_id, *hash, out.clone()).await?;
        }
    }
    Ok(())
}
