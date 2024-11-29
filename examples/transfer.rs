use std::{path::PathBuf, str::FromStr};

use anyhow::Result;
use iroh::{protocol::Router, Endpoint};
use iroh_base::ticket::BlobTicket;
use iroh_blobs::{
    net_protocol::Blobs,
    rpc::client::blobs::{ReadAtLen, WrapOption},
    util::{local_pool::LocalPool, SetTagOption},
};

#[tokio::main]
async fn main() -> Result<()> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // We initialize the Blobs protocol in-memory
    let local_pool = LocalPool::default();
    let blobs = Blobs::memory().build(&local_pool, &endpoint);

    // Now we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let node = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn()
        .await?;

    let blobs = blobs.client();

    let args = std::env::args().collect::<Vec<_>>();
    match &args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        [_cmd, "send", path] => {
            let abs_path = PathBuf::from_str(path)?.canonicalize()?;

            println!("Analyzing file.");

            let blob = blobs
                .add_from_path(abs_path, true, SetTagOption::Auto, WrapOption::NoWrap)
                .await?
                .finish()
                .await?;

            let node_id = node.endpoint().node_id();
            let ticket = BlobTicket::new(node_id.into(), blob.hash, blob.format)?;

            println!("File analyzed. Fetch this file by running:");
            println!("cargo run --example transfer -- receive {ticket} {path}");

            tokio::signal::ctrl_c().await?;
        }
        [_cmd, "receive", ticket, path] => {
            let path_buf = PathBuf::from_str(path)?;
            let ticket = BlobTicket::from_str(ticket)?;

            println!("Starting download.");

            blobs
                .download(ticket.hash(), ticket.node_addr().clone())
                .await?
                .finish()
                .await?;

            println!("Finished download.");
            println!("Copying to destination.");

            let mut file = tokio::fs::File::create(path_buf).await?;
            let mut reader = blobs.read_at(ticket.hash(), 0, ReadAtLen::All).await?;
            tokio::io::copy(&mut reader, &mut file).await?;

            println!("Finished copying.");
        }
        _ => {
            println!("Couldn't parse command line arguments.");
            println!("Usage:");
            println!("    # to send:");
            println!("    cargo run --example transfer -- send [FILE]");
            println!("    # this will print a ticket.");
            println!();
            println!("    # to receive:");
            println!("    cargo run --example transfer -- receive [TICKET] [FILE]");
        }
    }

    // Gracefully shut down the node
    println!("Shutting down.");
    node.shutdown().await?;
    local_pool.shutdown().await;

    Ok(())
}
