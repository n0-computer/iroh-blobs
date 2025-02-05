use std::path::PathBuf;

use anyhow::{bail, Result};
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{
    net_protocol::Blobs,
    rpc::client::blobs::{self, WrapOption},
    store::ExportMode,
    ticket::BlobTicket,
    util::SetTagOption,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Create an endpoint, it allows creating and accepting
    // connections in the iroh p2p world
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;
    // We initialize the Blobs protocol in-memory
    let blobs = Blobs::memory().build(&endpoint);

    // Now we build a router that accepts blobs connections & routes them
    // to the blobs protocol.
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn()
        .await?;

    // Grab all passed in arguments, the first one is the binary itself, so we skip it.
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        print_usage();
        bail!("too few arguments");
    }

    match &*args[0] {
        "send" => {
            send(&router, blobs.client(), &args).await?;

            tokio::signal::ctrl_c().await?;
        }
        "receive" => {
            receive(blobs.client(), &args).await?;
        }
        cmd => {
            print_usage();
            bail!("unknown command {}", cmd);
        }
    }

    // Gracefully shut down the node
    println!("Shutting down.");
    router.shutdown().await?;

    Ok(())
}

async fn send(router: &Router, blobs: &blobs::MemClient, args: &[String]) -> Result<()> {
    let path: PathBuf = args[1].parse()?;
    let abs_path = path.canonicalize()?;

    println!("Analyzing file.");

    // keep the file in place, and link it
    let in_place = true;
    let blob = blobs
        .add_from_path(abs_path, in_place, SetTagOption::Auto, WrapOption::NoWrap)
        .await?
        .await?;

    let node_id = router.endpoint().node_id();
    let ticket = BlobTicket::new(node_id.into(), blob.hash, blob.format)?;

    println!("File analyzed. Fetch this file by running:");
    println!(
        "cargo run --example transfer -- receive {ticket} {}",
        path.display()
    );
    Ok(())
}

async fn receive(blobs: &blobs::MemClient, args: &[String]) -> Result<()> {
    if args.len() < 3 {
        print_usage();
        bail!("too few arguments");
    }
    let path_buf: PathBuf = args[1].parse()?;
    let ticket: BlobTicket = args[2].parse()?;

    println!("Starting download.");

    blobs
        .download(ticket.hash(), ticket.node_addr().clone())
        .await?
        .await?;

    println!("Finished download.");
    println!("Copying to destination.");

    blobs
        .export(
            ticket.hash(),
            path_buf,
            ticket.format().into(),
            ExportMode::Copy,
        )
        .await?;

    println!("Finished copying.");

    Ok(())
}

fn print_usage() {
    println!("Couldn't parse command line arguments.");
    println!("Usage:");
    println!("    # to send:");
    println!("    cargo run --example transfer -- send [FILE]");
    println!("    # this will print a ticket.");
    println!();
    println!("    # to receive:");
    println!("    cargo run --example transfer -- receive [TICKET] [FILE]");
}
