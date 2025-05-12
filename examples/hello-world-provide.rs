//! The smallest possible example to spin up a node and serve a single blob.
//!
//! This is using an in memory database and a random node id.
//! run this example from the project root:
//!     $ cargo run --example hello-world-provide
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{net_protocol::Blobs, ticket::BlobTicket};
use tracing_subscriber::{prelude::*, EnvFilter};

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();
    println!("'Hello World' provide example!");

    // create a new node
    let endpoint = Endpoint::builder().bind().await?;
    let builder = Router::builder(endpoint);
    let blobs = Blobs::memory().build(builder.endpoint());
    let builder = builder.accept(iroh_blobs::ALPN, blobs.clone());
    let blobs_client = blobs.client();
    let node = builder.spawn();

    // add some data and remember the hash
    let res = blobs_client.add_bytes("Hello, world!").await?;

    // create a ticket
    let addr = node.endpoint().node_addr().await?;
    let ticket = BlobTicket::new(addr, res.hash, res.format)?;

    // print some info about the node
    println!("serving hash:    {}", ticket.hash());
    println!("node id:         {}", ticket.node_addr().node_id);
    println!("node listening addresses:");
    for addr in ticket.node_addr().direct_addresses() {
        println!("\t{:?}", addr);
    }
    println!(
        "node relay server url: {:?}",
        ticket
            .node_addr()
            .relay_url()
            .expect("a default relay url should be provided")
            .to_string()
    );
    // print the ticket, containing all the above information
    println!("\nin another terminal, run:");
    println!("\t cargo run --example hello-world-fetch {}", ticket);
    // block until SIGINT is received (ctrl+c)
    tokio::signal::ctrl_c().await?;
    node.shutdown().await?;
    Ok(())
}
