//! An example how to download a single blob or collection from a node and write it to stdout using the `get` finite state machine directly.
//!
//! Since this example does not use [`iroh-net::Endpoint`], it does not do any holepunching, and so will only work locally or between two processes that have public IP addresses.
//!
//! Run the provide-bytes example first. It will give instructions on how to run this example properly.
use std::str::FromStr;

use anyhow::{Context, Result};
use iroh_blobs::{
    get::fsm::{AtInitial, ConnectedNext, EndBlobNext},
    hashseq::HashSeq,
    protocol::GetRequest,
    BlobFormat,
};
use iroh_io::ConcatenateSliceWriter;
use tracing_subscriber::{prelude::*, EnvFilter};

const EXAMPLE_ALPN: &[u8] = b"n0/iroh/examples/bytes/0";

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("\nfetch fsm example!");
    setup_logging();
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        anyhow::bail!("usage: fetch-fsm [TICKET]");
    }
    let ticket =
        iroh_blobs::ticket::BlobTicket::from_str(&args[1]).context("unable to parse [TICKET]")?;

    let (node, hash, format) = ticket.into_parts();

    // create an endpoint to listen for incoming connections
    let endpoint = iroh::Endpoint::builder()
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![EXAMPLE_ALPN.into()])
        .bind()
        .await?;
    println!(
        "\nlistening on {:?}",
        endpoint.node_addr().await?.direct_addresses
    );
    println!("fetching hash {hash} from {:?}", node.node_id);

    // connect
    let connection = endpoint.connect(node, EXAMPLE_ALPN).await?;

    match format {
        BlobFormat::HashSeq => {
            // create a request for a collection
            let request = GetRequest::all(hash);
            // create the initial state of the finite state machine
            let initial = iroh_blobs::get::fsm::start(connection, request);

            write_collection(initial).await
        }
        BlobFormat::Raw => {
            // create a request for a single blob
            let request = GetRequest::single(hash);
            // create the initial state of the finite state machine
            let initial = iroh_blobs::get::fsm::start(connection, request);

            write_blob(initial).await
        }
    }
}

async fn write_blob(initial: AtInitial) -> Result<()> {
    // connect (create a stream pair)
    let connected = initial.next().await?;

    // we expect a start root message, since we requested a single blob
    let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
        panic!("expected start root")
    };
    // we can just call next to proceed to the header, since we know the root hash
    let header = start_root.next();

    // we need to wrap stdout in a struct that implements AsyncSliceWriter. Since we can not
    // seek in stdout we use ConcatenateSliceWriter which just concatenates all the writes.
    let writer = ConcatenateSliceWriter::new(tokio::io::stdout());

    // make the spacing nicer in the terminal
    println!();
    // use the utility function write_all to write the entire blob
    let end = header.write_all(writer).await?;

    // we requested a single blob, so we expect to enter the closing state
    let EndBlobNext::Closing(closing) = end.next() else {
        panic!("expected closing")
    };

    // close the connection and get the stats
    let _stats = closing.next().await?;
    Ok(())
}

async fn write_collection(initial: AtInitial) -> Result<()> {
    // connect
    let connected = initial.next().await?;
    // read the first bytes
    let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
        anyhow::bail!("failed to parse collection");
    };
    // check that we requested the whole collection
    if !start_root.ranges().is_all() {
        anyhow::bail!("collection was not requested completely");
    }

    // move to the header
    let header: iroh_blobs::get::fsm::AtBlobHeader = start_root.next();
    let (root_end, hashes_bytes) = header.concatenate_into_vec().await?;
    let next = root_end.next();
    let EndBlobNext::MoreChildren(at_meta) = next else {
        anyhow::bail!("missing meta blob, got {next:?}");
    };
    // parse the hashes from the hash sequence bytes
    let hashes = HashSeq::try_from(bytes::Bytes::from(hashes_bytes))
        .context("failed to parse hashes")?
        .into_iter()
        .collect::<Vec<_>>();
    let meta_hash = hashes.first().context("missing meta hash")?;

    let (meta_end, _meta_bytes) = at_meta.next(*meta_hash).concatenate_into_vec().await?;
    let mut curr = meta_end.next();
    let closing = loop {
        match curr {
            EndBlobNext::MoreChildren(more) => {
                let Some(hash) = hashes.get(more.child_offset() as usize) else {
                    break more.finish();
                };
                let header = more.next(*hash);

                // we need to wrap stdout in a struct that implements AsyncSliceWriter. Since we can not
                // seek in stdout we use ConcatenateSliceWriter which just concatenates all the writes.
                let writer = ConcatenateSliceWriter::new(tokio::io::stdout());

                // use the utility function write_all to write the entire blob
                let end = header.write_all(writer).await?;
                println!();
                curr = end.next();
            }
            EndBlobNext::Closing(closing) => {
                break closing;
            }
        }
    };
    // close the connection
    let _stats = closing.next().await?;
    Ok(())
}
