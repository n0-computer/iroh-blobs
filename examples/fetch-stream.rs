//! An example how to download a single blob or collection from a node and write it to stdout, using a helper method to turn the `get` finite state machine into a stream.
//!
//! Since this example does not use [`iroh-net::Endpoint`], it does not do any holepunching, and so will only work locally or between two processes that have public IP addresses.
//!
//! Run the provide-bytes example first. It will give instructions on how to run this example properly.
use std::{io, str::FromStr};

use anyhow::{Context, Result};
use bao_tree::io::fsm::BaoContentItem;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
use genawaiter::sync::{Co, Gen};
use iroh_blobs::{
    get::fsm::{AtInitial, BlobContentNext, ConnectedNext, EndBlobNext},
    hashseq::HashSeq,
    protocol::GetRequest,
    BlobFormat,
};
use tokio::io::AsyncWriteExt;
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
    println!("\nfetch stream example!");
    setup_logging();
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        anyhow::bail!("usage: fetch-stream [TICKET]");
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

    let mut stream = match format {
        BlobFormat::HashSeq => {
            // create a request for a collection
            let request = GetRequest::all(hash);

            // create the initial state of the finite state machine
            let initial = iroh_blobs::get::fsm::start(connection, request);

            // create a stream that yields all the data of the blob
            stream_children(initial).boxed_local()
        }
        BlobFormat::Raw => {
            // create a request for a single blob
            let request = GetRequest::single(hash);

            // create the initial state of the finite state machine
            let initial = iroh_blobs::get::fsm::start(connection, request);

            // create a stream that yields all the data of the blob
            stream_blob(initial).boxed_local()
        }
    };
    while let Some(item) = stream.next().await {
        let item = item?;
        tokio::io::stdout().write_all(&item).await?;
        println!();
    }
    Ok(())
}

/// Stream the response for a request for a single blob.
///
/// If the request was for a part of the blob, this will stream just the requested
/// blocks.
///
/// This will stream the root blob and close the connection.
fn stream_blob(initial: AtInitial) -> impl Stream<Item = io::Result<Bytes>> + 'static {
    async fn inner(initial: AtInitial, co: &Co<io::Result<Bytes>>) -> io::Result<()> {
        // connect
        let connected = initial.next().await?;
        // read the first bytes
        let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
            return Err(io::Error::new(io::ErrorKind::Other, "expected start root"));
        };
        // move to the header
        let header = start_root.next();
        // get the size of the content
        let (mut content, _size) = header.next().await?;
        // manually loop over the content and yield all data
        let done = loop {
            match content.next().await {
                BlobContentNext::More((next, data)) => {
                    if let BaoContentItem::Leaf(leaf) = data? {
                        // yield the data
                        co.yield_(Ok(leaf.data)).await;
                    }
                    content = next;
                }
                BlobContentNext::Done(done) => {
                    // we are done with the root blob
                    break done;
                }
            }
        };
        // close the connection even if there is more data
        let closing = match done.next() {
            EndBlobNext::Closing(closing) => closing,
            EndBlobNext::MoreChildren(more) => more.finish(),
        };
        // close the connection
        let _stats = closing.next().await?;
        Ok(())
    }

    Gen::new(|co| async move {
        if let Err(e) = inner(initial, &co).await {
            co.yield_(Err(e)).await;
        }
    })
}

/// Stream the response for a request for an iroh collection and its children.
///
/// If the request was for a part of the children, this will stream just the requested
/// blocks.
///
/// The root blob is not streamed. It must be fully included in the response.
fn stream_children(initial: AtInitial) -> impl Stream<Item = io::Result<Bytes>> + 'static {
    async fn inner(initial: AtInitial, co: &Co<io::Result<Bytes>>) -> io::Result<()> {
        // connect
        let connected = initial.next().await?;
        // read the first bytes
        let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to parse collection",
            ));
        };
        // check that we requested the whole collection
        if !start_root.ranges().is_all() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "collection was not requested completely",
            ));
        }
        // move to the header
        let header: iroh_blobs::get::fsm::AtBlobHeader = start_root.next();
        let (root_end, hashes_bytes) = header.concatenate_into_vec().await?;

        // parse the hashes from the hash sequence bytes
        let hashes = HashSeq::try_from(bytes::Bytes::from(hashes_bytes))
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("failed to parse hashes: {e}"))
            })?
            .into_iter()
            .collect::<Vec<_>>();

        let next = root_end.next();
        let EndBlobNext::MoreChildren(at_meta) = next else {
            return Err(io::Error::new(io::ErrorKind::Other, "missing meta blob"));
        };
        let meta_hash = hashes
            .first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "missing meta link"))?;
        let (meta_end, _meta_bytes) = at_meta.next(*meta_hash).concatenate_into_vec().await?;
        let mut curr = meta_end.next();
        let closing = loop {
            match curr {
                EndBlobNext::MoreChildren(more) => {
                    let Some(hash) = hashes.get(more.child_offset() as usize) else {
                        break more.finish();
                    };
                    let header = more.next(*hash);
                    let (mut content, _size) = header.next().await?;
                    // manually loop over the content and yield all data
                    let done = loop {
                        match content.next().await {
                            BlobContentNext::More((next, data)) => {
                                if let BaoContentItem::Leaf(leaf) = data? {
                                    // yield the data
                                    co.yield_(Ok(leaf.data)).await;
                                }
                                content = next;
                            }
                            BlobContentNext::Done(done) => {
                                // we are done with the root blob
                                break done;
                            }
                        }
                    };
                    curr = done.next();
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

    Gen::new(|co| async move {
        if let Err(e) = inner(initial, &co).await {
            co.yield_(Err(e)).await;
        }
    })
}
