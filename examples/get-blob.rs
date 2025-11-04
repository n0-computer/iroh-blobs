/// Example how to request a blob from a remote node without using a store.
mod common;
use bao_tree::io::BaoContentItem;
use clap::Parser;
use common::setup_logging;
use iroh::discovery::pkarr::PkarrResolver;
use iroh_blobs::{get::request::GetBlobItem, ticket::BlobTicket, BlobFormat};
use n0_future::StreamExt;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    /// Ticket describing the content to fetch and the node to fetch it from
    ///
    /// This example only supports raw blobs.
    ticket: BlobTicket,
    /// True to print data as it arrives, false to complete the download and then
    /// print the data. Defaults to true.
    ///
    /// Note that setting progress to false can lead to an out-of-memory error
    /// for very large blobs.
    #[arg(long, default_value = "true")]
    progress: bool,
}

#[tokio::main]
async fn main() -> n0_error::Result<()> {
    setup_logging();
    let cli = Cli::parse();
    let ticket = cli.ticket;
    let endpoint = iroh::Endpoint::empty_builder(iroh::RelayMode::Default)
        .discovery(PkarrResolver::n0_dns())
        .bind()
        .await?;
    n0_error::ensure!(
        ticket.format() == BlobFormat::Raw,
        "This example only supports raw blobs."
    );
    let connection = endpoint.connect(ticket.addr().id, iroh_blobs::ALPN).await?;
    let mut progress = iroh_blobs::get::request::get_blob(connection, ticket.hash());
    let stats = if cli.progress {
        loop {
            match progress.next().await {
                Some(GetBlobItem::Item(item)) => match item {
                    BaoContentItem::Leaf(leaf) => {
                        tokio::io::stdout().write_all(&leaf.data).await?;
                    }
                    BaoContentItem::Parent(parent) => {
                        tracing::info!("Parent: {parent:?}");
                    }
                },
                Some(GetBlobItem::Done(stats)) => {
                    break stats;
                }
                Some(GetBlobItem::Error(err)) => {
                    n0_error::bail!("Error while streaming blob: {err}");
                }
                None => {
                    n0_error::bail!("Stream ended unexpectedly.");
                }
            }
        }
    } else {
        let (bytes, stats) = progress.bytes_and_stats().await?;
        tokio::io::stdout().write_all(&bytes).await?;
        stats
    };
    tracing::info!("Stream done with stats: {stats:?}");
    Ok(())
}
