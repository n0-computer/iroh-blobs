use std::path::PathBuf;

use bao_tree::ChunkRanges;
use iroh::NodeId;
use iroh_blobs::{downloader2::{DownloadRequest, Downloader, StaticContentDiscovery}, store::Store, Hash};
use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, Parser)]
enum Subcommand {
    Download(DownloadArgs),
    Provide(ProvideArgs),
}

#[derive(Debug, Parser)]
struct DownloadArgs {
    #[clap(help = "hash to download")]
    hash: Hash,

    providers: Vec<NodeId>,
}

#[derive(Debug, Parser)]
struct ProvideArgs {
    #[clap(help = "path to provide")]
    path: Vec<PathBuf>,
}

async fn provide(args: ProvideArgs) -> anyhow::Result<()> {
    let store = iroh_blobs::store::mem::Store::new();
    let mut tags = Vec::new();
    for path in args.path {
        let data = std::fs::read(&path)?;
        let len = data.len();
        let tag = store.import_bytes(data.into(), iroh_blobs::BlobFormat::Raw).await?;
        println!("added {} as {}, {} bytes, {} chunks", path.display(), tag.hash(), len, (len + 1023) / 1024);
        tags.push((path, tag));
    }
    let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
    let id = endpoint.node_id();
    let blobs = iroh_blobs::net_protocol::Blobs::builder(store).build(&endpoint);
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn().await?;
    println!("listening on {}", id);
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

async fn download(args: DownloadArgs) -> anyhow::Result<()> {
    let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
    let store = iroh_blobs::store::mem::Store::new();
    let discovery = StaticContentDiscovery::new(Default::default(), args.providers);
    let downloader = Downloader::builder(endpoint, store).discovery(discovery).build();
    let request = DownloadRequest {
        hash: args.hash,
        ranges: ChunkRanges::all(),
    };
    downloader.download(request).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.subcommand {
        Subcommand::Download(args) => download(args).await?,
        Subcommand::Provide(args) => provide(args).await?,
    }
    Ok(())
}