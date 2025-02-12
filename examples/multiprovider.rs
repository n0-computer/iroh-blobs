use std::{env::VarError, path::PathBuf, str::FromStr};

use bao_tree::{ChunkNum, ChunkRanges};
use clap::Parser;
use console::Term;
use iroh::{NodeId, SecretKey};
use iroh_blobs::{
    downloader2::{
        print_bitmap, BitfieldEvent, BitfieldState, BitfieldUpdate, DownloadRequest, Downloader,
        ObserveRequest, StaticContentDiscovery,
    },
    store::Store,
    util::total_bytes,
    Hash,
};

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

    #[clap(help = "providers to download from")]
    providers: Vec<NodeId>,

    #[clap(long, help = "path to save to")]
    path: Option<PathBuf>,
}

#[derive(Debug, Parser)]
struct ProvideArgs {
    #[clap(help = "path to provide")]
    path: Vec<PathBuf>,
}

fn load_secret_key() -> anyhow::Result<Option<iroh::SecretKey>> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => Ok(Some(SecretKey::from_str(&secret)?)),
        Err(VarError::NotPresent) => Ok(None),
        Err(x) => Err(x.into()),
    }
}

fn get_or_create_secret_key() -> iroh::SecretKey {
    match load_secret_key() {
        Ok(Some(secret)) => return secret,
        Ok(None) => {}
        Err(cause) => {
            println!("failed to load secret key: {}", cause);
        }
    };
    let secret = SecretKey::generate(rand::thread_rng());
    println!("Using secret key {secret}. Set IROH_SECRET env var to use the same key next time.");
    secret
}

async fn provide(args: ProvideArgs) -> anyhow::Result<()> {
    let store = iroh_blobs::store::mem::Store::new();
    let mut tags = Vec::new();
    for path in args.path {
        let data = std::fs::read(&path)?;
        let len = data.len();
        let tag = store
            .import_bytes(data.into(), iroh_blobs::BlobFormat::Raw)
            .await?;
        println!(
            "added {} as {}, {} bytes, {} chunks",
            path.display(),
            tag.hash(),
            len,
            (len + 1023) / 1024
        );
        tags.push((path, tag));
    }
    let secret_key = get_or_create_secret_key();
    let endpoint = iroh::Endpoint::builder()
        .discovery_n0()
        .secret_key(secret_key)
        .bind()
        .await?;
    let id = endpoint.node_id();
    let blobs = iroh_blobs::net_protocol::Blobs::builder(store).build(&endpoint);
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn()
        .await?;
    println!("listening on {}", id);
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

/// Progress for a single download
struct BlobDownloadProgress {
    request: DownloadRequest,
    current: ChunkRanges,
}

impl BlobDownloadProgress {
    fn new(request: DownloadRequest) -> Self {
        Self {
            request,
            current: ChunkRanges::empty(),
        }
    }

    fn update(&mut self, ev: BitfieldEvent) {
        match ev {
            BitfieldEvent::State(BitfieldState { ranges, .. }) => {
                self.current = ranges;
            }
            BitfieldEvent::Update(BitfieldUpdate { added, removed, .. }) => {
                self.current |= added;
                self.current -= removed;
            }
        }
    }

    #[allow(dead_code)]
    fn get_stats(&self) -> (u64, u64) {
        let total = total_bytes(&self.request.ranges, u64::MAX);
        let downloaded = total_bytes(&self.current, u64::MAX);
        (downloaded, total)
    }

    #[allow(dead_code)]
    fn get_bitmap(&self) -> String {
        format!("{:?}", self.current)
    }

    fn is_done(&self) -> bool {
        self.current == self.request.ranges
    }
}

async fn download(args: DownloadArgs) -> anyhow::Result<()> {
    match &args.path {
        Some(path) => {
            tokio::fs::create_dir_all(path).await?;
            let store = iroh_blobs::store::fs::Store::load(path).await?;
            // make sure we properly shut down the store on ctrl-c
            let res = tokio::select! {
                x = download_impl(args, store.clone()) => x,
                _ = tokio::signal::ctrl_c() => Ok(()),
            };
            store.shutdown().await;
            res
        }
        None => {
            let store = iroh_blobs::store::mem::Store::new();
            download_impl(args, store).await
        }
    }
}

async fn download_impl<S: Store>(args: DownloadArgs, store: S) -> anyhow::Result<()> {
    let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
    let discovery = StaticContentDiscovery::new(Default::default(), args.providers);
    let downloader = Downloader::builder(endpoint, store)
        .discovery(discovery)
        .build();
    let request = DownloadRequest {
        hash: args.hash,
        ranges: ChunkRanges::from(ChunkNum(0)..ChunkNum(25421)),
    };
    let downloader2 = downloader.clone();
    let mut progress = BlobDownloadProgress::new(request.clone());
    tokio::spawn(async move {
        let request = ObserveRequest {
            hash: args.hash,
            ranges: ChunkRanges::from(ChunkNum(0)..ChunkNum(25421)),
            buffer: 1024,
        };
        let mut observe = downloader2.observe(request).await?;
        let term = Term::stdout();
        let (_, rows) = term.size();
        while let Some(chunk) = observe.recv().await {
            progress.update(chunk);
            let current = progress.current.boundaries();
            let requested = progress.request.ranges.boundaries();
            let bitmap = print_bitmap(current, requested, rows as usize);
            print!("\r{bitmap}");
            if progress.is_done() {
                println!();
                break;
            }
        }
        anyhow::Ok(())
    });
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
