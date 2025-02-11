use std::{env::VarError, path::PathBuf, str::FromStr};

use bao_tree::{ChunkNum, ChunkRanges};
use clap::Parser;
use console::Term;
use iroh::{NodeId, SecretKey};
use iroh_blobs::{
    downloader2::{
        DownloadRequest, Downloader, ObserveEvent, ObserveRequest, StaticContentDiscovery,
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

    providers: Vec<NodeId>,
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

    fn update(&mut self, ev: ObserveEvent) {
        match ev {
            ObserveEvent::Bitfield { ranges } => {
                self.current = ranges;
            }
            ObserveEvent::BitfieldUpdate { added, removed } => {
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

fn bitmap(current: &[ChunkNum], requested: &[ChunkNum], n: usize) -> String {
    // If n is 0, return an empty string.
    if n == 0 {
        return String::new();
    }

    // Determine the overall bitfield size.
    // Since the ranges are sorted, we take the last element as the total size.
    let total = if let Some(&last) = requested.last() {
        last.0
    } else {
        // If there are no ranges, we assume the bitfield is empty.
        0
    };

    // If total is 0, output n spaces.
    if total == 0 {
        return " ".repeat(n);
    }

    let mut result = String::with_capacity(n);

    // For each of the n output buckets:
    for bucket in 0..n {
        // Calculate the bucket's start and end in the overall bitfield.
        let bucket_start = bucket as u64 * total / n as u64;
        let bucket_end = (bucket as u64 + 1) * total / n as u64;
        let bucket_size = bucket_end.saturating_sub(bucket_start);

        // Sum the number of bits that are set in this bucket.
        let mut set_bits = 0u64;
        for pair in current.chunks_exact(2) {
            let start = pair[0];
            let end = pair[1];
            // Determine the overlap between the bucket and the current range.
            let overlap_start = start.0.max(bucket_start);
            let overlap_end = end.0.min(bucket_end);
            if overlap_start < overlap_end {
                set_bits += overlap_end - overlap_start;
            }
        }

        // Calculate the fraction of the bucket that is set.
        let fraction = if bucket_size > 0 {
            set_bits as f64 / bucket_size as f64
        } else {
            0.0
        };

        // Map the fraction to a grayscale character.
        let ch = if fraction == 0.0 {
            ' ' // completely empty
        } else if fraction == 1.0 {
            '█' // completely full
        } else if fraction < 0.25 {
            '░'
        } else if fraction < 0.5 {
            '▒'
        } else {
            '▓'
        };

        result.push(ch);
    }

    result
}

async fn download(args: DownloadArgs) -> anyhow::Result<()> {
    let endpoint = iroh::Endpoint::builder().discovery_n0().bind().await?;
    let store = iroh_blobs::store::mem::Store::new();
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
            let bitmap = bitmap(current, requested, rows as usize);
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
