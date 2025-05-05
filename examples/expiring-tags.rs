use std::time::{Duration, SystemTime};

use chrono::Utc;
use futures_lite::StreamExt;
use iroh::endpoint;
use iroh_blobs::store::GcConfig;
use iroh_blobs::{hashseq::HashSeq, BlobFormat, HashAndFormat};
use iroh_blobs::Hash;

use iroh_blobs::rpc::client::blobs::MemClient as BlobsClient;
use tokio::signal::ctrl_c;

/// Using an iroh rpc client, create a tag that is marked to expire at `expiry` for all the given hashes.
///
/// The tag name will be `prefix`- followed by the expiry date in iso8601 format (e.g. `expiry-2025-01-01T12:00:00Z`).
/// 
async fn create_expiring_tag(
    iroh: &BlobsClient,
    hashes: &[Hash],
    prefix: &str,
    expiry: SystemTime,
) -> anyhow::Result<()> {
    let expiry = chrono::DateTime::<chrono::Utc>::from(expiry);
    let expiry = expiry.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let tagname = format!("{}-{}", prefix, expiry);
    let batch = iroh.batch().await?;
    let tt = if hashes.is_empty() {
        return Ok(());
    } else if hashes.len() == 1 {
        let hash = hashes[0];
        batch.temp_tag(HashAndFormat::raw(hash)).await?
    } else {
        let hs = hashes.into_iter().copied().collect::<HashSeq>();
        batch
            .add_bytes_with_opts(hs.into_inner(), BlobFormat::HashSeq)
            .await?
    };
    batch.persist_to(tt, tagname.as_str().into()).await?;
    println!("Created tag {}", tagname);
    Ok(())
}

async fn delete_expired_tags(iroh: &BlobsClient, prefix: &str) -> anyhow::Result<()> {
    let mut tags = iroh.tags().list().await?;
    let prefix = format!("{}-", prefix);
    let now = chrono::Utc::now();
    let mut to_delete = Vec::new();
    while let Some(tag) = tags.next().await {
        let tag = tag?.name;
        if let Some(rest) = tag.0.strip_prefix(prefix.as_bytes()) {
            let Ok(expiry) = std::str::from_utf8(rest) else {
                tracing::warn!("Tag {} does have non utf8 expiry", tag);
                continue;
            };
            let Ok(expiry) = chrono::DateTime::parse_from_rfc3339(expiry) else {
                tracing::warn!("Tag {} does have invalid expiry date", tag);
                continue;
            };
            let expiry = expiry.with_timezone(&Utc);
            if expiry < now {
                to_delete.push(tag);
            }
        }
    }
    for tag in to_delete {
        println!("Deleting expired tag {}", tag);
        iroh.tags().delete(tag).await?;
    }
    Ok(())
}

async fn print_tags_task(blobs: BlobsClient) -> anyhow::Result<()> {
    loop {
        let now = chrono::Utc::now();
        let mut tags = blobs.tags().list().await?;
        println!("Tags at {}:\n", now);
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            println!("  {:?}", tag);
        }
        println!();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn print_blobs_task(blobs: BlobsClient) -> anyhow::Result<()> {
    loop {
        let now = chrono::Utc::now();
        let mut blobs = blobs.list().await?;
        println!("Blobs at {}:\n", now);
        while let Some(info) = blobs.next().await {
            println!("  {:?}", info?);
        }
        println!();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn delete_expired_tags_task(blobs: BlobsClient, prefix: &str, ) -> anyhow::Result<()> {
    loop {
        delete_expired_tags(&blobs, prefix).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let endpoint = endpoint::Endpoint::builder().bind().await?;
    let store = iroh_blobs::store::fs::Store::load("blobs").await?;
    let blobs = iroh_blobs::net_protocol::Blobs::builder(store)
        .build(&endpoint);
    // enable gc with a short period
    blobs.start_gc(GcConfig {
        period: Duration::from_secs(1),
        done_callback: None,
    })?;
    // create a router and add blobs as a service
    //
    // You can skip this if you don't want to serve the data over the network.
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn().await?;

    // setup: add some data and tag it
    {
        // add several blobs and tag them with an expiry date 10 seconds in the future
        let batch = blobs.client().batch().await?;
        let a = batch.add_bytes("blob 1".as_bytes()).await?;
        let b = batch.add_bytes("blob 2".as_bytes()).await?;
        let expires_at = SystemTime::now().checked_add(Duration::from_secs(10)).unwrap();
        create_expiring_tag(blobs.client(), &[*a.hash(), *b.hash()], "expiring", expires_at).await?;

        // add a single blob and tag it with an expiry date 60 seconds in the future
        let c = batch.add_bytes("blob 3".as_bytes()).await?;
        let expires_at = SystemTime::now().checked_add(Duration::from_secs(60)).unwrap();
        create_expiring_tag(blobs.client(), &[*c.hash()], "expiring", expires_at).await?;
        // batch goes out of scope, so data is only protected by the tags we created
    }
    let client = blobs.client().clone();

    // delete expired tags every 5 seconds
    let check_task = tokio::spawn(delete_expired_tags_task(client.clone(), "expiring"));
    // print tags every 5 seconds
    let print_tags_task = tokio::spawn(print_tags_task(client.clone()));
    // print blobs every 5 seconds
    let print_blobs_task = tokio::spawn(print_blobs_task(client));

    ctrl_c().await?;
    router.shutdown().await?;
    check_task.abort();
    print_tags_task.abort();
    print_blobs_task.abort();
    Ok(())
}