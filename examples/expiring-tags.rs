//! This example shows how to create tags that expire after a certain time.
//!
//! We use a prefix so we can distinguish between expiring and normal tags, and
//! then encode the expiry date in the tag name after the prefix, in a format
//! that sorts in the same order as the expiry date.
//!
//! Then we can just use
use std::time::{Duration, SystemTime};

use chrono::Utc;
use futures_lite::StreamExt;
use iroh::endpoint;
use iroh_blobs::{
    hashseq::HashSeq, rpc::client::blobs::MemClient as BlobsClient, store::GcConfig, BlobFormat,
    Hash, HashAndFormat, Tag,
};
use tokio::signal::ctrl_c;

/// Using an iroh rpc client, create a tag that is marked to expire at `expiry` for all the given hashes.
///
/// The tag name will be `prefix`- followed by the expiry date in iso8601 format (e.g. `expiry-2025-01-01T12:00:00Z`).
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

async fn delete_expired_tags(blobs: &BlobsClient, prefix: &str, bulk: bool) -> anyhow::Result<()> {
    let mut tags = blobs.tags().list().await?;
    let prefix = format!("{}-", prefix);
    let now = chrono::Utc::now();
    let end = format!(
        "{}-{}",
        prefix,
        now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
    );
    if bulk {
        // delete all tags with the prefix and an expiry date before now
        //
        // this should be very efficient, since it is just a single database operation
        blobs
            .tags()
            .delete_range(Tag::from(prefix.clone())..Tag::from(end))
            .await?;
    } else {
        // find tags to delete one by one and then delete them
        //
        // this allows us to print the tags before deleting them
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
            blobs.tags().delete(tag).await?;
        }
    }
    Ok(())
}

async fn info_task(blobs: BlobsClient) -> anyhow::Result<()> {
    tokio::time::sleep(Duration::from_secs(1)).await;
    loop {
        let now = chrono::Utc::now();
        let mut tags = blobs.tags().list().await?;
        println!("Current time: {}", now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        println!("Tags:");
        while let Some(tag) = tags.next().await {
            let tag = tag?;
            println!("  {:?}", tag);
        }
        let mut blobs = blobs.list().await?;
        println!("Blobs:");
        while let Some(info) = blobs.next().await {
            let info = info?;
            println!("  {} {} bytes", info.hash, info.size);
        }
        println!();
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn delete_expired_tags_task(blobs: BlobsClient, prefix: &str) -> anyhow::Result<()> {
    loop {
        delete_expired_tags(&blobs, prefix, false).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let endpoint = endpoint::Endpoint::builder().bind().await?;
    let store = iroh_blobs::store::fs::Store::load("blobs").await?;
    let blobs = iroh_blobs::net_protocol::Blobs::builder(store).build(&endpoint);
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
        .spawn()
        .await?;

    // setup: add some data and tag it
    {
        // add several blobs and tag them with an expiry date 10 seconds in the future
        let batch = blobs.client().batch().await?;
        let a = batch.add_bytes("blob 1".as_bytes()).await?;
        let b = batch.add_bytes("blob 2".as_bytes()).await?;
        let expires_at = SystemTime::now()
            .checked_add(Duration::from_secs(10))
            .unwrap();
        create_expiring_tag(
            blobs.client(),
            &[*a.hash(), *b.hash()],
            "expiring",
            expires_at,
        )
        .await?;

        // add a single blob and tag it with an expiry date 60 seconds in the future
        let c = batch.add_bytes("blob 3".as_bytes()).await?;
        let expires_at = SystemTime::now()
            .checked_add(Duration::from_secs(60))
            .unwrap();
        create_expiring_tag(blobs.client(), &[*c.hash()], "expiring", expires_at).await?;
        // batch goes out of scope, so data is only protected by the tags we created
    }
    let client = blobs.client().clone();

    // delete expired tags every 5 seconds
    let delete_task = tokio::spawn(delete_expired_tags_task(client.clone(), "expiring"));
    // print all tags and blobs every 5 seconds
    let info_task = tokio::spawn(info_task(client.clone()));

    ctrl_c().await?;
    delete_task.abort();
    info_task.abort();
    router.shutdown().await?;
    Ok(())
}
