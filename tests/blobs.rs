#![cfg(all(feature = "net_protocol", feature = "rpc"))]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::Endpoint;
use iroh_blobs::{net_protocol::Blobs, store::GcConfig, util::local_pool::LocalPool};
use testresult::TestResult;

#[tokio::test]
async fn blobs_gc_smoke() -> TestResult<()> {
    let pool = LocalPool::default();
    let endpoint = Endpoint::builder().bind().await?;
    let blobs = Blobs::memory().build(pool.handle(), &endpoint);
    let client = blobs.client();
    blobs.start_gc(GcConfig {
        period: Duration::from_millis(1),
        done_callback: None,
    })?;
    let h1 = client.add_bytes(b"test".to_vec()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(client.has(h1.hash).await?);
    client.tags().delete(h1.tag).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!client.has(h1.hash).await?);
    Ok(())
}

#[tokio::test]
async fn blobs_gc_protected() -> TestResult<()> {
    let pool = LocalPool::default();
    let endpoint = Endpoint::builder().bind().await?;
    let blobs = Blobs::memory().build(pool.handle(), &endpoint);
    let client = blobs.client();
    let h1 = client.add_bytes(b"test".to_vec()).await?;
    let protected = Arc::new(Mutex::new(Vec::new()));
    blobs.add_protected(Box::new({
        let protected = protected.clone();
        move |x| {
            let protected = protected.clone();
            Box::pin(async move {
                let protected = protected.lock().unwrap();
                for h in protected.as_slice() {
                    x.insert(*h);
                }
            })
        }
    }))?;
    blobs.start_gc(GcConfig {
        period: Duration::from_millis(1),
        done_callback: None,
    })?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // protected from gc due to tag
    assert!(client.has(h1.hash).await?);
    protected.lock().unwrap().push(h1.hash);
    client.tags().delete(h1.tag).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // protected from gc due to being in protected set
    assert!(client.has(h1.hash).await?);
    protected.lock().unwrap().clear();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // not protected, must be gone
    assert!(!client.has(h1.hash).await?);
    Ok(())
}
