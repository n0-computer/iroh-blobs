#![cfg(feature = "net_protocol")]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh_blobs::{net_protocol::Blobs, store::GcConfig, util::local_pool::LocalPool, Hash};
use iroh_net::Endpoint;
use testresult::TestResult;

#[tokio::test]
async fn blobs_gc_smoke() -> TestResult<()> {
    let pool = LocalPool::default();
    let endpoint = Endpoint::builder().bind().await?;
    let blobs = Blobs::memory().build(pool.handle(), &endpoint);
    let client = blobs.clone().client();
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
    let client: iroh_blobs::rpc::client::blobs::Client<
        quic_rpc::transport::flume::FlumeConnector<
            iroh_blobs::rpc::proto::Response,
            iroh_blobs::rpc::proto::Request,
        >,
    > = blobs.clone().client();
    let h1 = client.add_bytes(b"test".to_vec()).await?;
    let protected: Arc<Mutex<Vec<Hash>>> = Arc::new(Mutex::new(Vec::new()));
    let protected2 = protected.clone();
    blobs.add_protected(Box::new(move |x| {
        let protected = protected2.clone();
        Box::pin(async move {
            let protected = protected.lock().unwrap();
            for h in protected.as_slice() {
                x.insert(*h);
            }
        })
    }))?;
    blobs.start_gc(GcConfig {
        period: Duration::from_millis(1),
        done_callback: None,
    })?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // protected from gc due to tag
    assert!(client.has(h1.hash).await?);
    client.tags().delete(h1.tag).await?;
    protected.lock().unwrap().push(h1.hash);
    tokio::time::sleep(Duration::from_millis(100)).await;
    // protected from gc due to being in protected set
    assert!(client.has(h1.hash).await?);
    protected.lock().unwrap().clear();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // not protected, must be gone
    assert!(!client.has(h1.hash).await?);
    Ok(())
}
