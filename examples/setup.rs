use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{net_protocol::Blobs, util::local_pool::LocalPool};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // create an iroh endpoint that includes the standard discovery mechanisms
    // we've built at number0
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // spawn a local pool with one thread per CPU
    // for a single threaded pool use `LocalPool::single`
    let local_pool = LocalPool::default();

    // create an in-memory blob store
    // use `iroh_blobs::net_protocol::Blobs::persistent` to load or create a
    // persistent blob store from a path
    let blobs = Blobs::memory().build(local_pool.handle(), &endpoint);

    // turn on the "rpc" feature if you need to create blobs and tags clients
    let blobs_client = blobs.client();
    let tags_client = blobs_client.tags();

    // build the router
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn()
        .await?;

    // do fun stuff with the blobs protocol!
    // make sure not to drop the local_pool before you are finished
    router.shutdown().await?;
    drop(local_pool);
    drop(tags_client);
    Ok(())
}
