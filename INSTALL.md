# Getting started

The `iroh-blobs` protocol is meant to be use in conjunction with `iroh`. [Iroh](https://docs.rs/iroh/latest/iroh/index.html) is a networking library for making direct connections, these connections are what power the data transfers in `iroh-blobs`.

Iroh provides a [`Router`](https://docs.rs/iroh/latest/iroh/protocol/struct.Router.html) that takes an [`Endpoint`](https://docs.rs/iroh/latest/iroh/endpoint/struct.Endpoint.html) and any protocols needed for the application. Similar to a router in webserver library, it runs a loop accepting incoming connections and routes them to the specific protocol handler, based on `ALPN`.

Here is a basic example of how to set up `iroh-blobs` with `iroh`:

```rust
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{net_protocol::Blobs, util::local_pool::LocalPool};

#[tokio::main]
async fn main() -> Result<(), std::fmt::Error> {
    // create an iroh endpoint that includes the standard discovery mechanisms
    // we've built at number0
    let endpoint = Endpoint::builder().discovery_n0().bind().await.unwrap();

    // spawn a local pool with one thread per CPU
    // for a single threaded pool use `LocalPool::single`
    let local_pool = LocalPool::default();

    // create an in-memory blob store
    // use `iroh_blobs::net_protocol::Blobs::persistent` to load or create a
    // persistent blob store from a path
    let blobs = Blobs::memory().build(local_pool.handle(), &endpoint);

    // turn on the "rpc" feature if you need to create blobs and tags clients
    let blobs_client = blobs.clone().client();
    let tags_client = blobs_client.tags();

    // build the router
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    // do fun stuff with the blobs protocol!
    // make sure not to drop the local_pool before you are finished
    Ok(())
}
```
