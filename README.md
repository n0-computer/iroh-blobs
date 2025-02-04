# iroh-blobs

This crate provides blob and collection transfer support for iroh. It implements a simple request-response protocol based on blake3 verified streaming.

A request describes data in terms of blake3 hashes and byte ranges. It is possible to
request blobs or ranges of blobs, as well as collections.

The requester opens a quic stream to the provider and sends the request. The provider answers with the requested data, encoded as [blake3](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf) verified streams, on the same quic stream.

This crate is usually used together with [iroh](https://crates.io/crates/iroh), but can also be used with normal [quinn](https://crates.io/crates/quinn) connections. Connection establishment is left up to the user or a higher level APIs such as the iroh CLI.

## Concepts

- **Blob:** a sequence of bytes of arbitrary size, without any metadata.

- **Link:** a 32 byte blake3 hash of a blob.

- **HashSeq:** a blob that contains a sequence of links. Its size is a multiple of 32.

- **Provider:** The side that provides data and answers requests. Providers wait for incoming requests from Requests.

- **Requester:** The side that asks for data. It is initiating requests to one or many providers.


## Getting started

The `iroh-blobs` protocol was designed to be used in conjunction with `iroh`. [Iroh](https://docs.rs/iroh) is a networking library for making direct connections, these connections are what power the data transfers in `iroh-blobs`.

Iroh provides a [`Router`](https://docs.rs/iroh/latest/iroh/protocol/struct.Router.html) that takes an [`Endpoint`](https://docs.rs/iroh/latest/iroh/endpoint/struct.Endpoint.html) and any protocols needed for the application. Similar to a router in webserver library, it runs a loop accepting incoming connections and routes them to the specific protocol handler, based on `ALPN`.

Here is a basic example of how to set up `iroh-blobs` with `iroh`:

```rust
use iroh::{protocol::Router, Endpoint};
use iroh_blobs::{store::traits::Store, net_protocol::Blobs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // create an iroh endpoint that includes the standard discovery mechanisms
    // we've built at number0
    let endpoint = Endpoint::builder().discovery_n0().bind().await?;

    // create an in-memory blob store
    // use `iroh_blobs::net_protocol::Blobs::persistent` to load or create a
    // persistent blob store from a path
    let blobs = Blobs::memory().build(&endpoint);

    // turn on the "rpc" feature if you need to create blobs and tags clients
    let blobs_client = blobs.client();
    let tags_client = blobs_client.tags();

    // build the router
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn()
        .await?;

    // do fun stuff with the blobs protocol!
    router.shutdown().await?;
    drop(tags_client);
    Ok(())
}
```

## Examples

Examples that use `iroh-blobs` can be found in the `iroh` crate. the iroh crate publishes `iroh_blobs` as `iroh::bytes`.


# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
