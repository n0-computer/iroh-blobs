//! An example that provides a blob or a collection over a Quinn connection.
//!
//! Since this example does not use [`iroh-net::Endpoint`], it does not do any holepunching, and so will only work locally or between two processes that have public IP addresses.
//!
//! Run this example with
//!    cargo run --example provide-bytes blob
//! To provide a blob (single file)
//!
//! Run this example with
//!    cargo run --example provide-bytes collection
//! To provide a collection (multiple blobs)
use anyhow::Result;
use iroh_blobs::{format::collection::Collection, util::local_pool::LocalPool, Hash};
use tracing::warn;
use tracing_subscriber::{prelude::*, EnvFilter};

mod connect;
use connect::{make_and_write_certs, make_server_endpoint, CERT_PATH};

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        anyhow::bail!(
            "usage: provide-bytes [FORMAT], where [FORMAT] is either 'blob' or 'collection'\n\nThe 'blob' example demonstrates sending a single blob of bytes. The 'collection' example demonstrates sending multiple blobs of bytes, grouped together in a 'collection'."
        );
    }
    let format = {
        if args[1] != "blob" && args[1] != "collection" {
            anyhow::bail!(
                "expected either 'blob' or 'collection' for FORMAT argument, got {}",
                args[1]
            );
        }
        args[1].clone()
    };
    println!("\nprovide bytes {format} example!");

    let (db, hash) = if format == "collection" {
        let (mut db, names) = iroh_blobs::store::readonly_mem::Store::new([
            ("blob1", b"the first blob of bytes".to_vec()),
            ("blob2", b"the second blob of bytes".to_vec()),
        ]); // create a collection
        let collection: Collection = names
            .into_iter()
            .map(|(name, hash)| (name, Hash::from(hash)))
            .collect();
        // add it to the db
        let hash = db.insert_many(collection.to_blobs()).unwrap();
        (db, hash)
    } else {
        // create a new database and add a blob
        let (db, names) =
            iroh_blobs::store::readonly_mem::Store::new([("hello", b"Hello World!".to_vec())]);

        // get the hash of the content
        let hash = names.get("hello").unwrap();
        (db, Hash::from(hash.as_bytes()))
    };

    // create tls certs and save to CERT_PATH
    let (key, cert) = make_and_write_certs().await?;

    // create an endpoint to listen for incoming connections
    let endpoint = make_server_endpoint(key, cert)?;
    let addr = endpoint.local_addr()?;
    println!("\nlistening on {addr}");
    println!("providing hash {hash}");

    println!("\nfetch the content using a finite state machine by running the following example:\n\ncargo run --example fetch-fsm {hash} \"{addr}\" {format}");
    println!("\nfetch the content using a stream by running the following example:\n\ncargo run --example fetch-stream {hash} \"{addr}\" {format}\n");

    // create a new local pool handle with 1 worker thread
    let lp = LocalPool::single();

    let accept_task = tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            println!("connection incoming");

            let conn = match incoming.accept() {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("incoming connection failed: {err:#}");
                    // we can carry on in these cases:
                    // this can be caused by retransmitted datagrams
                    continue;
                }
            };
            let db = db.clone();
            let lp = lp.clone();

            // spawn a task to handle the connection
            tokio::spawn(async move {
                let remote_addr = conn.remote_address();
                let conn = match conn.await {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!(%remote_addr, "Error connecting: {err:#}");
                        return;
                    }
                };
                iroh_blobs::provider::handle_connection(conn, db, Default::default(), lp).await
            });
        }
    });

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tokio::fs::remove_dir_all(std::path::PathBuf::from(CERT_PATH)).await?;
            accept_task.abort();
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("unable to listen for ctrl-c: {e}")),
    }
}
