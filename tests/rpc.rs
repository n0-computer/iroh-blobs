#![cfg(feature = "test")]
use std::{net::SocketAddr, path::PathBuf, vec};

use iroh_blobs::{net_protocol::Blobs, util::local_pool::LocalPool};
use quic_rpc::client::QuinnConnector;
use tempfile::TempDir;
use testresult::TestResult;
use tokio_util::task::AbortOnDropHandle;

type QC = QuinnConnector<iroh_blobs::rpc::proto::RpcService>;
type BlobsClient = iroh_blobs::rpc::client::blobs::Client<QC>;

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node {
    pub router: iroh::protocol::Router,
    pub blobs: Blobs<iroh_blobs::store::fs::Store>,
    pub local_pool: LocalPool,
    pub rpc_task: AbortOnDropHandle<()>,
}

impl Node {
    pub async fn new(path: PathBuf) -> anyhow::Result<(Self, SocketAddr, Vec<u8>)> {
        let store = iroh_blobs::store::fs::Store::load(path).await?;
        let local_pool = LocalPool::default();
        let endpoint = iroh::Endpoint::builder().bind().await?;
        let blobs = Blobs::builder(store).build(local_pool.handle(), &endpoint);
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn()
            .await?;
        let (config, key) = quic_rpc::transport::quinn::configure_server()?;
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap())?;
        let local_addr = endpoint.local_addr()?;
        let rpc_server = quic_rpc::transport::quinn::QuinnListener::new(endpoint)?;
        let rpc_server =
            quic_rpc::RpcServer::<iroh_blobs::rpc::proto::RpcService, _>::new(rpc_server);
        let blobs2 = blobs.clone();
        let rpc_task = rpc_server
            .spawn_accept_loop(move |msg, chan| blobs2.clone().handle_rpc_request(msg, chan));
        let node = Self {
            router,
            blobs,
            local_pool,
            rpc_task,
        };
        Ok((node, local_addr, key))
    }
}

async fn node_and_client() -> TestResult<(Node, BlobsClient, TempDir)> {
    let testdir = tempfile::tempdir()?;
    let (node, addr, key) = Node::new(testdir.path().join("blobs")).await?;
    let client = quic_rpc::transport::quinn::make_client_endpoint(
        "127.0.0.1:0".parse().unwrap(),
        &[key.as_slice()],
    )?;
    let client = QuinnConnector::<iroh_blobs::rpc::proto::RpcService>::new(
        client,
        addr,
        "localhost".to_string(),
    );
    let client = quic_rpc::RpcClient::<iroh_blobs::rpc::proto::RpcService, _>::new(client);
    let client = iroh_blobs::rpc::client::blobs::Client::new(client);
    Ok((node, client, testdir))
}

#[tokio::test]
async fn quinn_rpc_smoke() -> TestResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_node, client, _testdir) = node_and_client().await?;
    let data = b"hello";
    let hash = client.add_bytes(data.to_vec()).await?.hash;
    assert_eq!(hash, iroh_blobs::Hash::new(data));
    let data2 = client.read_to_bytes(hash).await?;
    assert_eq!(data, &data2[..]);
    Ok(())
}

#[tokio::test]
async fn quinn_rpc_large() -> TestResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let (_node, client, _testdir) = node_and_client().await?;
    let data = vec![0; 1024 * 1024 * 16];
    let hash = client.add_bytes(data.clone()).await?.hash;
    assert_eq!(hash, iroh_blobs::Hash::new(&data));
    let data2 = client.read_to_bytes(hash).await?;
    assert_eq!(data, &data2[..]);
    Ok(())
}
