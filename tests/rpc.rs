#![cfg(feature = "test")]
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use iroh_blobs::{net_protocol::Blobs, util::local_pool::LocalPool};
use quic_rpc::transport::quinn::QuinnConnector;
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls, ClientConfig, Endpoint, ServerConfig,
};
use rcgen::CertifiedKey;
use tempfile::TempDir;
use testresult::TestResult;
use tokio_util::task::AbortOnDropHandle;

type QC = QuinnConnector<iroh_blobs::rpc::proto::Response, iroh_blobs::rpc::proto::Request>;
type BlobsClient = iroh_blobs::rpc::client::blobs::Client<QC>;

/// Builds default quinn client config and trusts given certificates.
///
/// ## Args
///
/// - server_certs: a list of trusted certificates in DER format.
fn configure_client(server_certs: &[CertifiedKey]) -> anyhow::Result<ClientConfig> {
    let mut certs = rustls::RootCertStore::empty();
    for cert in server_certs {
        let cert = cert.cert.der().clone();
        certs.add(cert)?;
    }

    let crypto_client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("valid versions")
    .with_root_certificates(certs)
    .with_no_client_auth();
    let quic_client_config = QuicClientConfig::try_from(crypto_client_config)?;

    Ok(ClientConfig::new(Arc::new(quic_client_config)))
}

/// Returns default server configuration along with its certificate.
#[allow(clippy::field_reassign_with_default)] // https://github.com/rust-lang/rust-clippy/issues/6527
fn configure_server() -> anyhow::Result<(ServerConfig, CertifiedKey)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = cert.cert.der();
    let priv_key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let cert_chain = vec![cert_der.clone()];

    let crypto_server_config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("valid versions")
    .with_no_client_auth()
    .with_single_cert(cert_chain, priv_key.into())?;
    let quic_server_config = QuicServerConfig::try_from(crypto_server_config)?;
    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));

    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert))
}

pub fn make_server_endpoint(bind_addr: SocketAddr) -> anyhow::Result<(Endpoint, CertifiedKey)> {
    let (server_config, server_cert) = configure_server()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, server_cert))
}

pub fn make_client_endpoint(
    bind_addr: SocketAddr,
    server_certs: &[CertifiedKey],
) -> anyhow::Result<Endpoint> {
    let client_cfg = configure_client(server_certs)?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node {
    pub router: iroh::protocol::Router,
    pub blobs: Blobs<iroh_blobs::store::fs::Store>,
    pub local_pool: LocalPool,
    pub rpc_task: AbortOnDropHandle<()>,
}

impl Node {
    pub async fn new(path: PathBuf) -> anyhow::Result<(Self, SocketAddr, CertifiedKey)> {
        let store = iroh_blobs::store::fs::Store::load(path).await?;
        let local_pool = LocalPool::default();
        let endpoint = iroh::Endpoint::builder().bind().await?;
        let blobs = Blobs::builder(store).build(local_pool.handle(), &endpoint);
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn()
            .await?;
        let (config, key) = configure_server()?;
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
    let client = make_client_endpoint("127.0.0.1:0".parse().unwrap(), &[key])?;
    let client = QuinnConnector::new(client, addr, "localhost".to_string());
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
