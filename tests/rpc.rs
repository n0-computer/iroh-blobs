#![cfg(feature = "test")]
use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use iroh_blobs::{net_protocol::Blobs, util::local_pool::{self, LocalPool}};

use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls, ClientConfig, Endpoint, ServerConfig,
};


/// Returns default server configuration along with its certificate.
#[allow(clippy::field_reassign_with_default)] // https://github.com/rust-lang/rust-clippy/issues/6527
fn configure_server() -> anyhow::Result<(ServerConfig, Vec<u8>)> {
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

    Ok((server_config, cert_der.to_vec()))
}

pub fn make_server_endpoint(bind_addr: SocketAddr) -> anyhow::Result<(Endpoint, Vec<u8>)> {
    let (server_config, server_cert) = configure_server()?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, server_cert))
}

/// An iroh node that just has the blobs transport
#[derive(Debug)]
pub struct Node {
    pub router: iroh::protocol::Router,
    pub blobs: Blobs<iroh_blobs::store::fs::Store>,
    pub _local_pool: LocalPool,
}

impl Node {
    pub async fn new(path: PathBuf) -> anyhow::Result<Self> {
        let store = iroh_blobs::store::fs::Store::load(path).await?;
        let local_pool = LocalPool::default();
        let endpoint = iroh::Endpoint::builder().bind().await?;
        let blobs = Blobs::builder(store).build(local_pool.handle(), &endpoint);
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn()
            .await?;
        let endpoint = quinn::Endpoint::server(config, "0.0.0.0:12345".parse().unwrap())?;
        let rpc_server = quic_rpc::transport::quinn::QuinnListener::new(endpoint)
        Ok(Self {
            router,
            blobs,
            _local_pool: local_pool,
        })
    }
}