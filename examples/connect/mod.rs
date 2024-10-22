//! Common code used to created quinn connections in the examples
use std::{path::PathBuf, sync::Arc};

use anyhow::{bail, Context, Result};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use tokio::fs;

pub const EXAMPLE_ALPN: &[u8] = b"n0/iroh/examples/bytes/0";

// Path where the tls certificates are saved. This example expects that you have run the `provide-bytes` example first, which generates the certificates.
pub const CERT_PATH: &str = "./certs";

// derived from `quinn/examples/client.rs`
// load the certificates from CERT_PATH
// Assumes that you have already run the `provide-bytes` example, that generates the certificates
#[allow(unused)]
pub async fn load_certs() -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    let path = PathBuf::from(CERT_PATH).join("cert.der");
    match fs::read(path).await {
        Ok(cert) => {
            roots.add(rustls::pki_types::CertificateDer::from(cert))?;
        }
        Err(e) => {
            bail!("failed to open local server certificate: {}\nYou must run the `provide-bytes` example to create the certificate.\n\tcargo run --example provide-bytes", e);
        }
    }
    Ok(roots)
}

// derived from `quinn/examples/server.rs`
// creates a self signed certificate and saves it to "./certs"
#[allow(unused)]
pub async fn make_and_write_certs() -> Result<(
    rustls::pki_types::PrivateKeyDer<'static>,
    rustls::pki_types::CertificateDer<'static>,
)> {
    let path = std::path::PathBuf::from(CERT_PATH);
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key_path = path.join("key.der");
    let cert_path = path.join("cert.der");

    let key = cert.serialize_private_key_der();
    let cert = cert.serialize_der().unwrap();
    tokio::fs::create_dir_all(path)
        .await
        .context("failed to create certificate directory")?;
    tokio::fs::write(cert_path, &cert)
        .await
        .context("failed to write certificate")?;
    tokio::fs::write(key_path, &key)
        .await
        .context("failed to write private key")?;

    Ok((
        rustls::pki_types::PrivateKeyDer::try_from(key).unwrap(),
        rustls::pki_types::CertificateDer::from(cert),
    ))
}

// derived from `quinn/examples/client.rs`
// Creates a client quinn::Endpoint
#[allow(unused)]
pub fn make_client_endpoint(roots: rustls::RootCertStore) -> Result<quinn::Endpoint> {
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    client_crypto.alpn_protocols = vec![EXAMPLE_ALPN.to_vec()];
    let client_config: QuicClientConfig = client_crypto.try_into()?;
    let client_config = quinn::ClientConfig::new(Arc::new(client_config));
    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

// derived from `quinn/examples/server.rs`
// makes a quinn server endpoint
#[allow(unused)]
pub fn make_server_endpoint(
    key: rustls::pki_types::PrivateKeyDer<'static>,
    cert: rustls::pki_types::CertificateDer<'static>,
) -> Result<quinn::Endpoint> {
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    server_crypto.alpn_protocols = vec![EXAMPLE_ALPN.to_vec()];
    let server_config: QuicServerConfig = server_crypto.try_into()?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(server_config));
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    let endpoint = quinn::Endpoint::server(server_config, "[::1]:4433".parse()?)?;
    Ok(endpoint)
}
