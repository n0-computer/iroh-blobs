//! Network setup for tests that connect endpoints.
use std::sync::{mpsc, OnceLock};

use iroh::{endpoint::presets, tls::CaTlsConfig, Endpoint, RelayMap, RelayMode};
use testresult::TestResult;

/// Returns the relay map of a local relay server shared by all tests in this process.
///
/// The server runs on its own thread and runtime, so it outlives the runtime of any
/// single test.
fn test_relay_map() -> RelayMap {
    static RELAY_MAP: OnceLock<RelayMap> = OnceLock::new();
    RELAY_MAP
        .get_or_init(|| {
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("relay runtime");
                rt.block_on(async move {
                    let (relay_map, _url, _server) = iroh::test_utils::run_relay_server()
                        .await
                        .expect("relay server");
                    tx.send(relay_map).ok();
                    std::future::pending::<()>().await;
                })
            });
            rx.recv().expect("relay server started")
        })
        .clone()
}

/// Binds an endpoint that uses the local test relay and direct addresses on loopback.
///
/// Returns once the endpoint is connected to the relay, so its address is complete.
/// Tests using it need neither outside network access nor, on macOS, the local
/// network permission.
pub(crate) async fn test_endpoint() -> TestResult<Endpoint> {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Custom(test_relay_map()))
        .ca_tls_config(CaTlsConfig::insecure_skip_verify())
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), endpoint.online()).await?;
    Ok(endpoint)
}
