//! Runs the conformance cases against the file system store, over an rpc
//! connection.
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Deref,
};

use iroh_blobs::{api::Store, store::fs::FsStore};
use iroh_blobs_testkit::{TestStore, Testkit};

async fn fs_rpc_store() -> anyhow::Result<TestStore> {
    let localhost = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let (server, cert) = irpc::util::make_server_endpoint(localhost)?;
    let client = irpc::util::make_client_endpoint(localhost, &[cert.as_ref()])?;
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    let server_task = tokio::spawn(store.deref().clone().listen(server.clone()));
    let remote = Store::connect(client, server.local_addr()?);
    let guard = (dir, AbortOnDrop(server_task));
    Ok(TestStore::new(remote).with_guard(guard))
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// bao-tree 0.16.1 serializes `Parent` with a wrong sequence length, so bao
/// encodings of blobs larger than one chunk group can't cross an rpc
/// connection. Fixed in n0-computer/bao-tree#103, not released yet.
const PARENT_SERDE: &str = "bao-tree 0.16.1 misframes Parent over rpc (n0-computer/bao-tree#103)";

/// Cases that send bao encodings with parents over the connection.
const NEEDS_PARENT_SERDE: &[&str] = &[
    "add_bytes",
    "add_stream",
    "add_path",
    "export_path",
    "export_bao",
    "import_bao_complete",
    "import_bao_partial",
    "import_bao_incremental",
    "observe_import",
    "sync_and_wait_idle",
    "dropped_exports",
    "dropped_import_bao",
    "dropped_import_byte_stream",
    "dropped_import_bytes",
    "delete_blobs",
    "delete_protected",
];

fn main() {
    NEEDS_PARENT_SERDE
        .iter()
        .fold(Testkit::new(fs_rpc_store), |testkit, case| {
            testkit.skip(case, PARENT_SERDE)
        })
        .main()
}
