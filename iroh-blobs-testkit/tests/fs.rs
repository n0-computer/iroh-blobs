//! Runs the conformance cases against the file system store.
use iroh_blobs::store::fs::FsStore;
use iroh_blobs_testkit::{TestStore, Testkit};

async fn fs_store() -> anyhow::Result<TestStore> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    Ok(TestStore::new(store).with_guard(dir))
}

fn main() {
    Testkit::new(fs_store).main()
}
