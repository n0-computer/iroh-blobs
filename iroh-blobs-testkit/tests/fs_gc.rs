//! Runs the conformance cases against the file system store, with garbage
//! collection running every few milliseconds.
//!
//! Every case pins the blobs it uses, so any failure here means that gc
//! deleted something that was pinned.
use std::time::Duration;

use iroh_blobs::store::{fs, GcConfig};
use iroh_blobs_testkit::{TestStore, Testkit};

async fn fs_gc_store() -> anyhow::Result<TestStore> {
    let dir = tempfile::tempdir()?;
    let mut options = fs::options::Options::new(dir.path());
    options.gc = Some(GcConfig {
        interval: Duration::from_millis(5),
        add_protected: None,
    });
    let store = fs::FsStore::load_with_opts(dir.path().join("blobs.db"), options).await?;
    Ok(TestStore::new(store).with_guard(dir))
}

/// Concurrent gc runs clear the protected set at any time.
const GC_CLEARS_PROTECTED: &str = "gc running concurrently clears the protected set";

fn main() {
    Testkit::new(fs_gc_store)
        .skip("delete_blobs", GC_CLEARS_PROTECTED)
        .skip("delete_protected", GC_CLEARS_PROTECTED)
        .main()
}
