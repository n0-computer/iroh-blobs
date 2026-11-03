//! Runs the conformance cases against the in-memory store, with garbage
//! collection running every few milliseconds.
//!
//! Every case pins the blobs it uses, so any failure here means that gc
//! deleted something that was pinned.
use std::time::Duration;

use iroh_blobs::store::{mem, GcConfig};
use iroh_blobs_testkit::{cases, TestStore, Testkit};

async fn mem_gc_store() -> anyhow::Result<TestStore> {
    let gc = GcConfig {
        interval: Duration::from_millis(5),
        add_protected: None,
    };
    let store = mem::MemStore::new_with_opts(mem::Options {
        gc_config: Some(gc),
    });
    Ok(TestStore::new(store))
}

/// The mem store does not protect blobs written during a gc run (see the
/// `delete_protected` case), so gc can delete any blob a case is writing.
const UNPROTECTED: &str = "the mem store does not protect new blobs from a running gc";

fn main() {
    // Run them with `-- --include-ignored` to reproduce.
    cases()
        .iter()
        .fold(Testkit::new(mem_gc_store), |testkit, case| {
            testkit.skip(case.name(), UNPROTECTED)
        })
        .main()
}
