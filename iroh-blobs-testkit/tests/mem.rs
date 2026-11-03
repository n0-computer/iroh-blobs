//! Runs the conformance cases against the in-memory store.
use iroh_blobs::store::mem::MemStore;
use iroh_blobs_testkit::{TestStore, Testkit};

async fn mem_store() -> anyhow::Result<TestStore> {
    Ok(TestStore::new(MemStore::new()))
}

fn main() {
    Testkit::new(mem_store)
        .skip(
            "unfinished_import_byte_stream",
            "the mem store logs a failed import instead of sending AddProgressItem::Error",
        )
        .skip(
            "delete_protected",
            "the mem store does not protect new blobs from deletion without force",
        )
        .main()
}
