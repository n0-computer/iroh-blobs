//! Tests for the memory store that run in wasm, under `wasm-bindgen-test`.
#![cfg(wasm_browser)]
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use iroh_blobs::store::mem::MemStore;
use n0_future::{task, time::sleep};
use wasm_bindgen_test::wasm_bindgen_test;

/// Importing a large blob must give the event loop turns while it hashes.
///
/// The ticker only advances when a timer fires, which needs the event loop to
/// get a turn. If the import hashed everything in one go, the ticker would not
/// have advanced by the time the import completes.
#[wasm_bindgen_test]
async fn import_lets_timers_fire() {
    let store = MemStore::new();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = task::spawn({
        let ticks = ticks.clone();
        async move {
            loop {
                sleep(Duration::from_millis(1)).await;
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    let data = vec![7u8; 8 * 1024 * 1024];
    let tag = store.blobs().add_bytes(data).await.unwrap();
    let ticks_at_completion = ticks.load(Ordering::Relaxed);
    drop(ticker);
    assert!(tag.hash != iroh_blobs::Hash::EMPTY);
    assert!(
        ticks_at_completion > 0,
        "no timer fired while the import was hashing"
    );
}
