//! Listing blobs.
use std::collections::BTreeSet;

use anyhow::ensure;
use iroh_blobs::Hash;

use super::setup;
use crate::{harness::Harness, inputs};

pub fn list_blobs(h: &Harness) -> anyhow::Result<()> {
    let inputs = proptest::collection::vec(inputs::blob(), 0..6);
    h.check_prop(inputs, async |store, blobs| {
        let mut added = BTreeSet::new();
        let mut pins = Vec::new();
        for blob in &blobs {
            let reference = blob.reference();
            pins.push(setup::add(&store, &reference).await?);
            added.insert(reference.hash());
        }

        let listed = store.list().hashes().await?;
        let unique = listed.iter().copied().collect::<BTreeSet<_>>();
        ensure!(
            unique.len() == listed.len(),
            "contract: ListBlobs returns each blob once; got {listed:?}"
        );
        // Every store has the empty blob, so listing it is optional.
        let non_empty = |set: &BTreeSet<Hash>| set - &BTreeSet::from([Hash::EMPTY]);
        ensure!(
            non_empty(&unique) == non_empty(&added),
            "contract: ListBlobs returns exactly the complete blobs; got {unique:?}, expected {added:?}"
        );
        Ok(())
    })
}
