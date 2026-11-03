//! Temp tags: pinning blobs for as long as a client needs them.
use std::collections::BTreeSet;

use iroh_blobs::{api::Store, BlobFormat, Hash, HashAndFormat};
use proptest::prelude::*;

use super::check;
use crate::{harness::Harness, raw};

/// Dropping a temp tag releases it only in process. Over a connection, temp
/// tags are released when their batch ends, which works in both cases.
pub fn temp_tags(h: &Harness) -> anyhow::Result<()> {
    let inputs = proptest::collection::vec(value(), 1..5);
    h.check_prop(inputs, async |store, values| {
        let values = values.into_iter().collect::<BTreeSet<_>>();

        if store.client().as_local().is_some() {
            let mut pins = Vec::new();
            for value in &values {
                pins.push(store.tags().temp_tag(*value).await?);
            }
            check_listed(&store, &values).await?;
            drop(pins);
            check::eventually("in process, dropping a temp tag releases it", async || {
                none_listed(&store, &values).await
            })
            .await?;
        }

        let batch = store.blobs().batch().await?;
        let mut pins = Vec::new();
        for value in &values {
            pins.push(batch.temp_tag(*value).await?);
        }
        check_listed(&store, &values).await?;
        // Ending the batch releases its temp tags, even those still held.
        drop(batch);
        check::eventually("ending a batch releases its temp tags", async || {
            none_listed(&store, &values).await
        })
        .await
    })
}

async fn check_listed(store: &Store, values: &BTreeSet<HashAndFormat>) -> anyhow::Result<()> {
    let listed = raw::list_temp_tags(store).await?;
    let missing = values
        .iter()
        .filter(|value| !listed.contains(value))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        missing.is_empty(),
        "contract: ListTempTags lists every live temp tag; missing {missing:?}"
    );
    Ok(())
}

async fn none_listed(store: &Store, values: &BTreeSet<HashAndFormat>) -> anyhow::Result<bool> {
    let listed = raw::list_temp_tags(store).await?;
    Ok(!listed.iter().any(|value| values.contains(value)))
}

/// A small set of values, so that temp tags for the same value overlap.
/// Temp tags may pin blobs the store does not have.
fn value() -> impl Strategy<Value = HashAndFormat> {
    let hash = (0u8..3).prop_map(|i| Hash::new([i]));
    let format = prop_oneof![Just(BlobFormat::Raw), Just(BlobFormat::HashSeq)];
    (hash, format).prop_map(|(hash, format)| HashAndFormat { hash, format })
}
