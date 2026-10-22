//! Putting a store into the state a case starts from.
use anyhow::Context;
use bao_tree::ChunkRanges;
use iroh_blobs::{
    api::{Store, TempTag},
    Hash,
};

use super::check;
use crate::{raw, reference::Reference};

/// Adds the blob and pins it, so a garbage collector keeps it while the
/// returned temp tag is alive.
pub async fn add(store: &Store, reference: &Reference) -> anyhow::Result<TempTag> {
    let tt = store.add_bytes(reference.data().clone()).temp_tag().await?;
    Ok(tt)
}

/// Pins a hash, e.g. before importing data for it.
pub async fn pin(store: &Store, hash: Hash) -> anyhow::Result<TempTag> {
    Ok(store.tags().temp_tag(hash).await?)
}

/// Imports the bao encoding of `ranges` of the blob and returns the chunks
/// the store must have afterwards.
pub async fn import(
    store: &Store,
    reference: &Reference,
    ranges: &ChunkRanges,
) -> anyhow::Result<ChunkRanges> {
    let items = reference.items(ranges);
    let chunks = check::covered_chunks(&items);
    raw::import_bao(store, reference.hash(), reference.bao_size(), items)
        .await
        .context("contract: importing a valid bao encoding succeeds")?;
    Ok(chunks)
}
