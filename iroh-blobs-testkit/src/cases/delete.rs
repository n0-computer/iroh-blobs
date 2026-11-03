//! Deleting blobs, and the protection of new data against deletion.
//!
//! A garbage collector first sends `ClearProtected`, then finds the blobs to
//! keep, then deletes the rest without `force`. Blobs written in the
//! meantime must survive, so the store protects every blob written since the
//! last `ClearProtected` from deletion without `force`.
use anyhow::{bail, ensure};
use bao_tree::{io::mixed::EncodedItem, ChunkRanges};
use iroh_blobs::api::blobs::BlobStatus;
use proptest::prelude::*;

use super::{check, setup};
use crate::{harness::Harness, inputs, raw, reference::Reference};

pub fn delete_blobs(h: &Harness) -> anyhow::Result<()> {
    let inputs = (
        proptest::collection::vec(inputs::non_empty_blob(), 1..4),
        any::<bool>(),
    );
    h.check_prop(inputs, async |store, (blobs, force)| {
        let references = blobs
            .iter()
            .map(|blob| blob.reference())
            .collect::<Vec<_>>();
        for reference in &references {
            // Temp tags protect from garbage collection, not from deletion.
            drop(setup::add(&store, reference).await?);
        }
        if !force {
            raw::clear_protected(&store).await?;
        }
        let hashes = references.iter().map(|r| r.hash()).collect();
        raw::delete_blobs(&store, hashes, force).await?;

        for reference in &references {
            check_deleted(&store, reference).await?;
        }
        for reference in &references {
            let tt = setup::add(&store, reference).await?;
            check::added(&store, tt.hash(), reference)
                .await
                .map_err(|cause| cause.context("after deleting a blob, adding it again works"))?;
        }
        Ok(())
    })
}

/// The blob is written after `ClearProtected`, by adding it or by a bao
/// import.
pub fn delete_protected(h: &Harness) -> anyhow::Result<()> {
    let inputs = (inputs::non_empty_blob(), any::<bool>());
    h.check_prop(inputs, async |store, (blob, via_bao)| {
        let reference = blob.reference();
        raw::clear_protected(&store).await?;
        let _tt = if via_bao {
            let tt = setup::pin(&store, reference.hash()).await?;
            setup::import(&store, &reference, &ChunkRanges::all()).await?;
            tt
        } else {
            setup::add(&store, &reference).await?
        };

        raw::delete_blobs(&store, vec![reference.hash()], false).await?;
        check::complete(&store, &reference).await.map_err(|cause| {
            cause.context("contract: DeleteBlobs without force keeps blobs written since the last ClearProtected")
        })?;

        raw::delete_blobs(&store, vec![reference.hash()], true).await?;
        check_deleted(&store, &reference).await
    })
}

async fn check_deleted(
    store: &iroh_blobs::api::Store,
    reference: &Reference,
) -> anyhow::Result<()> {
    let status = store.status(reference.hash()).await?;
    ensure!(
        status == BlobStatus::NotFound,
        "contract: a deleted blob has status NotFound; got {status:?}"
    );
    let items = raw::export_bao(store, reference.hash(), ChunkRanges::all()).await?;
    if !matches!(items.as_slice(), [EncodedItem::Error(_)]) {
        bail!("contract: ExportBao of a deleted blob sends only an error; got {items:?}");
    }
    Ok(())
}
