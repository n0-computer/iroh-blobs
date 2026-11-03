//! Exporting blobs to files, as bao encoded ranges and as byte ranges.
use anyhow::{bail, ensure};
use bao_tree::io::mixed::EncodedItem;
use iroh_blobs::api::{
    blobs::{ExportMode, ExportOptions},
    proto::ExportRangesItem,
};
use proptest::prelude::*;

use super::{check, setup};
use crate::{harness::Harness, inputs, raw};

pub fn export_path(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(
        (inputs::blob(), export_mode()),
        async |store, (blob, mode)| {
            let reference = blob.reference();
            let _tt = setup::add(&store, &reference).await?;
            let dir = tempfile::tempdir()?;
            let target = dir.path().join("target");
            let options = ExportOptions {
                hash: reference.hash(),
                mode,
                target: target.clone(),
            };
            store.export_with_opts(options).finish().await?;
            let written = std::fs::read(&target)?;
            ensure!(
                written == reference.data()[..],
                "contract: an exported file has the content of the blob"
            );
            check::complete(&store, &reference).await
        },
    )
}

pub fn export_bao(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_chunk_ranges(inputs::blob());
    h.check_prop(inputs, async |store, (blob, ranges)| {
        let reference = blob.reference();
        let _tt = setup::add(&store, &reference).await?;
        let items = raw::export_bao(&store, reference.hash(), ranges.clone()).await?;
        check::export_bao_items(&reference, &ranges, items)
    })
}

pub fn export_ranges(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_byte_ranges(inputs::blob());
    h.check_prop(inputs, async |store, (blob, ranges)| {
        let reference = blob.reference();
        let _tt = setup::add(&store, &reference).await?;
        let items = raw::export_ranges(&store, reference.hash(), ranges.clone()).await?;
        check::export_ranges_items(&reference, &ranges, items)
    })
}

/// A stream that just ends would look like a successful export of nothing,
/// so the store must send an error.
pub fn export_bao_absent(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_chunk_ranges(inputs::non_empty_blob());
    h.check_prop(inputs, async |store, (blob, ranges)| {
        let hash = blob.reference().hash();
        let items = raw::export_bao(&store, hash, ranges).await?;
        if !matches!(items.as_slice(), [EncodedItem::Error(_)]) {
            bail!("contract: ExportBao of a blob the store does not have sends only an error; got {items:?}");
        }
        Ok(())
    })
}

pub fn export_ranges_absent(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_byte_ranges(inputs::non_empty_blob());
    h.check_prop(inputs, async |store, (blob, ranges)| {
        let hash = blob.reference().hash();
        let items = raw::export_ranges(&store, hash, ranges).await?;
        if !matches!(items.as_slice(), [ExportRangesItem::Error(_)]) {
            bail!("contract: ExportRanges of a blob the store does not have sends only an error; got {items:?}");
        }
        Ok(())
    })
}

fn export_mode() -> impl Strategy<Value = ExportMode> {
    prop_oneof![Just(ExportMode::Copy), Just(ExportMode::TryReference)]
}
