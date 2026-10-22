//! Importing bao encoded data: complete, partial and in several steps.
use bao_tree::ChunkRanges;

use super::{check, setup};
use crate::{harness::Harness, inputs};

pub fn import_bao_complete(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(inputs::non_empty_blob(), async |store, blob| {
        let reference = blob.reference();
        let _tt = setup::pin(&store, reference.hash()).await?;
        // The second import finds the blob complete already.
        for _ in 0..2 {
            setup::import(&store, &reference, &ChunkRanges::all()).await?;
        }
        check::complete(&store, &reference).await
    })
}

pub fn import_bao_partial(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_chunk_ranges(inputs::non_empty_blob());
    h.check_prop(inputs, async |store, (blob, ranges)| {
        let reference = blob.reference();
        let _tt = setup::pin(&store, reference.hash()).await?;
        let present = setup::import(&store, &reference, &ranges).await?;
        check::has_chunks(&store, &reference, &present).await
    })
}

pub fn import_bao_incremental(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_chunk_range_steps(inputs::non_empty_blob());
    h.check_prop(inputs, async |store, (blob, steps)| {
        let reference = blob.reference();
        let _tt = setup::pin(&store, reference.hash()).await?;
        let mut present = ChunkRanges::empty();
        // The last step completes the blob.
        for ranges in steps.iter().chain([&ChunkRanges::all()]) {
            present |= setup::import(&store, &reference, ranges).await?;
            check::has_chunks(&store, &reference, &present).await?;
        }
        Ok(())
    })
}
