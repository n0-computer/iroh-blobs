//! Observing the bitfield of a blob.
use anyhow::{ensure, Context};
use bao_tree::ChunkRanges;

use super::setup;
use crate::{harness::Harness, inputs, raw};

pub fn observe_complete(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(inputs::non_empty_blob(), async |store, blob| {
        let reference = blob.reference();
        let _tt = setup::add(&store, &reference).await?;
        let bitfield = raw::current_bitfield(&store, reference.hash()).await?;
        ensure!(
            bitfield.is_complete() && bitfield.size() == reference.size(),
            "contract: Observe of a complete blob starts with a complete bitfield with size {}; got {bitfield:?}",
            reference.size()
        );
        Ok(())
    })
}

/// The observer may miss intermediate states, but never sees chunks
/// disappear.
pub fn observe_import(h: &Harness) -> anyhow::Result<()> {
    let inputs = inputs::with_chunk_range_steps(inputs::non_empty_blob());
    h.check_prop(inputs, async |store, (blob, steps)| {
        let reference = blob.reference();
        let _tt = setup::pin(&store, reference.hash()).await?;
        let mut observer = raw::observe(&store, reference.hash()).await?;
        let first = observer.recv().await?.context(
            "contract: an Observe stream starts with the current bitfield, but it was empty",
        )?;
        let mut seen = reference.clip(&first.ranges);
        ensure!(
            seen.is_empty(),
            "contract: Observe of a blob the store does not have starts with an empty bitfield; got {first:?}"
        );

        // The last step completes the blob.
        for ranges in steps.iter().chain([&ChunkRanges::all()]) {
            setup::import(&store, &reference, ranges).await?;
        }

        loop {
            let bitfield = observer.recv().await?.context(
                "contract: an Observe stream stays open until the observer drops it",
            )?;
            let have = reference.clip(&bitfield.ranges);
            ensure!(
                seen.is_subset(&have),
                "contract: observed bitfields only grow; got {have:?} after {seen:?}"
            );
            seen = have;
            if bitfield.is_complete() {
                ensure!(
                    bitfield.size() == reference.size(),
                    "contract: a complete bitfield has the size of the blob ({}); got {}",
                    reference.size(),
                    bitfield.size()
                );
                return Ok(());
            }
        }
    })
}
