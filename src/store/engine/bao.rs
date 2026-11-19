//! Bao import and export over synchronous access to an entry's data.
//!
//! Any entity implementation can use these: it implements [`SyncEntityApi`]
//! for the duration of a request, and gets validated bao import, range export
//! and bao export.
use std::{io, num::NonZeroU64};

use bao_tree::{
    blake3,
    io::{
        mixed::{traverse_ranges_validated, EncodedItem, ReadBytesAt},
        outboard::PreOrderOutboard,
        sync::ReadAt,
        BaoContentItem, Leaf,
    },
    BaoTree, ChunkNum, ChunkRanges,
};
use irpc::channel::mpsc;
use range_collections::range_set::RangeSetRange;
use tracing::trace;

use crate::{
    api::{
        self,
        blobs::Bitfield,
        proto::{bitfield::is_validated, ExportBaoRequest, ExportRangesItem, ExportRangesRequest},
    },
    protocol::ChunkRangesExt,
    store::{util::BaoTreeSender, IROH_BLOCK_SIZE},
    Hash,
};

/// Synchronous access to one entry's data, which the bao import and export code is generic over.
///
/// Not tied to files: the fs entity implements it on its files, a memory
/// backed entity on its buffers.
pub(crate) trait SyncEntityApi {
    /// The hash of the entry.
    fn id(&self) -> Hash;

    /// Load the entry state from the database. This must make sure that it is
    /// not run concurrently, so if load is called multiple times, all but one
    /// must wait. You can use a tokio::sync::OnceCell or similar to achieve this.
    async fn load(&self);

    /// Get a synchronous reader for the data file.
    fn data_reader(&self) -> impl ReadBytesAt;

    /// Get a synchronous reader for the outboard file.
    fn outboard_reader(&self) -> impl ReadAt;

    /// Get the best known size of the data file.
    fn current_size(&self) -> io::Result<u64>;

    /// Get the bitfield of the entry.
    fn bitfield(&self) -> io::Result<Bitfield>;

    /// Write a batch of content items to the entry.
    async fn write_batch(&self, batch: &[BaoContentItem], bitfield: &Bitfield) -> io::Result<()>;

    /// The outboard for the entry.
    fn outboard(&self) -> io::Result<PreOrderOutboard<impl ReadAt>> {
        let tree = BaoTree::new(self.current_size()?, IROH_BLOCK_SIZE);
        let outboard = self.outboard_reader();
        Ok(PreOrderOutboard {
            root: blake3::Hash::from(self.id()),
            tree,
            data: outboard,
        })
    }
}

fn chunk_range(leaf: &Leaf) -> ChunkRanges {
    let start = ChunkNum::chunks(leaf.offset);
    let end = ChunkNum::chunks(leaf.offset + leaf.data.len() as u64);
    (start..end).into()
}

pub(crate) async fn import_bao_impl(
    ctx: &impl SyncEntityApi,
    size: NonZeroU64,
    rx: &mut mpsc::Receiver<BaoContentItem>,
) -> api::Result<()> {
    trace!("importing bao: {} {} bytes", ctx.id().fmt_short(), size);
    let mut batch = Vec::<BaoContentItem>::new();
    let mut ranges = ChunkRanges::empty();
    while let Some(item) = rx.recv().await? {
        // if the batch is not empty, the last item is a leaf and the current item is a parent, write the batch
        if !batch.is_empty() && batch[batch.len() - 1].is_leaf() && item.is_parent() {
            let bitfield = Bitfield::new_unchecked(ranges, size.into());
            ctx.write_batch(&batch, &bitfield).await?;
            batch.clear();
            ranges = ChunkRanges::empty();
        }
        if let BaoContentItem::Leaf(leaf) = &item {
            let leaf_range = chunk_range(leaf);
            if is_validated(size, &leaf_range) && size.get() != leaf.offset + leaf.data.len() as u64
            {
                return Err(api::Error::io(io::ErrorKind::InvalidData, "invalid size"));
            }
            ranges |= leaf_range;
        }
        batch.push(item);
    }
    if !batch.is_empty() {
        let bitfield = Bitfield::new_unchecked(ranges, size.into());
        ctx.write_batch(&batch, &bitfield).await?;
    }
    Ok(())
}

pub(crate) async fn export_ranges_impl(
    ctx: &impl SyncEntityApi,
    cmd: ExportRangesRequest,
    tx: &mut mpsc::Sender<ExportRangesItem>,
) -> io::Result<()> {
    let ExportRangesRequest { ranges, hash } = cmd;
    trace!(
        "exporting ranges: {hash} {ranges:?} size={}",
        ctx.current_size()?
    );
    let bitfield = ctx.bitfield()?;
    let data = ctx.data_reader();
    let size = bitfield.size();
    tx.send(ExportRangesItem::Size(size)).await?;
    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::Range(range) => size.min(*range.start)..size.min(*range.end),
            RangeSetRange::RangeFrom(range) => size.min(*range.start)..size,
        };
        let requested = ChunkRanges::bytes(range.start..range.end);
        if !bitfield.ranges.is_superset(&requested) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing range: {requested:?}, present: {bitfield:?}"),
            ));
        }
        let bs = 1024;
        let mut offset = range.start;
        loop {
            let end: u64 = (offset + bs).min(range.end);
            let size = (end - offset) as usize;
            let res = data.read_bytes_at(offset, size);
            tx.send(ExportRangesItem::Data(Leaf { offset, data: res? }))
                .await?;
            offset = end;
            if offset >= range.end {
                break;
            }
        }
    }
    Ok(())
}

pub(crate) async fn export_bao_impl(
    ctx: &impl SyncEntityApi,
    cmd: ExportBaoRequest,
    tx: &mut mpsc::Sender<EncodedItem>,
) -> io::Result<()> {
    let ExportBaoRequest { ranges, hash, .. } = cmd;
    let outboard = ctx.outboard()?;
    let size = outboard.tree.size();
    if size == 0 {
        // we have no data whatsoever, so we stop here
        return Ok(());
    }
    trace!("exporting bao: {hash} {ranges:?} size={size}",);
    let data = ctx.data_reader();
    let tx = BaoTreeSender::new(tx);
    traverse_ranges_validated(data, outboard, &ranges, tx).await?;
    Ok(())
}
