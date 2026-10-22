//! Checks shared by several cases.
//!
//! Every failure message starts with "contract:" and states what the store
//! must do.
use std::{
    fmt,
    time::{Duration, Instant},
};

use anyhow::{bail, ensure};
use bao_tree::{
    blake3,
    io::{mixed::EncodedItem, BaoContentItem},
    ChunkNum, ChunkRanges,
};
use iroh_blobs::{
    api::{blobs::BlobStatus, proto::ExportRangesItem, Store},
    Hash,
};
use range_collections::RangeSet2;

use crate::{
    raw,
    reference::{chunks_to_bytes, Reference},
};

/// Checks that the blob in `reference` was added under its hash, is complete
/// and reads back unchanged.
pub async fn added(store: &Store, hash: Hash, reference: &Reference) -> anyhow::Result<()> {
    ensure!(
        hash == reference.hash(),
        "contract: a blob is added under the BLAKE3 hash of its content; got {hash}, expected {}",
        reference.hash()
    );
    complete(store, reference).await
}

/// Checks that the blob in `reference` is complete and reads back unchanged.
pub async fn complete(store: &Store, reference: &Reference) -> anyhow::Result<()> {
    let hash = reference.hash();
    let size = reference.size();
    let status = store.status(hash).await?;
    ensure!(
        status == BlobStatus::Complete { size },
        "contract: a complete blob has status Complete {{ size: {size} }}; got {status:?}"
    );
    let bitfield = raw::current_bitfield(store, hash).await?;
    ensure!(
        bitfield.is_complete() && bitfield.size() == size,
        "contract: a complete blob has a complete bitfield with size {size}; got {bitfield:?}"
    );
    let data = store.get_bytes(hash).await?;
    ensure!(
        data == reference.data(),
        "contract: a complete blob reads back unchanged; got {} bytes that differ",
        data.len()
    );
    Ok(())
}

/// Checks that the store has exactly the chunks `present` of the blob in
/// `reference`, and that they read back unchanged.
pub async fn has_chunks(
    store: &Store,
    reference: &Reference,
    present: &ChunkRanges,
) -> anyhow::Result<()> {
    let hash = reference.hash();
    let size = reference.size();
    if *present == reference.all_chunks() {
        return complete(store, reference).await;
    }

    let bitfield = raw::current_bitfield(store, hash).await?;
    let have = reference.clip(&bitfield.ranges);
    ensure!(
        have == *present,
        "contract: the bitfield has exactly the chunks that were imported; got {have:?}, expected {present:?}"
    );
    let validated = present.contains(&last_chunk(size));
    if validated {
        ensure!(
            bitfield.size() == size,
            "contract: once the last chunk is present, the bitfield has the size of the blob ({size}); got {}",
            bitfield.size()
        );
    }

    let status = store.status(hash).await?;
    match status {
        BlobStatus::Partial { size: None } if !validated => {}
        BlobStatus::Partial { size: Some(s) } if s == size => {}
        _ => bail!(
            "contract: a partial blob has status Partial with its size{}; got {status:?}",
            if validated {
                format!(" ({size}), since the last chunk is present")
            } else {
                format!(" ({size}) or no size")
            }
        ),
    }

    let ranges = chunks_to_bytes(present, size);
    let items = raw::export_ranges(store, hash, ranges.clone()).await?;
    export_ranges_items(reference, &ranges, items)
}

/// Checks the items of an `ExportBao` response for a blob the store has
/// completely: the size, then exactly the encoding bao-tree produces.
pub fn export_bao_items(
    reference: &Reference,
    ranges: &ChunkRanges,
    items: Vec<EncodedItem>,
) -> anyhow::Result<()> {
    let mut items = items.into_iter();
    match items.next() {
        Some(EncodedItem::Size(size)) => ensure!(
            size == reference.size(),
            "contract: ExportBao starts with the size of the blob; got {size}, expected {}",
            reference.size()
        ),
        other => bail!("contract: ExportBao starts with the size of the blob; got {other:?}"),
    }
    let mut content = Vec::new();
    let mut end = None;
    for item in items.by_ref() {
        match item {
            EncodedItem::Parent(parent) => content.push(BaoContentItem::Parent(parent)),
            EncodedItem::Leaf(leaf) => content.push(BaoContentItem::Leaf(leaf)),
            item => {
                end = Some(item);
                break;
            }
        }
    }
    let content = Encoded::normalize(content);
    let expected = Encoded::normalize(reference.items(ranges));
    if let Some(i) =
        (0..content.len().max(expected.len())).find(|&i| content.get(i) != expected.get(i))
    {
        bail!(
            "contract: ExportBao sends the bao encoding of the requested ranges; part {i} is {:?}, expected {:?}",
            content.get(i),
            expected.get(i)
        );
    }
    // Done is allowed but not required: consumers infer the end of the
    // stream from the size and the ranges.
    match end {
        None | Some(EncodedItem::Done) => {}
        Some(item) => bail!("contract: ExportBao sends nothing after the encoding; got {item:?}"),
    }
    ensure!(
        items.next().is_none(),
        "contract: ExportBao sends nothing after Done"
    );
    Ok(())
}

/// Checks the items of an `ExportRanges` response for byte `ranges` the store
/// has: the size, then the requested bytes in ascending order.
pub fn export_ranges_items(
    reference: &Reference,
    ranges: &RangeSet2<u64>,
    items: Vec<ExportRangesItem>,
) -> anyhow::Result<()> {
    let data = reference.data();
    let mut items = items.into_iter();
    match items.next() {
        Some(ExportRangesItem::Size(size)) => ensure!(
            size == reference.size(),
            "contract: ExportRanges starts with the size of the blob; got {size}, expected {}",
            reference.size()
        ),
        other => bail!("contract: ExportRanges starts with the size of the blob; got {other:?}"),
    }
    let mut sent = RangeSet2::empty();
    let mut end = 0;
    for item in items {
        let leaf = match item {
            ExportRangesItem::Data(leaf) => leaf,
            item => bail!("contract: ExportRanges sends only data after the size; got {item:?}"),
        };
        let start = leaf.offset;
        ensure!(
            start >= end,
            "contract: ExportRanges sends data in ascending order without overlap; got offset {start} after data up to {end}"
        );
        end = start + leaf.data.len() as u64;
        ensure!(
            end <= reference.size() && leaf.data == data[start as usize..end as usize],
            "contract: ExportRanges sends the content of the blob; data at {start}..{end} differs"
        );
        sent |= RangeSet2::from(start..end);
    }
    let expected = reference.clip_bytes(ranges);
    ensure!(
        sent == expected,
        "contract: ExportRanges sends exactly the requested bytes within the blob; got {sent:?}, expected {expected:?}"
    );
    Ok(())
}

/// The chunks a store has after importing `items`.
pub fn covered_chunks(items: &[BaoContentItem]) -> ChunkRanges {
    items
        .iter()
        .filter_map(|item| match item {
            BaoContentItem::Leaf(leaf) if !leaf.data.is_empty() => {
                let start = ChunkNum::full_chunks(leaf.offset);
                let end = ChunkNum::chunks(leaf.offset + leaf.data.len() as u64);
                Some(ChunkRanges::from(start..end))
            }
            _ => None,
        })
        .fold(ChunkRanges::empty(), |acc, range| acc | range)
}

fn last_chunk(size: u64) -> ChunkNum {
    ChunkNum::chunks(size) - 1
}

/// A bao encoding with adjacent leaves merged.
///
/// How a store splits data into leaves is up to it: the encoding on the wire
/// is the same.
#[derive(PartialEq)]
enum Encoded {
    /// A parent hash pair. The node of a parent is not part of the contract:
    /// it is not part of the encoding on the wire, and bao-tree itself does
    /// not set it for parents within a chunk group.
    Parent(blake3::Hash, blake3::Hash),
    Data {
        offset: u64,
        data: Vec<u8>,
    },
}

impl Encoded {
    fn normalize(items: Vec<BaoContentItem>) -> Vec<Self> {
        let mut res = Vec::<Self>::new();
        for item in items {
            match item {
                BaoContentItem::Parent(parent) => {
                    res.push(Self::Parent(parent.pair.0, parent.pair.1))
                }
                BaoContentItem::Leaf(leaf) => match res.last_mut() {
                    Some(Self::Data { offset, data })
                        if *offset + data.len() as u64 == leaf.offset =>
                    {
                        data.extend_from_slice(&leaf.data)
                    }
                    _ => res.push(Self::Data {
                        offset: leaf.offset,
                        data: leaf.data.to_vec(),
                    }),
                },
            }
        }
        res
    }
}

impl fmt::Debug for Encoded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parent(left, right) => write!(f, "Parent({left}, {right})"),
            Self::Data { offset, data } => {
                // A short content hash, so that data that differs looks different.
                let hash = blake3::hash(data).to_hex();
                write!(
                    f,
                    "Data({offset}..{}, {})",
                    offset + data.len() as u64,
                    &hash[..8]
                )
            }
        }
    }
}

/// How long [`eventually`] waits for a condition.
const EVENTUALLY: Duration = Duration::from_secs(5);

/// Waits until `condition` holds, for things a store does in the
/// background, such as releasing temp tags. Fails with `contract` if it
/// does not hold within a few seconds.
pub async fn eventually(
    contract: &str,
    mut condition: impl AsyncFnMut() -> anyhow::Result<bool>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + EVENTUALLY;
    while !condition().await? {
        ensure!(
            Instant::now() < deadline,
            "contract: {contract}, but it did not happen within {EVENTUALLY:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}
