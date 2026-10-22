//! Requests whose client goes away before they are finished.
//!
//! This is normal use: an irpc connection can drop at any time, which closes
//! all channels of the requests on it. The store must keep working, and must
//! never claim data it did not get.
use anyhow::{bail, ensure};
use bao_tree::ChunkRanges;
use bytes::Bytes;
use iroh_blobs::{
    api::{
        blobs::BlobStatus,
        proto::{
            AddProgressItem, ExportBaoRequest, ExportRangesRequest, ImportBaoRequest,
            ImportByteStreamRequest, ImportByteStreamUpdate, ImportBytesRequest, ListRequest,
            ObserveRequest, Scope,
        },
        Store,
    },
    BlobFormat, Hash,
};
use irpc::channel::mpsc;
use proptest::{prelude::*, sample::Index};
use range_collections::RangeSet2;

use super::{check, setup};
use crate::{harness::Harness, inputs, raw, reference::chunks_to_bytes};

/// Channel capacity for requests in this module. Small, so the store blocks
/// on sending when the client goes away.
const CAP: usize = 1;

/// Each request reads `read` items, then drops the receiver.
pub fn dropped_exports(h: &Harness) -> anyhow::Result<()> {
    let inputs = (inputs::non_empty_blob(), 0usize..3);
    h.check_prop(inputs, async |store, (blob, read)| {
        let reference = blob.reference();
        let hash = reference.hash();
        let _tt = setup::add(&store, &reference).await?;
        let client = store.client();

        let ranges = ChunkRanges::all();
        let export_bao = client.server_streaming(ExportBaoRequest { hash, ranges }, CAP);
        read_and_drop(export_bao.await?, read).await?;
        let ranges = RangeSet2::all();
        let export_ranges = client.server_streaming(ExportRangesRequest { hash, ranges }, CAP);
        read_and_drop(export_ranges.await?, read).await?;
        let list = client.server_streaming(ListRequest, CAP);
        read_and_drop(list.await?, read).await?;
        // An observer of a complete blob gets one item, then waits for
        // changes that never come.
        let observe = client.server_streaming(ObserveRequest { hash }, CAP);
        read_and_drop(observe.await?, read.min(1)).await?;

        check::complete(&store, &reference).await
    })
}

/// The import is cut off after a random number of items.
pub fn dropped_import_bao(h: &Harness) -> anyhow::Result<()> {
    let inputs = (inputs::non_empty_blob(), any::<Index>());
    h.check_prop(inputs, async |store, (blob, cut)| {
        let reference = blob.reference();
        let hash = reference.hash();
        let _tt = setup::pin(&store, hash).await?;
        let mut items = reference.items(&ChunkRanges::all());
        items.truncate(cut.index(items.len()));
        let sent = check::covered_chunks(&items);

        let request = ImportBaoRequest {
            hash,
            size: reference.bao_size(),
        };
        let (tx, rx) = store.client().client_streaming(request, CAP).await?;
        for item in items {
            if tx.send(item).await.is_err() {
                break;
            }
        }
        drop((tx, rx));

        let bitfield = raw::current_bitfield(&store, hash).await?;
        let claimed = reference.clip(&bitfield.ranges);
        ensure!(
            claimed.is_subset(&sent),
            "contract: a store only claims chunks it got; claims {claimed:?}, but got only {sent:?}"
        );
        if !claimed.is_empty() {
            let ranges = chunks_to_bytes(&claimed, reference.size());
            let items = raw::export_ranges(&store, hash, ranges.clone()).await?;
            check::export_ranges_items(&reference, &ranges, items)?;
        }

        setup::import(&store, &reference, &ChunkRanges::all()).await?;
        check::complete(&store, &reference).await
    })
}

/// The connection drops while a byte stream import is in progress.
pub fn dropped_import_byte_stream(h: &Harness) -> anyhow::Result<()> {
    let inputs = (
        inputs::non_empty_blob(),
        inputs::piece_sizes(),
        any::<Index>(),
    );
    h.check_prop(inputs, async |store, (blob, sizes, cut)| {
        let reference = blob.reference();
        let pieces = cut_pieces(reference.data(), &sizes, cut);
        let prefix = pieces.concat();

        let (tx, rx) = start_byte_stream(&store).await?;
        drop(rx);
        send_pieces(tx, pieces).await;

        check_not_added(&store, &prefix).await?;
        let tt = setup::add(&store, &reference).await?;
        check::added(&store, tt.hash(), &reference).await
    })
}

/// The client stops sending without `Done`, but still listens for the result.
pub fn unfinished_import_byte_stream(h: &Harness) -> anyhow::Result<()> {
    let inputs = (
        inputs::non_empty_blob(),
        inputs::piece_sizes(),
        any::<Index>(),
    );
    h.check_prop(inputs, async |store, (blob, sizes, cut)| {
        let reference = blob.reference();
        let pieces = cut_pieces(reference.data(), &sizes, cut);
        let prefix = pieces.concat();

        let (tx, mut rx) = start_byte_stream(&store).await?;
        // Receive while sending, since the store may block on progress updates.
        let receive = async {
            let mut items = Vec::new();
            while let Some(item) = rx.recv().await? {
                items.push(item);
            }
            anyhow::Ok(items)
        };
        let ((), items) = tokio::join!(send_pieces(tx, pieces), receive);
        let items = items?;
        if !matches!(items.last(), Some(AddProgressItem::Error(_))) {
            bail!("contract: an ImportByteStream that ends without Done ends with an error; got {items:?}");
        }

        check_not_added(&store, &prefix).await
    })
}

pub fn dropped_import_bytes(h: &Harness) -> anyhow::Result<()> {
    h.check_prop((inputs::blob(), 0usize..3), async |store, (blob, read)| {
        let reference = blob.reference();
        let request = ImportBytesRequest {
            data: reference.data().clone(),
            format: BlobFormat::Raw,
            scope: Scope::default(),
        };
        read_and_drop(store.client().server_streaming(request, CAP).await?, read).await?;

        let tt = setup::add(&store, &reference).await?;
        check::added(&store, tt.hash(), &reference).await
    })
}

/// Reads up to `n` items, then drops the receiver.
async fn read_and_drop<T: irpc::RpcMessage>(
    mut rx: mpsc::Receiver<T>,
    n: usize,
) -> anyhow::Result<()> {
    for _ in 0..n {
        if rx.recv().await?.is_none() {
            break;
        }
    }
    Ok(())
}

/// Splits `data` into pieces and keeps only those before `cut`, so the
/// stream is always incomplete.
fn cut_pieces(data: &Bytes, sizes: &[usize], cut: Index) -> Vec<Bytes> {
    let mut pieces = inputs::split(data, sizes);
    pieces.truncate(cut.index(pieces.len()));
    pieces
}

async fn start_byte_stream(
    store: &Store,
) -> anyhow::Result<(
    mpsc::Sender<ImportByteStreamUpdate>,
    mpsc::Receiver<AddProgressItem>,
)> {
    let request = ImportByteStreamRequest {
        format: BlobFormat::Raw,
        scope: Scope::default(),
    };
    Ok(store.client().bidi_streaming(request, CAP, CAP).await?)
}

/// Sends `pieces` without `Done`, then drops the sender.
async fn send_pieces(tx: mpsc::Sender<ImportByteStreamUpdate>, pieces: Vec<Bytes>) {
    for piece in pieces {
        if tx.send(ImportByteStreamUpdate::Bytes(piece)).await.is_err() {
            break;
        }
    }
}

/// Checks that the data of an unfinished byte stream import was not added.
async fn check_not_added(store: &Store, prefix: &[u8]) -> anyhow::Result<()> {
    // The store always has the empty blob.
    if prefix.is_empty() {
        return Ok(());
    }
    let status = store.status(Hash::new(prefix)).await?;
    ensure!(
        status == BlobStatus::NotFound,
        "contract: an ImportByteStream that ends without Done does not add a blob; the {} bytes sent have status {status:?}",
        prefix.len()
    );
    Ok(())
}
