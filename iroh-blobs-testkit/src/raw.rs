//! Raw protocol requests.
//!
//! The client API interprets what the store sends: it stops at the first
//! error, infers the end of streams and special cases the empty blob. The
//! helpers here return exactly what the store sent, so cases can check it.
use std::num::NonZeroU64;

use anyhow::Context;
use bao_tree::{
    io::{mixed::EncodedItem, BaoContentItem},
    ChunkRanges,
};
use iroh_blobs::{
    api::{
        proto::{
            Bitfield, BlobDeleteRequest, ClearProtectedRequest, ExportBaoRequest, ExportRangesItem,
            ExportRangesRequest, ImportBaoRequest, ListTempTagsRequest, ObserveRequest,
        },
        Store,
    },
    Hash, HashAndFormat,
};
use irpc::channel::mpsc;
use range_collections::RangeSet2;

/// Buffer size for local channels.
const CAP: usize = 32;

/// Sends an `ExportBao` request and returns all items until the store ends
/// the stream.
pub async fn export_bao(
    store: &Store,
    hash: Hash,
    ranges: ChunkRanges,
) -> anyhow::Result<Vec<EncodedItem>> {
    let rx = store
        .client()
        .server_streaming(ExportBaoRequest { hash, ranges }, CAP)
        .await
        .context("ExportBao request failed")?;
    collect(rx).await
}

/// Sends an `ExportRanges` request and returns all items until the store
/// ends the stream.
pub async fn export_ranges(
    store: &Store,
    hash: Hash,
    ranges: RangeSet2<u64>,
) -> anyhow::Result<Vec<ExportRangesItem>> {
    let rx = store
        .client()
        .server_streaming(ExportRangesRequest { hash, ranges }, CAP)
        .await
        .context("ExportRanges request failed")?;
    collect(rx).await
}

/// Sends an `Observe` request and returns the stream of bitfields.
pub async fn observe(store: &Store, hash: Hash) -> anyhow::Result<mpsc::Receiver<Bitfield>> {
    store
        .client()
        .server_streaming(ObserveRequest { hash }, CAP)
        .await
        .context("Observe request failed")
}

/// Returns the first bitfield of an `Observe` stream: the current state.
pub async fn current_bitfield(store: &Store, hash: Hash) -> anyhow::Result<Bitfield> {
    observe(store, hash)
        .await?
        .recv()
        .await
        .context("Observe stream failed")?
        .context("contract: an Observe stream starts with the current bitfield, but it was empty")
}

/// Sends an `ImportBao` request with `items` and returns the store's answer.
///
/// Stops sending early if the store stops reading, e.g. because it rejected
/// an item.
pub async fn import_bao(
    store: &Store,
    hash: Hash,
    size: NonZeroU64,
    items: impl IntoIterator<Item = BaoContentItem>,
) -> anyhow::Result<()> {
    let (tx, rx) = store
        .client()
        .client_streaming(ImportBaoRequest { hash, size }, CAP)
        .await
        .context("ImportBao request failed")?;
    for item in items {
        if tx.send(item).await.is_err() {
            break;
        }
    }
    // Dropping the sender ends the import.
    drop(tx);
    let result = rx
        .await
        .context("contract: ImportBao answers with a result, but the store dropped the request")?;
    Ok(result?)
}

/// Sends a `DeleteBlobs` request.
///
/// Without `force`, the store keeps blobs written since the last
/// `ClearProtected`.
pub async fn delete_blobs(store: &Store, hashes: Vec<Hash>, force: bool) -> anyhow::Result<()> {
    let result = store
        .client()
        .rpc(BlobDeleteRequest { hashes, force })
        .await
        .context("DeleteBlobs request failed")?;
    Ok(result?)
}

/// Sends a `ClearProtected` request.
pub async fn clear_protected(store: &Store) -> anyhow::Result<()> {
    let result = store
        .client()
        .rpc(ClearProtectedRequest)
        .await
        .context("ClearProtected request failed")?;
    Ok(result?)
}

/// Sends a `ListTempTags` request.
pub async fn list_temp_tags(store: &Store) -> anyhow::Result<Vec<HashAndFormat>> {
    store
        .client()
        .rpc(ListTempTagsRequest)
        .await
        .context("ListTempTags request failed")
}

async fn collect<T: irpc::RpcMessage>(mut rx: mpsc::Receiver<T>) -> anyhow::Result<Vec<T>> {
    let mut items = Vec::new();
    while let Some(item) = rx.recv().await.context("stream failed")? {
        items.push(item);
    }
    Ok(items)
}
