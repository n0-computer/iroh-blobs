//! The conformance cases.
//!
//! To add a case, write a `fn(&Harness) -> anyhow::Result<()>` and add it to
//! [`CASES`].
mod add;
mod check;
mod delete;
mod dropped;
mod export;
mod import_bao;
mod lifecycle;
mod list;
mod observe;
mod pins;
mod setup;
mod tags;

use crate::Case;

/// All cases, grouped by area.
pub(crate) const CASES: &[Case] = &[
    Case::new(
        "add_bytes",
        "Adding bytes stores them under their hash.",
        add::add_bytes,
    ),
    Case::new(
        "add_stream",
        "Adding a stream of bytes stores their concatenation, however the stream is split.",
        add::add_stream,
    ),
    Case::new(
        "add_path",
        "Adding a file stores its content; with ImportMode::Copy the blob survives deleting the file.",
        add::add_path,
    ),
    Case::new(
        "export_path",
        "Exporting a blob to a file writes its content, and the blob stays readable.",
        export::export_path,
    ),
    Case::new(
        "export_bao",
        "ExportBao of a complete blob sends the size, then exactly the bao encoding of the ranges.",
        export::export_bao,
    ),
    Case::new(
        "export_ranges",
        "ExportRanges of a complete blob sends the size, then the requested bytes in ascending order.",
        export::export_ranges,
    ),
    Case::new(
        "export_bao_absent",
        "ExportBao of a blob the store does not have sends only an error.",
        export::export_bao_absent,
    ),
    Case::new(
        "export_ranges_absent",
        "ExportRanges of a blob the store does not have sends only an error.",
        export::export_ranges_absent,
    ),
    Case::new(
        "import_bao_complete",
        "Importing the complete bao encoding makes a blob complete; importing it again changes nothing.",
        import_bao::import_bao_complete,
    ),
    Case::new(
        "import_bao_partial",
        "Importing the bao encoding of some ranges makes exactly those chunks available.",
        import_bao::import_bao_partial,
    ),
    Case::new(
        "import_bao_incremental",
        "Importing ranges one after another accumulates them until the blob is complete.",
        import_bao::import_bao_incremental,
    ),
    Case::new(
        "observe_complete",
        "Observing a complete blob starts with its complete bitfield.",
        observe::observe_complete,
    ),
    Case::new(
        "observe_import",
        "Observing a blob during an import yields growing bitfields until it is complete.",
        observe::observe_import,
    ),
    Case::new(
        "list_blobs",
        "ListBlobs returns each complete blob exactly once.",
        list::list_blobs,
    ),
    Case::new(
        "tags_model",
        "Any sequence of tag operations behaves like a sorted map.",
        tags::tags_model,
    ),
    Case::new(
        "temp_tags",
        "Temp tags are listed while alive, and released when dropped in process or when their batch ends.",
        pins::temp_tags,
    ),
    Case::new(
        "delete_blobs",
        "Deleted blobs are gone, and can be added again.",
        delete::delete_blobs,
    ),
    Case::new(
        "delete_protected",
        "DeleteBlobs without force keeps blobs written since the last ClearProtected.",
        delete::delete_protected,
    ),
    Case::new(
        "sync_and_wait_idle",
        "SyncDb and WaitIdle answer and leave the store usable.",
        lifecycle::sync_and_wait_idle,
    ),
    Case::new(
        "dropped_exports",
        "Exports and observers whose client goes away leave the blob and the store intact.",
        dropped::dropped_exports,
    ),
    Case::new(
        "dropped_import_bao",
        "A bao import whose client goes away keeps at most the chunks it got.",
        dropped::dropped_import_bao,
    ),
    Case::new(
        "dropped_import_byte_stream",
        "A byte stream import whose client goes away adds no blob.",
        dropped::dropped_import_byte_stream,
    ),
    Case::new(
        "unfinished_import_byte_stream",
        "A byte stream import that ends without Done adds no blob and reports an error.",
        dropped::unfinished_import_byte_stream,
    ),
    Case::new(
        "dropped_import_bytes",
        "A bytes import whose client goes away leaves the store working.",
        dropped::dropped_import_bytes,
    ),
];
