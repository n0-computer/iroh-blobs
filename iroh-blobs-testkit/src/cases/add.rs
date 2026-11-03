//! Adding blobs from bytes, from a stream and from a file.
use iroh_blobs::{
    api::blobs::{AddPathOptions, ImportMode},
    BlobFormat,
};
use n0_future::stream;
use proptest::prelude::*;

use super::check;
use crate::{harness::Harness, inputs};

pub fn add_bytes(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(inputs::blob(), async |store, blob| {
        let reference = blob.reference();
        let tt = store.add_bytes(reference.data().clone()).temp_tag().await?;
        check::added(&store, tt.hash(), &reference).await
    })
}

pub fn add_stream(h: &Harness) -> anyhow::Result<()> {
    let inputs = (inputs::blob(), inputs::piece_sizes());
    h.check_prop(inputs, async |store, (blob, sizes)| {
        let reference = blob.reference();
        let pieces = inputs::split(reference.data(), &sizes);
        let data = stream::iter(pieces.into_iter().map(Ok));
        let tt = store.add_stream(data).await.temp_tag().await?;
        check::added(&store, tt.hash(), &reference).await
    })
}

pub fn add_path(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(
        (inputs::blob(), import_mode()),
        async |store, (blob, mode)| {
            let reference = blob.reference();
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("source");
            std::fs::write(&path, reference.data())?;
            let options = AddPathOptions {
                path: path.clone(),
                mode,
                format: BlobFormat::Raw,
            };
            let tt = store.add_path_with_opts(options).temp_tag().await?;
            if mode == ImportMode::Copy {
                // A copy must not depend on the file.
                std::fs::remove_file(&path)?;
            }
            check::added(&store, tt.hash(), &reference).await
        },
    )
}

fn import_mode() -> impl Strategy<Value = ImportMode> {
    prop_oneof![Just(ImportMode::Copy), Just(ImportMode::TryReference)]
}
