//! Path export and file copying using the filesystem metadata layout.
use super::{
    entity::{wait_writable, EntityApi, HashContext, StorageState},
    entity_storage::EntityStorage,
    entry_state::{DataLocation, EntryState},
};
use crate::{
    api::{
        self,
        blobs::{AddProgressItem, ExportMode, ExportProgressItem},
        proto::ExportPathRequest,
    },
    store::util::MemOrFile,
};
use bao_tree::io::sync::ReadAt;
use irpc::{channel::mpsc, RpcMessage};
use n0_future::future::yield_now;
use std::{
    fs,
    io::{self, Write},
    path::Path,
};
use tracing::trace;

/// Maximum number of external paths we track per blob.
const MAX_EXTERNAL_PATHS: usize = 8;

pub(super) async fn export_path_impl<S: EntityStorage>(
    ctx: &HashContext<S>,
    cmd: ExportPathRequest,
    tx: &mut mpsc::Sender<ExportProgressItem>,
) -> api::Result<()> {
    let ExportPathRequest { mode, target, .. } = cmd;
    if !target.is_absolute() {
        return Err(api::Error::io(
            io::ErrorKind::InvalidInput,
            "path is not absolute",
        ));
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let state = ctx.meta().get().await?;
    let (data_location, outboard_location) = match state {
        Some(EntryState::Complete {
            data_location,
            outboard_location,
        }) => (data_location, outboard_location),
        Some(EntryState::Partial { .. }) => {
            return Err(api::Error::io(
                io::ErrorKind::InvalidInput,
                "cannot export partial entry",
            ));
        }
        None => {
            return Err(api::Error::io(io::ErrorKind::NotFound, "no entry found"));
        }
    };
    trace!("exporting {} to {}", cmd.hash.to_hex(), target.display());
    let (data, mut external) = match data_location {
        DataLocation::Inline(data) => (MemOrFile::Mem(data), vec![]),
        DataLocation::Owned(size) => (
            MemOrFile::File((ctx.global.options.path.data_path(&cmd.hash), size)),
            vec![],
        ),
        DataLocation::External(paths, size) => (
            MemOrFile::File((
                paths.first().cloned().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "no external data path")
                })?,
                size,
            )),
            paths,
        ),
    };
    let size = match &data {
        MemOrFile::Mem(data) => data.len() as u64,
        MemOrFile::File((_, size)) => *size,
    };
    tx.send(ExportProgressItem::Size(size)).await?;
    match data {
        MemOrFile::Mem(data) => {
            let mut target = fs::File::create(&target)?;
            target.write_all(&data)?;
        }
        MemOrFile::File((source_path, size)) => match mode {
            ExportMode::Copy => {
                let res = reflink_or_copy_with_progress(&source_path, &target, size, tx).await?;
                trace!(
                    "exported {} to {}, {res:?}",
                    source_path.display(),
                    target.display()
                );
            }
            ExportMode::TryReference => {
                wait_writable(ctx).await;
                if !external.is_empty() {
                    // the file already exists externally, so we need to copy it.
                    // if the OS supports reflink, we might as well use that.
                    let res =
                        reflink_or_copy_with_progress(&source_path, &target, size, tx).await?;
                    trace!(
                        "exported {} also to {}, {res:?}",
                        source_path.display(),
                        target.display()
                    );
                    external.push(target);
                    external.sort();
                    external.dedup();
                    external.truncate(MAX_EXTERNAL_PATHS);
                } else {
                    // the file was previously owned, so we can just move it.
                    // if the target is on another file system or drive, we
                    // fall back to copy: EXDEV on unix, ERROR_NOT_SAME_DEVICE
                    // on windows.
                    match std::fs::rename(&source_path, &target) {
                        Ok(()) => {}
                        Err(cause) => {
                            if cause.kind() == io::ErrorKind::CrossesDevices {
                                reflink_or_copy_with_progress(&source_path, &target, size, tx)
                                    .await?;
                            } else {
                                return Err(cause.into());
                            }
                        }
                    }
                    external.push(target);
                };
                // The copy took a while, so the entry may have been deleted
                // meanwhile. Then it stays deleted, and this was just a copy.
                // No await between the check and sending the set.
                wait_writable(ctx).await;
                if matches!(
                    &ctx.state.borrow().storage,
                    StorageState::ReadWrite(Some(storage)) if storage.is_complete()
                ) {
                    // After the commit, the owned data file comes back as an
                    // unreferenced part, and this entity deletes it unless its
                    // state still uses it.
                    // TODO: after a copy across file systems it does, so the
                    // owned file leaks.
                    ctx.meta()
                        .set(EntryState::Complete {
                            data_location: DataLocation::External(external, size),
                            outboard_location,
                        })
                        .await?;
                }
            }
        },
    }
    tx.send(ExportProgressItem::Done).await?;
    Ok(())
}

pub(super) trait CopyProgress: RpcMessage {
    fn from_offset(offset: u64) -> Self;
}

impl CopyProgress for ExportProgressItem {
    fn from_offset(offset: u64) -> Self {
        ExportProgressItem::CopyProgress(offset)
    }
}

impl CopyProgress for AddProgressItem {
    fn from_offset(offset: u64) -> Self {
        AddProgressItem::CopyProgress(offset)
    }
}

#[derive(Debug)]
pub(super) enum CopyResult {
    Reflinked,
    Copied,
}

pub(super) async fn reflink_or_copy_with_progress(
    from: impl AsRef<Path>,
    to: impl AsRef<Path>,
    size: u64,
    tx: &mut mpsc::Sender<impl CopyProgress>,
) -> io::Result<CopyResult> {
    let from = from.as_ref();
    let to = to.as_ref();
    if reflink_copy::reflink(from, to).is_ok() {
        return Ok(CopyResult::Reflinked);
    }
    let source = fs::File::open(from)?;
    let mut target = fs::File::create(to)?;
    copy_with_progress(source, size, &mut target, tx).await?;
    Ok(CopyResult::Copied)
}

async fn copy_with_progress<T: CopyProgress>(
    file: impl ReadAt,
    size: u64,
    target: &mut impl Write,
    tx: &mut mpsc::Sender<T>,
) -> io::Result<()> {
    let mut offset = 0;
    let mut buf = vec![0u8; 1024 * 1024];
    while offset < size {
        let remaining = buf.len().min((size - offset) as usize);
        let buf: &mut [u8] = &mut buf[..remaining];
        file.read_exact_at(offset, buf)?;
        target.write_all(buf)?;
        tx.try_send(T::from_offset(offset)).await?;
        yield_now().await;
        offset += buf.len() as u64;
    }
    Ok(())
}
