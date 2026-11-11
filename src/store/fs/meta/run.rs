//! The record of the run the store is open in, kept in the meta table until a clean shutdown.
//!
//! Finding a record on open means the last run did not shut down cleanly. The
//! record says in which boot session that run was, and whether it synced data
//! files, so the next run knows if an OS crash may have damaged entries.

use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use super::tables::RUN_KEY;

/// A run of the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Run {
    /// The boot session it runs in, if the platform has an id for it.
    pub boot_id: Option<String>,
    /// Whether it syncs data files, so an OS crash cannot leave entries that claim lost data.
    pub syncs_files: bool,
}

/// How the store was left the last time it was open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LastShutdown {
    /// Shut down cleanly, or never opened before.
    Clean,
    /// Not shut down cleanly, but the machine kept running, so nothing written was lost.
    ProcessCrash,
    /// Not shut down cleanly, and the machine may have gone down too.
    MaybeOsCrash {
        /// The last run synced its files, so entries cannot claim lost data.
        synced: bool,
        /// Both runs have a boot id, so the machine did go down.
        boot_known: bool,
    },
}

/// Records `current` as the open run, and returns how the store was left before.
///
/// A recorded run that cannot be read, for example one written by a newer
/// version, counts as an unsynced run on an unknown boot.
pub(super) fn record(
    meta: &mut redb::Table<&str, &[u8]>,
    current: &Run,
) -> Result<LastShutdown, redb::StorageError> {
    let previous = meta.get(RUN_KEY)?.map(|v| Run::from_bytes(v.value()));
    let last = match previous {
        None => LastShutdown::Clean,
        Some(Some(previous))
            if previous.boot_id.is_some() && previous.boot_id == current.boot_id =>
        {
            LastShutdown::ProcessCrash
        }
        Some(previous) => LastShutdown::MaybeOsCrash {
            synced: previous.as_ref().is_some_and(|p| p.syncs_files),
            boot_known: previous.is_some_and(|p| p.boot_id.is_some()) && current.boot_id.is_some(),
        },
    };
    meta.insert(RUN_KEY, current.to_bytes().as_slice())?;
    Ok(last)
}

/// Removes the record, so the next open knows the store was shut down cleanly.
pub(super) fn clear(meta: &mut redb::Table<&str, &[u8]>) -> Result<(), redb::StorageError> {
    meta.remove(RUN_KEY)?;
    Ok(())
}

/// Reads the recorded run, if there is one that can be read.
#[cfg(test)]
pub(super) fn read(meta: &impl ReadableTable<&'static str, &'static [u8]>) -> Option<Run> {
    let bytes = meta.get(RUN_KEY).ok()??;
    Run::from_bytes(bytes.value())
}

/// Records `run` without looking at what was recorded before.
#[cfg(test)]
pub(super) fn write(
    meta: &mut redb::Table<&str, &[u8]>,
    run: &Run,
) -> Result<(), redb::StorageError> {
    meta.insert(RUN_KEY, run.to_bytes().as_slice())?;
    Ok(())
}

impl Run {
    fn to_bytes(&self) -> Vec<u8> {
        let data = RunWireFormat::Variant0(Variant0Run {
            boot_id: self.boot_id.clone(),
            syncs_files: self.syncs_files,
        });
        postcard::to_stdvec(&data).expect("a run always serializes")
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let RunWireFormat::Variant0(Variant0Run {
            boot_id,
            syncs_files,
        }) = postcard::from_bytes(bytes).ok()?;
        Some(Self {
            boot_id,
            syncs_files,
        })
    }
}

/// Wire format for [`Run`].
#[derive(Serialize, Deserialize)]
enum RunWireFormat {
    Variant0(Variant0Run),
}

#[derive(Serialize, Deserialize)]
struct Variant0Run {
    boot_id: Option<String>,
    syncs_files: bool,
}

#[cfg(test)]
mod tests {
    use redb::Database;

    use super::*;
    use crate::store::fs::meta::tables::META_TABLE;

    fn run(boot_id: Option<&str>, syncs_files: bool) -> Run {
        Run {
            boot_id: boot_id.map(str::to_owned),
            syncs_files,
        }
    }

    #[test]
    fn record_tells_how_the_store_was_left() -> testresult::TestResult<()> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        let tx = db.begin_write()?;
        let mut meta = tx.open_table(META_TABLE)?;
        let os_crash = |synced, boot_known| LastShutdown::MaybeOsCrash { synced, boot_known };
        let a = run(Some("a"), false);
        assert_eq!(record(&mut meta, &a)?, LastShutdown::Clean);
        assert_eq!(record(&mut meta, &a)?, LastShutdown::ProcessCrash);
        let b = run(Some("b"), false);
        assert_eq!(record(&mut meta, &b)?, os_crash(false, true));
        let unknown = run(None, false);
        assert_eq!(record(&mut meta, &unknown)?, os_crash(false, false));
        assert_eq!(record(&mut meta, &unknown)?, os_crash(false, false));
        let synced = run(Some("b"), true);
        assert_eq!(record(&mut meta, &synced)?, os_crash(false, false));
        // the mode of the last run counts, not the one of this run
        assert_eq!(
            record(&mut meta, &run(Some("c"), false))?,
            os_crash(true, true)
        );
        meta.insert(RUN_KEY, b"garbage".as_slice())?;
        assert_eq!(record(&mut meta, &a)?, os_crash(false, false));
        clear(&mut meta)?;
        assert_eq!(record(&mut meta, &a)?, LastShutdown::Clean);
        Ok(())
    }

    #[test]
    fn a_run_survives_the_wire_format() {
        for run in [run(Some("a"), true), run(None, false)] {
            assert_eq!(Run::from_bytes(&run.to_bytes()), Some(run));
        }
    }
}
