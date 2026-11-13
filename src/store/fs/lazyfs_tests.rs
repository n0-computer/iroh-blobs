//! Crash tests that lose unsynced writes, with the store's directory mounted through LazyFS.
//!
//! LazyFS holds every write in its cache until it is synced, and drops the
//! cache on command, as a power cut would. A child process (this test binary
//! again) writes to the store and exits without shutting it down. The parent
//! then drops the cache and opens the store again.
//!
//! The LazyFS tests are ignored unless asked for, and need the LazyFS binary
//! in `IROH_BLOBS_LAZYFS`, see the `lazyfs` job in CI. LazyFS runs on Linux
//! only. The process crash test needs no LazyFS and always runs.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use bao_tree::{ChunkNum, ChunkRanges};
use testresult::TestResult;

use super::{
    meta::move_run_to_another_boot,
    options::{CrashConsistency, Options},
    FsStore,
};
use crate::{
    store::{mem::MemStore, util::tests::create_n0_bao},
    Hash,
};

/// The LazyFS binary to mount with.
const LAZYFS: &str = "IROH_BLOBS_LAZYFS";
/// Set in the child, to the store directory it writes to.
const CHILD: &str = "IROH_BLOBS_LAZYFS_CHILD";
/// What the canary file holds until LazyFS drops its cache.
const CANARY: &[u8] = b"never synced";
/// How long LazyFS is given to mount or to answer a command.
const ANSWERING: Duration = Duration::from_secs(30);

/// The blobs a child writes, the same in every process.
struct Blob {
    data: Vec<u8>,
    /// What the child imports, all of it unless the blob is partial.
    ranges: ChunkRanges,
}

impl Blob {
    fn hash(&self) -> Hash {
        Hash::new(&self.data)
    }
}

/// Inline data, owned data with inline outboard, owned data and outboard, and a partial blob.
fn blobs() -> Vec<Blob> {
    let data = |seed: u8, len: usize| -> Vec<u8> {
        (0..len)
            .map(|i| seed.wrapping_add((i / 1024) as u8) ^ (i as u8))
            .collect()
    };
    vec![
        Blob {
            data: data(1, 1_000),
            ranges: ChunkRanges::all(),
        },
        Blob {
            data: data(2, 100_000),
            ranges: ChunkRanges::all(),
        },
        Blob {
            data: data(3, 5_000_000),
            ranges: ChunkRanges::all(),
        },
        Blob {
            data: data(4, 1_000_000),
            ranges: ChunkRanges::from(..ChunkNum(256)),
        },
    ]
}

/// Opens the store in `dir` with `crash_consistency`.
async fn open(dir: &Path, crash_consistency: CrashConsistency) -> TestResult<FsStore> {
    let mut options = Options::new(dir);
    options.crash_consistency = crash_consistency;
    Ok(FsStore::load_with_opts(dir.join("blobs.db"), options).await?)
}

/// In the child: writes every blob, records it durably, and exits as a crash would.
async fn write_and_crash(dir: &Path, crash_consistency: CrashConsistency) -> TestResult<()> {
    let store = open(dir, crash_consistency).await?;
    for blob in blobs() {
        if blob.ranges.is_all() {
            store.add_bytes(blob.data.clone()).await?;
        } else {
            let (hash, bao) = create_n0_bao(&blob.data, &blob.ranges)?;
            store.import_bao_bytes(hash, blob.ranges, bao).await?;
        }
    }
    // The metadata is durable, and only PowerLoss synced the files it points to.
    store.sync_db().await?;
    // No shutdown and no destructors, as in a crash.
    std::process::exit(0);
}

/// Checks that no blob claims more than its files hold, and returns how many are complete.
///
/// Every claimed range is exported and imported into a fresh memory store,
/// which verifies it against the hash.
async fn check_claims(store: &FsStore) -> TestResult<usize> {
    let mut complete = 0;
    for blob in blobs() {
        let hash = blob.hash();
        let bitfield = store.observe(hash).await?;
        assert!(
            blob.ranges.is_superset(&bitfield.ranges),
            "{hash} claims ranges that were never written"
        );
        if bitfield.ranges.is_empty() {
            continue;
        }
        let bao = store
            .export_bao(hash, bitfield.ranges.clone())
            .bao_to_vec()
            .await?;
        let verify = MemStore::new();
        verify
            .import_bao_bytes(hash, bitfield.ranges.clone(), bao)
            .await?;
        if bitfield.is_complete() {
            assert_eq!(store.get_bytes(hash).await?.as_ref(), blob.data.as_slice());
            complete += 1;
        }
    }
    Ok(complete)
}

/// Runs `test` in a child process with `dir` as the store directory, and fails if the child did.
fn run_child(test: &str, dir: &Path) -> TestResult<()> {
    let name = concat!(module_path!(), "::")
        .strip_prefix("iroh_blobs::")
        .expect("in iroh_blobs")
        .to_owned()
        + test;
    let status = Command::new(env::current_exe()?)
        .args([name.as_str(), "--exact", "--include-ignored", "--nocapture"])
        .env(CHILD, dir)
        .status()?;
    assert!(status.success(), "the child writing the store failed");
    Ok(())
}

/// After a process crash, every blob the store recorded is there, synced or not.
///
/// This one needs no LazyFS, so it runs everywhere: the OS keeps every write.
#[tokio::test]
async fn process_crash_keeps_every_recorded_blob() -> TestResult<()> {
    if let Some(dir) = env::var_os(CHILD) {
        return write_and_crash(Path::new(&dir), CrashConsistency::ProcessCrash).await;
    }
    let root = tempfile::tempdir()?;
    run_child("process_crash_keeps_every_recorded_blob", root.path())?;
    let store = open(root.path(), CrashConsistency::ProcessCrash).await?;
    assert_eq!(
        check_claims(&store).await?,
        3,
        "every complete blob survives"
    );
    let partial = blobs().pop().expect("a partial blob");
    let bitfield = store.observe(partial.hash()).await?;
    assert_eq!(bitfield.ranges, partial.ranges, "the partial blob survives");
    store.shutdown().await?;
    Ok(())
}

/// After an OS crash, a store that synced nothing claims no data it lost.
#[tokio::test]
#[ignore = "needs LazyFS, see the module docs"]
async fn os_crash_without_sync_claims_no_lost_data() -> TestResult<()> {
    if let Some(dir) = env::var_os(CHILD) {
        return write_and_crash(Path::new(&dir), CrashConsistency::ProcessCrash).await;
    }
    let root = tempfile::tempdir()?;
    let lazyfs = LazyFs::mount(root.path())?;
    run_child(
        "os_crash_without_sync_claims_no_lost_data",
        lazyfs.mount_point(),
    )?;
    lazyfs.clear_cache()?;
    // LazyFS keeps the boot id, so stand in for the reboot.
    assert!(
        move_run_to_another_boot(&lazyfs.mount_point().join("blobs.db"))?,
        "the crashed run must still be recorded"
    );
    let store = open(lazyfs.mount_point(), CrashConsistency::ProcessCrash).await?;
    check_claims(&store).await?;
    store.shutdown().await?;
    Ok(())
}

/// After a power cut, a PowerLoss store has every blob it recorded, without recovery.
#[tokio::test]
#[ignore = "needs LazyFS, see the module docs"]
async fn power_loss_keeps_every_recorded_blob() -> TestResult<()> {
    if let Some(dir) = env::var_os(CHILD) {
        return write_and_crash(Path::new(&dir), CrashConsistency::PowerLoss).await;
    }
    let root = tempfile::tempdir()?;
    let lazyfs = LazyFs::mount(root.path())?;
    run_child("power_loss_keeps_every_recorded_blob", lazyfs.mount_point())?;
    lazyfs.clear_cache()?;
    // The same boot: a PowerLoss run must not depend on noticing the crash.
    let store = open(lazyfs.mount_point(), CrashConsistency::PowerLoss).await?;
    let complete = check_claims(&store).await?;
    assert_eq!(complete, 3, "every complete blob survives");
    let partial = blobs().pop().expect("a partial blob");
    let bitfield = store.observe(partial.hash()).await?;
    assert_eq!(bitfield.ranges, partial.ranges, "the partial blob survives");
    store.shutdown().await?;
    Ok(())
}

/// LazyFS mounted over a directory, with its own files beside it.
///
/// The same setup as valisk's robustness tests and etcd's `e2e.LazyFS`.
struct LazyFs {
    mount: PathBuf,
    dir: PathBuf,
    child: Child,
    logged: Arc<(Mutex<Logged>, Condvar)>,
}

/// How often LazyFS has logged each line a caller waits on.
#[derive(Debug, Default)]
struct Logged {
    ready: usize,
    cleared: usize,
    exited: bool,
}

impl LazyFs {
    /// Mounts LazyFS at `root/mount`, and waits until it takes commands.
    fn mount(root: &Path) -> TestResult<Self> {
        let binary = env::var_os(LAZYFS).expect("IROH_BLOBS_LAZYFS must point to LazyFS");
        let mount = root.join("mount");
        let dir = root.join("lazyfs");
        let backing = dir.join("data");
        fs::create_dir_all(&mount)?;
        fs::create_dir_all(&backing)?;
        let config = dir.join("config.toml");
        // A cache that never evicts, so nothing reaches the disk unless synced.
        fs::write(
            &config,
            format!(
                "[faults]\nfifo_path={:?}\n[cache]\napply_eviction=false\n[cache.simple]\n\
                 custom_size=\"1gb\"\nblocks_per_page=1\n[filesystem]\nlog_all_operations=false\n",
                dir.join("sock.fifo")
            ),
        )?;
        let log = dir.join("lazyfs.log");
        let errors = File::create(&log)?;
        let mut child = Command::new(binary)
            .arg(&mount)
            .arg("--config-path")
            .arg(&config)
            .args(["-o", "modules=subdir", "-o"])
            .arg(format!("subdir={}", backing.display()))
            .arg("-f")
            .stdout(Stdio::piped())
            .stderr(errors.try_clone()?)
            .spawn()?;
        let stdout = child.stdout.take().expect("piped stdout");
        let logged = Arc::new((Mutex::new(Logged::default()), Condvar::new()));
        let counting = logged.clone();
        std::thread::spawn(move || listen(stdout, errors, &counting));
        let lazyfs = Self {
            mount,
            dir,
            child,
            logged,
        };
        lazyfs.until(|logged| logged.ready > 0, "waiting for fault commands")?;
        Ok(lazyfs)
    }

    fn mount_point(&self) -> &Path {
        &self.mount
    }

    /// Drops every write not yet synced, as a power cut would.
    ///
    /// Checks that it really did with an unsynced canary file, so a test
    /// cannot pass because nothing was lost.
    fn clear_cache(&self) -> TestResult<()> {
        let canary = self.mount.join("canary");
        fs::write(&canary, CANARY)?;
        let before = self.logged.0.lock().expect("not poisoned").cleared;
        // LazyFS reads the fifo from the start, so opening it does not block.
        OpenOptions::new()
            .write(true)
            .open(self.dir.join("sock.fifo"))?
            .write_all(b"lazyfs::clear-cache\n")?;
        self.until(|logged| logged.cleared > before, "cache is cleared")?;
        let survived = fs::read(&canary).is_ok_and(|data| data == CANARY);
        assert!(!survived, "LazyFS kept a write that was never synced");
        fs::remove_file(&canary).ok();
        Ok(())
    }

    /// Waits until what LazyFS logged meets `done`.
    fn until(&self, done: impl Fn(&Logged) -> bool, line: &str) -> TestResult<()> {
        let (lock, changed) = &*self.logged;
        let logged = lock.lock().expect("not poisoned");
        let (logged, _) = changed
            .wait_timeout_while(logged, ANSWERING, |logged| !done(logged) && !logged.exited)
            .expect("not poisoned");
        assert!(
            done(&logged),
            "LazyFS never logged {line:?}, see {}",
            self.dir.join("lazyfs.log").display()
        );
        Ok(())
    }
}

/// Copies what LazyFS logs into `log`, counting the lines a caller waits on.
fn listen(stdout: impl std::io::Read, mut log: File, logged: &(Mutex<Logged>, Condvar)) {
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        {
            let mut counts = logged.0.lock().expect("not poisoned");
            if line.contains("waiting for fault commands") {
                counts.ready += 1;
            }
            if line.contains("cache is cleared") {
                counts.cleared += 1;
            }
        }
        logged.1.notify_all();
        writeln!(log, "{line}").ok();
    }
    logged.0.lock().expect("not poisoned").exited = true;
    logged.1.notify_all();
}

impl Drop for LazyFs {
    /// Unmounts, which ends LazyFS. What was never synced is gone.
    fn drop(&mut self) {
        let unmounted = Command::new("fusermount3")
            .arg("-u")
            .arg(&self.mount)
            .status()
            .is_ok_and(|status| status.success());
        if !unmounted {
            Command::new("fusermount3")
                .arg("-uz")
                .arg(&self.mount)
                .status()
                .ok();
            self.child.kill().ok();
        }
        self.child.wait().ok();
    }
}
