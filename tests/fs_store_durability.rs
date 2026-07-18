#![cfg(feature = "fs-store")]

use std::{path::PathBuf, process::Command};

use bytes::Bytes;
use iroh_blobs::{store::fs::FsStore, Hash};
use testresult::TestResult;

#[tokio::test]
async fn durable_add_ack_survives_immediate_process_exit() -> TestResult<()> {
    const CHILD_STORE_ENV: &str = "IROH_BLOBS_DURABLE_ACK_CHILD_STORE";
    const TEST_NAME: &str = "durable_add_ack_survives_immediate_process_exit";
    let data = Bytes::from_static(b"durable metadata acknowledgement");
    let hash = Hash::new(&data);

    if let Some(db_dir) = std::env::var_os(CHILD_STORE_ENV) {
        let store = FsStore::load(PathBuf::from(db_dir)).await?;
        let tag = store.blobs().add_bytes(data).await?;
        assert_eq!(tag.hash, hash);
        std::process::exit(0);
    }

    let testdir = tempfile::tempdir()?;
    let db_dir = testdir.path().join("db");
    let output = Command::new(std::env::current_exe()?)
        .args(["--exact", TEST_NAME, "--nocapture", "--test-threads=1"])
        .env(CHILD_STORE_ENV, &db_dir)
        .output()?;
    assert!(
        output.status.success(),
        "durability child failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let store = FsStore::load(&db_dir).await?;
    assert_eq!(store.blobs().get_bytes(hash).await?, data);
    store.shutdown().await?;
    Ok(())
}
