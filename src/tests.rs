use std::{collections::HashSet, io, ops::Range, path::PathBuf};

use bao_tree::ChunkRanges;
use bytes::Bytes;
use iroh::{address_lookup::MemoryLookup, protocol::Router, EndpointId};
use irpc::RpcMessage;
use n0_future::{task::AbortOnDropHandle, StreamExt};
use n0_tracing_test::traced_test;
use tempfile::TempDir;
use testresult::TestResult;
use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::{
    api::{
        blobs::{Bitfield, BlobStatus},
        RequestError, Store,
    },
    get,
    hashseq::HashSeq,
    net_protocol::BlobsProtocol,
    protocol::{ChunkRangesSeq, GetManyRequest, ObserveRequest, PushRequest},
    provider::events::{
        AbortReason, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
    },
    store::{
        fs::{
            tests::{test_data, INTERESTING_SIZES},
            FsStore,
        },
        mem::MemStore,
        util::{observer::Combine, tests::create_n0_bao},
    },
    test_net::test_endpoint,
    util::sink::Drain,
    BlobFormat, Hash, HashAndFormat,
};

// #[tokio::test]
// #[traced_test]
// async fn two_nodes_blobs_downloader_smoke() -> TestResult<()> {
//     let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup().await?;
//     let sizes = INTERESTING_SIZES;
//     let mut tts = Vec::new();
//     for size in sizes {
//         tts.push(store1.add_bytes(test_data(size)).await?);
//     }
//     let addr1 = r1.endpoint().node_addr().await?;
//     // test that we can download directly, without a downloader
//     // let no_downloader_tt = store1.add_slice("test").await?;
//     // let conn = r2.endpoint().connect(addr1.clone(), crate::ALPN).await?;
//     // let stats = store2.download().fetch(conn, *no_downloader_tt.hash_and_format(), None).await?;
//     // println!("stats: {:?}", stats);
//     // return Ok(());

//     // tell ep2 about the addr of ep1, so we don't need to rely on node address_lookup
//     r2.endpoint().add_node_addr(addr1.clone())?;
//     let d1 = Downloader::new(store2.clone(), r2.endpoint().clone());
//     for (tt, size) in tts.iter().zip(sizes) {
//         // protect the downloaded data from being deleted
//         let _tt2 = store2.tags().temp_tag(*tt.hash()).await?;
//         let request = DownloadRequest::new(*tt.hash_and_format(), [addr1.node_id]);
//         let handle = d1.queue(request).await;
//         handle.await?;
//         assert_eq!(store2.get_bytes(*tt.hash()).await?, test_data(size));
//     }
//     Ok(())
// }

// #[tokio::test]
// #[traced_test]
// async fn two_nodes_blobs_downloader_progress() -> TestResult<()> {
//     let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup().await?;
//     let size = 1024 * 1024 * 8 + 1;
//     let tt = store1.add_bytes(test_data(size)).await?;
//     let addr1 = r1.endpoint().node_addr().await?;

//     // tell ep2 about the addr of ep1, so we don't need to rely on node address_lookup
//     r2.endpoint().add_node_addr(addr1.clone())?;
//     // create progress channel - big enough to not block
//     let (tx, rx) = mpsc::channel(1024 * 1024);
//     let d1 = Downloader::new(store2.clone(), r2.endpoint().clone());
//     // protect the downloaded data from being deleted
//     let _tt2 = store2.tags().temp_tag(*tt.hash()).await?;
//     let request = DownloadRequest::new(*tt.hash_and_format(), [addr1.node_id]).progress_sender(tx);
//     let handle = d1.queue(request).await;
//     handle.await?;
//     let progress = drain(rx).await;
//     assert!(!progress.is_empty());
//     assert_eq!(store2.get_bytes(*tt.hash()).await?, test_data(size));
//     Ok(())
// }

// #[tokio::test]
// async fn three_nodes_blobs_downloader_switch() -> TestResult<()> {
//     // tracing_subscriber::fmt::try_init().ok();
//     let testdir = tempfile::tempdir()?;
//     let (r1, store1, _) = node_test_setup(testdir.path().join("a")).await?;
//     let (r2, store2, _) = node_test_setup(testdir.path().join("b")).await?;
//     let (r3, store3, _) = node_test_setup(testdir.path().join("c")).await?;
//     let addr1 = r1.endpoint().node_addr().await?;
//     let addr2 = r2.endpoint().node_addr().await?;
//     let size = 1024 * 1024 * 8 + 1;
//     let data = test_data(size);
//     // a has the data just partially
//     let ranges = ChunkRanges::from(..ChunkNum(17));
//     let (hash, bao) = create_n0_bao(&data, &ranges)?;
//     let _tt1 = store1.tags().temp_tag(hash).await?;
//     store1.import_bao_bytes(hash, ranges, bao).await?;
//     // b has the data completely
//     let _tt2: crate::api::TempTag = store2.add_bytes(data).await?;

//     // tell ep3 about the addr of ep1 and ep2, so we don't need to rely on node address_lookup
//     r3.endpoint().add_node_addr(addr1.clone())?;
//     r3.endpoint().add_node_addr(addr2.clone())?;
//     // create progress channel - big enough to not block
//     let (tx, rx) = mpsc::channel(1024 * 1024);
//     let d1 = Downloader::new(store3.clone(), r3.endpoint().clone());
//     // protect the downloaded data from being deleted
//     let _tt3 = store3.tags().temp_tag(hash).await?;
//     let request = DownloadRequest::new(HashAndFormat::raw(hash), [addr1.node_id, addr2.node_id])
//         .progress_sender(tx);
//     let handle = d1.queue(request).await;
//     handle.await?;
//     let progress = drain(rx).await;
//     assert!(!progress.is_empty());
//     assert_eq!(store3.get_bytes(hash).await?, test_data(size));
//     Ok(())
// }

// #[tokio::test]
// async fn three_nodes_blobs_downloader_range_switch() -> TestResult<()> {
//     // tracing_subscriber::fmt::try_init().ok();
//     let testdir = tempfile::tempdir()?;
//     let (r1, store1, _) = node_test_setup(testdir.path().join("a")).await?;
//     let (r2, store2, _) = node_test_setup(testdir.path().join("b")).await?;
//     let (r3, store3, _) = node_test_setup(testdir.path().join("c")).await?;
//     let addr1 = r1.endpoint().node_addr().await?;
//     let addr2 = r2.endpoint().node_addr().await?;
//     let size = 1024 * 1024 * 8 + 1;
//     let data = test_data(size);
//     // a has the data just partially
//     let ranges = ChunkRanges::chunks(..15);
//     let (hash, bao) = create_n0_bao(&data, &ranges)?;
//     let _tt1 = store1.tags().temp_tag(hash).await?;
//     store1.import_bao_bytes(hash, ranges, bao).await?;
//     // b has the data completely
//     let _tt2: crate::api::TempTag = store2.add_bytes(data.clone()).await?;

//     // tell ep3 about the addr of ep1 and ep2, so we don't need to rely on node address_lookup
//     r3.endpoint().add_node_addr(addr1.clone())?;
//     r3.endpoint().add_node_addr(addr2.clone())?;
//     let range = 10000..20000;
//     let request = GetRequest::builder()
//         .root(ChunkRanges::bytes(range.clone()))
//         .build(hash);
//     // create progress channel - big enough to not block
//     let (tx, rx) = mpsc::channel(1024 * 1024);
//     let d1 = Downloader::new(store3.clone(), r3.endpoint().clone());
//     // protect the downloaded data from being deleted
//     let _tt3 = store3.tags().temp_tag(hash).await?;
//     let request = DownloadRequest::new(request, [addr1.node_id, addr2.node_id]).progress_sender(tx);
//     let handle = d1.queue(request).await;
//     handle.await?;
//     let progress = drain(rx).await;
//     assert!(!progress.is_empty());
//     let bitfield = store3.observe(hash).await?;
//     assert_eq!(bitfield.ranges, ChunkRanges::bytes(range.clone()));
//     let bytes = store3
//         .export_ranges(hash, range.clone())
//         .concatenate()
//         .await?;
//     assert_eq!(&bytes, &data[range.start as usize..range.end as usize]);
//     // assert_eq!(store3.get_bytes(hash).await?, test_data(size));
//     Ok(())
// }

// #[tokio::test]
// async fn threee_nodes_hash_seq_downloader_switch() -> TestResult<()> {
//     let testdir = tempfile::tempdir()?;
//     let (r1, store1, _) = node_test_setup(testdir.path().join("a")).await?;
//     let (r2, store2, _) = node_test_setup(testdir.path().join("b")).await?;
//     let (r3, store3, _) = node_test_setup(testdir.path().join("c")).await?;
//     let addr1 = r1.endpoint().node_addr().await?;
//     let addr2 = r2.endpoint().node_addr().await?;
//     let sizes = INTERESTING_SIZES;
//     // store1 contains the hash seq, but just the first 3 items
//     add_test_hash_seq_incomplete(&store1, sizes, |x| {
//         if x < 6 {
//             ChunkRanges::all()
//         } else {
//             ChunkRanges::from(..ChunkNum(1))
//         }
//     })
//     .await?;
//     let content = add_test_hash_seq(&store2, sizes).await?;

//     // tell ep3 about the addr of ep1 and ep2, so we don't need to rely on node address_lookup
//     r3.endpoint().add_node_addr(addr1.clone())?;
//     r3.endpoint().add_node_addr(addr2.clone())?;
//     // create progress channel - big enough to not block
//     let (tx, rx) = mpsc::channel(1024 * 1024);
//     let d1 = Downloader::new(store3.clone(), r3.endpoint().clone());
//     // protect the downloaded data from being deleted
//     let _tt3 = store3.tags().temp_tag(content).await?;
//     let request = DownloadRequest::new(content, [addr1.node_id, addr2.node_id]).progress_sender(tx);
//     let handle = d1.queue(request).await;
//     handle.await?;
//     let progress = drain(rx).await;
//     assert!(!progress.is_empty());
//     println!("progress: {:?}", progress);
//     check_presence(&store3, &sizes).await?;
//     Ok(())
// }

#[allow(dead_code)]
async fn drain<T: RpcMessage>(mut rx: mpsc::Receiver<T>) -> Vec<T> {
    let mut items = Vec::new();
    while let Some(item) = rx.recv().await {
        items.push(item);
    }
    items
}

async fn two_nodes_get_blobs(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let sizes = INTERESTING_SIZES;
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store1.add_bytes(test_data(size)).await?);
    }
    let addr1 = r1.endpoint().addr();
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    for size in sizes {
        let hash = Hash::new(test_data(size));
        store2.remote().fetch(conn.clone(), hash).await?;
        let actual = store2.get_bytes(hash).await?;
        assert_eq!(actual, test_data(size));
    }
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

/// A get over a closed connection fails to open its stream, which is a
/// connection error, not a local failure.
#[tokio::test]
async fn get_over_closed_connection_is_open_error() -> TestResult<()> {
    let ((r1, _store1), (r2, store2)) = two_node_test_setup_mem().await?;
    let conn = r2
        .endpoint()
        .connect(r1.endpoint().addr(), crate::ALPN)
        .await?;
    conn.close(0u32.into(), b"done");
    let Err(err) = store2.remote().fetch(conn, Hash::new(b"blob")).await else {
        panic!("get over a closed connection succeeded");
    };
    assert!(err.open().is_some(), "{err:?}");
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_get_blobs_fs() -> TestResult<()> {
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup_fs().await?;
    two_nodes_get_blobs(r1, &store1, r2, &store2).await
}

/// A get must fail if the local store fails to write the last batch.
#[cfg(unix)]
#[tokio::test]
#[traced_test]
async fn get_blob_store_write_error_fs() -> TestResult<()> {
    use std::os::unix::fs::PermissionsExt;

    use iroh::endpoint::VarInt;

    use crate::{
        get::StreamPair,
        protocol::GetRequest,
        util::{AsyncWriteSendStream, AsyncWriteSendStreamExtra},
    };

    struct Sink(tokio::io::Sink);

    impl AsyncWriteSendStreamExtra for Sink {
        fn inner(&mut self) -> &mut (impl tokio::io::AsyncWrite + Unpin + Send) {
            &mut self.0
        }

        fn reset(&mut self, _code: VarInt) -> io::Result<()> {
            Ok(())
        }

        async fn stopped(&mut self) -> io::Result<Option<VarInt>> {
            std::future::pending().await
        }

        fn id(&self) -> u64 {
            0
        }
    }

    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = FsStore::load(&db_path).await?;
    // just over the inline limit, so the entry has to go to disk. This is
    // written in a single batch, so the first batch is also the last one.
    let data = test_data(1024 * 16 + 1);
    let (hash, response) = create_n0_bao(&data, &ChunkRanges::all())?;
    // make it impossible to create the data file
    let data_dir = db_path.join("data");
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o555))?;
    if std::fs::File::create(data_dir.join("probe")).is_ok() {
        // running as root, permissions are not enforced
        return Ok(());
    }
    let sink = AsyncWriteSendStream::new(Sink(tokio::io::sink()));
    let sp = StreamPair::new(0, Bytes::from(response), sink);
    let res = store.remote().execute_get(sp, GetRequest::blob(hash)).await;
    std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o755))?;
    assert!(
        matches!(res, Err(get::GetError::LocalFailure { .. })),
        "{res:?}"
    );
    store.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_get_blobs_mem() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    two_nodes_get_blobs(r1, &store1, r2, &store2).await
}

async fn two_nodes_observe(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let size = 1024 * 1024 * 8 + 1;
    let data = test_data(size);
    let (hash, bao) = create_n0_bao(&data, &ChunkRanges::all())?;
    let addr1 = r1.endpoint().addr();
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    let mut stream = store2
        .remote()
        .observe(conn.clone(), ObserveRequest::new(hash));
    let remote_observe_task = n0_future::task::spawn(async move {
        let mut current = Bitfield::empty();
        while let Some(item) = stream.next().await {
            current = current.combine(item?);
            if current.is_validated() {
                break;
            }
        }
        io::Result::Ok(())
    });
    store1
        .import_bao_bytes(hash, ChunkRanges::all(), bao)
        .await?;
    remote_observe_task.await??;
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

/// Events for a provider that allows observe requests, which are disabled by default.
fn observe_enabled() -> EventSender {
    EventSender::tracing()
}

#[tokio::test]
#[traced_test]
async fn two_nodes_observe_fs() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let (r1, store1, _, sp1) =
        node_test_setup_with_events_fs(testdir.path().join("a"), observe_enabled()).await?;
    let (r2, store2, _, sp2) = node_test_setup_fs(testdir.path().join("b")).await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    two_nodes_observe(r1, &store1, r2, &store2).await
}

#[tokio::test]
#[traced_test]
async fn two_nodes_observe_mem() -> TestResult<()> {
    let (r1, store1, sp1) = node_test_setup_with_events_mem(observe_enabled()).await?;
    let (r2, store2, sp2) = node_test_setup_mem().await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    two_nodes_observe(r1, &store1, r2, &store2).await
}

/// Observe requests must be rejected with the default event mask.
#[tokio::test]
#[traced_test]
async fn two_nodes_observe_disabled_by_default() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    let hash = store1.add_bytes(test_data(1024)).await?.hash;
    let conn = r2
        .endpoint()
        .connect(r1.endpoint().addr(), crate::ALPN)
        .await?;
    let mut stream = store2.remote().observe(conn, ObserveRequest::new(hash));
    let item = stream.next().await;
    assert!(
        matches!(item, Some(Err(_))),
        "observe request was not rejected: {item:?}"
    );
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

async fn two_nodes_get_many(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let sizes = INTERESTING_SIZES;
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store1.add_bytes(test_data(size)).await?);
    }
    let hashes = tts.iter().map(|tt| tt.hash).collect::<Vec<_>>();
    let addr1 = r1.endpoint().addr();
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    store2
        .remote()
        .execute_get_many(conn, GetManyRequest::new(hashes, ChunkRangesSeq::all()))
        .await?;
    for size in sizes {
        let expected = test_data(size);
        let hash = Hash::new(&expected);
        let actual = store2.get_bytes(hash).await?;
        assert_eq!(actual, expected);
    }
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn two_nodes_get_many_fs() -> TestResult<()> {
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup_fs().await?;
    two_nodes_get_many(r1, &store1, r2, &store2).await
}

#[tokio::test]
#[traced_test]
async fn two_nodes_get_many_mem() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    two_nodes_get_many(r1, &store1, r2, &store2).await
}

fn event_handler(
    allowed_nodes: impl IntoIterator<Item = EndpointId>,
) -> (EventSender, watch::Receiver<usize>, AbortOnDropHandle<()>) {
    let (count_tx, count_rx) = tokio::sync::watch::channel(0usize);
    let mask = EventMask {
        push: RequestMode::InterceptLog,
        ..EventMask::ALL_READONLY
    };
    let (events_tx, mut events_rx) = EventSender::channel(16, mask);
    let allowed_nodes = allowed_nodes.into_iter().collect::<HashSet<_>>();
    let task = AbortOnDropHandle::new(n0_future::task::spawn(async move {
        while let Some(event) = events_rx.recv().await {
            match event {
                ProviderMessage::ClientConnected(msg) => {
                    let res = match msg.endpoint_id {
                        Some(endpoint_id) if allowed_nodes.contains(&endpoint_id) => Ok(()),
                        Some(_) => Err(AbortReason::Permission),
                        None => Err(AbortReason::Permission),
                    };
                    msg.tx.send(res).await.ok();
                }
                ProviderMessage::PushRequestReceived(mut msg) => {
                    msg.tx.send(Ok(())).await.ok();
                    let count_tx = count_tx.clone();
                    n0_future::task::spawn(async move {
                        while let Ok(Some(update)) = msg.rx.recv().await {
                            if let RequestUpdate::Completed(_) = update {
                                count_tx.send_modify(|x| *x += 1);
                            }
                        }
                    });
                }
                _ => {}
            }
        }
    }));
    (events_tx, count_rx, task)
}

async fn two_nodes_push_blobs(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
    mut count_rx: tokio::sync::watch::Receiver<usize>,
) -> TestResult<()> {
    let sizes = INTERESTING_SIZES;
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store1.add_bytes(test_data(size)).await?);
    }
    let addr2 = r2.endpoint().addr();
    let conn = r1.endpoint().connect(addr2, crate::ALPN).await?;
    for size in sizes {
        let hash = Hash::new(test_data(size));
        // let data = get::request::get_blob(conn.clone(), hash).bytes().await?;
        store1
            .remote()
            .execute_push_sink(
                conn.clone(),
                PushRequest::new(hash, ChunkRangesSeq::root()),
                Drain,
            )
            .await?;
        // a successful push returns once the data is stored on the target
        let actual = store2.get_bytes(hash).await?;
        assert_eq!(actual, test_data(size));
        count_rx.changed().await?;
    }
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn two_nodes_push_blobs_fs() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let (r1, store1, _, sp1) = node_test_setup_fs(testdir.path().join("a")).await?;
    let (events_tx, count_rx, _task) = event_handler([r1.endpoint().id()]);
    let (r2, store2, _, sp2) =
        node_test_setup_with_events_fs(testdir.path().join("b"), events_tx).await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    two_nodes_push_blobs(r1, &store1, r2, &store2, count_rx).await
}

/// Push a blob from `r1` to `r2` and check that the push fails and the blob
/// does not end up in `store2`.
async fn two_nodes_push_rejected(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let data = test_data(1024 * 64);
    let hash = store1.add_bytes(data).await?.hash;
    let conn = r1
        .endpoint()
        .connect(r2.endpoint().addr(), crate::ALPN)
        .await?;
    let res = store1
        .remote()
        .execute_push_sink(
            conn.clone(),
            PushRequest::new(hash, ChunkRangesSeq::root()),
            Drain,
        )
        .await;
    assert!(res.is_err(), "push was not rejected");
    let status = store2.blobs().status(hash).await?;
    assert!(
        matches!(status, BlobStatus::NotFound),
        "blob exists on target, status: {status:?}"
    );
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

/// Push requests must be rejected with the default event mask.
#[tokio::test]
#[traced_test]
async fn two_nodes_push_rejected_default() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    two_nodes_push_rejected(r1, &store1, r2, &store2).await
}

/// Push requests must be rejected with [`EventMask::ALL_READONLY`], even if
/// the event handler accepts every request it is asked about.
#[tokio::test]
#[traced_test]
async fn two_nodes_push_rejected_readonly() -> TestResult<()> {
    let (r1, store1, sp1) = node_test_setup_mem().await?;
    let events = EventSender::tracing();
    let (r2, store2, sp2) = node_test_setup_with_events_mem(events).await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    two_nodes_push_rejected(r1, &store1, r2, &store2).await
}

#[tokio::test]
#[traced_test]
async fn two_nodes_push_blobs_mem() -> TestResult<()> {
    let (r1, store1, sp1) = node_test_setup_mem().await?;
    let (events_tx, count_rx, _task) = event_handler([r1.endpoint().id()]);
    let (r2, store2, sp2) = node_test_setup_with_events_mem(events_tx).await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    two_nodes_push_blobs(r1, &store1, r2, &store2, count_rx).await
}

pub async fn add_test_hash_seq(
    blobs: &Store,
    sizes: impl IntoIterator<Item = usize>,
) -> TestResult<HashAndFormat> {
    let batch = blobs.batch().await?;
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(batch.add_bytes(test_data(size)).await?);
    }
    let hash_seq = tts.iter().map(|tt| tt.hash()).collect::<HashSeq>();
    let root = batch
        .add_bytes_with_opts((hash_seq, BlobFormat::HashSeq))
        .with_named_tag("hs")
        .await?;
    Ok(root)
}

pub async fn add_test_hash_seq_incomplete(
    blobs: &Store,
    sizes: impl IntoIterator<Item = usize>,
    present: impl Fn(usize) -> ChunkRanges,
) -> TestResult<HashAndFormat> {
    let batch = blobs.batch().await?;
    let mut tts = Vec::new();
    for (i, size) in sizes.into_iter().enumerate() {
        let data = test_data(size);
        // figure out the ranges to import, and manually create a temp tag.
        let ranges = present(i + 1);
        let (hash, bao) = create_n0_bao(&data, &ranges)?;
        // why isn't import_bao_bytes returning a temp tag anyway?
        tts.push(batch.temp_tag(hash).await?);
        if !ranges.is_empty() {
            blobs.import_bao_bytes(hash, ranges, bao).await?;
        }
    }
    let hash_seq = tts.iter().map(|tt| tt.hash()).collect::<HashSeq>();
    let hash_seq_bytes = Bytes::from(hash_seq);
    let ranges = present(0);
    let (root, bao) = create_n0_bao(&hash_seq_bytes, &ranges)?;
    let content = HashAndFormat::hash_seq(root);
    blobs.tags().create(content).await?;
    blobs.import_bao_bytes(root, ranges, bao).await?;
    Ok(content)
}

async fn check_presence(store: &Store, sizes: &[usize]) -> TestResult<()> {
    for size in sizes {
        let expected = test_data(*size);
        let hash = Hash::new(&expected);
        let actual = store
            .export_bao(hash, ChunkRanges::all())
            .data_to_bytes()
            .await?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

pub async fn node_test_setup_fs(
    db_path: PathBuf,
) -> TestResult<(Router, FsStore, PathBuf, MemoryLookup)> {
    node_test_setup_with_events_fs(db_path, EventSender::DEFAULT).await
}

pub async fn node_test_setup_with_events_fs(
    db_path: PathBuf,
    events: EventSender,
) -> TestResult<(Router, FsStore, PathBuf, MemoryLookup)> {
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let sp = MemoryLookup::new();
    let ep = test_endpoint().await?;
    ep.address_lookup()?.add(sp.clone());
    let blobs = BlobsProtocol::new(&store, Some(events));
    let router = Router::builder(ep).accept(crate::ALPN, blobs).spawn();
    Ok((router, store, db_path, sp))
}

pub async fn node_test_setup_mem() -> TestResult<(Router, MemStore, MemoryLookup)> {
    node_test_setup_with_events_mem(EventSender::DEFAULT).await
}

pub async fn node_test_setup_with_events_mem(
    events: EventSender,
) -> TestResult<(Router, MemStore, MemoryLookup)> {
    let store = MemStore::new();
    let sp = MemoryLookup::new();
    let ep = test_endpoint().await?;
    ep.address_lookup()?.add(sp.clone());
    let blobs = BlobsProtocol::new(&store, Some(events));
    let router = Router::builder(ep).accept(crate::ALPN, blobs).spawn();
    Ok((router, store, sp))
}

/// The empty blob is always complete, whether or not it was ever added.
///
/// The client answers reads without asking the store. File exports reach the
/// store, but neither exports nor imports create an entry for the empty hash.
async fn empty_hash_is_complete(store: &Store, dir: &std::path::Path) -> TestResult<()> {
    use tokio::io::AsyncReadExt;

    use crate::api::blobs::BlobStatus;
    let status = store.blobs().status(Hash::EMPTY).await?;
    assert_eq!(status, BlobStatus::Complete { size: 0 });
    assert!(store.blobs().has(Hash::EMPTY).await?);
    let observed = store.blobs().observe(Hash::EMPTY).await?;
    assert!(observed.is_complete());
    assert_eq!(observed.size(), 0);
    let exported = store
        .export_bao(Hash::EMPTY, ChunkRanges::all())
        .data_to_bytes()
        .await?;
    assert!(exported.is_empty());
    let bao = store
        .export_bao(Hash::EMPTY, ChunkRanges::all())
        .bao_to_vec()
        .await?;
    assert_eq!(bao, 0u64.to_le_bytes());
    let ranges = store
        .export_ranges(Hash::EMPTY, 0..u64::MAX)
        .concatenate()
        .await?;
    assert!(ranges.is_empty());
    let mut read = Vec::new();
    store
        .blobs()
        .reader(Hash::EMPTY)
        .read_to_end(&mut read)
        .await?;
    assert!(read.is_empty());
    let target = dir.join("export").join("empty");
    assert_eq!(store.blobs().export(Hash::EMPTY, &target).await?, 0);
    assert_eq!(std::fs::read(&target)?, b"");
    let list = store.blobs().list().hashes().await?;
    assert_eq!(list, vec![Hash::EMPTY]);
    store.blobs().delete([Hash::EMPTY]).await?;
    assert!(store.blobs().has(Hash::EMPTY).await?);
    // adding it does not change any of this, and it is listed once
    let tt = store.add_slice(b"").temp_tag().await?;
    assert_eq!(tt.hash(), Hash::EMPTY);
    let source = dir.join("empty-source");
    std::fs::write(&source, b"")?;
    let tt = store.add_path(&source).temp_tag().await?;
    assert_eq!(tt.hash(), Hash::EMPTY);
    let tt = store
        .add_stream(n0_future::stream::empty())
        .await
        .temp_tag()
        .await?;
    assert_eq!(tt.hash(), Hash::EMPTY);
    let list = store.blobs().list().hashes().await?;
    assert_eq!(list, vec![Hash::EMPTY]);
    // the store itself did not create an entry for it
    let mut rx = store
        .client()
        .server_streaming(crate::api::proto::ListRequest, 32)
        .await?;
    assert!(matches!(
        rx.recv().await?,
        Some(crate::api::proto::ListBlobsItem::Done)
    ));
    Ok(())
}

#[tokio::test]
async fn empty_hash_is_complete_mem() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = MemStore::new();
    empty_hash_is_complete(&store, testdir.path()).await
}

#[tokio::test]
async fn empty_hash_is_complete_fs() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = FsStore::load(testdir.path().join("db")).await?;
    empty_hash_is_complete(&store, testdir.path()).await
}

/// Even an empty export must reach the store and respect its response, rather
/// than creating or truncating a file on the client filesystem.
#[tokio::test]
async fn empty_export_reaches_store() -> TestResult<()> {
    use crate::api::proto::{Command, ExportProgressItem};

    let dir = tempfile::tempdir()?;
    let target = dir.path().join("keep");
    std::fs::write(&target, b"keep me")?;
    let (tx, mut rx) = mpsc::channel(1);
    let store = Store::from_client(tx);
    let export = async { store.blobs().export(Hash::EMPTY, &target).await };
    tokio::pin!(export);
    let cmd = tokio::select! {
        cmd = rx.recv() => cmd.unwrap(),
        result = &mut export => panic!("export did not reach the store: {result:?}"),
    };
    let Command::ExportPath(cmd) = cmd else {
        panic!("expected a file export");
    };
    assert_eq!(cmd.hash, Hash::EMPTY);
    assert_eq!(cmd.target, target);
    cmd.tx
        .send(ExportProgressItem::Error(crate::api::Error::io(
            io::ErrorKind::PermissionDenied,
            "export denied by store",
        )))
        .await?;
    assert!(export.await.is_err());
    assert_eq!(std::fs::read(&target)?, b"keep me");
    Ok(())
}

/// An export whose store drops the sender without [`Done`] fails instead of
/// waiting forever.
///
/// [`Done`]: crate::api::proto::ExportProgressItem::Done
#[tokio::test]
async fn export_ended_early_fails() -> TestResult<()> {
    use crate::api::proto::{Command, ExportProgressItem};

    let dir = tempfile::tempdir()?;
    let (tx, mut rx) = mpsc::channel(1);
    let store = Store::from_client(tx);
    let export = store
        .blobs()
        .export(Hash::new(b"a"), dir.path().join("a"))
        .finish();
    let store_side = async {
        let Some(Command::ExportPath(cmd)) = rx.recv().await else {
            panic!("expected a file export");
        };
        cmd.tx.send(ExportProgressItem::Size(1)).await.ok();
    };
    let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(export, store_side)
    })
    .await?;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn empty_hash_readonly_mem() -> TestResult<()> {
    let store = crate::store::readonly_mem::ReadonlyMemStore::new([b"".as_slice()]);
    assert_eq!(store.blobs().list().hashes().await?, vec![Hash::EMPTY]);
    let mut rx = store
        .client()
        .server_streaming(crate::api::proto::ListRequest, 32)
        .await?;
    assert!(matches!(
        rx.recv().await?,
        Some(crate::api::proto::ListBlobsItem::Done)
    ));
    let dir = tempfile::tempdir()?;
    let target = dir.path().join("nested").join("empty");
    assert_eq!(store.blobs().export(Hash::EMPTY, &target).await?, 0);
    assert_eq!(std::fs::read(target)?, b"");
    Ok(())
}

/// Sets up two nodes with a router and a blob store each.
async fn two_node_test_setup_fs() -> TestResult<(
    TempDir,
    (Router, FsStore, PathBuf),
    (Router, FsStore, PathBuf),
)> {
    let testdir = tempfile::tempdir().unwrap();
    let db1_path = testdir.path().join("db1");
    let db2_path = testdir.path().join("db2");
    let (r1, store1, p1, sp1) = node_test_setup_fs(db1_path).await?;
    let (r2, store2, p2, sp2) = node_test_setup_fs(db2_path).await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    Ok((testdir, (r1, store1, p1), (r2, store2, p2)))
}

/// Sets up two nodes with a router and a blob store each.
///
/// Note that this does not configure address_lookup, so nodes will only find each other
/// with full node addresses, not just endpoint ids!
async fn two_node_test_setup_mem() -> TestResult<((Router, MemStore), (Router, MemStore))> {
    let (r1, store1, sp1) = node_test_setup_mem().await?;
    let (r2, store2, sp2) = node_test_setup_mem().await?;
    sp1.add_endpoint_info(r2.endpoint().addr());
    sp2.add_endpoint_info(r1.endpoint().addr());
    Ok(((r1, store1), (r2, store2)))
}

async fn two_nodes_hash_seq(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let addr1 = r1.endpoint().addr();
    let sizes = INTERESTING_SIZES;
    let root = add_test_hash_seq(store1, sizes).await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    store2.remote().fetch(conn, root).await?;
    check_presence(store2, &sizes).await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn two_nodes_hash_seq_fs() -> TestResult<()> {
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup_fs().await?;
    two_nodes_hash_seq(r1, &store1, r2, &store2).await
}

#[tokio::test]
#[traced_test]
async fn two_nodes_hash_seq_mem() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    two_nodes_hash_seq(r1, &store1, r2, &store2).await
}

#[tokio::test]
#[traced_test]
async fn two_nodes_hash_seq_progress() -> TestResult<()> {
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup_fs().await?;
    let addr1 = r1.endpoint().addr();
    let sizes = INTERESTING_SIZES;
    let root = add_test_hash_seq(&store1, sizes).await?;
    let conn = r2.endpoint().connect(addr1, crate::ALPN).await?;
    let mut stream = store2.remote().fetch(conn, root).stream();
    while stream.next().await.is_some() {}
    check_presence(&store2, &sizes).await?;
    Ok(())
}

/// A node serves a hash sequence with all the interesting sizes.
///
/// The client requests the hash sequence and the children, but does not store the data.
#[tokio::test]
#[traced_test]
async fn node_serve_hash_seq() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    let mut tts = Vec::new();
    // add all the sizes
    for size in sizes {
        let tt = store.add_bytes(test_data(size)).await?;
        tts.push(tt);
    }
    let hash_seq = tts.iter().map(|x| x.hash).collect::<HashSeq>();
    let root_tt = store.add_bytes(hash_seq).await?;
    let root = root_tt.hash;
    let endpoint = test_endpoint().await?;
    let blobs = crate::net_protocol::BlobsProtocol::new(&store, None);
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn();
    let addr1 = r1.endpoint().addr();
    info!("node addr: {addr1:?}");
    let endpoint2 = test_endpoint().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    let (hs, sizes) = get::request::get_hash_seq_and_sizes(&conn, &root, 1024, None).await?;
    println!("hash seq: {hs:?}");
    println!("sizes: {sizes:?}");
    r1.shutdown().await?;
    endpoint2.close().await;
    Ok(())
}

/// A node serves individual blobs with all the interesting sizes.
///
/// The client requests them all one by one, but does not store it.
#[tokio::test]
#[traced_test]
async fn node_serve_blobs() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let sizes = INTERESTING_SIZES;
    // add all the sizes
    let mut tts = Vec::new();
    for size in sizes {
        tts.push(store.add_bytes(test_data(size)).await?);
    }
    let endpoint = test_endpoint().await?;
    let blobs = crate::net_protocol::BlobsProtocol::new(&store, None);
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn();
    let addr1 = r1.endpoint().addr();
    info!("node addr: {addr1:?}");
    let endpoint2 = test_endpoint().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    for size in sizes {
        let expected = test_data(size);
        let hash = Hash::new(&expected);
        let mut stream = get::request::get_blob(conn.clone(), hash);
        while stream.next().await.is_some() {}
        let actual = get::request::get_blob(conn.clone(), hash).await?;
        assert_eq!(actual.len(), expected.len(), "size: {size}");
    }
    r1.shutdown().await?;
    endpoint2.close().await;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn node_smoke_fs() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    node_smoke(&store).await
}

#[tokio::test]
#[traced_test]
async fn node_smoke_mem() -> TestResult<()> {
    let store = crate::store::mem::MemStore::new();
    node_smoke(&store).await
}

async fn node_smoke(store: &Store) -> TestResult<()> {
    let tt = store.add_bytes(b"hello world".to_vec()).temp_tag().await?;
    let hash = tt.hash();
    let endpoint = test_endpoint().await?;
    let blobs = crate::net_protocol::BlobsProtocol::new(store, None);
    let r1 = Router::builder(endpoint)
        .accept(crate::protocol::ALPN, blobs)
        .spawn();
    let addr1 = r1.endpoint().addr();
    info!("node addr: {addr1:?}");
    let endpoint2 = test_endpoint().await?;
    let conn = endpoint2.connect(addr1, crate::protocol::ALPN).await?;
    let (size, stats) = get::request::get_unverified_size(&conn, &hash).await?;
    info!("size: {} stats: {:?}", size, stats);
    let data = get::request::get_blob(conn, hash).await?;
    assert_eq!(data.as_ref(), b"hello world");
    r1.shutdown().await?;
    endpoint2.close().await;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_export_chunk() -> TestResult {
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    let blobs = store.blobs();
    for size in [1024 * 18 + 1] {
        let data = vec![0u8; size];
        let tt = store.add_slice(&data).temp_tag().await?;
        let hash = tt.hash();
        let c = blobs.export_chunk(hash, 0).await;
        println!("{c:?}");
        let c = blobs.export_chunk(hash, 1000000).await;
        println!("{c:?}");
    }
    Ok(())
}

async fn test_export_ranges(
    store: &Store,
    hash: Hash,
    data: &[u8],
    range: Range<u64>,
) -> TestResult {
    let actual = store
        .export_ranges(hash, range.clone())
        .concatenate()
        .await?;
    let start = (range.start as usize).min(data.len());
    let end = (range.end as usize).min(data.len());
    assert_eq!(&actual, &data[start..end]);
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn export_ranges_smoke_fs() -> TestResult {
    let testdir = tempfile::tempdir()?;
    let db_path = testdir.path().join("db");
    let store = crate::store::fs::FsStore::load(&db_path).await?;
    export_ranges_smoke(&store).await
}

#[tokio::test]
#[traced_test]
async fn export_ranges_smoke_mem() -> TestResult {
    let store = MemStore::new();
    export_ranges_smoke(&store).await
}

async fn export_ranges_smoke(store: &Store) -> TestResult {
    let sizes = INTERESTING_SIZES;
    for size in sizes {
        let data = test_data(size);
        let tt = store.add_bytes(data.clone()).await?;
        let hash = tt.hash;
        let size = size as u64;
        test_export_ranges(store, hash, &data, 0..size).await?;
        test_export_ranges(store, hash, &data, 0..(size / 2)).await?;
        test_export_ranges(store, hash, &data, (size / 2)..size).await?;
        test_export_ranges(store, hash, &data, (size / 2)..(size + size / 2)).await?;
        test_export_ranges(store, hash, &data, size * 4..size * 5).await?;
    }
    Ok(())
}

/// A source stream that yields an error must make `add_stream` fail with
/// that error, not hang or import the truncated prefix.
async fn check_add_stream_source_error(store: &Store) -> TestResult<()> {
    let items: Vec<io::Result<Bytes>> = vec![
        Ok(Bytes::from_static(b"first chunk of data")),
        Err(io::Error::other("source stream failed")),
    ];
    let stream = n0_future::stream::iter(items);
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        store.blobs().add_stream(stream).await.temp_tag(),
    )
    .await?;
    let err = res.expect_err("add_stream must fail when the source fails");
    assert!(
        format!("{err:?}").contains("source stream failed"),
        "unexpected error: {err:?}"
    );
    // the store is still usable afterwards
    let _tt = store.add_bytes(b"after".to_vec()).temp_tag().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn mem_add_stream_source_error() -> TestResult<()> {
    let store = MemStore::new();
    check_add_stream_source_error(&store).await
}

#[tokio::test]
#[traced_test]
async fn fs_add_stream_source_error() -> TestResult<()> {
    let testdir = tempfile::tempdir()?;
    let store = FsStore::load(testdir.path().join("db")).await?;
    check_add_stream_source_error(&store).await?;
    store.shutdown().await?;
    Ok(())
}

/// `add_stream` takes a stream that is `Send` but not `Sync`.
#[tokio::test]
#[traced_test]
async fn mem_add_stream_not_sync() -> TestResult<()> {
    let store = MemStore::new();
    let items = std::cell::Cell::new(0);
    let stream = n0_future::stream::iter([Ok(Bytes::from_static(b"not sync"))]).map(move |item| {
        items.set(items.get() + 1);
        item
    });
    let tt = store.blobs().add_stream(stream).await.temp_tag().await?;
    assert_eq!(tt.hash(), Hash::new(b"not sync"));
    Ok(())
}

/// A store that shuts down in the middle of an export_ranges must not make
/// the export look complete.
#[tokio::test(flavor = "multi_thread")]
#[traced_test]
async fn mem_export_ranges_shutdown_mid_export() -> TestResult<()> {
    const TOTAL: u64 = 8 << 20;
    let data = (0..TOTAL).map(|i| (i % 251) as u8).collect::<Vec<_>>();

    // the stream ends with an error, unless all the data made it out
    let store = MemStore::new();
    let tt = store.add_slice(&data).temp_tag().await?;
    let mut stream = store.export_ranges(tt.hash(), 0..TOTAL).stream();
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
        if items.len() == 3 {
            store.shutdown().await?;
        }
    }
    let received = items
        .iter()
        .map(|item| match item {
            crate::api::proto::ExportRangesItem::Data(leaf) => leaf.data.len() as u64,
            _ => 0,
        })
        .sum::<u64>();
    let ends_with_error = matches!(
        items.last(),
        Some(crate::api::proto::ExportRangesItem::Error(_))
    );
    assert!(
        ends_with_error || received == TOTAL,
        "stream ended without error after {received} of {TOTAL} bytes"
    );

    // concatenate returns an error, unless it got all the data
    let store = MemStore::new();
    let tt = store.add_slice(&data).temp_tag().await?;
    let blobs = store.blobs().clone();
    let hash = tt.hash();
    let task = tokio::spawn(async move { blobs.export_ranges(hash, 0..TOTAL).concatenate().await });
    tokio::task::yield_now().await;
    store.shutdown().await?;
    if let Ok(res) = task.await? {
        assert_eq!(
            res.len() as u64,
            TOTAL,
            "concatenate returned truncated data"
        );
    }
    Ok(())
}

/// A store that shuts down in the middle of an export_bao must not make the
/// export look complete.
#[tokio::test(flavor = "multi_thread")]
#[traced_test]
async fn mem_export_bao_shutdown_mid_export() -> TestResult<()> {
    use bao_tree::io::mixed::EncodedItem;
    const TOTAL: u64 = 8 << 20;
    let data = (0..TOTAL).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let request = |hash| crate::api::proto::ExportBaoRequest {
        hash,
        ranges: ChunkRanges::all(),
    };

    // the stream ends with an error, unless all the data made it out
    let store = MemStore::new();
    let tt = store.add_slice(&data).temp_tag().await?;
    let mut stream = store.export_bao_with_opts(request(tt.hash()), 1).stream();
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
        if items.len() == 3 {
            store.shutdown().await?;
        }
    }
    let received = items
        .iter()
        .map(|item| match item {
            EncodedItem::Leaf(leaf) => leaf.data.len() as u64,
            _ => 0,
        })
        .sum::<u64>();
    let ends_with_error = matches!(items.last(), Some(EncodedItem::Error(_)));
    assert!(
        ends_with_error || received == TOTAL,
        "stream ended without error after {received} of {TOTAL} bytes"
    );

    // data_to_vec returns an error, unless it got all the data
    let store = MemStore::new();
    let tt = store.add_slice(&data).temp_tag().await?;
    let blobs = store.blobs().clone();
    let hash = tt.hash();
    let task = tokio::spawn(async move {
        blobs
            .export_bao_with_opts(request(hash), 1)
            .data_to_vec()
            .await
    });
    tokio::task::yield_now().await;
    store.shutdown().await?;
    if let Ok(res) = task.await? {
        assert_eq!(
            res.len() as u64,
            TOTAL,
            "data_to_vec returned truncated data"
        );
    }
    Ok(())
}

/// A provider that has an entry for a blob but no data for it, not even the
/// size, must answer like for a blob it has never seen, not with data that
/// fails verification.
async fn two_nodes_get_dataless_entry(
    r1: Router,
    store1: &Store,
    r2: Router,
    store2: &Store,
) -> TestResult<()> {
    let dataless = Hash::new(b"dataless");
    let unknown = Hash::new(b"unknown");
    // asking what is missing leaves an entry without data in the mem store
    store1.remote().local(HashAndFormat::raw(dataless)).await?;
    let res = store1
        .export_bao(dataless, ChunkRanges::all())
        .bao_to_vec()
        .await;
    assert!(
        matches!(
            &res,
            Err(RequestError::Inner {
                source: crate::api::Error::Io(cause),
                ..
            }) if cause.kind() == io::ErrorKind::NotFound
        ),
        "local export: {res:?}"
    );
    let conn = r2
        .endpoint()
        .connect(r1.endpoint().addr(), crate::ALPN)
        .await?;
    let get = |hash| {
        store2.remote().execute_get(
            conn.clone(),
            crate::protocol::GetRequest::from(HashAndFormat::raw(hash)),
        )
    };
    let Err(dataless_err) = get(dataless).await else {
        panic!("get of a dataless entry succeeded");
    };
    let Err(unknown_err) = get(unknown).await else {
        panic!("get of an unknown hash succeeded");
    };
    assert!(unknown_err.iroh_error_code().is_some());
    assert_eq!(
        dataless_err.iroh_error_code(),
        unknown_err.iroh_error_code(),
        "dataless: {dataless_err:?}, unknown: {unknown_err:?}"
    );
    tokio::try_join!(r1.shutdown(), r2.shutdown())?;
    Ok(())
}

#[tokio::test]
async fn two_nodes_get_dataless_entry_mem() -> TestResult<()> {
    let ((r1, store1), (r2, store2)) = two_node_test_setup_mem().await?;
    two_nodes_get_dataless_entry(r1, &store1, r2, &store2).await
}

#[tokio::test]
async fn two_nodes_get_dataless_entry_fs() -> TestResult<()> {
    let (_testdir, (r1, store1, _), (r2, store2, _)) = two_node_test_setup_fs().await?;
    two_nodes_get_dataless_entry(r1, &store1, r2, &store2).await
}
