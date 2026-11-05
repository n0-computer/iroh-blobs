#![cfg(feature = "fs-store")]
use std::{collections::HashSet, time::Duration};
#[cfg(feature = "rpc")]
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    ops::Deref,
};

use iroh_blobs::{
    api::{
        self,
        tags::{TagInfo, Tags},
        Store,
    },
    store::{fs::FsStore, mem::MemStore},
    BlobFormat, Hash, HashAndFormat,
};
use n0_future::{Stream, StreamExt};
use n0_tracing_test::traced_test;
use testresult::TestResult;

async fn to_vec<T>(stream: impl Stream<Item = api::Result<T>>) -> api::Result<Vec<T>> {
    let res = stream.collect::<Vec<_>>().await;
    res.into_iter().collect::<api::Result<Vec<_>>>()
}

fn expected(tags: impl IntoIterator<Item = &'static str>) -> Vec<TagInfo> {
    tags.into_iter()
        .map(|tag| TagInfo::new(tag, Hash::new(tag)))
        .collect()
}

async fn set(tags: &Tags, names: impl IntoIterator<Item = &str>) -> TestResult<()> {
    for name in names {
        tags.set(name, Hash::new(name)).await?;
    }
    Ok(())
}

async fn tags_smoke(tags: &Tags) -> TestResult<()> {
    set(tags, ["a", "b", "c", "d", "e"]).await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "b", "c", "d", "e"]));

    let stream = tags.list_range("b".."d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["b", "c"]));

    let stream = tags.list_range("b"..).await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["b", "c", "d", "e"]));

    let stream = tags.list_range(.."d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "b", "c"]));

    let stream = tags.list_range(..="d").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "b", "c", "d"]));

    tags.delete_range("b"..).await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a"]));

    tags.delete_range(..="a").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected([]));

    set(tags, ["a", "aa", "aaa", "aab", "b"]).await?;

    let stream = tags.list_prefix("aa").await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["aa", "aaa", "aab"]));

    tags.delete_prefix("aa").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "b"]));

    tags.delete_prefix("").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected([]));

    set(tags, ["a", "b", "c"]).await?;

    assert_eq!(
        tags.get("b").await?,
        Some(TagInfo::new("b", Hash::new("b")))
    );

    tags.delete("b").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "c"]));

    assert_eq!(tags.get("b").await?, None);

    tags.delete_all().await?;

    tags.set("a", HashAndFormat::hash_seq(Hash::new("a")))
        .await?;
    tags.set("b", HashAndFormat::raw(Hash::new("b"))).await?;
    let stream = tags.list_hash_seq().await?;
    let res = to_vec(stream).await?;
    assert_eq!(
        res,
        vec![TagInfo {
            name: "a".into(),
            hash: Hash::new("a"),
            format: BlobFormat::HashSeq,
        }]
    );

    tags.delete_all().await?;
    set(tags, ["c"]).await?;
    tags.rename("c", "f").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(
        res,
        vec![TagInfo {
            name: "f".into(),
            hash: Hash::new("c"),
            format: BlobFormat::Raw,
        }]
    );

    let res = tags.rename("y", "z").await;
    assert!(res.is_err());
    Ok(())
}

/// Listings larger than the channel capacity must not block other store requests.
async fn listings_keep_store_responsive(store: &Store) -> TestResult<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let batch = store.batch().await?;
        let mut guards = Vec::new();
        for i in 0..96 {
            let name = format!("stream/{i:03}");
            let hash = Hash::new(&name);
            store.tags().set(&name, hash).await?;
            guards.push(batch.temp_tag(hash).await?);
        }

        let mut named = store.tags().list_prefix("stream/").await?;
        assert!(named.next().await.transpose()?.is_some());
        store.tags().set("after-list", Hash::new("after")).await?;
        drop(named);
        assert_eq!(
            to_vec(store.tags().list_prefix("stream/").await?)
                .await?
                .len(),
            96
        );

        let mut temporary = store.tags().list_temp_tags().await?;
        assert!(temporary.next().await.transpose()?.is_some());
        store.sync_db().await?;
        drop(temporary);
        let actual: HashSet<_> = to_vec(store.tags().list_temp_tags().await?)
            .await?
            .into_iter()
            .collect();
        let expected: HashSet<_> = guards.iter().map(|tag| tag.hash_and_format()).collect();
        assert_eq!(actual, expected);

        drop(guards);
        drop(batch);
        // Cancelled listing tasks must finish as well.
        store.wait_idle().await?;
        TestResult::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn tags_smoke_mem() -> TestResult<()> {
    let store = MemStore::new();
    tags_smoke(store.tags()).await?;
    listings_keep_store_responsive(&store).await
}

#[tokio::test]
#[traced_test]
async fn tags_smoke_fs() -> TestResult<()> {
    let td = tempfile::tempdir()?;
    let store = FsStore::load(td.path().join("a")).await?;
    tags_smoke(store.tags()).await?;
    listings_keep_store_responsive(&store).await
}

#[tokio::test]
#[traced_test]
#[cfg(feature = "rpc")]
async fn tags_smoke_fs_rpc() -> TestResult<()> {
    let unspecified = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    let (server, cert) = irpc::util::make_server_endpoint(unspecified)?;
    let client = irpc::util::make_client_endpoint(unspecified, &[cert.as_ref()])?;
    let td = tempfile::tempdir()?;
    let store = FsStore::load(td.path().join("a")).await?;
    n0_future::task::spawn(store.deref().clone().listen(server.clone()));
    let api = Store::connect(client, server.local_addr()?);
    tags_smoke(api.tags()).await?;
    listings_keep_store_responsive(&api).await?;
    api.shutdown().await?;
    Ok(())
}
