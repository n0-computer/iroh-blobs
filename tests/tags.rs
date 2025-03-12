#![cfg(all(feature = "net_protocol", feature = "rpc"))]
use futures_lite::StreamExt;
use futures_util::Stream;
use iroh::Endpoint;
use iroh_blobs::{
    net_protocol::Blobs,
    rpc::{
        client::tags::{self, TagInfo},
        proto::RpcService,
    },
    Hash, HashAndFormat,
};
use testresult::TestResult;

async fn to_vec<T>(stream: impl Stream<Item = anyhow::Result<T>>) -> anyhow::Result<Vec<T>> {
    let res = stream.collect::<Vec<_>>().await;
    res.into_iter().collect::<anyhow::Result<Vec<_>>>()
}

fn expected(tags: impl IntoIterator<Item = &'static str>) -> Vec<TagInfo> {
    tags.into_iter()
        .map(|tag| TagInfo::new(tag, Hash::new(tag)))
        .collect()
}

async fn set<C: quic_rpc::Connector<RpcService>>(
    tags: &tags::Client<C>,
    names: impl IntoIterator<Item = &str>,
) -> TestResult<()> {
    for name in names {
        tags.set(name, Hash::new(name)).await?;
    }
    Ok(())
}

async fn tags_smoke<C: quic_rpc::Connector<RpcService>>(tags: tags::Client<C>) -> TestResult<()> {
    set(&tags, ["a", "b", "c", "d", "e"]).await?;
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

    set(&tags, ["a", "aa", "aaa", "aab", "b"]).await?;

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

    set(&tags, ["a", "b", "c"]).await?;

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
            format: iroh_blobs::BlobFormat::HashSeq,
        }]
    );

    tags.delete_all().await?;
    set(&tags, ["c"]).await?;
    tags.rename("c", "f").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(
        res,
        vec![TagInfo {
            name: "f".into(),
            hash: Hash::new("c"),
            format: iroh_blobs::BlobFormat::Raw,
        }]
    );

    let res = tags.rename("y", "z").await;
    assert!(res.is_err());
    Ok(())
}

#[tokio::test]
async fn tags_smoke_mem() -> TestResult<()> {
    let endpoint = Endpoint::builder().bind().await?;
    let blobs = Blobs::memory().build(&endpoint);
    let client = blobs.client();
    tags_smoke(client.tags()).await
}

#[tokio::test]
async fn tags_smoke_fs() -> TestResult<()> {
    let td = tempfile::tempdir()?;
    let endpoint = Endpoint::builder().bind().await?;
    let blobs = Blobs::persistent(td.path().join("blobs.db"))
        .await?
        .build(&endpoint);
    let client = blobs.client();
    tags_smoke(client.tags()).await
}
