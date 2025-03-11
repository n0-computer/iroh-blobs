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
    BlobFormat, Hash,
};
use testresult::TestResult;

async fn to_vec<T>(stream: impl Stream<Item = anyhow::Result<T>>) -> anyhow::Result<Vec<T>> {
    let res = stream.collect::<Vec<_>>().await;
    res.into_iter().collect::<anyhow::Result<Vec<_>>>()
}

fn expected(tags: impl IntoIterator<Item = &'static str>) -> Vec<TagInfo> {
    tags.into_iter()
        .map(|tag| TagInfo {
            name: tag.into(),
            hash: Hash::new(tag),
            format: BlobFormat::Raw,
        })
        .collect()
}

async fn tags_smoke<C: quic_rpc::Connector<RpcService>>(tags: tags::Client<C>) -> TestResult<()> {
    tags.set("a", Hash::new("a")).await?;
    tags.set("b", Hash::new("b")).await?;
    tags.set("c", Hash::new("c")).await?;
    tags.set("d", Hash::new("d")).await?;
    tags.set("e", Hash::new("e")).await?;
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

    tags.set("a", Hash::new("a")).await?;
    tags.set("aa", Hash::new("aa")).await?;
    tags.set("aaa", Hash::new("aaa")).await?;
    tags.set("aab", Hash::new("aab")).await?;
    tags.set("b", Hash::new("b")).await?;

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

    tags.set("a", Hash::new("a")).await?;
    tags.set("b", Hash::new("b")).await?;
    tags.set("c", Hash::new("c")).await?;

    assert_eq!(
        tags.get("b").await?,
        Some(TagInfo {
            name: "b".into(),
            hash: Hash::new("b"),
            format: BlobFormat::Raw,
        })
    );

    tags.delete("b").await?;
    let stream = tags.list().await?;
    let res = to_vec(stream).await?;
    assert_eq!(res, expected(["a", "c"]));

    assert_eq!(tags.get("b").await?, None);

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
