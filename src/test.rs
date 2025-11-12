use std::future::IntoFuture;

use n0_future::{stream, StreamExt};
use rand::{RngCore, SeedableRng};

use crate::{
    api::{blobs::AddBytesOptions, tags::TagInfo, RequestResult, Store},
    hashseq::HashSeq,
    BlobFormat,
};

pub async fn create_random_blobs<R: rand::Rng>(
    store: &Store,
    num_blobs: usize,
    blob_size: impl Fn(usize, &mut R) -> usize,
    mut rand: R,
) -> n0_error::Result<Vec<TagInfo>> {
    // generate sizes and seeds, non-parrallelized so it is deterministic
    let sizes = (0..num_blobs)
        .map(|n| (blob_size(n, &mut rand), rand.random::<u64>()))
        .collect::<Vec<_>>();
    // generate random data and add it to the store
    let infos = stream::iter(sizes)
        .then(|(size, seed)| {
            let mut rand = rand::rngs::StdRng::seed_from_u64(seed);
            let mut data = vec![0u8; size];
            rand.fill_bytes(&mut data);
            store.add_bytes(data).into_future()
        })
        .collect::<Vec<_>>()
        .await;
    let infos = infos.into_iter().collect::<RequestResult<Vec<_>>>()?;
    Ok(infos)
}

pub async fn add_hash_sequences<R: rand::Rng>(
    store: &Store,
    tags: &[TagInfo],
    num_seqs: usize,
    seq_size: impl Fn(usize, &mut R) -> usize,
    mut rand: R,
) -> n0_error::Result<Vec<TagInfo>> {
    let infos = stream::iter(0..num_seqs)
        .then(|n| {
            let size = seq_size(n, &mut rand);
            let hs = (0..size)
                .map(|_| {
                    let j = rand.random_range(0..tags.len());
                    tags[j].hash
                })
                .collect::<HashSeq>();
            store
                .add_bytes_with_opts(AddBytesOptions {
                    data: hs.into(),
                    format: BlobFormat::HashSeq,
                })
                .into_future()
        })
        .collect::<Vec<_>>()
        .await;
    let infos = infos.into_iter().collect::<RequestResult<Vec<_>>>()?;
    Ok(infos)
}
