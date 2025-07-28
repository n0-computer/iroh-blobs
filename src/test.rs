use std::future::IntoFuture;

use bao_tree::{
    io::{outboard::PreOrderMemOutboard, round_up_to_chunks_groups},
    ChunkNum, ChunkRanges,
};
use bytes::Bytes;
use n0_future::{stream, StreamExt};
use rand::{RngCore, SeedableRng};

use crate::{
    api::{blobs::AddBytesOptions, tags::TagInfo, RequestResult, Store},
    hashseq::HashSeq,
    store::IROH_BLOCK_SIZE,
    BlobFormat, Hash,
};

pub async fn create_random_blobs<R: rand::Rng>(
    store: &Store,
    num_blobs: usize,
    blob_size: impl Fn(usize, &mut R) -> usize,
    mut rand: R,
) -> anyhow::Result<Vec<TagInfo>> {
    // generate sizes and seeds, non-parrallelized so it is deterministic
    let sizes = (0..num_blobs)
        .map(|n| (blob_size(n, &mut rand), rand.r#gen::<u64>()))
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
) -> anyhow::Result<Vec<TagInfo>> {
    let infos = stream::iter(0..num_seqs)
        .then(|n| {
            let size = seq_size(n, &mut rand);
            let hs = (0..size)
                .map(|_| {
                    let j = rand.gen_range(0..tags.len());
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

/// Generate test data for size n.
///
/// We don't really care about the content, since we assume blake3 works.
/// The only thing it should not be is all zeros, since that is what you
/// will get for a gap.
pub(crate) fn test_data(n: usize) -> Bytes {
    let mut res = Vec::with_capacity(n);
    // Using uppercase A-Z (65-90), 26 possible characters
    for i in 0..n {
        // Change character every 1024 bytes
        let block_num = i / 1024;
        // Map to uppercase A-Z range (65-90)
        let ascii_val = 65 + (block_num % 26) as u8;
        res.push(ascii_val);
    }
    Bytes::from(res)
}

/// Interesting sizes for testing.
pub const INTERESTING_SIZES: [usize; 8] = [
    0,               // annoying corner case - always present, handled by the api
    1,               // less than 1 chunk, data inline, outboard not needed
    1024,            // exactly 1 chunk, data inline, outboard not needed
    1024 * 16 - 1,   // less than 1 chunk group, data inline, outboard not needed
    1024 * 16,       // exactly 1 chunk group, data inline, outboard not needed
    1024 * 16 + 1,   // data file, outboard inline (just 1 hash pair)
    1024 * 1024,     // data file, outboard inline (many hash pairs)
    1024 * 1024 * 8, // data file, outboard file
];

/// Create n0 flavoured bao. Note that this can be used to request ranges below a chunk group size,
/// which can not be exported via bao because we don't store hashes below the chunk group level.
pub(crate) fn create_n0_bao(data: &[u8], ranges: &ChunkRanges) -> anyhow::Result<(Hash, Vec<u8>)> {
    let outboard = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
    let mut encoded = Vec::new();
    let size = data.len() as u64;
    encoded.extend_from_slice(&size.to_le_bytes());
    bao_tree::io::sync::encode_ranges_validated(data, &outboard, ranges, &mut encoded)?;
    Ok((outboard.root.into(), encoded))
}

pub(crate) fn round_up_request(size: u64, ranges: &ChunkRanges) -> ChunkRanges {
    let last_chunk = ChunkNum::chunks(size);
    let data_range = ChunkRanges::from(..last_chunk);
    let ranges = if !data_range.intersects(ranges) && !ranges.is_empty() {
        if last_chunk == 0 {
            ChunkRanges::all()
        } else {
            ChunkRanges::from(last_chunk - 1..)
        }
    } else {
        ranges.clone()
    };
    round_up_to_chunks_groups(ranges, IROH_BLOCK_SIZE)
}

pub(crate) fn create_n0_bao_full(
    data: &[u8],
    ranges: &ChunkRanges,
) -> anyhow::Result<(Hash, ChunkRanges, Vec<u8>)> {
    let ranges = round_up_request(data.len() as u64, ranges);
    let (hash, encoded) = create_n0_bao(data, &ranges)?;
    Ok((hash, ranges, encoded))
}
