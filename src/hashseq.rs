//! Helpers for blobs that contain a sequence of hashes.
use std::fmt::Debug;

use range_collections::range_set::RangeSetRange;

use bao_tree::{ChunkNum, ChunkRanges};
use bytes::Bytes;
use n0_error::{anyerr, e, stack_error, AnyError};

use crate::{
    api::{blobs::Blobs, RequestError},
    Hash,
};

/// A sequence of links, backed by a [`Bytes`] object.
#[derive(Clone, derive_more::Into)]
pub struct HashSeq(Bytes);

impl Debug for HashSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> FromIterator<&'a Hash> for HashSeq {
    fn from_iter<T: IntoIterator<Item = &'a Hash>>(iter: T) -> Self {
        iter.into_iter().copied().collect()
    }
}

impl FromIterator<Hash> for HashSeq {
    fn from_iter<T: IntoIterator<Item = Hash>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _upper) = iter.size_hint();
        let mut bytes = Vec::with_capacity(lower * 32);
        for hash in iter {
            bytes.extend_from_slice(hash.as_ref());
        }
        Self(bytes.into())
    }
}

impl TryFrom<Bytes> for HashSeq {
    type Error = AnyError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::new(bytes).ok_or_else(|| anyerr!("invalid hash sequence"))
    }
}

impl IntoIterator for HashSeq {
    type Item = Hash;
    type IntoIter = HashSeqIter;

    fn into_iter(self) -> Self::IntoIter {
        HashSeqIter(self)
    }
}

impl HashSeq {
    /// Create a new sequence of hashes.
    pub fn new(bytes: Bytes) -> Option<Self> {
        if bytes.len().is_multiple_of(32) {
            Some(Self(bytes))
        } else {
            None
        }
    }

    /// Iterate over the hashes in this sequence.
    pub fn iter(&self) -> impl Iterator<Item = Hash> + '_ {
        let (hashes, _) = self.0.as_chunks::<32>();
        hashes.iter().map(|hash| Hash::from(*hash))
    }

    /// Get the number of hashes in this sequence.
    pub fn len(&self) -> usize {
        self.0.len() / 32
    }

    /// Check if this sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get the hash at the given index.
    pub fn get(&self, index: usize) -> Option<Hash> {
        if index < self.len() {
            let hash: [u8; 32] = self.0[index * 32..(index + 1) * 32].try_into().unwrap();
            Some(hash.into())
        } else {
            None
        }
    }

    /// Get and remove the first hash in this sequence.
    pub fn pop_front(&mut self) -> Option<Hash> {
        if self.is_empty() {
            None
        } else {
            let hash = self.get(0).unwrap();
            self.0 = self.0.slice(32..);
            Some(hash)
        }
    }

    /// Get the underlying bytes.
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

/// Iterator over the hashes in a [`HashSeq`].
#[derive(Debug, Clone)]
pub struct HashSeqIter(HashSeq);

impl Iterator for HashSeqIter {
    type Item = Hash;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

/// Number of hashes [`LazyHashSeq`] reads from the store at a time.
const WINDOW: u64 = 1024;

/// Error when reading a [`LazyHashSeq`].
#[stack_error(derive, add_meta, from_sources)]
pub(crate) enum LazyHashSeqError {
    #[error(transparent)]
    Request { source: RequestError },
    #[error("invalid hash sequence")]
    InvalidHashSeq {},
}

/// A hash sequence in the store that is read in windows of [`WINDOW`] hashes,
/// so it never has to be loaded into memory as a whole.
///
/// Optimized for sequential access. Accessing an index outside the current
/// window loads the window starting at that index.
#[derive(Debug)]
pub(crate) struct LazyHashSeq {
    blobs: Blobs,
    hash: Hash,
    /// Index of the first hash in `window`.
    start: u64,
    window: HashSeq,
}

impl LazyHashSeq {
    pub fn new(blobs: Blobs, hash: Hash) -> Self {
        Self {
            blobs,
            hash,
            start: 0,
            window: HashSeq(Bytes::new()),
        }
    }

    /// Get the hash at `index`, or `None` if the sequence is shorter.
    pub async fn get(&mut self, index: u64) -> Result<Option<Hash>, LazyHashSeqError> {
        let end = self.start + self.window.len() as u64;
        if index < self.start || index >= end {
            self.load(index).await?;
        }
        let i = usize::try_from(index - self.start).ok();
        Ok(i.and_then(|i| self.window.get(i)))
    }

    async fn load(&mut self, index: u64) -> Result<(), LazyHashSeqError> {
        self.start = index;
        self.window = HashSeq(Bytes::new());
        let Some(start) = index.checked_mul(32) else {
            // no blob is that large
            return Ok(());
        };
        // only read data that is present, so a hole after `index` does not
        // fail the whole window
        let bitfield = self.blobs.observe(self.hash).await?;
        let end = present_end(&bitfield.ranges, start).min(start.saturating_add(WINDOW * 32));
        let bytes = self
            .blobs
            .export_ranges(self.hash, start..end)
            .concatenate()
            .await?;
        self.window =
            HashSeq::new(bytes.into()).ok_or_else(|| e!(LazyHashSeqError::InvalidHashSeq))?;
        Ok(())
    }
}

/// End of the run of present chunks that contains `offset`, in bytes.
///
/// If `offset` is not present, this returns the end of the hash at `offset`,
/// so that reading it reports the missing data.
fn present_end(ranges: &ChunkRanges, offset: u64) -> u64 {
    let chunk = ChunkNum::full_chunks(offset);
    for range in ranges.iter() {
        match range {
            RangeSetRange::Range(r) if *r.start <= chunk && chunk < *r.end => {
                return r.end.to_bytes();
            }
            RangeSetRange::RangeFrom(r) if *r.start <= chunk => return u64::MAX,
            _ => {}
        }
    }
    offset + 32
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use bao_tree::{ChunkNum, ChunkRanges};

    use super::{LazyHashSeq, WINDOW};
    use crate::{
        hashseq::HashSeq,
        store::{mem::MemStore, util::tests::create_n0_bao},
        Hash,
    };

    #[tokio::test]
    async fn lazy_hash_seq() -> TestResult<()> {
        let store = MemStore::new();
        let n = WINDOW * 2 + 1;
        let hashes = (0..n)
            .map(|i| Hash::new(i.to_le_bytes()))
            .collect::<Vec<_>>();
        let hs = hashes.iter().collect::<HashSeq>();
        let tt = store.add_bytes(hs.clone().into_inner()).await?;
        let mut lazy = LazyHashSeq::new(store.blobs().clone(), tt.hash);
        for (i, hash) in hashes.iter().enumerate() {
            assert_eq!(lazy.get(i as u64).await?, Some(*hash));
        }
        assert_eq!(lazy.get(n).await?, None);
        assert_eq!(lazy.get(u64::MAX).await?, None);
        // only the first block of 512 hashes is present
        let store = MemStore::new();
        let ranges = ChunkRanges::from(..ChunkNum(16));
        let (hash, bao) = create_n0_bao(hs.into_inner().as_ref(), &ranges)?;
        store.import_bao_bytes(hash, ranges, bao).await?;
        let mut lazy = LazyHashSeq::new(store.blobs().clone(), hash);
        assert_eq!(lazy.get(511).await?, Some(hashes[511]));
        assert!(lazy.get(512).await.is_err());
        // not a multiple of 32
        let tt = store.add_bytes(vec![0u8; 33]).await?;
        let mut lazy = LazyHashSeq::new(store.blobs().clone(), tt.hash);
        assert!(lazy.get(0).await.is_err());
        Ok(())
    }
}
