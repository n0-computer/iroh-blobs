//! Helpers for blobs that contain a sequence of hashes.
use std::fmt::Debug;

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
///
/// This is one 1 KiB chunk, the unit in which the store tracks which data is
/// present. So a window is either present or missing as a whole. Note that
/// this is smaller than a chunk group, since a chunk can be present without
/// the rest of its chunk group.
const WINDOW: u64 = 32;

/// Error when reading a [`LazyHashSeq`].
#[stack_error(derive, add_meta, from_sources)]
pub(crate) enum LazyHashSeqError {
    #[error(transparent)]
    Request { source: RequestError },
    #[error("invalid hash sequence")]
    InvalidHashSeq {},
}

/// A hash sequence in the store that is read one chunk at a time, so it never
/// has to be loaded into memory as a whole.
///
/// Optimized for sequential access. Accessing an index outside the current
/// chunk loads the chunk containing that index.
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
        self.start = index - index % WINDOW;
        self.window = HashSeq(Bytes::new());
        let Some(start) = self.start.checked_mul(32) else {
            // no blob is that large
            return Ok(());
        };
        let (size, bytes) = self
            .blobs
            .export_ranges(self.hash, start..start.saturating_add(WINDOW * 32))
            .concatenate_with_size()
            .await?;
        if size.is_some_and(|size| size % 32 != 0) {
            return Err(e!(LazyHashSeqError::InvalidHashSeq));
        }
        self.window =
            HashSeq::new(bytes.into()).ok_or_else(|| e!(LazyHashSeqError::InvalidHashSeq))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use bao_tree::{ChunkNum, ChunkRanges};

    use super::LazyHashSeq;
    use crate::{
        hashseq::HashSeq,
        store::{mem::MemStore, util::tests::create_n0_bao},
        Hash,
    };

    #[tokio::test]
    async fn lazy_hash_seq() -> TestResult<()> {
        let store = MemStore::new();
        let n = 1000u64;
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
        // only chunk 3, with hashes 96..128, is present, not the rest of its
        // chunk group
        let store = MemStore::new();
        let ranges = ChunkRanges::from(ChunkNum(3)..ChunkNum(4));
        let (hash, bao) = create_n0_bao(hs.into_inner().as_ref(), &ranges)?;
        store.import_bao_bytes(hash, ranges, bao).await?;
        let mut lazy = LazyHashSeq::new(store.blobs().clone(), hash);
        assert!(lazy.get(95).await.is_err());
        for i in 96..128 {
            assert_eq!(lazy.get(i).await?, Some(hashes[i as usize]));
        }
        assert!(lazy.get(128).await.is_err());
        // not a multiple of 32, detected from the size even though the first
        // chunk is a valid hash seq
        let tt = store.add_bytes(vec![0u8; 1025]).await?;
        let mut lazy = LazyHashSeq::new(store.blobs().clone(), tt.hash);
        assert!(lazy.get(0).await.is_err());
        Ok(())
    }
}
