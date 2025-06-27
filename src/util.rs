use std::ops::{Bound, RangeBounds};

use bao_tree::{io::round_up_to_chunks, ChunkNum, ChunkRanges};
use range_collections::{range_set::RangeSetEntry, RangeSet2};

pub mod channel;
pub(crate) mod temp_tag;
pub mod serde {
    // Module that handles io::Error serialization/deserialization
    pub mod io_error_serde {
        use std::{fmt, io};

        use serde::{
            de::{self, Visitor},
            Deserializer, Serializer,
        };

        pub fn serialize<S>(error: &io::Error, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            // Serialize the error kind and message
            serializer.serialize_str(&format!("{:?}:{}", error.kind(), error))
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<io::Error, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct IoErrorVisitor;

            impl<'de> Visitor<'de> for IoErrorVisitor {
                type Value = io::Error;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("an io::Error string representation")
                }

                fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
                {
                    // For simplicity, create a generic error
                    // In a real app, you might want to parse the kind from the string
                    Ok(io::Error::other(value))
                }
            }

            deserializer.deserialize_str(IoErrorVisitor)
        }
    }
}

pub trait ChunkRangesExt {
    fn last_chunk() -> Self;
    fn chunk(offset: u64) -> Self;
    fn bytes(ranges: impl RangeBounds<u64>) -> Self;
    fn chunks(ranges: impl RangeBounds<u64>) -> Self;
    fn offset(offset: u64) -> Self;
}

impl ChunkRangesExt for ChunkRanges {
    fn last_chunk() -> Self {
        ChunkRanges::from(ChunkNum(u64::MAX)..)
    }

    /// Create a chunk range that contains a single chunk.
    fn chunk(offset: u64) -> Self {
        ChunkRanges::from(ChunkNum(offset)..ChunkNum(offset + 1))
    }

    /// Create a range of chunks that contains the given byte ranges.
    /// The byte ranges are rounded up to the nearest chunk size.
    fn bytes(ranges: impl RangeBounds<u64>) -> Self {
        round_up_to_chunks(&bounds_from_range(ranges, |v| v))
    }

    /// Create a range of chunks from u64 chunk bounds.
    ///
    /// This is equivalent but more convenient than using the ChunkNum newtype.
    fn chunks(ranges: impl RangeBounds<u64>) -> Self {
        bounds_from_range(ranges, ChunkNum)
    }

    /// Create a chunk range that contains a single byte offset.
    fn offset(offset: u64) -> Self {
        Self::bytes(offset..offset + 1)
    }
}

// todo: move to range_collections
pub(crate) fn bounds_from_range<R, T, F>(range: R, f: F) -> RangeSet2<T>
where
    R: RangeBounds<u64>,
    T: RangeSetEntry,
    F: Fn(u64) -> T,
{
    let from = match range.start_bound() {
        Bound::Included(start) => Some(*start),
        Bound::Excluded(start) => {
            let Some(start) = start.checked_add(1) else {
                return RangeSet2::empty();
            };
            Some(start)
        }
        Bound::Unbounded => None,
    };
    let to = match range.end_bound() {
        Bound::Included(end) => end.checked_add(1),
        Bound::Excluded(end) => Some(*end),
        Bound::Unbounded => None,
    };
    match (from, to) {
        (Some(from), Some(to)) => RangeSet2::from(f(from)..f(to)),
        (Some(from), None) => RangeSet2::from(f(from)..),
        (None, Some(to)) => RangeSet2::from(..f(to)),
        (None, None) => RangeSet2::all(),
    }
}

pub mod outboard_with_progress {
    use std::io::{self, BufReader, Read};

    use bao_tree::{
        blake3,
        io::{
            outboard::PreOrderOutboard,
            sync::{OutboardMut, WriteAt},
        },
        iter::BaoChunk,
        BaoTree, ChunkNum,
    };
    use smallvec::SmallVec;

    use super::sink::Sink;

    fn hash_subtree(start_chunk: u64, data: &[u8], is_root: bool) -> blake3::Hash {
        use blake3::hazmat::{ChainingValue, HasherExt};
        if is_root {
            debug_assert!(start_chunk == 0);
            blake3::hash(data)
        } else {
            let mut hasher = blake3::Hasher::new();
            hasher.set_input_offset(start_chunk * 1024);
            hasher.update(data);
            let non_root_hash: ChainingValue = hasher.finalize_non_root();
            blake3::Hash::from(non_root_hash)
        }
    }

    fn parent_cv(
        left_child: &blake3::Hash,
        right_child: &blake3::Hash,
        is_root: bool,
    ) -> blake3::Hash {
        use blake3::hazmat::{merge_subtrees_non_root, merge_subtrees_root, ChainingValue, Mode};
        let left_child: ChainingValue = *left_child.as_bytes();
        let right_child: ChainingValue = *right_child.as_bytes();
        if is_root {
            merge_subtrees_root(&left_child, &right_child, Mode::Hash)
        } else {
            blake3::Hash::from(merge_subtrees_non_root(
                &left_child,
                &right_child,
                Mode::Hash,
            ))
        }
    }

    pub async fn init_outboard<R, W, P>(
        data: R,
        outboard: &mut PreOrderOutboard<W>,
        progress: &mut P,
    ) -> std::io::Result<std::result::Result<(), P::Error>>
    where
        W: WriteAt,
        R: Read,
        P: Sink<ChunkNum>,
    {
        // wrap the reader in a buffered reader, so we read in large chunks
        // this reduces the number of io ops
        let size = usize::try_from(outboard.tree.size()).unwrap_or(usize::MAX);
        let read_buf_size = size.min(1024 * 1024);
        let chunk_buf_size = size.min(outboard.tree.block_size().bytes());
        let reader = BufReader::with_capacity(read_buf_size, data);
        let mut buffer = SmallVec::<[u8; 128]>::from_elem(0u8, chunk_buf_size);
        let res = init_impl(outboard.tree, reader, outboard, &mut buffer, progress).await?;
        Ok(res)
    }

    async fn init_impl<W, P>(
        tree: BaoTree,
        mut data: impl Read,
        outboard: &mut PreOrderOutboard<W>,
        buffer: &mut [u8],
        progress: &mut P,
    ) -> io::Result<std::result::Result<(), P::Error>>
    where
        W: WriteAt,
        P: Sink<ChunkNum>,
    {
        // do not allocate for small trees
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        // debug_assert!(buffer.len() == tree.chunk_group_bytes());
        for item in tree.post_order_chunks_iter() {
            match item {
                BaoChunk::Parent { is_root, node, .. } => {
                    let right_hash = stack.pop().unwrap();
                    let left_hash = stack.pop().unwrap();
                    outboard.save(node, &(left_hash, right_hash))?;
                    let parent = parent_cv(&left_hash, &right_hash, is_root);
                    stack.push(parent);
                }
                BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                    ..
                } => {
                    if let Err(err) = progress.send(start_chunk).await {
                        return Ok(Err(err));
                    }
                    let buf = &mut buffer[..size];
                    data.read_exact(buf)?;
                    let hash = hash_subtree(start_chunk.0, buf, is_root);
                    stack.push(hash);
                }
            }
        }
        debug_assert_eq!(stack.len(), 1);
        outboard.root = stack.pop().unwrap();
        Ok(Ok(()))
    }

    #[cfg(test)]
    mod tests {
        use bao_tree::{
            blake3,
            io::{outboard::PreOrderOutboard, sync::CreateOutboard},
            BaoTree,
        };
        use testresult::TestResult;

        use crate::{
            store::{fs::tests::test_data, IROH_BLOCK_SIZE},
            util::{outboard_with_progress::init_outboard, sink::Drain},
        };

        #[tokio::test]
        async fn init_outboard_with_progress() -> TestResult<()> {
            for size in [1024 * 18 + 1] {
                let data = test_data(size);
                let mut o1 = PreOrderOutboard::<Vec<u8>> {
                    tree: BaoTree::new(data.len() as u64, IROH_BLOCK_SIZE),
                    ..Default::default()
                };
                let mut o2 = o1.clone();
                o1.init_from(data.as_ref())?;
                init_outboard(data.as_ref(), &mut o2, &mut Drain).await??;
                assert_eq!(o1.root, blake3::hash(&data));
                assert_eq!(o1.root, o2.root);
                assert_eq!(o1.data, o2.data);
            }
            Ok(())
        }
    }
}

pub mod sink {
    use std::{future::Future, io};

    use irpc::RpcMessage;

    /// Our version of a sink, that can be mapped etc.
    pub trait Sink<Item> {
        type Error;
        fn send(
            &mut self,
            value: Item,
        ) -> impl Future<Output = std::result::Result<(), Self::Error>>;

        fn with_map_err<F, U>(self, f: F) -> WithMapErr<Self, F>
        where
            Self: Sized,
            F: Fn(Self::Error) -> U + Send + 'static,
        {
            WithMapErr { inner: self, f }
        }

        fn with_map<F, U>(self, f: F) -> WithMap<Self, F>
        where
            Self: Sized,
            F: Fn(U) -> Item + Send + 'static,
        {
            WithMap { inner: self, f }
        }
    }

    impl<I, T> Sink<T> for &mut I
    where
        I: Sink<T>,
    {
        type Error = I::Error;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            (*self).send(value).await
        }
    }

    pub struct IrpcSenderSink<T>(pub irpc::channel::mpsc::Sender<T>);

    impl<T> Sink<T> for IrpcSenderSink<T>
    where
        T: RpcMessage,
    {
        type Error = irpc::channel::SendError;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            self.0.send(value).await
        }
    }

    pub struct IrpcSenderRefSink<'a, T>(pub &'a mut irpc::channel::mpsc::Sender<T>);

    impl<'a, T> Sink<T> for IrpcSenderRefSink<'a, T>
    where
        T: RpcMessage,
    {
        type Error = irpc::channel::SendError;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            self.0.send(value).await
        }
    }

    pub struct TokioMpscSenderSink<T>(pub tokio::sync::mpsc::Sender<T>);

    impl<T> Sink<T> for TokioMpscSenderSink<T> {
        type Error = tokio::sync::mpsc::error::SendError<T>;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            self.0.send(value).await
        }
    }

    pub struct WithMapErr<P, F> {
        inner: P,
        f: F,
    }

    impl<P, F, E, U> Sink<U> for WithMapErr<P, F>
    where
        P: Sink<U>,
        F: Fn(P::Error) -> E + Send + 'static,
    {
        type Error = E;

        async fn send(&mut self, value: U) -> std::result::Result<(), Self::Error> {
            match self.inner.send(value).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    let err = (self.f)(err);
                    Err(err)
                }
            }
        }
    }

    pub struct WithMap<P, F> {
        inner: P,
        f: F,
    }

    impl<P, F, T, U> Sink<T> for WithMap<P, F>
    where
        P: Sink<U>,
        F: Fn(T) -> U + Send + 'static,
    {
        type Error = P::Error;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            self.inner.send((self.f)(value)).await
        }
    }

    pub struct Drain;

    impl<T> Sink<T> for Drain {
        type Error = io::Error;

        async fn send(&mut self, _offset: T) -> std::result::Result<(), Self::Error> {
            io::Result::Ok(())
        }
    }
}
