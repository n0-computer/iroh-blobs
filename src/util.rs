//! Utilities
pub(crate) mod channel;
mod stream;
pub(crate) mod temp_tag;
pub use iroh_util::connection_pool;
pub use stream::{
    AsyncReadRecvStream, AsyncReadRecvStreamExtra, AsyncWriteSendStream, AsyncWriteSendStreamExtra,
    RecvStream, RecvStreamAsyncStreamReader, SendStream,
};
pub(crate) use stream::{RecvStreamExt, SendStreamExt};

pub(crate) mod serde {
    // Module that handles io::Error serialization/deserialization
    pub mod io_error_serde {
        use std::{fmt, io};

        use serde::{
            de::{self, SeqAccess, Visitor},
            ser::SerializeTuple,
            Deserializer, Serializer,
        };

        fn error_kind_to_u8(kind: io::ErrorKind) -> u8 {
            match kind {
                io::ErrorKind::AddrInUse => 0,
                io::ErrorKind::AddrNotAvailable => 1,
                io::ErrorKind::AlreadyExists => 2,
                io::ErrorKind::ArgumentListTooLong => 3,
                io::ErrorKind::BrokenPipe => 4,
                io::ErrorKind::ConnectionAborted => 5,
                io::ErrorKind::ConnectionRefused => 6,
                io::ErrorKind::ConnectionReset => 7,
                io::ErrorKind::CrossesDevices => 8,
                io::ErrorKind::Deadlock => 9,
                io::ErrorKind::DirectoryNotEmpty => 10,
                io::ErrorKind::ExecutableFileBusy => 11,
                io::ErrorKind::FileTooLarge => 12,
                io::ErrorKind::HostUnreachable => 13,
                io::ErrorKind::Interrupted => 14,
                io::ErrorKind::InvalidData => 15,
                io::ErrorKind::InvalidInput => 17,
                io::ErrorKind::IsADirectory => 18,
                io::ErrorKind::NetworkDown => 19,
                io::ErrorKind::NetworkUnreachable => 20,
                io::ErrorKind::NotADirectory => 21,
                io::ErrorKind::NotConnected => 22,
                io::ErrorKind::NotFound => 23,
                io::ErrorKind::NotSeekable => 24,
                io::ErrorKind::Other => 25,
                io::ErrorKind::OutOfMemory => 26,
                io::ErrorKind::PermissionDenied => 27,
                io::ErrorKind::QuotaExceeded => 28,
                io::ErrorKind::ReadOnlyFilesystem => 29,
                io::ErrorKind::ResourceBusy => 30,
                io::ErrorKind::StaleNetworkFileHandle => 31,
                io::ErrorKind::StorageFull => 32,
                io::ErrorKind::TimedOut => 33,
                io::ErrorKind::TooManyLinks => 34,
                io::ErrorKind::UnexpectedEof => 35,
                io::ErrorKind::Unsupported => 36,
                io::ErrorKind::WouldBlock => 37,
                io::ErrorKind::WriteZero => 38,
                _ => 25,
            }
        }

        fn u8_to_error_kind(num: u8) -> io::ErrorKind {
            match num {
                0 => io::ErrorKind::AddrInUse,
                1 => io::ErrorKind::AddrNotAvailable,
                2 => io::ErrorKind::AlreadyExists,
                3 => io::ErrorKind::ArgumentListTooLong,
                4 => io::ErrorKind::BrokenPipe,
                5 => io::ErrorKind::ConnectionAborted,
                6 => io::ErrorKind::ConnectionRefused,
                7 => io::ErrorKind::ConnectionReset,
                8 => io::ErrorKind::CrossesDevices,
                9 => io::ErrorKind::Deadlock,
                10 => io::ErrorKind::DirectoryNotEmpty,
                11 => io::ErrorKind::ExecutableFileBusy,
                12 => io::ErrorKind::FileTooLarge,
                13 => io::ErrorKind::HostUnreachable,
                14 => io::ErrorKind::Interrupted,
                15 => io::ErrorKind::InvalidData,
                // 16 => io::ErrorKind::InvalidFilename,
                17 => io::ErrorKind::InvalidInput,
                18 => io::ErrorKind::IsADirectory,
                19 => io::ErrorKind::NetworkDown,
                20 => io::ErrorKind::NetworkUnreachable,
                21 => io::ErrorKind::NotADirectory,
                22 => io::ErrorKind::NotConnected,
                23 => io::ErrorKind::NotFound,
                24 => io::ErrorKind::NotSeekable,
                25 => io::ErrorKind::Other,
                26 => io::ErrorKind::OutOfMemory,
                27 => io::ErrorKind::PermissionDenied,
                28 => io::ErrorKind::QuotaExceeded,
                29 => io::ErrorKind::ReadOnlyFilesystem,
                30 => io::ErrorKind::ResourceBusy,
                31 => io::ErrorKind::StaleNetworkFileHandle,
                32 => io::ErrorKind::StorageFull,
                33 => io::ErrorKind::TimedOut,
                34 => io::ErrorKind::TooManyLinks,
                35 => io::ErrorKind::UnexpectedEof,
                36 => io::ErrorKind::Unsupported,
                37 => io::ErrorKind::WouldBlock,
                38 => io::ErrorKind::WriteZero,
                _ => io::ErrorKind::Other,
            }
        }

        pub fn serialize<S>(error: &io::Error, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut tup = serializer.serialize_tuple(2)?;
            tup.serialize_element(&error_kind_to_u8(error.kind()))?;
            tup.serialize_element(&error.to_string())?;
            tup.end()
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<io::Error, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct IoErrorVisitor;

            impl<'de> Visitor<'de> for IoErrorVisitor {
                type Value = io::Error;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a tuple of (u32, String) representing io::Error")
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
                {
                    let num: u8 = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let message: String = seq
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    let kind = u8_to_error_kind(num);
                    Ok(io::Error::new(kind, message))
                }
            }

            deserializer.deserialize_tuple(2, IoErrorVisitor)
        }
    }

    #[cfg(test)]
    mod tests {
        use std::io::{self, ErrorKind};

        use postcard;
        use serde::{Deserialize, Serialize};

        use super::io_error_serde;

        #[derive(Serialize, Deserialize)]
        struct TestError(#[serde(with = "io_error_serde")] io::Error);

        #[test]
        fn test_roundtrip_error_kinds() {
            let message = "test error";
            let kinds = [
                ErrorKind::AddrInUse,
                ErrorKind::AddrNotAvailable,
                ErrorKind::AlreadyExists,
                ErrorKind::ArgumentListTooLong,
                ErrorKind::BrokenPipe,
                ErrorKind::ConnectionAborted,
                ErrorKind::ConnectionRefused,
                ErrorKind::ConnectionReset,
                ErrorKind::CrossesDevices,
                ErrorKind::Deadlock,
                ErrorKind::DirectoryNotEmpty,
                ErrorKind::ExecutableFileBusy,
                ErrorKind::FileTooLarge,
                ErrorKind::HostUnreachable,
                ErrorKind::Interrupted,
                ErrorKind::InvalidData,
                // ErrorKind::InvalidFilename,
                ErrorKind::InvalidInput,
                ErrorKind::IsADirectory,
                ErrorKind::NetworkDown,
                ErrorKind::NetworkUnreachable,
                ErrorKind::NotADirectory,
                ErrorKind::NotConnected,
                ErrorKind::NotFound,
                ErrorKind::NotSeekable,
                ErrorKind::Other,
                ErrorKind::OutOfMemory,
                ErrorKind::PermissionDenied,
                ErrorKind::QuotaExceeded,
                ErrorKind::ReadOnlyFilesystem,
                ErrorKind::ResourceBusy,
                ErrorKind::StaleNetworkFileHandle,
                ErrorKind::StorageFull,
                ErrorKind::TimedOut,
                ErrorKind::TooManyLinks,
                ErrorKind::UnexpectedEof,
                ErrorKind::Unsupported,
                ErrorKind::WouldBlock,
                ErrorKind::WriteZero,
            ];

            for kind in kinds {
                let err = TestError(io::Error::new(kind, message));
                let serialized = postcard::to_allocvec(&err).unwrap();
                let deserialized: TestError = postcard::from_bytes(&serialized).unwrap();

                assert_eq!(err.0.kind(), deserialized.0.kind());
                assert_eq!(err.0.to_string(), deserialized.0.to_string());
            }
        }
    }
}

#[cfg(feature = "fs-store")]
pub(crate) mod outboard_with_progress {
    use std::{
        io::{self, BufReader, Read},
        sync::Arc,
    };

    use bao_tree::{
        blake3,
        io::{
            outboard::PreOrderOutboard,
            sync::{OutboardMut, ReadAt, WriteAt},
        },
        iter::BaoChunk,
        BaoTree, BlockSize, ChunkNum, TreeNode,
    };
    use smallvec::SmallVec;

    use super::sink::Sink;

    /// Files from this size on are hashed on several threads, see [`init_outboard_parallel`].
    pub const PARALLEL_MIN_SIZE: u64 = 64 * 1024 * 1024;

    /// The most threads a single import hashes with.
    const MAX_THREADS: usize = 8;

    /// [`init_outboard_parallel`] hands the data to its threads in aligned subtrees of this
    /// size (1 MiB).
    const SLICE_SIZE: BlockSize = BlockSize::from_chunk_log(10);

    /// The number of threads to hash data of this size with: 1 below [`PARALLEL_MIN_SIZE`],
    /// otherwise one per core, at most [`MAX_THREADS`].
    pub fn hash_threads(size: u64) -> usize {
        if size < PARALLEL_MIN_SIZE {
            return 1;
        }
        std::thread::available_parallelism()
            .map_or(1, |n| n.get())
            .min(MAX_THREADS)
    }

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

    /// The outboard part of one subtree of [`SLICE_SIZE`].
    struct Part {
        /// The subtree's parent nodes, in pre-order, as they appear in the whole outboard.
        nodes: Vec<u8>,
        /// The level of the subtree's root node.
        root_level: u32,
    }

    /// Computes the same outboard and root as [`init_outboard`], on `threads` threads.
    ///
    /// The tree is cut into aligned subtrees of [`SLICE_SIZE`]. Thread `i` reads subtrees
    /// `i`, `i + threads`, ... with positioned reads, and computes the hash and the outboard
    /// part of each. This task takes them in order, writes the parts and the nodes above them,
    /// and reports progress at the start of each subtree. Each thread is at most a few subtrees
    /// ahead. When this future is dropped or fails, the threads stop after their current subtree.
    pub async fn init_outboard_parallel<R, W, P>(
        data: Arc<R>,
        outboard: &mut PreOrderOutboard<W>,
        progress: &mut P,
        threads: usize,
    ) -> io::Result<std::result::Result<(), P::Error>>
    where
        R: ReadAt + Send + Sync + 'static,
        W: WriteAt,
        P: Sink<ChunkNum>,
    {
        let tree = outboard.tree;
        debug_assert!(tree.block_size().chunk_log() <= SLICE_SIZE.chunk_log());
        // the same tree, with the subtrees as its blocks
        let top = BaoTree::new(tree.size(), SLICE_SIZE);
        let slices = top.blocks();
        let threads = threads.clamp(1, usize::try_from(slices).unwrap_or(usize::MAX));
        let mut parts = Vec::with_capacity(threads);
        for first in 0..threads as u64 {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            let data = data.clone();
            std::thread::Builder::new()
                .name("iroh-blobs-hash".into())
                .spawn(move || {
                    let mut buf = Vec::new();
                    for slice in (first..slices).step_by(threads) {
                        let res = hash_slice(&*data, tree, slice, slices == 1, &mut buf);
                        let failed = res.is_err();
                        // the receiver is gone when the import was dropped or failed
                        if tx.blocking_send(res).is_err() || failed {
                            break;
                        }
                    }
                })?;
            parts.push(rx);
        }
        let mut stack = Vec::<(blake3::Hash, Option<Part>)>::new();
        let mut next = 0;
        for item in top.post_order_chunks_iter() {
            match item {
                BaoChunk::Parent { is_root, node, .. } => {
                    let (right_hash, right) = stack.pop().unwrap();
                    let (left_hash, left) = stack.pop().unwrap();
                    if let Some(part) = left {
                        save_part(outboard, node.left_child().unwrap(), part)?;
                    }
                    if let Some(part) = right {
                        save_part(outboard, node.right_child().unwrap(), part)?;
                    }
                    outboard.save(node, &(left_hash, right_hash))?;
                    stack.push((parent_cv(&left_hash, &right_hash, is_root), None));
                }
                BaoChunk::Leaf { start_chunk, .. } => {
                    if let Err(err) = progress.send(start_chunk).await {
                        return Ok(Err(err));
                    }
                    let res = parts[next % threads].recv().await;
                    next += 1;
                    let (hash, part) =
                        res.ok_or_else(|| io::Error::other("hash thread stopped"))??;
                    stack.push((hash, Some(part)));
                }
            }
        }
        debug_assert_eq!(stack.len(), 1);
        let (root, part) = stack.pop().unwrap();
        if let Some(part) = part {
            // a single subtree is the whole tree
            outboard.data.write_all_at(0, &part.nodes)?;
        }
        outboard.root = root;
        Ok(Ok(()))
    }

    /// Reads and hashes subtree number `slice` of the subtrees of [`SLICE_SIZE`] of `tree`.
    fn hash_slice(
        data: &impl ReadAt,
        tree: BaoTree,
        slice: u64,
        is_root: bool,
        buf: &mut Vec<u8>,
    ) -> io::Result<(blake3::Hash, Part)> {
        let slice_bytes = SLICE_SIZE.bytes() as u64;
        let start = slice * slice_bytes;
        let size = (tree.size() - start).min(slice_bytes);
        buf.resize(size as usize, 0);
        data.read_exact_at(start, buf)?;
        // the subtree has the shape of a tree of its own size: compute that tree's outboard,
        // with the chunk numbers of the subtree's place in the whole tree
        let sub = BaoTree::new(size, tree.block_size());
        let mut part = PreOrderOutboard {
            tree: sub,
            root: blake3::Hash::from_bytes([0; 32]),
            data: Vec::with_capacity(sub.outboard_size() as usize),
        };
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        let mut root_level = 0;
        for item in sub.post_order_chunks_iter() {
            match item {
                BaoChunk::Parent {
                    is_root: top, node, ..
                } => {
                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    part.save(node, &(left, right))?;
                    stack.push(parent_cv(&left, &right, is_root && top));
                    root_level = node.level();
                }
                BaoChunk::Leaf {
                    is_root: top,
                    start_chunk,
                    size,
                    ..
                } => {
                    let offset = start_chunk.to_bytes() as usize;
                    let chunk = (start >> 10) + start_chunk.0;
                    let hash = hash_subtree(chunk, &buf[offset..offset + size], is_root && top);
                    stack.push(hash);
                }
            }
        }
        let hash = stack.pop().unwrap();
        let nodes = part.data;
        Ok((hash, Part { nodes, root_level }))
    }

    /// Writes a subtree's part to the outboard. `child` is the child node of the tree
    /// that the subtree is. A subtree at the end of the data can be smaller than that node,
    /// and then it is the node's left part.
    fn save_part<W: WriteAt>(
        outboard: &mut PreOrderOutboard<W>,
        mut child: TreeNode,
        part: Part,
    ) -> io::Result<()> {
        if part.nodes.is_empty() {
            // a subtree of one block has no parent nodes
            return Ok(());
        }
        while child.level() > part.root_level {
            child = child.left_child().unwrap();
        }
        let offset = outboard
            .tree
            .pre_order_offset(child)
            .ok_or_else(|| io::Error::other("subtree outside of the tree"))?;
        outboard.data.write_all_at(offset * 64, &part.nodes)
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;

        use bao_tree::{
            blake3,
            io::{outboard::PreOrderOutboard, sync::CreateOutboard},
            BaoTree, ChunkNum,
        };
        use testresult::TestResult;

        use super::{hash_threads, init_outboard_parallel, MAX_THREADS, PARALLEL_MIN_SIZE};
        use crate::{
            store::{fs::tests::test_data, IROH_BLOCK_SIZE},
            util::{
                outboard_with_progress::init_outboard,
                sink::{Drain, Sink},
            },
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

        const K: usize = 1024;
        const M: usize = 1024 * 1024;
        const GROUP: usize = 16 * K;

        /// Data that differs from byte to byte, so that a part in the wrong place shows.
        fn mixed_data(size: usize) -> Vec<u8> {
            (0..size as u64)
                .map(|i| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8)
                .collect()
        }

        fn new_outboard(size: usize) -> PreOrderOutboard<Vec<u8>> {
            PreOrderOutboard {
                tree: BaoTree::new(size as u64, IROH_BLOCK_SIZE),
                ..Default::default()
            }
        }

        /// The parallel outboard equals the single-threaded one, byte for byte.
        async fn check_parallel(size: usize, threads: &[usize]) -> TestResult<()> {
            let data = Arc::new(mixed_data(size));
            let mut expected = new_outboard(size);
            expected.init_from(data.as_slice())?;
            assert_eq!(expected.root, blake3::hash(&data));
            for &threads in threads {
                let mut actual = new_outboard(size);
                init_outboard_parallel(data.clone(), &mut actual, &mut Drain, threads).await??;
                assert_eq!(actual.root, expected.root, "size {size}, {threads} threads");
                assert!(
                    actual.data == expected.data,
                    "outboard differs: size {size}, {threads} threads"
                );
            }
            Ok(())
        }

        #[tokio::test]
        async fn parallel_outboard_is_the_same() -> TestResult<()> {
            let sizes = [
                0,
                1,
                K - 1,
                K,
                K + 1,
                GROUP - 1,
                GROUP,
                GROUP + 1,
                2 * GROUP - 1,
                2 * GROUP,
                2 * GROUP + 1,
                3 * GROUP + 7,
                M - GROUP,
                M - 1,
                M,
                M + 1,
                M + K,
                M + GROUP,
                M + GROUP + 1,
                2 * M - 1,
                2 * M,
                2 * M + 1,
                3 * M + 12345,
                5 * M + GROUP,
                8 * M + 1,
                9 * M - 1,
                17 * M + 3 * GROUP + 5,
            ];
            for size in sizes {
                check_parallel(size, &[1, 2, 3, 8]).await?;
            }
            Ok(())
        }

        #[tokio::test]
        async fn parallel_outboard_threshold() -> TestResult<()> {
            let min = PARALLEL_MIN_SIZE as usize;
            assert_eq!(hash_threads(PARALLEL_MIN_SIZE - 1), 1);
            assert!(hash_threads(PARALLEL_MIN_SIZE) <= MAX_THREADS);
            for size in [min - 1, min, min + 1] {
                check_parallel(size, &[8]).await?;
            }
            Ok(())
        }

        /// A few hundred MB, slow without optimizations.
        #[tokio::test]
        #[ignore = "slow in debug builds"]
        async fn parallel_outboard_large() -> TestResult<()> {
            check_parallel(300 * 1000 * 1000 + 1, &[3, 8]).await
        }

        /// A progress sink whose receiver goes away after the given number of items.
        struct FailAfter(usize);

        impl Sink<ChunkNum> for FailAfter {
            type Error = ();

            async fn send(&mut self, _: ChunkNum) -> std::result::Result<(), ()> {
                self.0 = self.0.checked_sub(1).ok_or(())?;
                Ok(())
            }
        }

        /// Each hashing thread holds a reference to the data until it has stopped.
        fn wait_for_threads(data: &Arc<Vec<u8>>) {
            let start = std::time::Instant::now();
            while Arc::strong_count(data) > 1 {
                assert!(
                    start.elapsed().as_secs() < 30,
                    "hashing threads still running"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }

        #[tokio::test]
        async fn parallel_outboard_stops() -> TestResult<()> {
            let size = 64 * M + 1;
            let data = Arc::new(mixed_data(size));
            // the progress receiver goes away
            let mut outboard = new_outboard(size);
            let res =
                init_outboard_parallel(data.clone(), &mut outboard, &mut FailAfter(3), 4).await?;
            assert!(res.is_err());
            wait_for_threads(&data);
            // the import is dropped
            let mut outboard = new_outboard(size);
            let mut progress = Drain;
            let import = init_outboard_parallel(data.clone(), &mut outboard, &mut progress, 4);
            assert!(n0_future::future::poll_once(Box::pin(import))
                .await
                .is_none());
            wait_for_threads(&data);
            Ok(())
        }
    }
}

pub(crate) mod sink {
    use std::future::Future;

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

    #[allow(dead_code)]
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
        type Error = irpc::channel::SendError;

        async fn send(&mut self, value: T) -> std::result::Result<(), Self::Error> {
            self.0
                .send(value)
                .await
                .map_err(|_| n0_error::e!(irpc::channel::SendError::ReceiverClosed))
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
        type Error = irpc::channel::SendError;

        async fn send(&mut self, _offset: T) -> std::result::Result<(), Self::Error> {
            Ok(())
        }
    }
}
