//! The low level client side API
//!
//! Note that while using this API directly is fine, a simpler way to get data
//! to a store is to use the [`crate::api::remote`] API, in particular the
//! [`crate::api::remote::Remote::fetch`] function to download data to your
//! local store.
//!
//! To get data, create a connection using an [`iroh::Endpoint`].
//!
//! Create a [`crate::protocol::GetRequest`] describing the data you want to get.
//!
//! Then create a state machine using [fsm::start] and
//! drive it to completion by calling next on each state.
//!
//! For some states you have to provide additional arguments when calling next,
//! or you can choose to finish early.
//!
//! [iroh]: https://docs.rs/iroh
use std::{
    error::Error,
    fmt::{self, Debug},
    time::{Duration, Instant},
};

use anyhow::Result;
use bao_tree::{io::fsm::BaoContentItem, ChunkNum};
use fsm::RequestCounters;
use iroh::endpoint::{self, RecvStream, SendStream};
use iroh_io::TokioStreamReader;
use n0_snafu::SpanTrace;
use nested_enum_utils::common_fields;
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, IntoError, ResultExt, Snafu};
use tracing::{debug, error};

use crate::{protocol::ChunkRangesSeq, store::IROH_BLOCK_SIZE, Hash};

mod error;
pub mod request;
pub(crate) use error::{BadRequestSnafu, LocalFailureSnafu};
pub use error::{GetError, GetResult};

type WrappedRecvStream = TokioStreamReader<RecvStream>;

/// Stats about the transfer.
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    derive_more::Deref,
    derive_more::DerefMut,
)]
pub struct Stats {
    /// Counters
    #[deref]
    #[deref_mut]
    pub counters: RequestCounters,
    /// The time it took to transfer the data
    pub elapsed: Duration,
}

impl Stats {
    /// Transfer rate in megabits per second
    pub fn mbits(&self) -> f64 {
        let data_len_bit = self.total_bytes_read() * 8;
        data_len_bit as f64 / (1000. * 1000.) / self.elapsed.as_secs_f64()
    }

    pub fn total_bytes_read(&self) -> u64 {
        self.payload_bytes_read + self.other_bytes_read
    }

    pub fn combine(&mut self, that: &Stats) {
        self.payload_bytes_written += that.payload_bytes_written;
        self.other_bytes_written += that.other_bytes_written;
        self.payload_bytes_read += that.payload_bytes_read;
        self.other_bytes_read += that.other_bytes_read;
        self.elapsed += that.elapsed;
    }
}

/// Finite state machine for get responses.
///
/// This is the low level API for getting data from a peer.
#[doc = include_str!("../docs/img/get_machine.drawio.svg")]
pub mod fsm {
    use std::{io, result};

    use bao_tree::{
        io::fsm::{OutboardMut, ResponseDecoder, ResponseDecoderNext},
        BaoTree, ChunkRanges, TreeNode,
    };
    use derive_more::From;
    use iroh::endpoint::Connection;
    use iroh_io::{AsyncSliceWriter, AsyncStreamReader, TokioStreamReader};

    use super::*;
    use crate::{
        get::error::BadRequestSnafu,
        protocol::{
            GetManyRequest, GetRequest, NonEmptyRequestRangeSpecIter, Request, MAX_MESSAGE_SIZE,
        },
    };

    self_cell::self_cell! {
        struct RangesIterInner {
            owner: ChunkRangesSeq,
            #[not_covariant]
            dependent: NonEmptyRequestRangeSpecIter,
        }
    }

    /// The entry point of the get response machine
    pub fn start(
        connection: Connection,
        request: GetRequest,
        counters: RequestCounters,
    ) -> AtInitial {
        AtInitial::new(connection, request, counters)
    }

    /// Start with a get many request. Todo: turn this into distinct states.
    pub async fn start_get_many(
        connection: Connection,
        request: GetManyRequest,
        counters: RequestCounters,
    ) -> std::result::Result<Result<AtStartChild, AtClosing>, GetError> {
        let start = Instant::now();
        let (mut writer, reader) = connection.open_bi().await?;
        let request = Request::GetMany(request);
        let request_bytes = postcard::to_stdvec(&request)
            .map_err(|source| BadRequestSnafu.into_error(source.into()))?;
        writer.write_all(&request_bytes).await?;
        writer.finish()?;
        let Request::GetMany(request) = request else {
            unreachable!();
        };
        let reader = TokioStreamReader::new(reader);
        let mut ranges_iter = RangesIter::new(request.ranges.clone());
        let first_item = ranges_iter.next();
        let misc = Box::new(Misc {
            counters,
            start,
            ranges_iter,
        });
        Ok(match first_item {
            Some((child_offset, child_ranges)) => Ok(AtStartChild {
                ranges: child_ranges,
                reader,
                misc,
                offset: child_offset,
            }),
            None => Err(AtClosing::new(misc, reader, true)),
        })
    }

    /// Owned iterator for the ranges in a request
    ///
    /// We need an owned iterator for a fsm style API, otherwise we would have
    /// to drag a lifetime around every single state.
    struct RangesIter(RangesIterInner);

    impl fmt::Debug for RangesIter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RangesIter").finish()
        }
    }

    impl RangesIter {
        pub fn new(owner: ChunkRangesSeq) -> Self {
            Self(RangesIterInner::new(owner, |owner| {
                owner.iter_non_empty_infinite()
            }))
        }

        pub fn offset(&self) -> u64 {
            self.0.with_dependent(|_owner, iter| iter.offset())
        }
    }

    impl Iterator for RangesIter {
        type Item = (u64, ChunkRanges);

        fn next(&mut self) -> Option<Self::Item> {
            self.0.with_dependent_mut(|_owner, iter| {
                iter.next().map(|(offset, ranges)| (offset, ranges.clone()))
            })
        }
    }

    /// Initial state of the get response machine
    #[derive(Debug)]
    pub struct AtInitial {
        connection: Connection,
        request: GetRequest,
        counters: RequestCounters,
    }

    impl AtInitial {
        /// Create a new get response
        ///
        /// `connection` is an existing connection
        /// `request` is the request to be sent
        pub fn new(connection: Connection, request: GetRequest, counters: RequestCounters) -> Self {
            Self {
                connection,
                request,
                counters,
            }
        }

        /// Initiate a new bidi stream to use for the get response
        pub async fn next(self) -> Result<AtConnected, endpoint::ConnectionError> {
            let start = Instant::now();
            let (writer, reader) = self.connection.open_bi().await?;
            let reader = TokioStreamReader::new(reader);
            Ok(AtConnected {
                start,
                reader,
                writer,
                request: self.request,
                counters: self.counters,
            })
        }
    }

    /// State of the get response machine after the handshake has been sent
    #[derive(Debug)]
    pub struct AtConnected {
        start: Instant,
        reader: WrappedRecvStream,
        writer: SendStream,
        request: GetRequest,
        counters: RequestCounters,
    }

    /// Possible next states after the handshake has been sent
    #[derive(Debug, From)]
    pub enum ConnectedNext {
        /// First response is either a collection or a single blob
        StartRoot(AtStartRoot),
        /// First response is a child
        StartChild(AtStartChild),
        /// Request is empty
        Closing(AtClosing),
    }

    /// Error that you can get from [`AtConnected::next`]
    #[common_fields({
        backtrace: Option<Backtrace>,
        #[snafu(implicit)]
        span_trace: SpanTrace,
    })]
    #[allow(missing_docs)]
    #[derive(Debug, Snafu)]
    #[non_exhaustive]
    pub enum ConnectedNextError {
        /// Error when serializing the request
        #[snafu(display("postcard ser: {source}"))]
        PostcardSer { source: postcard::Error },
        /// The serialized request is too long to be sent
        #[snafu(display("request too big"))]
        RequestTooBig {},
        /// Error when writing the request to the [`SendStream`].
        #[snafu(display("write: {source}"))]
        Write { source: quinn::WriteError },
        /// Quic connection is closed.
        #[snafu(display("closed"))]
        Closed { source: quinn::ClosedStream },
        /// A generic io error
        #[snafu(transparent)]
        Io { source: io::Error },
    }

    impl AtConnected {
        /// Send the request and move to the next state
        ///
        /// The next state will be either `StartRoot` or `StartChild` depending on whether
        /// the request requests part of the collection or not.
        ///
        /// If the request is empty, this can also move directly to `Finished`.
        pub async fn next(self) -> Result<ConnectedNext, ConnectedNextError> {
            let Self {
                start,
                reader,
                mut writer,
                mut request,
                mut counters,
            } = self;
            // 1. Send Request
            counters.other_bytes_written += {
                debug!("sending request");
                let wrapped = Request::Get(request);
                let request_bytes = postcard::to_stdvec(&wrapped).context(PostcardSerSnafu)?;
                let Request::Get(x) = wrapped else {
                    unreachable!();
                };
                request = x;

                if request_bytes.len() > MAX_MESSAGE_SIZE {
                    return Err(RequestTooBigSnafu.build());
                }

                // write the request itself
                writer.write_all(&request_bytes).await.context(WriteSnafu)?;
                request_bytes.len() as u64
            };

            // 2. Finish writing before expecting a response
            writer.finish().context(ClosedSnafu)?;

            let hash = request.hash;
            let ranges_iter = RangesIter::new(request.ranges);
            // this is in a box so we don't have to memcpy it on every state transition
            let mut misc = Box::new(Misc {
                counters,
                start,
                ranges_iter,
            });
            Ok(match misc.ranges_iter.next() {
                Some((offset, ranges)) => {
                    if offset == 0 {
                        AtStartRoot {
                            reader,
                            ranges,
                            misc,
                            hash,
                        }
                        .into()
                    } else {
                        AtStartChild {
                            reader,
                            ranges,
                            misc,
                            offset,
                        }
                        .into()
                    }
                }
                None => AtClosing::new(misc, reader, true).into(),
            })
        }
    }

    /// State of the get response when we start reading a collection
    #[derive(Debug)]
    pub struct AtStartRoot {
        ranges: ChunkRanges,
        reader: TokioStreamReader<RecvStream>,
        misc: Box<Misc>,
        hash: Hash,
    }

    /// State of the get response when we start reading a child
    #[derive(Debug)]
    pub struct AtStartChild {
        ranges: ChunkRanges,
        reader: TokioStreamReader<RecvStream>,
        misc: Box<Misc>,
        offset: u64,
    }

    impl AtStartChild {
        /// The offset of the child we are currently reading
        ///
        /// This must be used to determine the hash needed to call next.
        /// If this is larger than the number of children in the collection,
        /// you can call finish to stop reading the response.
        pub fn offset(&self) -> u64 {
            self.offset
        }

        /// The ranges we have requested for the child
        pub fn ranges(&self) -> &ChunkRanges {
            &self.ranges
        }

        /// Go into the next state, reading the header
        ///
        /// This requires passing in the hash of the child for validation
        pub fn next(self, hash: Hash) -> AtBlobHeader {
            AtBlobHeader {
                reader: self.reader,
                ranges: self.ranges,
                misc: self.misc,
                hash,
            }
        }

        /// Finish the get response without reading further
        ///
        /// This is used if you know that there are no more children from having
        /// read the collection, or when you want to stop reading the response
        /// early.
        pub fn finish(self) -> AtClosing {
            AtClosing::new(self.misc, self.reader, false)
        }
    }

    impl AtStartRoot {
        /// The ranges we have requested for the child
        pub fn ranges(&self) -> &ChunkRanges {
            &self.ranges
        }

        /// Hash of the root blob
        pub fn hash(&self) -> Hash {
            self.hash
        }

        /// Go into the next state, reading the header
        ///
        /// For the collection we already know the hash, since it was part of the request
        pub fn next(self) -> AtBlobHeader {
            AtBlobHeader {
                reader: self.reader,
                ranges: self.ranges,
                hash: self.hash,
                misc: self.misc,
            }
        }

        /// Finish the get response without reading further
        pub fn finish(self) -> AtClosing {
            AtClosing::new(self.misc, self.reader, false)
        }
    }

    /// State before reading a size header
    #[derive(Debug)]
    pub struct AtBlobHeader {
        ranges: ChunkRanges,
        reader: TokioStreamReader<RecvStream>,
        misc: Box<Misc>,
        hash: Hash,
    }

    /// Error that you can get from [`AtBlobHeader::next`]
    #[common_fields({
        backtrace: Option<Backtrace>,
        #[snafu(implicit)]
        span_trace: SpanTrace,
    })]
    #[non_exhaustive]
    #[derive(Debug, Snafu)]
    pub enum AtBlobHeaderNextError {
        /// Eof when reading the size header
        ///
        /// This indicates that the provider does not have the requested data.
        #[snafu(display("not found"))]
        NotFound {},
        /// Quinn read error when reading the size header
        #[snafu(display("read: {source}"))]
        EndpointRead { source: endpoint::ReadError },
        /// Generic io error
        #[snafu(display("io: {source}"))]
        Io { source: io::Error },
    }

    impl From<AtBlobHeaderNextError> for io::Error {
        fn from(cause: AtBlobHeaderNextError) -> Self {
            match cause {
                AtBlobHeaderNextError::NotFound { .. } => {
                    io::Error::new(io::ErrorKind::UnexpectedEof, cause)
                }
                AtBlobHeaderNextError::EndpointRead { source, .. } => source.into(),
                AtBlobHeaderNextError::Io { source, .. } => source,
            }
        }
    }

    impl AtBlobHeader {
        /// Read the size header, returning it and going into the `Content` state.
        pub async fn next(mut self) -> Result<(AtBlobContent, u64), AtBlobHeaderNextError> {
            let size = self.reader.read::<8>().await.map_err(|cause| {
                if cause.kind() == io::ErrorKind::UnexpectedEof {
                    NotFoundSnafu.build()
                } else if let Some(e) = cause
                    .get_ref()
                    .and_then(|x| x.downcast_ref::<endpoint::ReadError>())
                {
                    EndpointReadSnafu.into_error(e.clone())
                } else {
                    IoSnafu.into_error(cause)
                }
            })?;
            self.misc.other_bytes_read += 8;
            let size = u64::from_le_bytes(size);
            let stream = ResponseDecoder::new(
                self.hash.into(),
                self.ranges,
                BaoTree::new(size, IROH_BLOCK_SIZE),
                self.reader,
            );
            Ok((
                AtBlobContent {
                    stream,
                    misc: self.misc,
                },
                size,
            ))
        }

        /// Drain the response and throw away the result
        pub async fn drain(self) -> result::Result<AtEndBlob, DecodeError> {
            let (content, _size) = self.next().await?;
            content.drain().await
        }

        /// Concatenate the entire response into a vec
        ///
        /// For a request that does not request the complete blob, this will just
        /// concatenate the ranges that were requested.
        pub async fn concatenate_into_vec(
            self,
        ) -> result::Result<(AtEndBlob, Vec<u8>), DecodeError> {
            let (content, _size) = self.next().await?;
            content.concatenate_into_vec().await
        }

        /// Write the entire blob to a slice writer.
        pub async fn write_all<D: AsyncSliceWriter>(
            self,
            data: D,
        ) -> result::Result<AtEndBlob, DecodeError> {
            let (content, _size) = self.next().await?;
            let res = content.write_all(data).await?;
            Ok(res)
        }

        /// Write the entire blob to a slice writer and to an optional outboard.
        ///
        /// The outboard is only written to if the blob is larger than a single
        /// chunk group.
        pub async fn write_all_with_outboard<D, O>(
            self,
            outboard: Option<O>,
            data: D,
        ) -> result::Result<AtEndBlob, DecodeError>
        where
            D: AsyncSliceWriter,
            O: OutboardMut,
        {
            let (content, _size) = self.next().await?;
            let res = content.write_all_with_outboard(outboard, data).await?;
            Ok(res)
        }

        /// The hash of the blob we are reading.
        pub fn hash(&self) -> Hash {
            self.hash
        }

        /// The ranges we have requested for the current hash.
        pub fn ranges(&self) -> &ChunkRanges {
            &self.ranges
        }

        /// The current offset of the blob we are reading.
        pub fn offset(&self) -> u64 {
            self.misc.ranges_iter.offset()
        }
    }

    /// State while we are reading content
    #[derive(Debug)]
    pub struct AtBlobContent {
        stream: ResponseDecoder<WrappedRecvStream>,
        misc: Box<Misc>,
    }

    /// Decode error that you can get once you have sent the request and are
    /// decoding the response, e.g. from [`AtBlobContent::next`].
    ///
    /// This is similar to [`bao_tree::io::DecodeError`], but takes into account
    /// that we are reading from a [`RecvStream`], so read errors will be
    /// propagated as [`DecodeError::Read`], containing a [`ReadError`].
    /// This carries more concrete information about the error than an [`io::Error`].
    ///
    /// When the provider finds that it does not have a chunk that we requested,
    /// or that the chunk is invalid, it will stop sending data without producing
    /// an error. This is indicated by either the [`DecodeError::ParentNotFound`] or
    /// [`DecodeError::LeafNotFound`] variant, which can be used to detect that data
    /// is missing but the connection as well that the provider is otherwise healthy.
    ///
    /// The [`DecodeError::ParentHashMismatch`] and [`DecodeError::LeafHashMismatch`]
    /// variants indicate that the provider has sent us invalid data. A well-behaved
    /// provider should never do this, so this is an indication that the provider is
    /// not behaving correctly.
    ///
    /// The [`DecodeError::DecodeIo`] variant is just a fallback for any other io error that
    /// is not actually a [`DecodeError::Read`].
    ///
    /// [`ReadError`]: endpoint::ReadError
    #[common_fields({
        backtrace: Option<Backtrace>,
        #[snafu(implicit)]
        span_trace: SpanTrace,
    })]
    #[non_exhaustive]
    #[derive(Debug, Snafu)]
    pub enum DecodeError {
        /// A chunk was not found or invalid, so the provider stopped sending data
        #[snafu(display("not found"))]
        ChunkNotFound {},
        /// A parent was not found or invalid, so the provider stopped sending data
        #[snafu(display("parent not found {node:?}"))]
        ParentNotFound { node: TreeNode },
        /// A parent was not found or invalid, so the provider stopped sending data
        #[snafu(display("chunk not found {num}"))]
        LeafNotFound { num: ChunkNum },
        /// The hash of a parent did not match the expected hash
        #[snafu(display("parent hash mismatch: {node:?}"))]
        ParentHashMismatch { node: TreeNode },
        /// The hash of a leaf did not match the expected hash
        #[snafu(display("leaf hash mismatch: {num}"))]
        LeafHashMismatch { num: ChunkNum },
        /// Error when reading from the stream
        #[snafu(display("read: {source}"))]
        Read { source: endpoint::ReadError },
        /// A generic io error
        #[snafu(display("io: {source}"))]
        DecodeIo { source: io::Error },
    }

    impl DecodeError {
        pub(crate) fn leaf_hash_mismatch(num: ChunkNum) -> Self {
            LeafHashMismatchSnafu { num }.build()
        }
    }

    impl From<AtBlobHeaderNextError> for DecodeError {
        fn from(cause: AtBlobHeaderNextError) -> Self {
            match cause {
                AtBlobHeaderNextError::NotFound { .. } => ChunkNotFoundSnafu.build(),
                AtBlobHeaderNextError::EndpointRead { source, .. } => ReadSnafu.into_error(source),
                AtBlobHeaderNextError::Io { source, .. } => DecodeIoSnafu.into_error(source),
            }
        }
    }

    impl From<DecodeError> for io::Error {
        fn from(cause: DecodeError) -> Self {
            match cause {
                DecodeError::ParentNotFound { .. } => {
                    io::Error::new(io::ErrorKind::UnexpectedEof, cause)
                }
                DecodeError::LeafNotFound { .. } => {
                    io::Error::new(io::ErrorKind::UnexpectedEof, cause)
                }
                DecodeError::Read { source, .. } => source.into(),
                DecodeError::DecodeIo { source, .. } => source,
                _ => io::Error::other(cause),
            }
        }
    }

    impl From<io::Error> for DecodeError {
        fn from(value: io::Error) -> Self {
            DecodeIoSnafu.into_error(value)
        }
    }

    impl From<bao_tree::io::DecodeError> for DecodeError {
        fn from(value: bao_tree::io::DecodeError) -> Self {
            match value {
                bao_tree::io::DecodeError::ParentNotFound(x) => {
                    ParentNotFoundSnafu { node: x }.build()
                }
                bao_tree::io::DecodeError::LeafNotFound(x) => LeafNotFoundSnafu { num: x }.build(),
                bao_tree::io::DecodeError::ParentHashMismatch(node) => {
                    ParentHashMismatchSnafu { node }.build()
                }
                bao_tree::io::DecodeError::LeafHashMismatch(chunk) => {
                    LeafHashMismatchSnafu { num: chunk }.build()
                }
                bao_tree::io::DecodeError::Io(cause) => {
                    if let Some(inner) = cause.get_ref() {
                        if let Some(e) = inner.downcast_ref::<endpoint::ReadError>() {
                            ReadSnafu.into_error(e.clone())
                        } else {
                            DecodeIoSnafu.into_error(cause)
                        }
                    } else {
                        DecodeIoSnafu.into_error(cause)
                    }
                }
            }
        }
    }

    /// The next state after reading a content item
    #[derive(Debug, From)]
    pub enum BlobContentNext {
        /// We expect more content
        More((AtBlobContent, result::Result<BaoContentItem, DecodeError>)),
        /// We are done with this blob
        Done(AtEndBlob),
    }

    impl AtBlobContent {
        /// Read the next item, either content, an error, or the end of the blob
        pub async fn next(self) -> BlobContentNext {
            match self.stream.next().await {
                ResponseDecoderNext::More((stream, res)) => {
                    let mut next = Self { stream, ..self };
                    let res = res.map_err(DecodeError::from);
                    match &res {
                        Ok(BaoContentItem::Parent(_)) => {
                            next.misc.other_bytes_read += 64;
                        }
                        Ok(BaoContentItem::Leaf(leaf)) => {
                            next.misc.payload_bytes_read += leaf.data.len() as u64;
                        }
                        _ => {}
                    }
                    BlobContentNext::More((next, res))
                }
                ResponseDecoderNext::Done(stream) => BlobContentNext::Done(AtEndBlob {
                    stream,
                    misc: self.misc,
                }),
            }
        }

        /// The geometry of the tree we are currently reading.
        pub fn tree(&self) -> bao_tree::BaoTree {
            self.stream.tree()
        }

        /// The hash of the blob we are reading.
        pub fn hash(&self) -> Hash {
            (*self.stream.hash()).into()
        }

        /// The current offset of the blob we are reading.
        pub fn offset(&self) -> u64 {
            self.misc.ranges_iter.offset()
        }

        /// Current stats
        pub fn stats(&self) -> Stats {
            Stats {
                counters: self.misc.counters,
                elapsed: self.misc.start.elapsed(),
            }
        }

        /// Drain the response and throw away the result
        pub async fn drain(self) -> result::Result<AtEndBlob, DecodeError> {
            let mut content = self;
            loop {
                match content.next().await {
                    BlobContentNext::More((content1, res)) => {
                        let _ = res?;
                        content = content1;
                    }
                    BlobContentNext::Done(end) => {
                        break Ok(end);
                    }
                }
            }
        }

        /// Concatenate the entire response into a vec
        pub async fn concatenate_into_vec(
            self,
        ) -> result::Result<(AtEndBlob, Vec<u8>), DecodeError> {
            let mut res = Vec::with_capacity(1024);
            let mut curr = self;
            let done = loop {
                match curr.next().await {
                    BlobContentNext::More((next, data)) => {
                        if let BaoContentItem::Leaf(leaf) = data? {
                            res.extend_from_slice(&leaf.data);
                        }
                        curr = next;
                    }
                    BlobContentNext::Done(done) => {
                        // we are done with the root blob
                        break done;
                    }
                }
            };
            Ok((done, res))
        }

        /// Write the entire blob to a slice writer and to an optional outboard.
        ///
        /// The outboard is only written to if the blob is larger than a single
        /// chunk group.
        pub async fn write_all_with_outboard<D, O>(
            self,
            mut outboard: Option<O>,
            mut data: D,
        ) -> result::Result<AtEndBlob, DecodeError>
        where
            D: AsyncSliceWriter,
            O: OutboardMut,
        {
            let mut content = self;
            loop {
                match content.next().await {
                    BlobContentNext::More((content1, item)) => {
                        content = content1;
                        match item? {
                            BaoContentItem::Parent(parent) => {
                                if let Some(outboard) = outboard.as_mut() {
                                    outboard.save(parent.node, &parent.pair).await?;
                                }
                            }
                            BaoContentItem::Leaf(leaf) => {
                                data.write_bytes_at(leaf.offset, leaf.data).await?;
                            }
                        }
                    }
                    BlobContentNext::Done(end) => {
                        return Ok(end);
                    }
                }
            }
        }

        /// Write the entire blob to a slice writer.
        pub async fn write_all<D>(self, mut data: D) -> result::Result<AtEndBlob, DecodeError>
        where
            D: AsyncSliceWriter,
        {
            let mut content = self;
            loop {
                match content.next().await {
                    BlobContentNext::More((content1, item)) => {
                        content = content1;
                        match item? {
                            BaoContentItem::Parent(_) => {}
                            BaoContentItem::Leaf(leaf) => {
                                data.write_bytes_at(leaf.offset, leaf.data).await?;
                            }
                        }
                    }
                    BlobContentNext::Done(end) => {
                        return Ok(end);
                    }
                }
            }
        }

        /// Immediately finish the get response without reading further
        pub fn finish(self) -> AtClosing {
            AtClosing::new(self.misc, self.stream.finish(), false)
        }
    }

    /// State after we have read all the content for a blob
    #[derive(Debug)]
    pub struct AtEndBlob {
        stream: WrappedRecvStream,
        misc: Box<Misc>,
    }

    /// The next state after the end of a blob
    #[derive(Debug, From)]
    pub enum EndBlobNext {
        /// Response is expected to have more children
        MoreChildren(AtStartChild),
        /// No more children expected
        Closing(AtClosing),
    }

    impl AtEndBlob {
        /// Read the next child, or finish
        pub fn next(mut self) -> EndBlobNext {
            if let Some((offset, ranges)) = self.misc.ranges_iter.next() {
                AtStartChild {
                    reader: self.stream,
                    offset,
                    ranges,
                    misc: self.misc,
                }
                .into()
            } else {
                AtClosing::new(self.misc, self.stream, true).into()
            }
        }
    }

    /// State when finishing the get response
    #[derive(Debug)]
    pub struct AtClosing {
        misc: Box<Misc>,
        reader: WrappedRecvStream,
        check_extra_data: bool,
    }

    impl AtClosing {
        fn new(misc: Box<Misc>, reader: WrappedRecvStream, check_extra_data: bool) -> Self {
            Self {
                misc,
                reader,
                check_extra_data,
            }
        }

        /// Finish the get response, returning statistics
        pub async fn next(self) -> result::Result<Stats, endpoint::ReadError> {
            // Shut down the stream
            let reader = self.reader;
            let mut reader = reader.into_inner();
            if self.check_extra_data {
                if let Some(chunk) = reader.read_chunk(8, false).await? {
                    reader.stop(0u8.into()).ok();
                    error!("Received unexpected data from the provider: {chunk:?}");
                }
            } else {
                reader.stop(0u8.into()).ok();
            }
            Ok(Stats {
                counters: self.misc.counters,
                elapsed: self.misc.start.elapsed(),
            })
        }
    }

    #[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq, Eq)]
    pub struct RequestCounters {
        /// payload bytes written
        pub payload_bytes_written: u64,
        /// request, hash pair and size bytes written
        pub other_bytes_written: u64,
        /// payload bytes read
        pub payload_bytes_read: u64,
        /// hash pair and size bytes read
        pub other_bytes_read: u64,
    }

    /// Stuff we need to hold on to while going through the machine states
    #[derive(Debug, derive_more::Deref, derive_more::DerefMut)]
    struct Misc {
        /// start time for statistics
        start: Instant,
        /// counters
        #[deref]
        #[deref_mut]
        counters: RequestCounters,
        /// iterator over the ranges of the collection and the children
        ranges_iter: RangesIter,
    }
}

/// Error when processing a response
#[common_fields({
    backtrace: Option<Backtrace>,
    #[snafu(implicit)]
    span_trace: SpanTrace,
})]
#[allow(missing_docs)]
#[non_exhaustive]
#[derive(Debug, Snafu)]
pub enum GetResponseError {
    /// Error when opening a stream
    #[snafu(display("connection: {source}"))]
    Connection { source: endpoint::ConnectionError },
    /// Error when writing the handshake or request to the stream
    #[snafu(display("write: {source}"))]
    Write { source: endpoint::WriteError },
    /// Error when reading from the stream
    #[snafu(display("read: {source}"))]
    Read { source: endpoint::ReadError },
    /// Error when decoding, e.g. hash mismatch
    #[snafu(display("decode: {source}"))]
    Decode { source: bao_tree::io::DecodeError },
    /// A generic error
    #[snafu(display("generic: {source}"))]
    Generic { source: anyhow::Error },
}

impl From<postcard::Error> for GetResponseError {
    fn from(cause: postcard::Error) -> Self {
        GenericSnafu.into_error(cause.into())
    }
}

impl From<bao_tree::io::DecodeError> for GetResponseError {
    fn from(cause: bao_tree::io::DecodeError) -> Self {
        match cause {
            bao_tree::io::DecodeError::Io(cause) => {
                // try to downcast to specific quinn errors
                if let Some(source) = cause.source() {
                    if let Some(error) = source.downcast_ref::<endpoint::ConnectionError>() {
                        return ConnectionSnafu.into_error(error.clone());
                    }
                    if let Some(error) = source.downcast_ref::<endpoint::ReadError>() {
                        return ReadSnafu.into_error(error.clone());
                    }
                    if let Some(error) = source.downcast_ref::<endpoint::WriteError>() {
                        return WriteSnafu.into_error(error.clone());
                    }
                }
                GenericSnafu.into_error(cause.into())
            }
            _ => DecodeSnafu.into_error(cause),
        }
    }
}

impl From<anyhow::Error> for GetResponseError {
    fn from(cause: anyhow::Error) -> Self {
        GenericSnafu.into_error(cause)
    }
}

impl From<GetResponseError> for std::io::Error {
    fn from(cause: GetResponseError) -> Self {
        Self::other(cause)
    }
}
