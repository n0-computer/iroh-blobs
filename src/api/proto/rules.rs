//! Rules of the protocol that the types can not express.
//!
//! Response stream rules are fed the items in order. They identify the last
//! item, whether completion is explicit or follows from the requested data,
//! and report which rule an item breaks. Callers stop at the last item and
//! handle premature closure; they do not wait for later items to check them.
//! Requests are checked with [`Command::validate`]. [`StreamRules::feed`] reports
//! violations, while [`StreamRules::check`] also asserts in debug builds.
use std::io;

use bao_tree::{
    io::{mixed::EncodedItem, Leaf},
    ChunkRanges,
};
use range_collections::RangeSet2;

use super::{
    AddProgressItem, Bitfield, Command, ExportProgressItem, ExportRangesItem, ListBlobsItem,
    ListTagsItem, ListTempTagsItem,
};
use crate::Hash;

/// What an item that follows the rules means for the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// More items follow.
    More,
    /// This is the last item, the caller must stop reading.
    Last,
}

/// The rule of the protocol that an item breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display)]
pub(crate) enum Violation {
    #[display("data before the size")]
    DataBeforeSize,
    #[display("the size was sent twice")]
    SizeTwice,
    #[display("data out of order")]
    DataOutOfOrder,
    #[display("data past the size")]
    DataPastSize,
    #[display("the empty hash was listed")]
    EmptyHashListed,
    #[display("a request for the empty hash")]
    EmptyHashRequested,
    #[display("the bitfield lost ranges")]
    BitfieldShrank,
    #[display("progress before the size")]
    ProgressBeforeSize,
    #[display("progress went backwards")]
    ProgressBackwards,
    #[display("progress past the size")]
    ProgressPastSize,
    #[display("done before the size")]
    DoneBeforeSize,
    #[display("copy progress after the copy")]
    CopyProgressAfterCopy,
    #[display("copy done before the size")]
    CopyDoneBeforeSize,
    #[display("copy done after the copy")]
    CopyDoneAfterCopy,
    #[display("outboard progress before the size")]
    OutboardProgressBeforeSize,
}

/// Where the data of an export stream is, once the size is known.
#[derive(Debug, Clone, Copy)]
struct DataPos {
    size: u64,
    /// The end of the data that completes the stream.
    end: u64,
    /// The end of the data received so far.
    pos: u64,
}

impl DataPos {
    fn new(size: u64, end: u64) -> (Self, Step) {
        (Self { size, end, pos: 0 }, last_if(end == 0))
    }

    /// Data comes in ascending order and ends at the size at the latest.
    fn feed(&mut self, leaf: &Leaf) -> Result<Step, Violation> {
        let leaf_end = leaf
            .offset
            .checked_add(leaf.data.len() as u64)
            .ok_or(Violation::DataPastSize)?;
        if leaf.offset < self.pos {
            return Err(Violation::DataOutOfOrder);
        }
        if leaf_end > self.size {
            return Err(Violation::DataPastSize);
        }
        self.pos = leaf_end;
        Ok(last_if(leaf_end >= self.end))
    }
}

/// The rules of a response stream with items of type `T`.
pub(crate) trait StreamRules<T> {
    /// Feeds the next item.
    fn feed(&mut self, item: &T) -> Result<Step, Violation>;

    /// Feeds the next item, treating a violation as a bug in the store.
    ///
    /// Panics on a violation in debug builds, and returns it as an error
    /// otherwise.
    fn check(&mut self, item: &T) -> io::Result<Step> {
        self.feed(item).map_err(|violation| {
            let rules = std::any::type_name::<Self>();
            debug_assert!(false, "protocol violation in {rules}: {violation}");
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("protocol violation in {rules}: {violation}"),
            )
        })
    }
}

/// The rules of an [`ExportRangesItem`] stream.
///
/// A store sends [`ExportRangesItem::Size`] first, then the requested ranges
/// clipped to the size, in ascending order, and then drops the sender. So the
/// stream is complete with the item that reaches the end of the last clipped
/// range, and a stream that ends before that was cut short. An error is always
/// the last item.
///
/// [`super::ExportRangesRequest`] has a diagram of these rules, keep the two
/// in sync.
#[derive(Debug)]
pub(crate) struct ExportRangesRules {
    ranges: RangeSet2<u64>,
    data: Option<DataPos>,
}

impl ExportRangesRules {
    pub(crate) fn new(ranges: RangeSet2<u64>) -> Self {
        Self { ranges, data: None }
    }
}

impl StreamRules<ExportRangesItem> for ExportRangesRules {
    fn feed(&mut self, item: &ExportRangesItem) -> Result<Step, Violation> {
        match (&mut self.data, item) {
            (_, ExportRangesItem::Error(_)) => Ok(Step::Last),
            (None, ExportRangesItem::Size(size)) => {
                let clipped = &self.ranges & &RangeSet2::from(0..*size);
                // clipped is bounded, so the last boundary is its end
                let end = clipped.boundaries().last().copied().unwrap_or(0);
                let (data, step) = DataPos::new(*size, end);
                self.data = Some(data);
                Ok(step)
            }
            (Some(data), ExportRangesItem::Data(leaf)) => data.feed(leaf),
            (None, ExportRangesItem::Data(_)) => Err(Violation::DataBeforeSize),
            (Some(_), ExportRangesItem::Size(_)) => Err(Violation::SizeTwice),
        }
    }
}

/// The rules of an [`EncodedItem`] stream.
///
/// A store sends [`EncodedItem::Size`] first, then parents and leaves for the
/// requested ranges in pre-order, so leaves come in ascending order, and then
/// drops the sender. A range past the end of the blob stands for the last
/// chunk, so the data ends at the end of the requested ranges or at the size,
/// whichever comes first. The leaf that reaches that point is the last item,
/// and a stream that ends before it was cut short. An error is always the last
/// item. [`EncodedItem::Done`] is not needed, but counts as the last item too.
///
/// [`super::ExportBaoRequest`] has a diagram of these rules, keep the two in
/// sync.
#[derive(Debug)]
pub(crate) struct ExportBaoRules {
    /// The end of the requested ranges in bytes, `u64::MAX` if open.
    requested: u64,
    data: Option<DataPos>,
}

impl ExportBaoRules {
    pub(crate) fn new(ranges: &ChunkRanges) -> Self {
        let boundaries = ranges.boundaries();
        let requested = if boundaries.len() % 2 == 1 {
            // open range, up to the end of the blob
            u64::MAX
        } else {
            boundaries
                .last()
                .map_or(0, |end| end.0.saturating_mul(1024))
        };
        Self {
            requested,
            data: None,
        }
    }
}

impl StreamRules<EncodedItem> for ExportBaoRules {
    fn feed(&mut self, item: &EncodedItem) -> Result<Step, Violation> {
        match (&mut self.data, item) {
            (_, EncodedItem::Error(_) | EncodedItem::Done) => Ok(Step::Last),
            (None, EncodedItem::Size(size)) => {
                let (data, step) = DataPos::new(*size, self.requested.min(*size));
                self.data = Some(data);
                Ok(step)
            }
            (Some(_), EncodedItem::Parent(_)) => Ok(Step::More),
            (Some(data), EncodedItem::Leaf(leaf)) => data.feed(leaf),
            (None, EncodedItem::Parent(_) | EncodedItem::Leaf(_)) => Err(Violation::DataBeforeSize),
            (Some(_), EncodedItem::Size(_)) => Err(Violation::SizeTwice),
        }
    }
}

/// The rules of a [`super::Request::ListBlobs`] stream.
///
/// `Done` and `Error` are the last item, and the store never lists
/// [`Hash::EMPTY`], since the client adds that.
#[derive(Debug, Default)]
pub(crate) struct ListBlobsRules;

impl StreamRules<ListBlobsItem> for ListBlobsRules {
    fn feed(&mut self, item: &ListBlobsItem) -> Result<Step, Violation> {
        match item {
            ListBlobsItem::Hash(hash) if *hash == Hash::EMPTY => Err(Violation::EmptyHashListed),
            ListBlobsItem::Hash(_) => Ok(Step::More),
            ListBlobsItem::Done | ListBlobsItem::Error(_) => Ok(Step::Last),
        }
    }
}

/// The shared rules of [`ListTagsItem`] and [`ListTempTagsItem`] streams.
///
/// Zero or more `Tag` items are followed by `Done` or `Error`. Both are terminal,
/// so a client stops immediately and reports closure before either as an error.
/// The diagram on [`ListBlobsItem`] and the clients in `api::tags` must agree.
#[derive(Debug, Default)]
pub(crate) struct TagListRules;

impl StreamRules<ListTagsItem> for TagListRules {
    fn feed(&mut self, item: &ListTagsItem) -> Result<Step, Violation> {
        Ok(match item {
            ListTagsItem::Tag(_) => Step::More,
            ListTagsItem::Done | ListTagsItem::Error(_) => Step::Last,
        })
    }
}

impl StreamRules<ListTempTagsItem> for TagListRules {
    fn feed(&mut self, item: &ListTempTagsItem) -> Result<Step, Violation> {
        Ok(match item {
            ListTempTagsItem::Tag(_) => Step::More,
            ListTempTagsItem::Done | ListTempTagsItem::Error(_) => Step::Last,
        })
    }
}

/// Rules that are only checked in debug builds, because they allocate.
///
/// Rules that only compare a few numbers are checked in every build. For the
/// others, completion of the stream must not depend on them, since in release
/// builds this is a zero sized type that accepts every item as [`Step::More`].
#[derive(Debug, Default)]
pub(crate) struct DebugOnly<R> {
    #[cfg(debug_assertions)]
    rules: R,
    #[cfg(not(debug_assertions))]
    rules: std::marker::PhantomData<fn() -> R>,
}

impl<R> DebugOnly<R> {
    #[allow(unused_variables)]
    pub(crate) fn new(rules: R) -> Self {
        Self {
            #[cfg(debug_assertions)]
            rules,
            #[cfg(not(debug_assertions))]
            rules: std::marker::PhantomData,
        }
    }
}

impl<T, R: StreamRules<T>> StreamRules<T> for DebugOnly<R> {
    #[allow(unused_variables)]
    fn feed(&mut self, item: &T) -> Result<Step, Violation> {
        #[cfg(debug_assertions)]
        return self.rules.feed(item);
        #[cfg(not(debug_assertions))]
        Ok(Step::More)
    }
}

/// The rules of a [`super::Request::Observe`] stream.
///
/// The first bitfield is the current state, and every following one is the
/// whole new state. The ranges only ever grow: if something is taken away, the
/// store ends the stream instead of sending a smaller bitfield.
#[derive(Debug, Default)]
pub(crate) struct ObserveRules {
    ranges: ChunkRanges,
}

impl StreamRules<Bitfield> for ObserveRules {
    fn feed(&mut self, item: &Bitfield) -> Result<Step, Violation> {
        if !item.ranges.is_superset(&self.ranges) {
            return Err(Violation::BitfieldShrank);
        }
        self.ranges = item.ranges.clone();
        Ok(Step::More)
    }
}

/// The rules of a [`super::Request::ExportPath`] stream.
///
/// The size comes first, then copy progress up to the size, and then
/// [`ExportProgressItem::Done`]. An error is always the last item.
///
/// [`ExportProgressItem`] has a diagram of these rules, keep the two in sync.
#[derive(Debug, Default)]
pub(crate) struct ExportPathRules {
    size: Option<u64>,
    /// The last copy progress.
    pos: u64,
}

impl StreamRules<ExportProgressItem> for ExportPathRules {
    fn feed(&mut self, item: &ExportProgressItem) -> Result<Step, Violation> {
        match (self.size, item) {
            (_, ExportProgressItem::Error(_)) => Ok(Step::Last),
            (None, ExportProgressItem::Size(size)) => {
                self.size = Some(*size);
                Ok(Step::More)
            }
            (Some(_), ExportProgressItem::Size(_)) => Err(Violation::SizeTwice),
            (None, ExportProgressItem::CopyProgress(_)) => Err(Violation::ProgressBeforeSize),
            (Some(size), ExportProgressItem::CopyProgress(offset)) => {
                if *offset < self.pos {
                    return Err(Violation::ProgressBackwards);
                }
                if *offset > size {
                    return Err(Violation::ProgressPastSize);
                }
                self.pos = *offset;
                Ok(Step::More)
            }
            (None, ExportProgressItem::Done) => Err(Violation::DoneBeforeSize),
            (Some(_), ExportProgressItem::Done) => Ok(Step::Last),
        }
    }
}

/// The rules of an [`AddProgressItem`] stream.
///
/// First comes the copy: copy progress, the size once, and at most one
/// [`AddProgressItem::CopyDone`] after the size. Then outboard progress, and
/// then [`AddProgressItem::Done`], which both need the size. Progress never
/// goes backwards or past the size. An error is always the last item.
///
/// [`AddProgressItem`] has a diagram of these rules, keep the two in sync.
#[derive(Debug, Default)]
pub(crate) struct AddProgressRules {
    size: Option<u64>,
    /// Copy done or outboard progress was sent, so the copy is over.
    copy_over: bool,
    /// The last copy progress.
    copy: u64,
    /// The last outboard progress.
    outboard: u64,
}

impl AddProgressRules {
    fn progress(&self, last: u64, offset: u64) -> Result<(), Violation> {
        if offset < last {
            return Err(Violation::ProgressBackwards);
        }
        if self.size.is_some_and(|size| offset > size) {
            return Err(Violation::ProgressPastSize);
        }
        Ok(())
    }
}

impl StreamRules<AddProgressItem> for AddProgressRules {
    fn feed(&mut self, item: &AddProgressItem) -> Result<Step, Violation> {
        match item {
            AddProgressItem::Error(_) => return Ok(Step::Last),
            AddProgressItem::CopyProgress(offset) => {
                if self.copy_over {
                    return Err(Violation::CopyProgressAfterCopy);
                }
                self.progress(self.copy, *offset)?;
                self.copy = *offset;
            }
            AddProgressItem::Size(size) => {
                if self.size.is_some() {
                    return Err(Violation::SizeTwice);
                }
                if self.copy > *size {
                    return Err(Violation::ProgressPastSize);
                }
                self.size = Some(*size);
            }
            AddProgressItem::CopyDone => {
                if self.copy_over {
                    return Err(Violation::CopyDoneAfterCopy);
                }
                if self.size.is_none() {
                    return Err(Violation::CopyDoneBeforeSize);
                }
                self.copy_over = true;
            }
            AddProgressItem::OutboardProgress(offset) => {
                if self.size.is_none() {
                    return Err(Violation::OutboardProgressBeforeSize);
                }
                self.progress(self.outboard, *offset)?;
                self.outboard = *offset;
                self.copy_over = true;
            }
            AddProgressItem::Done(_) => {
                if self.size.is_none() {
                    return Err(Violation::DoneBeforeSize);
                }
                return Ok(Step::Last);
            }
        }
        Ok(Step::More)
    }
}

impl Command {
    /// Checks that this command follows the rules of the protocol.
    ///
    /// The client answers reads and deletes of [`Hash::EMPTY`] itself, see the
    /// [module docs](super), so they never reach the store.
    pub(crate) fn validate(&self) -> Result<(), Violation> {
        let hash = match self {
            Self::BlobStatus(cmd) => cmd.hash,
            Self::ExportBao(cmd) => cmd.hash,
            Self::ExportRanges(cmd) => cmd.hash,
            Self::ImportBao(cmd) => cmd.hash,
            Self::Observe(cmd) => cmd.hash,
            Self::DeleteBlobs(cmd) if cmd.hashes.contains(&Hash::EMPTY) => Hash::EMPTY,
            _ => return Ok(()),
        };
        if hash == Hash::EMPTY {
            return Err(Violation::EmptyHashRequested);
        }
        Ok(())
    }
}

fn last_if(last: bool) -> Step {
    if last {
        Step::Last
    } else {
        Step::More
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;

    fn leaf(offset: u64, len: usize) -> Leaf {
        Leaf {
            offset,
            data: Bytes::from(vec![0u8; len]),
        }
    }

    #[test]
    fn export_leaf_end_overflow() {
        let mut bao = ExportBaoRules::new(&ChunkRanges::all());
        let mut ranges = ExportRangesRules::new(RangeSet2::all());
        bao.feed(&EncodedItem::Size(u64::MAX)).unwrap();
        ranges.feed(&ExportRangesItem::Size(u64::MAX)).unwrap();
        for (offset, len) in [(u64::MAX, 1), (u64::MAX - 1, 2)] {
            assert_eq!(
                bao.feed(&EncodedItem::Leaf(leaf(offset, len))),
                Err(Violation::DataPastSize)
            );
            assert_eq!(
                ranges.feed(&ExportRangesItem::Data(leaf(offset, len))),
                Err(Violation::DataPastSize)
            );
        }
        // Ending exactly at u64::MAX is valid and must not be confused with overflow.
        assert_eq!(
            bao.feed(&EncodedItem::Leaf(leaf(u64::MAX - 1, 1))),
            Ok(Step::Last)
        );
        assert_eq!(
            ranges.feed(&ExportRangesItem::Data(leaf(u64::MAX - 1, 1))),
            Ok(Step::Last)
        );
    }

    #[test]
    fn export_bao_violations() {
        let mut rules = ExportBaoRules::new(&ChunkRanges::all());
        assert_eq!(
            rules.feed(&EncodedItem::Leaf(leaf(0, 10))),
            Err(Violation::DataBeforeSize)
        );
        let mut rules = ExportBaoRules::new(&ChunkRanges::all());
        assert_eq!(rules.feed(&EncodedItem::Size(2048)), Ok(Step::More));
        assert_eq!(
            rules.feed(&EncodedItem::Size(2048)),
            Err(Violation::SizeTwice)
        );
    }

    #[test]
    fn export_bao_order() {
        let mut rules = ExportBaoRules::new(&ChunkRanges::all());
        assert_eq!(rules.feed(&EncodedItem::Size(4096)), Ok(Step::More));
        assert_eq!(
            rules.feed(&EncodedItem::Leaf(leaf(1024, 1024))),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&EncodedItem::Leaf(leaf(0, 1024))),
            Err(Violation::DataOutOfOrder)
        );
        assert_eq!(
            rules.feed(&EncodedItem::Leaf(leaf(2048, 4096))),
            Err(Violation::DataPastSize)
        );
        assert_eq!(
            rules.feed(&EncodedItem::Leaf(leaf(2048, 2048))),
            Ok(Step::Last)
        );
    }

    #[test]
    fn validate_requests() {
        use crate::api::proto::{BlobStatusRequest, ObserveRequest};

        let observe = |hash| {
            let (tx, _) = irpc::channel::mpsc::channel(1);
            Command::Observe((ObserveRequest { hash }, tx).into())
        };
        assert_eq!(observe(Hash::new(b"a")).validate(), Ok(()));
        assert_eq!(
            observe(Hash::EMPTY).validate(),
            Err(Violation::EmptyHashRequested)
        );
        let (tx, _) = irpc::channel::oneshot::channel();
        let status = Command::BlobStatus((BlobStatusRequest { hash: Hash::EMPTY }, tx).into());
        assert_eq!(status.validate(), Err(Violation::EmptyHashRequested));
    }

    #[test]
    fn list_blobs_violations() {
        let mut rules = ListBlobsRules;
        assert_eq!(
            rules.feed(&ListBlobsItem::Hash(Hash::new(b"a"))),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&ListBlobsItem::Hash(Hash::EMPTY)),
            Err(Violation::EmptyHashListed)
        );
        assert_eq!(rules.feed(&ListBlobsItem::Done), Ok(Step::Last));
        let error = || crate::api::Error::other("failed");
        assert_eq!(rules.feed(&ListBlobsItem::Error(error())), Ok(Step::Last));
    }

    #[test]
    fn tag_list_steps() {
        let error = || crate::api::Error::other("failed");
        let tag = crate::api::tags::TagInfo::new("a", Hash::new(b"a"));
        let mut rules = TagListRules;
        assert_eq!(rules.feed(&ListTagsItem::Tag(tag)), Ok(Step::More));
        assert_eq!(rules.feed(&ListTagsItem::Done), Ok(Step::Last));
        assert_eq!(rules.feed(&ListTagsItem::Error(error())), Ok(Step::Last));
        let tt = crate::HashAndFormat::raw(Hash::new(b"a"));
        assert_eq!(rules.feed(&ListTempTagsItem::Tag(tt)), Ok(Step::More));
        assert_eq!(rules.feed(&ListTempTagsItem::Done), Ok(Step::Last));
        assert_eq!(
            rules.feed(&ListTempTagsItem::Error(error())),
            Ok(Step::Last)
        );
    }

    #[test]
    fn observe_violations() {
        let mut rules = ObserveRules::default();
        let chunks = |r: std::ops::Range<u64>| {
            ChunkRanges::from(bao_tree::ChunkNum(r.start)..bao_tree::ChunkNum(r.end))
        };
        assert_eq!(
            rules.feed(&Bitfield::new(chunks(0..2), 4096)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&Bitfield::new(chunks(0..3), 4096)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&Bitfield::new(chunks(1..3), 4096)),
            Err(Violation::BitfieldShrank)
        );
    }

    #[test]
    fn export_path_violations() {
        let mut rules = ExportPathRules::default();
        assert_eq!(
            rules.feed(&ExportProgressItem::CopyProgress(0)),
            Err(Violation::ProgressBeforeSize)
        );
        assert_eq!(
            rules.feed(&ExportProgressItem::Done),
            Err(Violation::DoneBeforeSize)
        );
        assert_eq!(rules.feed(&ExportProgressItem::Size(100)), Ok(Step::More));
        assert_eq!(
            rules.feed(&ExportProgressItem::CopyProgress(50)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&ExportProgressItem::CopyProgress(40)),
            Err(Violation::ProgressBackwards)
        );
        assert_eq!(
            rules.feed(&ExportProgressItem::CopyProgress(200)),
            Err(Violation::ProgressPastSize)
        );
        assert_eq!(rules.feed(&ExportProgressItem::Done), Ok(Step::Last));
    }

    #[test]
    fn add_progress_violations() {
        let mut rules = AddProgressRules::default();
        assert_eq!(
            rules.feed(&AddProgressItem::CopyProgress(10)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::CopyDone),
            Err(Violation::CopyDoneBeforeSize)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::Size(5)),
            Err(Violation::ProgressPastSize)
        );
        assert_eq!(rules.feed(&AddProgressItem::Size(100)), Ok(Step::More));
        assert_eq!(rules.feed(&AddProgressItem::CopyDone), Ok(Step::More));
        assert_eq!(
            rules.feed(&AddProgressItem::CopyProgress(20)),
            Err(Violation::CopyProgressAfterCopy)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::OutboardProgress(50)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::OutboardProgress(40)),
            Err(Violation::ProgressBackwards)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::CopyDone),
            Err(Violation::CopyDoneAfterCopy)
        );
        // a reference import has no copy
        let mut rules = AddProgressRules::default();
        assert_eq!(
            rules.feed(&AddProgressItem::OutboardProgress(0)),
            Err(Violation::OutboardProgressBeforeSize)
        );
        assert_eq!(rules.feed(&AddProgressItem::Size(100)), Ok(Step::More));
        assert_eq!(
            rules.feed(&AddProgressItem::OutboardProgress(0)),
            Ok(Step::More)
        );
        assert_eq!(
            rules.feed(&AddProgressItem::CopyDone),
            Err(Violation::CopyDoneAfterCopy)
        );
    }

    #[test]
    fn export_ranges_violations() {
        let mut rules = ExportRangesRules::new(RangeSet2::all());
        assert_eq!(
            rules.feed(&ExportRangesItem::Data(leaf(0, 10))),
            Err(Violation::DataBeforeSize)
        );
        let mut rules = ExportRangesRules::new(RangeSet2::all());
        assert_eq!(rules.feed(&ExportRangesItem::Size(20)), Ok(Step::More));
        assert_eq!(
            rules.feed(&ExportRangesItem::Size(20)),
            Err(Violation::SizeTwice)
        );
        assert_eq!(
            rules.feed(&ExportRangesItem::Data(leaf(0, 30))),
            Err(Violation::DataPastSize)
        );
        assert_eq!(
            rules.feed(&ExportRangesItem::Data(leaf(0, 20))),
            Ok(Step::Last)
        );
    }
}
