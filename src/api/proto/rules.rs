//! Rules of the response streams that the types can not express.
//!
//! Each stream has a state machine that is fed the items in order. It says
//! whether an item is the last one, since a store drops the sender instead of
//! sending a terminal item, or which rule the item breaks. The state machines
//! never panic or log, the caller decides what to do with a [`Violation`].
use std::io;

use bao_tree::{
    io::{mixed::EncodedItem, Leaf},
    ChunkRanges,
};
use range_collections::RangeSet2;

use super::ExportRangesItem;
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
        let leaf_end = leaf.offset + leaf.data.len() as u64;
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
/// The store never lists [`Hash::EMPTY`], since the client adds that. An error
/// is always the last item.
#[derive(Debug, Default)]
pub(crate) struct ListBlobsRules;

impl StreamRules<crate::api::Result<Hash>> for ListBlobsRules {
    fn feed(&mut self, item: &crate::api::Result<Hash>) -> Result<Step, Violation> {
        match item {
            Err(_) => Ok(Step::Last),
            Ok(hash) if *hash == Hash::EMPTY => Err(Violation::EmptyHashListed),
            Ok(_) => Ok(Step::More),
        }
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
    fn list_blobs_violations() {
        let mut rules = ListBlobsRules;
        assert_eq!(rules.feed(&Ok(Hash::new(b"a"))), Ok(Step::More));
        assert_eq!(
            rules.feed(&Ok(Hash::EMPTY)),
            Err(Violation::EmptyHashListed)
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
