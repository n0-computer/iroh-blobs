use std::collections::{BTreeMap, BTreeSet};

use bao_tree::{ChunkNum, ChunkRanges};
use iroh::NodeId;

use crate::Hash;

/// Trait for a download planner.
///
/// A download planner has the option to be stateful and keep track of plans
/// depending on the hash, but many planners will be stateless.
///
/// Planners can do whatever they want with the chunk ranges. Usually, they
/// want to deduplicate the ranges, but they could also do other things, like
/// eliminate gaps or even extend ranges. The only thing they should not do is
/// to add new peers to the list of options.
pub trait DownloadPlanner: Send + std::fmt::Debug + 'static {
    /// Make a download plan for a hash, by reducing or eliminating the overlap of chunk ranges
    fn plan(&mut self, hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>);
}

/// A boxed download planner
pub type BoxedDownloadPlanner = Box<dyn DownloadPlanner>;

/// A download planner that just leaves everything as is.
///
/// Data will be downloaded from all peers wherever multiple peers have the same data.
#[derive(Debug, Clone, Copy)]
pub struct NoopPlanner;

impl DownloadPlanner for NoopPlanner {
    fn plan(&mut self, _hash: Hash, _options: &mut BTreeMap<NodeId, ChunkRanges>) {}
}

/// A download planner that fully removes overlap between peers.
///
/// It divides files into stripes of a fixed size `1 << stripe_size_log` chunks,
/// and for each stripe decides on a single peer to download from, based on the
/// peer id and a random seed.
#[derive(Debug)]
pub struct StripePlanner {
    /// seed for the score function. This can be set to 0 for testing for
    /// maximum determinism, but can be set to a random value for production
    /// to avoid multiple downloaders coming up with the same plan.
    seed: u64,
    /// The log of the stripe size in chunks. This planner is relatively
    /// dumb and does not try to come up with continuous ranges, but you can
    /// just set this to a large value to avoid fragmentation.
    ///
    /// In the very common case where you have small downloads, this will
    /// frequently just choose a single peer for the entire download.
    ///
    /// This is a feature, not a bug. For small downloads, it is not worth
    /// the effort to come up with a more sophisticated plan.
    stripe_size_log: u8,
}

impl StripePlanner {
    /// Create a new planner with the given seed and stripe size.
    #[allow(dead_code)]
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self {
            seed,
            stripe_size_log,
        }
    }

    /// The score function to decide which peer to download from.
    fn score(peer: &NodeId, seed: u64, stripe: u64) -> u64 {
        // todo: use fnv? blake3 is a bit overkill
        let mut data = [0u8; 32 + 8 + 8];
        data[..32].copy_from_slice(peer.as_bytes());
        data[32..40].copy_from_slice(&stripe.to_be_bytes());
        data[40..48].copy_from_slice(&seed.to_be_bytes());
        let hash = blake3::hash(&data);
        u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }
}

impl DownloadPlanner for StripePlanner {
    fn plan(&mut self, _hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>) {
        assert!(
            options.values().all(|x| x.boundaries().len() % 2 == 0),
            "open ranges not supported"
        );
        options.retain(|_, x| !x.is_empty());
        if options.len() <= 1 {
            return;
        }
        let ranges = get_continuous_ranges(options, self.stripe_size_log).unwrap();
        for range in ranges.windows(2) {
            let start = ChunkNum(range[0]);
            let end = ChunkNum(range[1]);
            let curr = ChunkRanges::from(start..end);
            let stripe = range[0] >> self.stripe_size_log;
            let mut best_peer = None;
            let mut best_score = 0;
            let mut matching = vec![];
            for (peer, peer_ranges) in options.iter_mut() {
                if peer_ranges.contains(&start) {
                    let score = Self::score(peer, self.seed, stripe);
                    if score > best_score && peer_ranges.contains(&start) {
                        best_peer = Some(*peer);
                        best_score = score;
                    }
                    matching.push((peer, peer_ranges));
                }
            }
            for (peer, peer_ranges) in matching {
                if *peer != best_peer.unwrap() {
                    peer_ranges.difference_with(&curr);
                }
            }
        }
        options.retain(|_, x| !x.is_empty());
    }
}

fn get_continuous_ranges(
    options: &mut BTreeMap<NodeId, ChunkRanges>,
    stripe_size_log: u8,
) -> Option<Vec<u64>> {
    let mut ranges = BTreeSet::new();
    for x in options.values() {
        ranges.extend(x.boundaries().iter().map(|x| x.0));
    }
    let min = ranges.iter().next().copied()?;
    let max = ranges.iter().next_back().copied()?;
    // add stripe subdividers
    for i in (min >> stripe_size_log)..(max >> stripe_size_log) {
        let x = i << stripe_size_log;
        if x > min && x < max {
            ranges.insert(x);
        }
    }
    let ranges = ranges.into_iter().collect::<Vec<_>>();
    Some(ranges)
}

/// A download planner that fully removes overlap between peers.
///
/// It divides files into stripes of a fixed size `1 << stripe_size_log` chunks,
/// and for each stripe decides on a single peer to download from, based on the
/// peer id and a random seed.
#[derive(Debug)]
pub struct StripePlanner2 {
    /// seed for the score function. This can be set to 0 for testing for
    /// maximum determinism, but can be set to a random value for production
    /// to avoid multiple downloaders coming up with the same plan.
    seed: u64,
    /// The log of the stripe size in chunks. This planner is relatively
    /// dumb and does not try to come up with continuous ranges, but you can
    /// just set this to a large value to avoid fragmentation.
    ///
    /// In the very common case where you have small downloads, this will
    /// frequently just choose a single peer for the entire download.
    ///
    /// This is a feature, not a bug. For small downloads, it is not worth
    /// the effort to come up with a more sophisticated plan.
    stripe_size_log: u8,
}

impl StripePlanner2 {
    /// Create a new planner with the given seed and stripe size.
    pub fn new(seed: u64, stripe_size_log: u8) -> Self {
        Self {
            seed,
            stripe_size_log,
        }
    }

    /// The score function to decide which peer to download from.
    fn score(peer: &NodeId, seed: u64) -> u64 {
        // todo: use fnv? blake3 is a bit overkill
        let mut data = [0u8; 32 + 8];
        data[..32].copy_from_slice(peer.as_bytes());
        data[32..40].copy_from_slice(&seed.to_be_bytes());
        let hash = blake3::hash(&data);
        u64::from_be_bytes(hash.as_bytes()[..8].try_into().unwrap())
    }
}

impl DownloadPlanner for StripePlanner2 {
    fn plan(&mut self, _hash: Hash, options: &mut BTreeMap<NodeId, ChunkRanges>) {
        assert!(
            options.values().all(|x| x.boundaries().len() % 2 == 0),
            "open ranges not supported"
        );
        options.retain(|_, x| !x.is_empty());
        if options.len() <= 1 {
            return;
        }
        let ranges = get_continuous_ranges(options, self.stripe_size_log).unwrap();
        for range in ranges.windows(2) {
            let start = ChunkNum(range[0]);
            let end = ChunkNum(range[1]);
            let curr = ChunkRanges::from(start..end);
            let stripe = range[0] >> self.stripe_size_log;
            let mut best_peer = None;
            let mut best_score = None;
            let mut matching = vec![];
            for (peer, peer_ranges) in options.iter_mut() {
                if peer_ranges.contains(&start) {
                    matching.push((peer, peer_ranges));
                }
            }
            let mut peer_and_score = matching
                .iter()
                .map(|(peer, _)| (Self::score(peer, self.seed), peer))
                .collect::<Vec<_>>();
            peer_and_score.sort();
            let peer_to_rank = peer_and_score
                .into_iter()
                .enumerate()
                .map(|(i, (_, peer))| (*peer, i as u64))
                .collect::<BTreeMap<_, _>>();
            let n = matching.len() as u64;
            for (peer, _) in matching.iter() {
                let score = Some((peer_to_rank[*peer] + stripe) % n);
                if score > best_score {
                    best_peer = Some(**peer);
                    best_score = score;
                }
            }
            for (peer, peer_ranges) in matching {
                if *peer != best_peer.unwrap() {
                    peer_ranges.difference_with(&curr);
                }
            }
        }
        options.retain(|_, x| !x.is_empty());
    }
}
