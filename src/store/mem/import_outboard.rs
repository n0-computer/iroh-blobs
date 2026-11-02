//! Outboard computation for the browser memory store.
//!
//! In the browser everything runs on one thread, so computing the outboard of
//! a large import in one go blocks the page until it is done. This computes the
//! same outboard incrementally and gives control back to the event loop once a
//! turn has taken longer than [`MAX_TURN`].
use std::io;

use bao_tree::{blake3, io::outboard::PreOrderMemOutboard, BaoTree};
use bytes::Bytes;
use iroh_io::AsyncStreamReader;
use n0_future::time::{sleep, Duration, Instant};

use super::IROH_BLOCK_SIZE;

/// Longest time the outboard computation runs before giving the event loop a turn.
///
/// Well below a frame, so the page stays responsive. Browsers clamp nested
/// timeouts to at least 4 ms, so yielding much more often would mostly wait.
const MAX_TURN: Duration = Duration::from_millis(8);

/// Computes the pre-order outboard for `data`, yielding between chunks of work.
///
/// The result is identical to `PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE)`.
pub(super) async fn create(data: Bytes) -> io::Result<PreOrderMemOutboard> {
    create_with_max_turn(data, MAX_TURN).await
}

async fn create_with_max_turn(data: Bytes, max_turn: Duration) -> io::Result<PreOrderMemOutboard> {
    // In the browser, `sleep` is backed by `setTimeout`, so this gives the event
    // loop a turn. A self-waking future would not.
    sleep(Duration::ZERO).await;
    let tree = BaoTree::new(data.len() as u64, IROH_BLOCK_SIZE);
    let outboard_size = usize::try_from(tree.outboard_size())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "outboard is too large"))?;
    let mut outboard = PreOrderMemOutboard {
        root: blake3::Hash::from([0; 32]),
        tree,
        data: vec![0; outboard_size],
    };
    let reader = YieldingReader {
        data,
        max_turn,
        turn_start: Instant::now(),
    };
    outboard.root = bao_tree::io::fsm::outboard(reader, tree, &mut outboard).await?;
    Ok(outboard)
}

/// Reads from shared [`Bytes`] and yields once the current turn took longer than `max_turn`.
struct YieldingReader {
    data: Bytes,
    max_turn: Duration,
    turn_start: Instant,
}

impl YieldingReader {
    async fn take(&mut self, len: usize) -> Bytes {
        if self.turn_start.elapsed() >= self.max_turn {
            sleep(Duration::ZERO).await;
            self.turn_start = Instant::now();
        }
        self.data.split_to(len.min(self.data.len()))
    }
}

impl AsyncStreamReader for YieldingReader {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        Ok(self.take(len).await)
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        if self.data.len() < L {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let bytes = self.take(L).await;
        let mut result = [0; L];
        result.copy_from_slice(&bytes);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use n0_future::time::Duration;
    use testresult::TestResult;

    use super::{create, create_with_max_turn, IROH_BLOCK_SIZE};

    const SIZES: [usize; 7] = [
        0,
        1,
        1024,
        16 * 1024,
        16 * 1024 + 1,
        256 * 1024,
        1024 * 1024 + 12345,
    ];

    fn test_data(size: usize) -> Bytes {
        (0..size)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>()
            .into()
    }

    fn assert_matches_sync(actual: &PreOrderMemOutboard, data: &[u8]) {
        let size = data.len();
        let expected = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
        assert_eq!(actual.root, expected.root, "root for size {size}");
        assert_eq!(actual.tree, expected.tree, "tree for size {size}");
        assert_eq!(actual.data, expected.data, "outboard for size {size}");
    }

    #[tokio::test]
    async fn matches_sync_outboard() -> TestResult<()> {
        for size in SIZES {
            let data = test_data(size);
            assert_matches_sync(&create(data.clone()).await?, &data);
        }
        Ok(())
    }

    /// With a zero max turn the computation yields before every read.
    #[tokio::test]
    async fn matches_sync_outboard_yielding_every_read() -> TestResult<()> {
        for size in SIZES {
            let data = test_data(size);
            assert_matches_sync(
                &create_with_max_turn(data.clone(), Duration::ZERO).await?,
                &data,
            );
        }
        Ok(())
    }
}
