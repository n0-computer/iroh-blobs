//! Outboard computation for the browser memory store.
//!
//! In the browser everything runs on one thread, so computing the outboard of
//! a large import in one go blocks the page until it is done. This computes the
//! same outboard incrementally and gives control back to the event loop after
//! every [`HASH_BYTES_PER_TURN`] bytes of input.
use std::io;

use bao_tree::{blake3, io::outboard::PreOrderMemOutboard, BaoTree};
use bytes::Bytes;
use iroh_io::AsyncStreamReader;
use n0_future::time::{sleep, Duration};

use super::IROH_BLOCK_SIZE;

/// Maximum number of input bytes hashed between two yields.
const HASH_BYTES_PER_TURN: usize = 256 * 1024;

/// Computes the pre-order outboard for `data`, yielding between chunks of work.
///
/// The result is identical to `PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE)`.
pub(super) async fn create(data: Bytes) -> io::Result<PreOrderMemOutboard> {
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
        remaining: HASH_BYTES_PER_TURN,
    };
    outboard.root = bao_tree::io::fsm::outboard(reader, tree, &mut outboard).await?;
    Ok(outboard)
}

/// Reads from shared [`Bytes`] and yields once the per-turn budget is used up.
///
/// The outboard computation reads at most one chunk group per call, which is far
/// below the budget. Larger reads are rejected so the budget can't be bypassed.
struct YieldingReader {
    data: Bytes,
    remaining: usize,
}

impl YieldingReader {
    async fn take(&mut self, len: usize) -> io::Result<Bytes> {
        if len > HASH_BYTES_PER_TURN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read exceeds the per-turn budget",
            ));
        }
        let len = len.min(self.data.len());
        if len > self.remaining {
            sleep(Duration::ZERO).await;
            self.remaining = HASH_BYTES_PER_TURN;
        }
        self.remaining -= len;
        Ok(self.data.split_to(len))
    }
}

impl AsyncStreamReader for YieldingReader {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        self.take(len).await
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        if self.data.len() < L {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let bytes = self.take(L).await?;
        let mut result = [0; L];
        result.copy_from_slice(&bytes);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use testresult::TestResult;

    use super::{create, HASH_BYTES_PER_TURN, IROH_BLOCK_SIZE};

    #[tokio::test]
    async fn matches_sync_outboard() -> TestResult<()> {
        let sizes = [
            0,
            1,
            1024,
            16 * 1024,
            16 * 1024 + 1,
            HASH_BYTES_PER_TURN - 1,
            HASH_BYTES_PER_TURN,
            HASH_BYTES_PER_TURN + 1,
            3 * HASH_BYTES_PER_TURN + 12345,
        ];
        for size in sizes {
            let data: Bytes = (0..size)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>()
                .into();
            let expected = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
            let actual = create(data).await?;
            assert_eq!(actual.root, expected.root, "root for size {size}");
            assert_eq!(actual.tree, expected.tree, "tree for size {size}");
            assert_eq!(actual.data, expected.data, "outboard for size {size}");
        }
        Ok(())
    }
}
