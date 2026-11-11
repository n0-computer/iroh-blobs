use std::io;

use bao_tree::io::sync::WriteAt;

use crate::store::IROH_BLOCK_SIZE;

/// Keep track of the most precise size we know of.
///
/// When in memory, we don't have to write the size for every chunk to a separate
/// slot, but can just keep the best one.
#[derive(Debug, Default)]
pub struct SizeInfo {
    pub offset: u64,
    pub size: u64,
}

/// Returns where in a sizes file the size for the chunk group at byte `offset` goes.
pub(crate) fn size_slot(offset: u64) -> u64 {
    offset / IROH_BLOCK_SIZE.bytes() as u64 * 8
}

#[allow(dead_code)]
impl SizeInfo {
    /// Write a size at the given offset. The size at the highest offset is going to be kept.
    pub fn write(&mut self, offset: u64, size: u64) {
        // >= instead of > because we want to be able to update size 0, the initial value.
        if offset >= self.offset {
            self.offset = offset;
            self.size = size;
        }
    }

    /// The current size, representing the most correct size we know.
    pub fn current_size(&self) -> u64 {
        self.size
    }

    /// Persist into a file where each chunk group has its own slot.
    pub fn persist(&self, mut target: impl WriteAt) -> io::Result<()> {
        target.write_all_at(size_slot(self.offset), self.size.to_le_bytes().as_slice())?;
        Ok(())
    }

    /// Convert to a vec in slot format.
    #[allow(dead_code)]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = Vec::new();
        self.persist(&mut res).expect("io error writing to vec");
        res
    }
}

#[cfg(test)]
mod tests {
    use super::SizeInfo;

    #[test]
    fn persist_uses_one_slot_per_chunk_group() {
        let mut info = SizeInfo::default();
        // a leaf of the third chunk group
        info.write(2 * 16 * 1024, 100_000);
        let slots = info.to_vec();
        assert_eq!(slots.len(), 3 * 8);
        assert_eq!(slots[2 * 8..], 100_000u64.to_le_bytes());
    }
}
