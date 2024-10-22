//! Implementations of blob stores
use crate::{BlobFormat, Hash, HashAndFormat};

#[cfg(feature = "fs-store")]
mod bao_file;
pub mod mem;
mod mutable_mem_storage;
pub mod readonly_mem;

#[cfg(feature = "fs-store")]
pub mod fs;

mod traits;
use tracing::warn;
pub use traits::*;

/// Create a 16 byte unique ID.
fn new_uuid() -> [u8; 16] {
    use rand::Rng;
    rand::thread_rng().gen::<[u8; 16]>()
}

/// Create temp file name based on a 16 byte UUID.
fn temp_name() -> String {
    format!("{}.temp", hex::encode(new_uuid()))
}

#[derive(Debug, Default, Clone)]
struct TempCounters {
    /// number of raw temp tags for a hash
    raw: u64,
    /// number of hash seq temp tags for a hash
    hash_seq: u64,
}

impl TempCounters {
    fn counter(&mut self, format: BlobFormat) -> &mut u64 {
        match format {
            BlobFormat::Raw => &mut self.raw,
            BlobFormat::HashSeq => &mut self.hash_seq,
        }
    }

    fn inc(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.checked_add(1).unwrap();
    }

    fn dec(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.saturating_sub(1);
    }

    fn is_empty(&self) -> bool {
        self.raw == 0 && self.hash_seq == 0
    }
}

#[derive(Debug, Clone, Default)]
struct TempCounterMap(std::collections::BTreeMap<Hash, TempCounters>);

impl TempCounterMap {
    fn inc(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        self.0.entry(*hash).or_default().inc(*format)
    }

    fn dec(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        let Some(counters) = self.0.get_mut(hash) else {
            warn!("Decrementing non-existent temp tag");
            return;
        };
        counters.dec(*format);
        if counters.is_empty() {
            self.0.remove(hash);
        }
    }

    fn contains(&self, hash: &Hash) -> bool {
        self.0.contains_key(hash)
    }

    fn keys(&self) -> impl Iterator<Item = HashAndFormat> {
        let mut res = Vec::new();
        for (k, v) in self.0.iter() {
            if v.raw > 0 {
                res.push(HashAndFormat::raw(*k));
            }
            if v.hash_seq > 0 {
                res.push(HashAndFormat::hash_seq(*k));
            }
        }
        res.into_iter()
    }
}
