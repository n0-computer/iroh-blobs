//! Where the fs store keeps the bytes of its entries.
//!
//! The store keeps all the logic: which parts an entry has, bao validation,
//! inlining, crash consistency. A [`Storage`] only maps a hash's parts to
//! locations, and opens and deletes the resources there.
//!
//! A location describes where bytes live, a `PathBuf` for files. A resource is
//! an open handle to them, a `File` today, a memory map for complete parts
//! later. The location type is fixed per store type, so a store never has to
//! tell kinds of locations apart.
//!
//! Draft: the store still uses [`PathOptions`] as its only storage. Staging
//! imports, syncing, external data, bitfields and recovery still use paths
//! directly.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::PathBuf,
};

use bao_tree::io::{
    mixed::ReadBytesAt,
    sync::{ReadAt, Size, WriteAt},
};

use super::{file_deletes::BaoFilePart, options::PathOptions};
use crate::Hash;

/// How to open a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Read a part of a complete entry, which is never written again.
    Read,
    /// Read and write a part of a partial entry, creating it if missing.
    ReadWrite,
}

/// Maps the parts of an entry to locations, and opens and deletes the resources there.
pub(crate) trait Storage: fmt::Debug + Send + Sync + 'static {
    /// Describes where bytes live.
    type Location: Clone + fmt::Debug + Send + Sync + 'static;
    /// An open handle to the bytes at a location.
    type Resource: ReadAt + ReadBytesAt + WriteAt + Size + fmt::Debug + Send + Sync + 'static;

    /// The location of a part of the entry for `hash`.
    fn location(&self, hash: &Hash, part: BaoFilePart) -> Self::Location;

    /// Opens the resource at `location`.
    fn open(&self, location: &Self::Location, mode: Mode) -> io::Result<Self::Resource>;

    /// Deletes the resource at `location`; one that does not exist is not an error.
    fn delete(&self, location: &Self::Location) -> io::Result<()>;

    /// Opens a part of the entry for `hash`.
    fn open_part(&self, hash: &Hash, part: BaoFilePart, mode: Mode) -> io::Result<Self::Resource> {
        self.open(&self.location(hash, part), mode)
    }

    /// Deletes a part of the entry for `hash`; one that does not exist is not an error.
    fn delete_part(&self, hash: &Hash, part: BaoFilePart) -> io::Result<()> {
        self.delete(&self.location(hash, part))
    }
}

/// Plain files in the store's data directory.
impl Storage for PathOptions {
    type Location = PathBuf;
    type Resource = File;

    fn location(&self, hash: &Hash, part: BaoFilePart) -> PathBuf {
        match part {
            BaoFilePart::Data => self.data_path(hash),
            BaoFilePart::Outboard => self.outboard_path(hash),
            BaoFilePart::Sizes => self.sizes_path(hash),
            BaoFilePart::Bitfield => self.bitfield_path(hash),
        }
    }

    fn open(&self, location: &PathBuf, mode: Mode) -> io::Result<File> {
        match mode {
            Mode::Read => File::open(location),
            // without truncating, the part may hold data already
            Mode::ReadWrite => OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(location),
        }
    }

    fn delete(&self, location: &PathBuf) -> io::Result<()> {
        match fs::remove_file(location) {
            Err(cause) if cause.kind() == io::ErrorKind::NotFound => Ok(()),
            res => res,
        }
    }
}
