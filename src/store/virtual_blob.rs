//! Providers of live data for virtual blobs.
//!
//! A virtual blob is a store entry that has an outboard but no stored data.
//! Instead, its data is produced on demand by an externally-registered
//! provider. This is used to serve a blob whose bytes are a transform (e.g.
//! encryption, compression) of another blob, without materializing the
//! transformed bytes.
//!
//! Each virtual entry durably stores the name of the provider that serves it
//! (see [`crate::api::blobs::Blobs::add_virtual`]). The [`VirtualProviders`]
//! registry is an in-process, shared map from provider name to a live
//! [`Provider`]. It is constructed alongside a store and shared between the
//! store actor (which consults it when serving a virtual entry) and the
//! application (which registers providers via
//! [`VirtualProviders::register`]). Because a live provider is not
//! serializable, this registry is intentionally local-only and never crosses
//! an RPC channel.
//!
//! The stored outboard and provider name are what make a virtual entry durable
//! across restarts: iroh-blobs keeps both in its database, while the
//! application re-registers its providers on startup from its own state. An
//! entry whose provider is not registered (or whose provider returns `None`
//! for its hash) is served as not found.
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};

use bao_tree::io::mixed::ReadBytesAt;
use bytes::Bytes;

use crate::Hash;

/// A boxed, thread-safe live data source for a virtual blob.
pub type DynVirtualSource = Arc<dyn ReadBytesAt + Send + Sync>;

/// A provider of data for virtual blobs.
///
/// A provider is registered under an application-chosen name and can be asked
/// for a random-access reader for a given hash. It returns `None` if it does
/// not serve that hash.
pub trait Provider: Send + Sync + 'static {
    /// Return a random-access reader for `hash`, or `None` if this provider
    /// does not serve that hash.
    fn reader_for(&self, hash: &Hash) -> Option<DynVirtualSource>;
}

/// A boxed, thread-safe provider.
pub type DynProvider = Arc<dyn Provider>;

/// A registry of live providers for virtual blobs, shared between a store
/// actor and the application.
#[derive(Clone, Default)]
pub struct VirtualProviders {
    inner: Arc<Mutex<HashMap<String, DynProvider>>>,
}

impl std::fmt::Debug for VirtualProviders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self
            .inner
            .lock()
            .expect("virtual providers lock poisoned")
            .len();
        f.debug_struct("VirtualProviders")
            .field("len", &len)
            .finish()
    }
}

impl VirtualProviders {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a provider under `name`.
    ///
    /// Re-registering a name replaces the previous provider. This is intended
    /// to be idempotent so that an application can register providers on
    /// startup without coordination.
    pub fn register(&self, name: impl Into<String>, provider: DynProvider) -> io::Result<()> {
        let mut map = self.inner.lock().expect("virtual providers lock poisoned");
        map.insert(name.into(), provider);
        Ok(())
    }

    /// Unregister the provider registered under `name`, if any.
    pub fn unregister(&self, name: impl AsRef<str>) {
        self.inner
            .lock()
            .expect("virtual providers lock poisoned")
            .remove(name.as_ref());
    }

    /// Look up the provider registered under `name`, if any.
    pub fn get(&self, name: &str) -> Option<DynProvider> {
        self.inner
            .lock()
            .expect("virtual providers lock poisoned")
            .get(name)
            .cloned()
    }
}

/// A wrapper that lets a boxed [`ReadBytesAt`] trait object be passed by value
/// to functions generic over `D: ReadBytesAt` (such as
/// [`bao_tree::io::mixed::traverse_ranges_validated`]).
#[derive(Clone)]
pub struct DynReadBytesAt(pub DynVirtualSource);

impl ReadBytesAt for DynReadBytesAt {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        self.0.read_bytes_at(offset, size)
    }
}
