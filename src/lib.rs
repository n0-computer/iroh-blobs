#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
//! # Module overview
//!
//! The crate is designed to be used from the [iroh] crate, though it can also
//! be used standalone.
//!
//! It implements a [protocol] for streaming content-addressed data transfer using
//! [BLAKE3] verified streaming.
//!
//! ## Stores
//!
//! It also provides a [store] module for storage of blobs and outboards.
//! Three store implementations are available:
//!
//! - [`store::mem::MemStore`] — mutable in-memory store; suitable for small
//!   datasets or tests.
//! - [`store::readonly_mem::ReadonlyMemStore`] — immutable in-memory store;
//!   populated at construction time, no writes allowed afterwards.
//! - [`store::fs::FsStore`] — persistent filesystem store backed by `redb`;
//!   enabled with the `fs-store` feature.
//!
//! ## Client API
//!
//! The client API is available in the [api] module. A [`api::Store`] handle can
//! be obtained by dereffing any of the store implementations, or by connecting
//! to a remote store with [`api::Store::connect`].
//!
//! The handle gives access to three namespaced sub-APIs:
//!
//! - [`api::blobs::Blobs`] — local blob operations (import, export, observe, …)
//! - [`api::tags::Tags`] — tag management (set, list, delete, rename, …)
//! - [`api::remote::Remote`] — downloads from a *single* remote node
//!
//! For downloads from *multiple* nodes with automatic failover see
//! [`api::downloader::Downloader`].
//!
//! ## Server side
//!
//! To serve blobs over an iroh p2p connection, attach a [`BlobsProtocol`]
//! handler to an [`iroh::protocol::Router`]. See [`net_protocol`] for details
//! and a self-contained example.
//!
//! For lower-level server helpers (handling individual connections or requests),
//! see the [provider] module.
//!
//! ## Low-level get state machine
//!
//! The [get] module exposes a step-by-step state machine (`get::fsm`) for
//! executing range requests against a remote node and writing the verified data
//! into a store.
//!
//! ## Features
//!
//! - `fs-store`: Enables the filesystem store. Pulls in `redb` and
//!   `reflink-copy`.
//! - `metrics`: Enables Prometheus metrics for stores and the protocol.
//!
//! [BLAKE3]: https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf
//! [iroh]: https://docs.rs/iroh
mod hash;
pub mod store;
pub use hash::{BlobFormat, Hash, HashAndFormat};
pub mod api;

pub mod format;
pub mod get;
pub mod hashseq;
mod metrics;
mod net_protocol;
pub use net_protocol::BlobsProtocol;
pub mod protocol;
pub mod provider;
pub mod ticket;

#[doc(hidden)]
pub mod test;
pub mod util;

#[cfg(test)]
#[cfg(feature = "fs-store")]
mod tests;

pub use protocol::ALPN;
