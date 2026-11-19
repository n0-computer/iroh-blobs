//! The store engine: what a store is built from, and the generic part that runs it.
//!
//! A store is the generic store actor plus a [`entity::Backend`], which names
//! the per-hash entity type and starts the metadata store. The engine owns
//! everything that is the same for every store: routing requests by hash,
//! temp tags, gc sessions, imports, and the order of operations between
//! entities and the metadata store. A backend owns how entries are stored.
//!
//! This module is being assembled from the fs store; for now some of the
//! generic parts still live there.
pub(crate) mod entity;
pub(crate) mod entity_manager;
