//! Protocol level conformance tests for [iroh-blobs] stores.
//!
//! An iroh-blobs store is anything that answers [`proto::Request`]s. This
//! crate checks that a store answers them the way iroh-blobs clients expect.
//! It only talks to the store through the protocol, either with the regular
//! client API or with raw requests where the client would hide what the store
//! does. So the same tests run against in-process and remote stores, and
//! against stores written outside of iroh-blobs.
//!
//! # Usage
//!
//! The cases are a plain list, see [`cases`]. A [`Testkit`] runs them against
//! fresh stores from a factory you provide.
//!
//! The easiest way to run them is a test target without the built-in test
//! harness, which shows every case as a separate test:
//!
//! ```toml
//! [[test]]
//! name = "conformance"
//! harness = false
//! ```
//!
//! ```no_run
//! // tests/conformance.rs
//! use iroh_blobs::store::mem::MemStore;
//! use iroh_blobs_testkit::{TestStore, Testkit};
//!
//! async fn mem_store() -> anyhow::Result<TestStore> {
//!     Ok(TestStore::new(MemStore::new()))
//! }
//!
//! fn main() {
//!     Testkit::new(mem_store)
//!         .skip("tags_model", "tags are not supported yet")
//!         .main()
//! }
//! ```
//!
//! `cargo test --test conformance` then runs one test per case. The usual
//! arguments work, e.g. `cargo test --test conformance export_` to filter,
//! `-- --list` to list, or `-- --include-ignored` to also run skipped cases.
//!
//! Without a separate test target, run cases from a normal test:
//!
//! ```no_run
//! # use iroh_blobs::store::mem::MemStore;
//! # use iroh_blobs_testkit::{TestStore, Testkit};
//! # async fn mem_store() -> anyhow::Result<TestStore> {
//! #     Ok(TestStore::new(MemStore::new()))
//! # }
//! #[test]
//! fn conformance() {
//!     let testkit = Testkit::new(mem_store);
//!     for case in iroh_blobs_testkit::cases() {
//!         if let Err(cause) = testkit.run(case) {
//!             panic!("{}: {cause:#}", case.name());
//!         }
//!     }
//! }
//! ```
//!
//! The factory is called once per generated input. Resources the store
//! needs, such as a temp dir, can be tied to it with
//! [`TestStore::with_guard`].
//!
//! # What is checked
//!
//! Most cases are properties: they run many times with generated blobs,
//! ranges and operations, each time against a fresh store, and shrink to a
//! minimal input on failure. Failure messages state the part of the contract
//! that was violated. Expected values come from [bao-tree] and from simple
//! models, never from another store implementation.
//!
//! The cases cover adding and exporting blobs, importing and exporting bao
//! encoded data (complete, partial and incremental), observing bitfields,
//! listing blobs, tags, and requests whose client goes away, as happens when
//! a connection drops. Every run also checks that the store shuts down
//! cleanly and that no request hangs.
//!
//! The tests assume a cooperative client, like the iroh-blobs clients: they
//! look for ways to break a store through correct use of the protocol, not
//! through malformed or adversarial request sequences. Persistence, crash and
//! fault behavior are out of scope.
//!
//! [iroh-blobs]: https://docs.rs/iroh-blobs
//! [bao-tree]: https://docs.rs/bao-tree
//! [`proto::Request`]: iroh_blobs::api::proto::Request
#![deny(missing_docs)]

mod cases;
mod harness;
mod inputs;
mod raw;
mod reference;
mod testkit;

pub use testkit::{cases, Case, TestStore, Testkit};
