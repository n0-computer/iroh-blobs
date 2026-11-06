# fs store: metadata protocol and file deletes

Status: design + proof of concept on branch `fs-entry-meta`. Not an API yet.

## Goal

Make the communication between entities and the metadata store pure message
passing, so the metadata store can later become an extension point. Today the
only thing in the way is file deletion, which uses shared mutable state
(`ProtectHandle`, an `Arc<Mutex<DeleteSet>>`) between the entities and the
metadata actor.

## Actors

- **Main actor**: dispatches commands, runs the entity manager, forwards
  tag/status/listing/lifecycle commands to the metadata actor.
- **Entity actors**, one per hash: run the per-hash handlers (import, export,
  observe, persist). They do synchronous file IO on the files of their hash
  directly, not through the metadata actor.
- **Metadata actor**: owns the redb database. Groups queued commands into
  read or write transactions (batching is private to it).

All senders to the metadata actor share one channel (one `meta::Db`), so the
metadata actor sees commands in the order the sends completed (FIFO).

## Protocol

Send side, as seen by an entity (`EntryMeta`, scoped to its hash):

| Method | Waits | Meaning |
|---|---|---|
| `load()` | for reply | get the entry state, mark the hash live |
| `get()` | for reply | get the entry state |
| `set(state)` | until applied | replace the entry state |
| `update(state)` | no | merge into the entry state |
| `update_await(state)` | until applied | merge into the entry state |
| `inactive()` | no (awaited send) | the hash is no longer live |

Receive side, the metadata store: entity messages (`Get` with a load flag,
`Set`, `Update`, `Inactive`) plus main actor commands (tags, blob status,
`DeleteBlobs`, listing, `ClearProtected`, `SyncDb`, `Shutdown`), all in one
FIFO.

"Applied" means visible to later commands, not durable. Durability comes with
the commit of the batch.

## Live entities

- An entity sends `load` exactly once per activation: `load()` in the entity
  runs the `Initial -> Loading` transition, and every handler calls it before
  touching files. A revived or recycled actor starts in `Initial` again.
- An entity sends `inactive` at the end of `on_shutdown`, after `persist`, so
  after all its file writes and metadata writes.
- The entity manager runs at most one actor per hash. A revive only happens
  after `on_shutdown` has returned, so per hash the messages strictly
  alternate: `load`, ..., `inactive`, `load`, ... No refcount needed.

Two facts follow from the FIFO:

1. An entity touches files of its hash only after the metadata actor has
   answered its load, so only after it processed the load and marked the hash
   live.
2. When the metadata actor processes `inactive`, it has already processed all
   metadata writes of that entity, and the entity touches no more files.

So at the end of a batch, for a hash that is not live, the entry state
reflects every file of that hash. Lag is harmless: a stale "live" only delays
deletes, a stale "not live" means the entity is still blocked on its load.

## File deletes

File names are determined by the hash (only imports use unique temp files
until they rename them into place). So the deleter must not remove a file that
a live entity is about to reference.

Per batch, the metadata actor:

1. records every hash written in a write transaction (`Update`, `Set`,
   `DeleteBlobs` that removed an entry),
2. after the batch, for each written or previously parked hash:
   - if the hash is live: park it,
   - otherwise: delete the owned parts that the state at the end of the batch
     does not reference (`unreferenced_parts`),
3. deletes the files after the commit, and before handling the next command.

Parked hashes are decided again after any later batch, read batches included,
since everything is committed there.

| State at end of batch | Deleted |
|---|---|
| none | data, outboard, sizes, bitfield |
| partial | nothing |
| complete, data and outboard owned | sizes, bitfield |
| complete, data or outboard inline or not needed | those parts too |
| complete, external data | the owned data file too |

This can name files that do not exist, for example when a complete entry is
written again. Deleting them is a `NotFound` no-op. External files and temp
files are never touched.

Intermediate states inside one batch do not matter: nothing is deleted before
the commit, and the rule looks only at the final state.

## What this replaces

- `ProtectHandle`, `DeleteHandle`, `FileTransaction` and the shared
  `DeleteSet`.
- `delete_partial_files` in `handle_update` / `handle_set`, and the per-part
  marking in `DeleteBlobs`.
- The `protect` calls in `finish_import_impl` and `PartialMemStorage::persist`.

The GC `protected` set and `ClearProtected` are unchanged.

## The bug this fixes

`protect` only cancelled deletes marked in the currently open write
transaction, and did nothing if none was open. A delete of an entry whose
entity was live and using the files (e.g. a forced delete while an observe
stream is open, or GC racing a re-import) removed the files under the entity.
`test_delete_waits_for_live_entity` reproduces this: it fails on the code
before this change.

## Known gaps / open points

- Parked deletes are lost on shutdown. The files leak until something writes
  the hash again. A startup scan could clean them up.
- Parked deletes need some later batch to run. An idle store keeps them until
  the next command.
- `DeleteBlobs` while the entity is live removes the entry but keeps the
  files. If the entity then writes again, its update recreates the entry with
  those files. If not, the files go once it is inactive.
- The extension point itself (a trait or protocol type for the metadata
  store, and a separate data-file deleter) is not designed yet. The redb
  actor still does both.
