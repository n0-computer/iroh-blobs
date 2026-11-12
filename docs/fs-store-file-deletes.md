# fs store: metadata protocol and file deletes

Status: design + proof of concept on branch `fs-entry-meta`. Not an API yet.

## Goal

Make the communication between entities and the metadata store pure message
passing, so the metadata store can later become an extension point. The
metadata store owns metadata, the entity owns the files of its hash.

An entity's in-memory state is a cache of its entry, valid only while the
entity is the only writer of that entry. Entries only grow, which is why a
write to a complete entry can be a no-op. A delete is the one change that does
not grow an entry, so it has to go through the entity too.

## Actors

- **Main actor**: dispatches commands, runs the entity manager, forwards
  tag/status/listing/lifecycle commands to the metadata actor, fans deletes
  out to the entities, and routes file notices.
- **Entity actors**, one per hash: run the per-hash handlers (import, export,
  observe, persist, delete). They are the only ones that write and delete the
  files of their hash, with one exception below.
- **Metadata actor**: owns the redb database. Groups queued commands into
  read or write transactions (batching is private to it). It never touches
  data files, except for recovery when the store opens, before any entity
  exists.

All senders to the metadata actor share one channel (one `meta::Db`), so the
metadata actor sees commands in the order the sends completed (FIFO).

## Protocol

Send side, as seen by an entity (`EntryMeta`, scoped to its hash):

| Method | Waits | Meaning |
|---|---|---|
| `get()` | for reply | get the entry state |
| `set(state)` | until applied | replace the entry state |
| `update(state)` | no | merge into the entry state |
| `update_await(state)` | until applied | merge into the entry state |
| `delete(force)` | until applied | delete the entry, say whether there was one |

"Applied" means visible to later commands, not durable. Durability comes with
the commit of the batch. Commits are durable: redb's default
`Durability::Immediate`, never overridden.

From the metadata actor to the main actor, over an unbounded channel: a
`FileNotice` per hash after each commit, saying whether the entry was deleted
and which files it referenced in the batch (before it, or in any state the
batch gave it) and no longer does. Unbounded, so the metadata actor never
waits on the main actor, which waits on it.

## Cooperative entities

An entity's tasks are futures polled by one actor, so they only interleave at
`.await` points. Every change of an entity's state or files runs after a
`wait_settled`, which waits while the state is `Loading` or `Deleting`, with no
`.await` between the check and the change. The state is the entity's existing
`watch`, so waiting needs no extra machinery.

## File notices

After each commit, the main actor routes each notice:

- **An entity exists** for the hash (running, or still shutting down, since
  its shutdown may write the bitfield): the notice goes to it, waiting for
  room in its inbox rather than dropping it. The entity unlinks the files
  its own state does not use. Its state is never behind its entry.
- **No entity exists**: the main actor unlinks the files itself. Only it
  creates entities, so none can appear in between, and none has writes in
  flight.

This also covers shutdown: once the entities are gone, the remaining
notices are handled directly.

## Deletes

`DeleteBlobs` is fanned out by the main actor to the entity of each hash,
creating it if needed (without loading). The entity:

1. Switches to `Deleting(old)`. Changes wait from here on, readers and
   observers look through to the old state.
2. Sends `delete` to the metadata actor. Deletes arriving together still land
   in one transaction.
3. **Skipped** (there is no entry): restores the old state. Nothing was lost,
   since writes waited. A gc delete of content protected by a temp tag never
   gets here, the main actor skips it before.
4. **Deleted**: bumps the entity's delete count, which ends observers even if
   they only wake after the entry was filled again, and switches to
   `Deleting(NonExisting)`. The task waits for the notice of the commit,
   which keeps the entity alive, so it cannot unload and come back with new
   files that the unlink would take.
5. The notice unlinks every file and switches to `NonExisting`. Waiting
   writes continue on an empty entry: a running download keeps going with
   only the ranges that arrive from now on.

The main actor answers `DeleteBlobs` once every entity is done, so the files
are gone when it returns. Shutdown waits for running deletes before shutting
down the entities, since an entity in shutdown takes no new tasks.

## What this replaces

- `ProtectHandle`, `DeleteHandle`, `FileTransaction` and the shared
  `DeleteSet`.
- `delete_partial_files` in `handle_update` / `handle_set`, and the per-part
  marking in `DeleteBlobs`.
- The `protect` calls in `finish_import_impl` and `PartialMemStorage::persist`.

Also the GC `protected` set and `ClearProtected`. A gc run is now a request
(`GcRun`) that lasts until gc drops it. While it lasts, a delete that is not
forced keeps content that has a temp tag, or had one at any time since the
run started. The main actor checks this before fanning a delete out, and it
is also where temp tags are created. So no tag, no protection: a download
without a temp tag can be cut in half by gc, and continues from scratch.

## The bug this fixes

`protect` only cancelled deletes marked in the currently open write
transaction, and did nothing if none was open. A delete of an entry whose
entity was using the files (e.g. a forced delete while an observe stream is
open, or GC racing a re-import) removed the files under the entity, and the
entity kept a stale state: a later import of the same blob was a no-op, and
the blob stayed gone. `write_after_read_sees_a_delete` and
`test_delete_ends_observers_and_removes_files` cover this.

## Known gaps / open points

- A crash between a commit and the unlink of its notice leaves files nothing
  references. A sweep at open after an unclean shutdown is planned.
- After `export_path` with `TryReference` copies across file systems, the
  entity still uses the owned data file, so the notice keeps it and it leaks.
- Every delete starts an entity, also for inline entries that have no files.
- The extension point itself (a trait or protocol type for the metadata
  store) is not designed yet.
