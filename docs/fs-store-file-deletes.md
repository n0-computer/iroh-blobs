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
| `delete()` | until applied | ensure the entry is absent; a notice confirms commit |

"Applied" means visible to later commands, not durable. Durability comes with
the commit of the batch. Commits are durable: redb's default
`Durability::Immediate`, never overridden.

From the metadata actor to the main actor, over an unbounded channel: a
`FileNotice` for each deleted hash or hash with obsolete files after each
commit, saying whether the entry was deleted and which files it referenced
in the batch (before it, or in any state the
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
  its shutdown may write the bitfield): the notice goes into its unbounded
  inbox. The main actor must keep processing shutdown completion so queued
  work can restart an idle entity. The entity unlinks the files
  its own state does not use. Its state is never behind its entry.
- **No entity exists**: the main actor unlinks the files itself. Only it
  creates entities, so none can appear in between, and none has writes in
  flight.

This also covers shutdown: once the entities are gone, the remaining
notices are handled directly.

## Deletes

`DeleteBlobs` is fanned out by the main actor to the entity of each hash,
creating it if needed (without loading), including inline entries. This
serializes deletion with concurrent requests even when no entity existed
when the delete arrived. The `force` flag only bypasses current and retained
temp-tag protection; it does not bypass the entity. The entity:

1. Bumps the delete counter, ending observers, and switches to `Deleting`.
   The cached state is discarded. Readers see the entry as absent; changes
   wait from here on.
2. Sends `delete` to the metadata actor. Deletes arriving together still land
   in one transaction.
3. Waits for the notice of the commit, including when no metadata row
   existed. The task keeps the entity alive, so it cannot unload and come
   back with new files that the unlink would take. Protected hashes are
   skipped by the main actor before reaching the entity.
4. The notice unlinks every file and switches to `NonExisting`. Waiting
   writes continue on an empty entry: a running download keeps going with
   only the ranges that arrive from now on.

The main actor answers `DeleteBlobs` once every entity is done, after its file
cleanup attempts. Unlink failures are logged. Shutdown waits for running
deletes before shutting down the entities, since an entity in shutdown takes
no new tasks.

Metadata errors abort the whole store. The main actor cancels workers and
releases pending callers instead of leaving entities waiting for notices
that will never arrive. On a transaction error, the database remains locked
until those workers stop. This path does not persist entity state, apply
pending cleanup, or clear the unclean-run marker. There is no per-entity
rollback. The known `TryReference` ordering gaps are separate from this
failure handling.

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

Marking reads named roots first, then the union of current temp roots and
temp roots retained since the oldest active gc run started. A continuously
tagged root missed by the named snapshot must have had a temp tag before
that snapshot, so it appears in the second listing even after repeated
handovers. HashSeq roots in that union are traversed to protect their
children too. Ordinary temp-tag listings still show only current tags.

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
- A same-filesystem `TryReference` export renames before committing the
  external location. If that commit fails, metadata can still refer to the
  old path. Aborting the store does not repair this ordering gap.
- The extension point itself (a trait or protocol type for the metadata
  store) is not designed yet.
