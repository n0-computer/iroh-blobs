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
`wait_writable`, which waits while the state is `Loading`, `Deleting`, or
`Frozen`, with no `.await` between the check and the change. Readers use
`wait_settled`, which permits `Frozen` and reads its retained state. Both use
the entity's existing state `watch`.

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

Also the GC `protected` set and `ClearProtected`.

## GC session

`GcRun` is a bidi streaming request that enters one exclusive GC session. The
store first sends a fixed candidate/root snapshot. GC then sends its sweep
deletes on the session's update channel and gets one reply per delete, in
order. Closing the update channel exits. A request for another session waits
until the current one exits.

The main actor suspends all tag mutations, including raw tags, named-tag
replacement/deletion/rename, temp-tag creation, and import completion (which
publishes a temp tag). It retains each request and its reply channel in a
queue and keeps processing reads and ordinary blob writes. A replacement
leaves the old tag in place. Temp-tag drops and scope ends cannot wait; their
protection is retained through the session by the existing `TempTags` logic.

In fs, a top-level metadata snapshot commits earlier tag operations before
capturing candidates and named roots. Those roots are combined with current
and retained temp roots. Each HashSeq root is loaded and frozen through its
entity inbox. Entry replies only after every freeze acknowledges; the main
actor keeps running while it waits, including handling entity restarts and
post-commit notices. Mem captures the same view and freezes handles under
their existing state watches.

`Frozen(old)` preserves the entry's readable state and bitfield. Writes,
replacement imports, and deletion wait on the existing state watch. Complete
and poisoned entries freeze too: a delete would turn them into an entry that
can grow again. A freeze task keeps the fs entity alive. All freezes of a
session wait on one release channel; dropping the session releases them, and
each freeze task restores `old` itself. A freeze waits for an earlier one to
end, so freezes never overlap and need no owner id. If a freeze task is
dropped at shutdown instead, `persist` looks through the freeze.

The shared GC loop traverses exactly the captured roots, invokes the external
protection callback, and sweeps only captured candidates. Incomplete HashSeqs
contribute their available children; missing chunks never hold up collection.
Their writes remain frozen until sweep ends, so new references cannot become
visible between traversal and deletion. Newly created hashes absent from the
candidate snapshot cannot be swept this round.

Dropping the guard on completion, error, or cancellation releases freezes and
replays queued tag operations. Sweep deletes travel on the session's own
channel, so none can arrive after its session ended. The session task runs
each delete to completion before it reads the next update, and the session
ends only after that, even if the GC caller or transport disappears. Deferred
tag publication therefore cannot overtake a sweep delete. Sweeping a frozen
root is rejected rather than waiting on the sweep's own freeze. A raw
`DeleteBlobs` is separate from the session: it can be sent at any time, and
during a session it skips protected content and waits on frozen roots.
Cancelling entry still settles all freeze acquisitions before unfreezing.
Shutdown stops admitting work and rejects deferred tag operations before
cancelling a held session, rather than waiting for its client to release it.
It drains already-dispatched deletes before closing metadata. Metadata failure
retains the existing whole-store abort behavior.

The callback runs in the shared GC task, not the main actor. It must only add
protected hashes and must not await suspended tag mutations or writes to a
frozen HashSeq. External systems must acquire store protection before
publishing references. If they acquire protection for previously unprotected
content, they must establish/check that content after acquisition: waiting
for a tag does not resurrect data that GC already deleted. A callback that
bypasses this publication contract cannot make concurrent external mutations
atomic with the store.

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
