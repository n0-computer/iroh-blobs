# Syzygy iroh-blobs patch

Base: `iroh-blobs v0.103.0` (`e82cbdcbdac9a78033174aad55e3199b2cf4c0dc`).

**Single iteration branch (mandatory):** `syzygy/fs-store-hardening-v0.103.0`

- All Syzygy fs-store fixes land on this branch only.
- Do **not** open parallel `syzygy/*`, `fix/fs-store-*`, or version-named side branches for the same patch line.
- Annotated release tags (`syzygy-v0.103.0-fs-store-recovery.N`) mark snapshots; they do not create a second line of development.
- Parent Syzygy pins this branch tip via submodule SHA; `.gitmodules` `branch=` must match.

Release line:

- `syzygy-v0.103.0-fs-store-recovery.1`: initial fs-store recovery baseline.
- `syzygy-v0.103.0-fs-store-recovery.2`: durable metadata, typed
  availability, tag CAS, and GC-cycle hardening described here.

This fork fixes store correctness below Syzygy's product-policy boundary. It
does not contain catch-up, realtime/on-demand, entitlement, retention, or UI
rules.

## Why the fork exists

The upstream `v0.103.0` filesystem store has coupled failures that cannot be
made correct by a consumer retry:

1. `HashContext::persist()` destructively takes healthy state before proving
   it is partial, while load/observe conflates recoverable external absence,
   transitional state, and terminal corruption
   ([issue #233](https://github.com/n0-computer/iroh-blobs/issues/233)).
2. Read-write metadata handlers acknowledge success before the enclosing redb
   transaction commits. A process exit after API success can lose metadata or
   tags ([issue #216](https://github.com/n0-computer/iroh-blobs/issues/216)).
3. A failed `write_batch` moves state before fallible I/O completes. The old
   notification path can leave an entry poisoned while observers remain asleep.
4. Metadata presence is not data availability. Missing owned files, stale or
   truncated external paths, and actor failures require distinct outcomes.
5. Garbage collection snapshots protection too early. Moving an external-root
   callback after mark closes one race, but a global unversioned protection set
   still permits concurrent cycles and stale cancellation cleanup to release a
   newer cycle.
6. Tag planning followed by unconditional delete can erase a concurrent tag
   replacement.
7. Store shutdown only forwards a database RPC while discarding the main
   actor, database actor, and GC task handles. The dedicated runtime is owned
   by the actor itself, so a successful reply cannot prove resource release.

These failures live in the entity actor, metadata actor, and GC primitive. An
outer timeout, polling loop, or unconditional re-fetch would only hide invalid
state transitions and create a second owner.

## Patch contracts

### Connection-owned provider streams

`provider::handle_connection` keeps its concurrent stream futures in one
`FuturesUnordered`, rather than detaching a Tokio task for each request. A
connection close, cancellation, or handler drop therefore destroys all of its
stream futures in the same ownership boundary. Stream panics stay isolated
with `catch_unwind`, as they were with separate tasks.

The consumer cannot repair this by awaiting `Router::shutdown`: the router
joins the connection handler, but the upstream handler discarded the stream
task handles. Pending QUIC streams retain the connection sender and ultimately
the UDP socket, even after the outer handler exits. `Store::shutdown` is a
storage acknowledgement and does not own these network tasks.

The regression cases in `src/tests/provider.rs` verify that aborting the outer
handler or closing the connection drops two intercepted requests before
returning and permits immediate UDP rebinding. A third case leaves one request
gated while another request on the same connection completes, preserving
concurrent I/O. The patch uses the existing `n0_future::FuturesUnordered` and
does not change protocol framing, storage, or public APIs.

Focused verification from the parent workspace:

```bash
cargo test -p iroh-blobs --lib tests::provider
cargo test -p syzygy-net-node-runtime --lib
cargo test -p syzygy-net --lib tests::shutdown
```

### File-store shutdown ownership

`FsStore::shutdown(&self)` retains and joins the actual main-actor,
database-actor, GC, and dedicated-runtime handles. External generic `Store`
clients and FS-specific senders anchor the same owner, so consuming an
`FsStore` into a `Store`, cloning the generic client, or dropping the original
`FsStore` does not terminate a still-owned runtime. Actor internals and GC use
unanchored senders; the retained close future holds only a weak command sender
and cannot keep its own owner alive.

The first poll fences off new external commands and stops and joins GC. It
then requests shutdown while joining both actors. The main actor rejects
queued external work and drains started operations, import-completion messages,
and entity-recycle notifications. The database remains available through final
entity persistence and drops before acknowledging shutdown. Last, a retained
blocking task drops the dedicated runtime and waits for its async and blocking
workers to exit. Started streams must finish or be released for graceful drain
to complete.

Concurrent callers borrow one close future. Cancelling a waiter leaves that
future and its real handles in the owner for the next waiter. Success or the
complete failure list is cached once, and every later waiter receives the same
result. Actor, database, GC panic, request, and runtime-join errors are recorded
without skipping the remaining joins. `ShutdownError::causes()` preserves the
original errors and their stage context. The existing `irpc::Result<()>` return
type is retained: an RPC receive error contains an `io::Error`, whose
`get_ref()` exposes the shared `ShutdownError`.

The generic `Store::shutdown()` RPC also enters orderly actor/database drain
and fences new work, but it does not join the dedicated runtime. Calling it
first causes a later initial `FsStore::shutdown()` request to fail against the
closed receiver; that failure is preserved after every resource is joined.
Dropping the last external owner without awaiting shutdown uses Tokio's
background shutdown path and makes no synchronous completion guarantee.
Initialization errors and initialization-task panics join the runtime before
`load_with_opts` returns an error; cancelling and dropping that entire load
future uses the same emergency drop boundary.

The regressions in `src/tests/store/fs.rs` cover database reopening with old
clients still present, active import drain, generic-client owner lifetime,
GC-cycle release on last-client drop, cancelled and concurrent waits on a real
blocked runtime worker, GC panic preservation, and failed-RPC error caching.
Use these focused filters in the parent workspace or its isolated patch
validation harness, then run the consuming store-owner and node-runtime suites:

```bash
cargo test -p iroh-blobs --lib tests::store::fs::
cargo test -p iroh-blobs --lib store::fs::tests::
cargo test -p iroh-blobs --lib tests::provider::
```

### Stable file-storage state

- `take_partial()` is destructive only for a real `Partial` state. Persisting
  `Complete`, `NonExisting`, transitional, or failed state is a no-op.
- `Initial` and `Loading` wait for a stable projection.
- `NonExisting` is the only state observed as empty progress.
- `Poisoned(BlobStorageFailure)` is terminal and preserves a stable failure
  kind: owned-data missing, owned-outboard missing, permission, metadata,
  external-invalid, write, I/O, or internal invariant.
- A batch-write failure installs that terminal cause and uses
  `watch::Sender::send_modify`, so existing and future observers receive the
  same failure rather than waiting forever.

### Durable acknowledgement with batching

- Read-write handlers return an internal `PendingWriteReply` instead of
  replying inside an uncommitted transaction.
- The actor commits redb and the file transaction before delivering each
  pending success exactly once.
- Commit failure is converted for every pending reply shape. No caller can
  observe success before the durable metadata fence.
- Intermediate partial progress uses queueable `update()`. Final visibility
  uses `update_await()` and waits for commit.
- Read and write batches have independent duration limits, preserving group
  commit without weakening final completion.

This uses a closed internal enum rather than boxed callbacks. Result ownership
and all reply types remain visible to the compiler.

### Typed availability and external reconciliation

`FsStore::availability(hash)` returns one point-in-time state:

```text
Missing
Partial { size }
Readable { size }
Broken(BlobStorageFailure)
```

Probe lifecycle failures (`StoreUnavailable`, `ResponseDropped`, `EntityBusy`,
`EntityDead`) remain separate from confirmed broken storage. `Readable` means
the data and required outboard opened and stored ranges are complete. It is not
a cryptographic re-hash: an external path can still change after the snapshot.

External behavior is now deterministic:

- each candidate must be a regular file with the expected length;
- all candidates are attempted, so a stale first path cannot mask a healthy
  later path;
- candidate retention keeps healthy paths and the newest path when the
  eight-path bound is reached;
- when every recorded path disappears, the entity requests a paths-and-size
  compare-and-swap from the metadata actor;
- only the exact stale reference is removed; a concurrent replacement wins and
  is reloaded;
- persistent tags survive reconciliation.

For cases where a cheap availability probe is insufficient, `FsStore` also
provides explicit hash-scoped maintenance:

```text
validate_external_reference(hash)
repair_external_reference(hash)
quarantine_external_reference(hash)
```

Validation is read-only. It fully hashes every external candidate, reports
missing and typed invalid paths separately, and validates the store-owned BAO
outboard against a cryptographically valid data candidate. Repair and
quarantine revalidate while holding the per-hash transition mutex, then use a
paths/size/outboard-shape compare-and-swap. Repair retains only verified paths
when the outboard is valid; no valid candidate or a broken owned outboard
detaches the stale metadata so normal import/fetch can rebuild it. Quarantine
explicitly detaches the current external metadata.

External files remain caller-owned: these APIs never move, overwrite, or
delete them. Persistent tags survive every maintenance action. Concurrent
import, export, persist, or metadata replacement wins over a stale maintenance
snapshot. This is an `FsStore` external-reference capability, not a generic
`Store` RPC or a store-wide repair database.

### Atomic tag replacement

The tag API provides compare-and-swap:

```text
CompareAndSwapTag(name, expected, value)
  -> Applied
  -> Mismatch { current }
```

This lets a consumer plan destructive work and later prove that it is deleting
the same root. It does not put consumer retention policy into the store.

### Store-owned GC protection cycle

The old unversioned `ClearProtected`/`FinishProtected` shape is replaced by:

```text
StartGcProtection -> GcProtectionCycleId
FinishGcProtection(cycle) -> Finished | Stale
```

`GcProtectionSet` owns exactly one active cycle. Starting a concurrent cycle is
rejected. A finish for an older cycle is `Stale` and cannot clear the hashes of
a newer cycle.

`gc_run_once_with_late_protection` performs:

```text
start store-owned cycle
  -> mark persistent tags and temporary tags
  -> load external roots immediately before sweep
     -> failure: Abort without sweep
     -> success: merge roots
  -> sweep
  -> finish matching cycle
```

The guard releases its cycle after normal completion, late-protection abort,
mark/sweep failure, or future cancellation. Imports that commit while a cycle
is active enter the protected set. Malformed hash-sequence traversal is a mark
failure and cannot degrade to a warning followed by sweep.

Scheduled GC records a failed round and continues later rounds instead of
terminating the background loop permanently.

## State transitions

```text
Initial / Loading                 -> wait for stable state
NonExisting                       -> Missing / empty observed bitfield
Partial / PartialMem              -> Partial
Complete with readable storage    -> Readable
Poisoned(cause)                   -> Broken(cause) / terminal observe error
all external candidates missing   -> metadata CAS -> NonExisting
stale first, valid later path      -> open valid candidate
wrong-length external candidate   -> reject and try next candidate
write success                     -> install valid state + notify
write failure                     -> Poisoned(typed cause) + notify
final metadata update             -> commit -> success reply
commit failure                    -> failure reply to every pending writer
GC late-root failure              -> Abort, no sweep
old GC cleanup                    -> Stale, newer cycle remains active
```

## Syzygy boundary

The fork supplies mechanisms, not product policy:

- Syzygy transport owns a temp tag for the complete active-fetch/singleflight
  lifetime, including retry gaps and cancellation.
- Syzygy transfer owns manifest expansion and must abort destructive history GC
  when its protection truth is incomplete.
- Syzygy history policy decides lease, keep-offline, manual download, and
  retention behavior.
- Syzygy presentation observes typed locality/progress and never reads fs-store
  metadata to recreate policy.

Unencrypted materialized files continue to use store-owned
`ImportMode::Copy`; encrypted files use stream import. Mutable user paths are
not reference-imported. The 2026-07-18 APFS profile found no benefit from
immutable staging plus reference: 64 MiB averaged 1155.525 ms versus 1126.890
ms for Copy, and 128 MiB averaged 1218.457 ms versus 1214.766 ms. Direct
reference was only 3.28%-6.23% faster and violates the mutable-source contract.

## Verification

Current fork gates, 2026-07-18:

```bash
cargo check --all-targets --all-features
cargo test --all-targets --all-features
cargo test --doc --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Results:

- unit tests: 116 passed, 2 upstream tests ignored;
- `tests/blobs.rs`: 3 passed;
- `tests/external_reference_maintenance.rs`: 5 passed;
- `tests/fs_store_durability.rs`: 1 passed;
- `tests/gc_late_protection.rs`: 14 passed;
- `tests/tags.rs`: 3 passed;
- doctests: 17 passed;
- check and clippy: passed.

Focused coverage includes:

- safe partial extraction and transitional observation;
- typed poisoned termination and write-failure observer wakeup;
- commit success/failure mapping across reply types;
- immediate-process-exit durability after a successful add;
- availability while commit is pending;
- missing owned data/outboard and actor/probe failure separation;
- read-only full-content external validation, same-length corruption repair,
  mixed-candidate retention, explicit quarantine, and user-file/tag
  preservation;
- stale external CAS reconciliation, wrong-length fallback, and candidate
  retention;
- tag compare-and-swap through local and RPC paths;
- mark-after-import/tag races for MemStore and FsStore;
- late-root failure and malformed hashseq abort before sweep;
- concurrent-cycle rejection and cancellation recovery for MemStore/FsStore;
- stale cycle finish cannot release a newer cycle.

Syzygy consumer gates also cover typed locality, active fetch guards, history GC
fail-closed/CAS rollback, durable task identity, and strongly typed progress
units.

## Known remaining risks

This fork deliberately does not claim to provide a complete repair database:

- upstream store-wide validation/repair work remains open in
  [#153](https://github.com/n0-computer/iroh-blobs/issues/153) and
  [#157](https://github.com/n0-computer/iroh-blobs/issues/157);
- owned and partial entries do not yet have an equivalent explicit repair API;
- orphan data/outboard files still need store-wide reconciliation;
- `Readable` is not a cryptographic full-content validation;
- real power-loss ordering for owned data, outboards, directory entries, and
  redb needs a platform crash harness beyond process `_exit`;
- deeper actor-level commit/file-transaction fault injection is still useful.

These are explicit store/upstream risks. They must not be hidden by mapping all
errors to `NonExisting` or by adding consumer retries.

## Upstream tracking and exit conditions

- Initial state-machine report:
  https://github.com/n0-computer/iroh-blobs/issues/233
- Syzygy upstream PR for the initial recovery slice:
  https://github.com/n0-computer/iroh-blobs/pull/243
- Early-ACK report:
  https://github.com/n0-computer/iroh-blobs/issues/216
- Manual-GC/temp-tag context:
  https://github.com/n0-computer/iroh-blobs/issues/235 and
  https://github.com/n0-computer/iroh-blobs/pull/236
- Late-protection proposal:
  https://github.com/n0-computer/iroh-blobs/pull/240
- Validation/repair:
  https://github.com/n0-computer/iroh-blobs/issues/153 and
  https://github.com/n0-computer/iroh-blobs/issues/157

Remove this fork only after a released upstream version preserves all
observable contracts above: stable transitions, typed terminal failure,
reply-after-commit, observer wakeup, validated availability/reconciliation, tag
CAS, and cancellation-safe late GC protection. Then remove the Cargo path patch
and submodule, update the lockfile, inspect `cargo tree`, and rerun fork,
consumer, GC/crash, deferred large-file, and cross-platform gates.
