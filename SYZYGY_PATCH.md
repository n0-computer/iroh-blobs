# Syzygy iroh-blobs patch

Base: `iroh-blobs v0.103.0` (`e82cbdcbdac9a78033174aad55e3199b2cf4c0dc`).

## Problem

The fs store has two state-machine bugs around `BaoFileStorage::Poisoned`:

1. `HashContext::persist()` calls the destructive `BaoFileStorage::take()`
   before checking that the state is `Partial`. Persisting a `Complete` or
   `NonExisting` handle therefore replaces healthy state with `Poisoned` during
   entity-manager shutdown.
2. `HashContext::load()` maps every storage-open failure to `Poisoned`. A stale
   `DataLocation::External` whose referenced files were removed is recoverable:
   the blob is absent and can be fetched again. It must not be conflated with a
   missing store-owned data/outboard file, invalid metadata, permission failure,
   or database failure.

The old Syzygy patch treated `Initial`, `Loading`, and `Poisoned` as an empty
bitfield. That avoided a panic but changed the meaning of progress: observers
could see a broken entry as a valid blob with zero verified chunks. It also left
both poisoning paths intact.

These failures are internal to the iroh-blobs entity and fs-store actors. A
Syzygy-side retry cannot restore state discarded by `take()`, distinguish an
external cache miss from store corruption, or prevent an observer from reading
the invalid state. The fix therefore belongs in this fork.

## Patch

- Add `BaoFileStorage::take_partial()`. The destructive transition is now owned
  by the enum and only occurs when the current variant is actually `Partial`.
  Persisting any other state is a no-op.
- Add a private typed `BaoFileOpenError`:
  - all external candidates missing -> `ExternalDataMissing` -> in-memory
    `NonExisting` so a normal fetch/import can repair the blob;
  - store-owned file loss, outboard loss, invalid external metadata, permission
    failure, and other I/O errors -> `Storage` -> terminal `Poisoned`;
  - metadata database failures remain terminal `Poisoned`.
- Try every path in `DataLocation::External(Vec<PathBuf>, ...)`. A stale first
  candidate no longer hides a later valid candidate during load or export.
- Observe `Initial` and `Loading` by waiting for a stable state. `NonExisting`
  still emits an empty bitfield. `Poisoned` terminates the observe stream and is
  logged; it is never projected as valid empty progress.
- Keep the existing public export of `gc_run_once`. Syzygy owns lease and
  retention policy, while iroh-blobs remains responsible only for mark/sweep
  execution.

The intended state projection is:

```text
Initial / Loading                 -> wait for a stable state
NonExisting                       -> empty bitfield
Partial / PartialMem / Complete   -> current verified bitfield
Poisoned                          -> terminal observe failure
External path 1 missing, 2 valid  -> open path 2
All external paths missing        -> NonExisting and normal re-fetch
Owned data/outboard missing       -> Poisoned and explicit failure
persist(Partial)                  -> sync and terminalize old handle
persist(any other state)          -> no-op, state preserved
```

## Syzygy boundary

This fork does not contain product policy. Catch-up windows, realtime versus
on-demand mode, manual download intent, byte budgets, entitlements, leases, and
`keep_offline` remain in Syzygy domain/application/infrastructure modules.

Syzygy currently imports unencrypted materialized files with `add_path()`, whose
mode is `ImportMode::Copy`; encrypted files use `add_stream()`. Do not switch the
current deferred/materialize path to `ImportMode::TryReference` without first
adding immutable source identity and availability contracts. Today:

- deferred source paths can be changed or deleted by the user;
- `content_local = true` means the body is actually readable from the store;
- lease GC may remove store-owned blobs based on retained source-path metadata;
- encrypted blobs cannot reference their plaintext source.

Using live user paths as blob-store truth would violate those contracts and can
turn a reclaimed store copy into permanent data loss. `TryReference` is only
appropriate for an application-owned immutable staging area with verified
source identity, restart reconciliation, and a defined fallback copy policy.

## Verification

- Six focused state/helper tests cover safe partial extraction, transitional
  observe waiting, poisoned termination, missing-versus-invalid external state,
  and candidate fallback.
- Three public fs-store tests cover candidate fallback after restart, all
  external candidates disappearing followed by successful re-import, and
  store-owned data loss terminating observe without a worker panic.
- Patch crate: 103 unit tests pass, two existing tests remain ignored.
- Patch crate: six integration tests and 17 doctests pass.
- `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings`
  pass.

## Upstream tracking

- Root-cause report: https://github.com/n0-computer/iroh-blobs/issues/233
- Earlier symptom-level observe PR: https://github.com/n0-computer/iroh-blobs/pull/214
- Current upstream fs store:
  https://github.com/n0-computer/iroh-blobs/blob/main/src/store/fs.rs
- Current upstream bao storage:
  https://github.com/n0-computer/iroh-blobs/blob/main/src/store/fs/bao_file.rs

As of 2026-07-18, upstream `main` still calls `take()` before matching
`Partial`, maps all open failures to `Poisoned`, panics from transitional or
poisoned observe projection, and opens only the first external path.

Remove this patch only after an upstream release preserves all observable
guarantees above. Then remove the Cargo patch and submodule, update the lockfile,
confirm `cargo tree` resolves the registry release, and rerun the patch tests,
Syzygy blob-transfer tests, deferred lease-GC/rematerialize tests, and large-file
E2E scenarios.
