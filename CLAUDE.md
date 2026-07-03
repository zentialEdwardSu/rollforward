# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

`rollforward` — a Rust library crate (Rust edition 2024). Currently at the initial scaffolding stage: `src/lib.rs` holds a placeholder `add` function and one unit test. No external dependencies yet.

## Commands

```bash
cargo build            # compile
cargo test             # run tests
cargo run              # (only once a binary target exists)
cargo fmt              # format
cargo clippy           # lint
cargo check            # fast type-check without producing a binary
```

Run a single test by name:

```bash
cargo test it_works
```

use linter:
```bash
cargo clippy --all-targets --all-features -- -D warnings -W unreachable_pub -D clippy::correctness -D clippy::single_call_fn -D clippy::complexity -W clippy::pedantic -W clippy::useless_attribute -W clippy::redundant_pub_crate -W clippy::excessive_precision -W clippy::missing_docs_in_private_items
```

## Layout

Decentralized sync engine (see `design.md` for the full architecture). Modules under `src/`:

- `lib.rs` — crate root: `uniffi::setup_scaffolding!()`, re-exports, crate-level pedantic-lint allows (justified inline).
- `types.rs` — FFI records/enums (`OpLogEntry`, `ChangeType`, …), `SyncError`, and the UI-only `EngineNotificationListener` callback trait.
- `remote.rs` — `RemoteStorage` trait (the dumb-remote abstraction) + `LocalFolderRemote` (std::fs impl used in tests). `put_oplog_cas` underpins CAS retry.
- `store.rs` — redb-backed `LocalStore`; multi-table steps run in one `WriteTransaction` (the transaction-rollback primitive).
- `text.rs` — yrs (y-crdt) text wrapper: incremental deltas, order-independent merge, baseline snapshots.
- `binary.rs` — fastcdc chunking + blake3 hashing, three-way manifest merge, chunk GC.
- `engine.rs` — the `SyncEngine` state machine: write flow (CAS retry), pull/merge, rollback, truncation, chunk-level resume.
- `tests/sync.rs` — end-to-end integration tests (two engines over a shared `LocalFolderRemote`).

Unit tests live inline in `#[cfg(test)] mod tests` blocks per module; cross-cutting scenarios live in `tests/`.

## Notes

- **yrs is pinned to `0.25`**, not the latest `0.27.x`: 0.27 uses an unstable `if-let` guard that does not compile on stable rustc 1.94. Revisit when the toolchain or yrs changes.
- **uniffi is pinned to `0.29`**, not the latest `0.32`: the installed C# generator (`uniffi-bindgen-cs 0.10.0+v0.29.4`) only reads 0.29-era metadata. Bump both together when a 0.32-compatible C# generator ships.
- Stored blobs use `serde_json` (not `rkyv` as the design suggested) — adequate for local state, avoids deriving `Archive` on every FFI/yrs type.

## C# bindings (`bindings/csharp/`)

The engine is exercised from C# through uniFFI-generated bindings, proving the FFI surface works cross-language.

Regenerate after changing the FFI surface (`src/types.rs`, exported `SyncEngine` methods, or `new_local`):

```bash
cargo build --release
uniffi-bindgen-cs --library target/release/rollforward.dll \
  --out-dir bindings/csharp/Rollforward/generated
```

Run the C# tests (builds the cdylib and copies `rollforward.dll` beside the test assembly automatically):

```bash
dotnet test bindings/csharp/Rollforward.Tests
```

The generated types are `internal`, so the test project compiles `generated/rollforward.cs` directly rather than referencing a separate class library. Generated namespace: `uniffi.rollforward`; free functions (e.g. `NewLocal`, `NewEngine`) live on the static `RollforwardMethods` class.

### FFI surface

- `NewLocal(client, db_path, remote_root, listener)` — convenience: wires a Rust `RedbStore` + `LocalFolderRemote` (KeepBoth policy).
- `NewEngine(client, store, remote, listener, policy)` — inject **host-implemented** `LocalStore` + `RemoteStorage` and choose the binary conflict policy. This is how C# tests exercise conflict paths (custom in-memory backends, fork injection via the non-CAS `PutOplog`).
- The `RemoteStorage` and `LocalStore` traits are exported `with_foreign`, so a host can implement them. uniFFI can't express tuples/slices, so their signatures use owned `String`/`Vec<u8>` and the `ClientStatus` / `OplogCacheEntry` records instead of `(String, u64)` / `(u64, Vec<u8>)`.
- Note: the built-in Rust `RedbStore`/`LocalFolderRemote` are exported as distinct Objects (via `OpenRedbStore`/`OpenLocalFolderRemote`) but do **not** implement the foreign-trait interfaces, so they can't be passed to `NewEngine` — a host wiring its own engine implements the traits in its own language.

# 12-rules for Implement

These rules apply to every task in this project unless explicitly overridden.
Bias: caution over speed on non-trivial work. Use judgment on trivial tasks.

## Rule 1 — Think Before Coding
State assumptions explicitly. If uncertain, ask rather than guess.
Present multiple interpretations when ambiguity exists.
Push back when a simpler approach exists.
Stop when confused. Name what's unclear.

## Rule 2 — Simplicity First
Minimum code that solves the problem. Nothing speculative.
No features beyond what was asked. No abstractions for single-use code.
Test: would a senior engineer say this is overcomplicated? If yes, simplify.

## Rule 3 — Surgical Changes
Touch only what you must. Clean up only your own mess.
Don't "improve" adjacent code, comments, or formatting.
Don't refactor what isn't broken. Match existing style.

## Rule 4 — Goal-Driven Execution
Define success criteria. Loop until verified.
Don't follow steps. Define success and iterate.
Strong success criteria let you loop independently.

## Rule 5 — Use the model only for judgment calls
Use me for: classification, drafting, summarization, extraction.
Do NOT use me for: routing, retries, deterministic transforms.
If code can answer, code answers.

## Rule 6 — Token budgets are not advisory
Per-task: 4,000 tokens. Per-session: 30,000 tokens.
If approaching budget, summarize and start fresh.
Surface the breach. Do not silently overrun.

## Rule 7 — Surface conflicts, don't average them
If two patterns contradict, pick one (more recent / more tested).
Explain why. Flag the other for cleanup.
Don't blend conflicting patterns.

## Rule 8 — Read before you write
Before adding code, read exports, immediate callers, shared utilities.
"Looks orthogonal" is dangerous. If unsure why code is structured a way, ask.

## Rule 9 — Tests verify intent, not just behavior
Tests must encode WHY behavior matters, not just WHAT it does.
A test that can't fail when business logic changes is wrong.

## Rule 10 — Checkpoint after every significant step
Summarize what was done, what's verified, what's left.
Don't continue from a state you can't describe back.
If you lose track, stop and restate.

## Rule 11 — Match the codebase's conventions, even if you disagree
Conformance > taste inside the codebase.
If you genuinely think a convention is harmful, surface it. Don't fork silently.

## Rule 12 — Fail loud
"Completed" is wrong if anything was skipped silently.
"Tests pass" is wrong if any were skipped.
Default to surfacing uncertainty, not hiding it.
