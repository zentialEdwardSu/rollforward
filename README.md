# rollforward

A decentralized file-sync engine for **dumb remote storage** (network drives, S3,
WebDAV) — no smart server required. Multiple clients read and write an
append-only operation log; the engine reconciles concurrent edits locally on
pull. It handles text (CRDT merge), binary files (content-defined chunking),
time-travel rollback, and history truncation.

See [`design.md`](design.md) for the full architecture and
[`CLAUDE.md`](CLAUDE.md) for contributor/build notes.

## Features

- **Text CRDT merge** (yrs) — concurrent edits converge without a server; no lost updates.
- **Binary chunking** (fastcdc + blake3) — only changed chunks transfer; content-addressed dedup.
- **Conflict policies** for genuinely divergent binary edits: `KeepLocal`, `KeepRemote`, `KeepBoth` (host duplicates the losing side).
- **Time-travel rollback** — restore an earlier version by appending a compensating entry (history is never mutated).
- **Truncation + GC** — bound log growth with a low-watermark baseline; orphaned chunks are collected.
- **Fault tolerance** — CAS retry on concurrent writes, atomic local transactions, chunk-level resume.
- **Pluggable backends** — remote storage and local store are traits you implement; redb + a local folder are provided for tests/local runs.
- **Cross-language** — a uniFFI surface with generated **C# bindings** (see below).

## Architecture at a glance

```
host (Rust / C# / …)
      │  modify_text / modify_binary / sync / rollback / truncate
      ▼
  SyncEngine ── RemoteStorage (trait)  → LocalFolderRemote | S3 | WebDAV | …
      │      └─ LocalStore   (trait)  → RedbStore | your store
      ▼
  EngineNotificationListener (callback: content updated / conflict copy)
```

The engine depends only on the two backend **traits**; the caller injects the
implementations. `LocalFolderRemote` and `RedbStore` are the built-in
test/local backends — both are directly inspectable (a folder tree and a redb
file), which is how the tests observe behavior.

## Usage (Rust)

```rust
use std::sync::Arc;
use rollforward::{
    SyncEngine, RedbStore, LocalFolderRemote,
    types::{BinaryConflictPolicy, EngineNotificationListener},
};

// A minimal notification sink.
struct Ui;
impl EngineNotificationListener for Ui {
    fn on_file_content_updated(&self, file_id: String) {
        println!("updated: {file_id}");
    }
    fn on_conflict_copy_requested(&self, file_id: String, suggested_name: String) {
        println!("conflict on {file_id}: duplicate as {suggested_name}");
    }
}

# fn main() -> Result<(), rollforward::SyncError> {
let store  = Arc::new(RedbStore::open("client.redb")?);
let remote = Arc::new(LocalFolderRemote::new("/path/to/shared/remote")?);
let engine = SyncEngine::with_backends(
    "clientA",
    store,
    remote,
    Arc::new(Ui),
    BinaryConflictPolicy::KeepBoth,
);

// Edit locally — publishes an oplog entry to the remote.
engine.modify_text("notes.txt".into(), "hello world".into())?;

// Pull + merge everyone else's changes.
engine.sync("notes.txt".into())?;
println!("{}", engine.get_text("notes.txt".into())?);

// Roll back to an earlier version (append-only).
let v1 = engine.modify_text("notes.txt".into(), "v1".into())?;
engine.modify_text("notes.txt".into(), "v2".into())?;
engine.rollback("notes.txt".into(), v1)?;

// Bound history: keep ~10 versions (respects other clients' progress).
engine.truncate("notes.txt".into(), 10)?;
# Ok(()) }
```

Binary files work the same way via `modify_binary(file_id, bytes)` /
`get_manifest(file_id)`. To plug in your own storage, implement the
[`RemoteStorage`] and/or [`LocalStore`] traits and pass them to
`with_backends`.

## Usage (C#)

The engine is callable from C# through uniFFI-generated bindings. `NewLocal`
wires the built-in backends; `NewEngine` injects host-implemented ones and a
conflict policy.

```csharp
using uniffi.rollforward;

class Ui : EngineNotificationListener {
    public void OnFileContentUpdated(string fileId) { }
    public void OnConflictCopyRequested(string fileId, string suggestedName) { }
}

var engine = RollforwardMethods.NewLocal(
    "clientA", "client.redb", @"C:\shared\remote", new Ui());

engine.ModifyText("notes.txt", "hello world");
engine.Sync("notes.txt");
Console.WriteLine(engine.GetText("notes.txt"));
```

Regenerate bindings after changing the FFI surface, then run the C# tests:

```bash
cargo build --release
uniffi-bindgen-cs --library target/release/rollforward.dll \
  --out-dir bindings/csharp/Rollforward/generated
dotnet test bindings/csharp/Rollforward.Tests
```

## Building & testing

```bash
cargo build                 # library + cdylib
cargo test                  # unit + integration tests
cargo clippy --all-targets  # lints (see CLAUDE.md for the full strict set)
```

## Version pins (environment-driven)

- **yrs `0.25`** — newer releases need an unstable compiler feature.
- **uniffi `0.29`** — matches the installed C# bindings generator.

Both are documented in [`CLAUDE.md`](CLAUDE.md); bump when the toolchain / C#
generator catch up.

## License

MIT — see [`LICENSE`](LICENSE).

## Status

Early-stage. The API surface and on-disk/remote formats may change.
