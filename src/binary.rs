//! Binary file chunking (fastcdc + blake3) and three-way manifest merge.
//!
//! A binary file is sliced into content-defined chunks with fastcdc; each
//! chunk is hashed with blake3. The ordered list of hashes is the file's
//! *manifest* (a [`ChangeType::BinarySnapshot`]). Identical content produces
//! identical chunks, so unchanged regions are naturally deduplicated across
//! versions and across files.
//!
//! Merging two concurrent binary versions is a three-way manifest compare
//! against their common base. If only one side changed, that side wins; if
//! both changed the content, it is a genuine conflict resolved per
//! [`BinaryConflictPolicy`].

use std::collections::HashSet;

use fastcdc::v2020::FastCDC;
use serde::{Deserialize, Serialize};

use crate::types::{BinaryConflictPolicy, ChunkInfo};

/// Minimum chunk size (bytes).
const MIN_SIZE: usize = 4 * 1024;
/// Average (target) chunk size (bytes).
const AVG_SIZE: usize = 16 * 1024;
/// Maximum chunk size (bytes).
const MAX_SIZE: usize = 64 * 1024;

/// Target size for a pack object (bytes). New chunks are concatenated into
/// packs up to this size before being flushed, so one binary write produces a
/// handful of pack objects instead of thousands of per-chunk objects. A pack
/// may exceed this only when a single chunk is larger (chunks are ≤ `MAX_SIZE`,
/// well under this target, so in practice packs land just over the target).
pub const TARGET_PACK_SIZE: usize = 4 * 1024 * 1024;

/// Where a chunk lives inside a pack object: a byte range `[offset, offset+length)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackedChunk {
    /// blake3 hash of the chunk (its content address).
    pub hash: String,
    /// Byte offset of the chunk within the pack.
    pub offset: u64,
    /// Chunk length in bytes.
    pub length: u32,
}

/// The index for one pack object: the chunks it contains and where. Stored as a
/// sibling object (`index_id == pack_id`); readers union every pack index to
/// resolve `hash -> (pack_id, offset, length)`. Immutable and content-addressed,
/// so concurrent writers need no coordination — identical content yields an
/// identical pack id and an idempotent index write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackIndex {
    /// blake3 hash of the whole pack object (its content address / id).
    pub pack_id: String,
    /// The chunks packed into this object, in storage order.
    pub chunks: Vec<PackedChunk>,
}

/// Group `new_chunks` into pack objects of up to [`TARGET_PACK_SIZE`] bytes.
/// Returns each pack's `(pack_id, pack_bytes, index)` ready to upload. Callers
/// pass only chunks not already stored remotely (dedup is decided against the
/// union pack index before calling this). A chunk appearing twice in the input
/// is packed once (its first occurrence).
pub fn build_packs(new_chunks: &[(ChunkInfo, Vec<u8>)]) -> Vec<(String, Vec<u8>, PackIndex)> {
    let mut packs = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut chunks: Vec<PackedChunk> = Vec::new();

    for (info, bytes) in new_chunks {
        if !seen.insert(info.hash.as_str()) {
            continue; // duplicate within this batch — pack once
        }
        chunks.push(PackedChunk {
            hash: info.hash.clone(),
            offset: buf.len() as u64,
            length: u32::try_from(bytes.len()).unwrap_or(u32::MAX),
        });
        buf.extend_from_slice(bytes);
        if buf.len() >= TARGET_PACK_SIZE {
            packs.push(seal_pack(std::mem::take(&mut buf), std::mem::take(&mut chunks)));
        }
    }
    if !buf.is_empty() {
        packs.push(seal_pack(buf, chunks));
    }
    packs
}

/// Finalize one pack: content-address it and stamp the id into its index.
fn seal_pack(buf: Vec<u8>, chunks: Vec<PackedChunk>) -> (String, Vec<u8>, PackIndex) {
    let pack_id = blake3::hash(&buf).to_hex().to_string();
    let index = PackIndex {
        pack_id: pack_id.clone(),
        chunks,
    };
    (pack_id, buf, index)
}

/// Slice `data` into content-defined chunks and hash each with blake3.
/// Deterministic: the same bytes always yield the same manifest.
pub fn chunk_data(data: &[u8]) -> Vec<(ChunkInfo, Vec<u8>)> {
    let chunker = FastCDC::new(data, MIN_SIZE, AVG_SIZE, MAX_SIZE);
    let mut out = Vec::new();
    for chunk in chunker {
        let bytes = &data[chunk.offset..chunk.offset + chunk.length];
        let hash = blake3::hash(bytes).to_hex().to_string();
        out.push((
            ChunkInfo {
                hash,
                offset: chunk.offset as u64,
                // Chunk length is bounded by MAX_SIZE (64 KiB), well within u32.
                length: u32::try_from(chunk.length).unwrap_or(u32::MAX),
            },
            bytes.to_vec(),
        ));
    }
    out
}

/// The ordered hash manifest for `data`.
pub fn manifest(data: &[u8]) -> Vec<String> {
    chunk_data(data).into_iter().map(|(c, _)| c.hash).collect()
}

/// Whether `bytes` hash to their claimed content-address `expected_hash`.
/// Chunks are addressed by blake3, so a mismatch means the bytes were corrupted
/// or truncated in storage or in transit — always detectable on read. Callers
/// verify every fetched chunk before trusting it (see `read_binary`, repack).
#[must_use]
pub fn verify_chunk(expected_hash: &str, bytes: &[u8]) -> bool {
    blake3::hash(bytes).to_hex().to_string() == expected_hash
}

/// Reassemble file bytes from an ordered manifest, given a chunk fetcher.
/// The fetcher returns the bytes for a hash (e.g. a pack range read resolved
/// through the union pack index).
pub fn reassemble<F, E>(manifest: &[String], mut fetch: F) -> Result<Vec<u8>, E>
where
    F: FnMut(&str) -> Result<Vec<u8>, E>,
{
    let mut out = Vec::new();
    for hash in manifest {
        out.extend_from_slice(&fetch(hash)?);
    }
    Ok(out)
}

/// Outcome of a three-way binary manifest merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryMerge {
    /// Neither side changed relative to base (or both changed identically).
    Unchanged(Vec<String>),
    /// Exactly one side changed; that manifest is the merged result.
    FastForward(Vec<String>),
    /// Both sides changed the content differently — a real conflict, resolved
    /// per the supplied policy.
    Conflict {
        /// The resolved manifest according to policy.
        resolved: Vec<String>,
        /// Whether the host must duplicate the losing side under a new name.
        needs_copy: bool,
    },
}

/// Three-way merge of two manifests against their common `base`.
///
/// - one side unchanged → take the other (fast-forward),
/// - both unchanged / identical → unchanged,
/// - both changed differently → conflict resolved by `policy`
///   (`KeepBoth` sets `needs_copy` so the host duplicates the remote side).
pub fn merge_manifests(
    base: &[String],
    local: &[String],
    remote: &[String],
    policy: BinaryConflictPolicy,
) -> BinaryMerge {
    let local_changed = local != base;
    let remote_changed = remote != base;

    match (local_changed, remote_changed) {
        (false, false) => BinaryMerge::Unchanged(base.to_vec()),
        (true, false) => BinaryMerge::FastForward(local.to_vec()),
        (false, true) => BinaryMerge::FastForward(remote.to_vec()),
        (true, true) => {
            if local == remote {
                // Both made the same change — no real divergence.
                BinaryMerge::Unchanged(local.to_vec())
            } else {
                match policy {
                    BinaryConflictPolicy::KeepLocal => BinaryMerge::Conflict {
                        resolved: local.to_vec(),
                        needs_copy: false,
                    },
                    BinaryConflictPolicy::KeepRemote => BinaryMerge::Conflict {
                        resolved: remote.to_vec(),
                        needs_copy: false,
                    },
                    BinaryConflictPolicy::KeepBoth => BinaryMerge::Conflict {
                        // Main pointer keeps local; host duplicates remote.
                        resolved: local.to_vec(),
                        needs_copy: true,
                    },
                }
            }
        }
    }
}

/// A pack exceeding this fraction of dead bytes is repacked; below it, the pack
/// is kept as-is (its dead bytes reclaimed only once it crosses the threshold).
/// The threshold trades reclaimed space against rewrite churn: repacking is a
/// full read+rewrite of the surviving chunks, so we only pay it once a pack is
/// mostly dead.
pub const REPACK_DEAD_FRACTION: f64 = 0.5;

/// What garbage collection should do with one pack, given which of its chunks
/// are still referenced by a live manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackGc {
    /// Every chunk is live (or the pack is empty) — leave it untouched.
    Keep,
    /// No chunk is live — delete the pack and its index outright.
    Delete,
    /// Some chunks are dead but not enough to cross [`REPACK_DEAD_FRACTION`] —
    /// keep the pack; its dead bytes wait for a later, deader pass.
    KeepMixed,
    /// Dead bytes cross [`REPACK_DEAD_FRACTION`] — rewrite `live` (the surviving
    /// chunk hashes, in pack order) into a fresh pack, then delete the old one.
    Repack {
        /// Surviving chunk hashes in their original pack order.
        live: Vec<String>,
    },
}

/// Classify a pack for GC from its chunks and the set of live chunk hashes.
/// Pure: decides *what* to do; the engine performs the reads/writes/deletes.
// Called once (by the truncate GC loop) but kept as a named, testable predicate
// so the threshold policy is unit-tested without engine/remote plumbing.
#[allow(clippy::single_call_fn)]
pub fn classify_pack<S: std::hash::BuildHasher>(
    chunks: &[PackedChunk],
    live: &HashSet<String, S>,
) -> PackGc {
    let mut live_bytes: u64 = 0;
    let mut dead_bytes: u64 = 0;
    let mut live_hashes = Vec::new();
    for c in chunks {
        if live.contains(&c.hash) {
            live_bytes += u64::from(c.length);
            live_hashes.push(c.hash.clone());
        } else {
            dead_bytes += u64::from(c.length);
        }
    }
    let total = live_bytes + dead_bytes;
    if dead_bytes == 0 || total == 0 {
        return PackGc::Keep;
    }
    if live_bytes == 0 {
        return PackGc::Delete;
    }
    // dead_bytes / total >= REPACK_DEAD_FRACTION, without float division on the
    // hot integers: compare cross-multiplied.
    #[allow(clippy::cast_precision_loss)]
    if dead_bytes as f64 >= total as f64 * REPACK_DEAD_FRACTION {
        PackGc::Repack { live: live_hashes }
    } else {
        PackGc::KeepMixed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_is_deterministic() {
        // Use enough data to force multiple chunks.
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let m1 = manifest(&data);
        let m2 = manifest(&data);
        assert_eq!(m1, m2);
        assert!(m1.len() > 1, "expected multiple chunks, got {}", m1.len());
    }

    #[test]
    fn reassemble_round_trips() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let chunks = chunk_data(&data);
        let store: std::collections::HashMap<String, Vec<u8>> = chunks
            .iter()
            .map(|(c, b)| (c.hash.clone(), b.clone()))
            .collect();
        let man = manifest(&data);
        let rebuilt = reassemble::<_, ()>(&man, |h| Ok(store[h].clone())).unwrap();
        assert_eq!(rebuilt, data);
    }

    #[test]
    fn shared_chunks_dedupe_across_versions() {
        // Appending to a file should reuse the leading chunks unchanged.
        let base: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let mut appended = base.clone();
        appended.extend((0..5000u32).map(|i| (i % 97) as u8));

        let m_base = manifest(&base);
        let m_app = manifest(&appended);
        // The first chunk hash is shared (content-defined boundary stability).
        assert_eq!(m_base[0], m_app[0]);
    }

    /// BI-202: a small edit in the middle of a large file changes only the
    /// chunk(s) covering that region; the vast majority of chunk hashes are
    /// identical, so incremental transfer moves only the changed chunk(s).
    #[test]
    fn small_edit_changes_few_chunks() {
        let base = pseudo_random(500_000);
        let mut edited = base.clone();
        // Flip 1 KB in the middle.
        for b in &mut edited[250_000..251_024] {
            *b ^= 0xff;
        }
        let m_base = manifest(&base);
        let m_edit = manifest(&edited);

        let same = m_base.iter().filter(|h| m_edit.contains(h)).count();
        // Almost all chunks survive unchanged; only the edited region differs.
        let changed = m_base.len() - same;
        assert!(
            changed <= 2,
            "expected <=2 changed chunks, got {changed} of {}",
            m_base.len()
        );
        assert!(same > 0, "most chunks must be shared");
    }

    /// BI-203: prepending a single byte does not cascade new boundaries through
    /// the whole file — content-defined chunking resynchronizes, so most later
    /// chunk hashes are preserved (no avalanche). Uses pseudo-random data (an
    /// LCG) rather than a periodic pattern, which models real file content and
    /// lets CDC boundaries resynchronize after the shift.
    #[test]
    fn prepend_byte_does_not_avalanche() {
        let base = pseudo_random(500_000);
        let mut prepended = vec![0xabu8];
        prepended.extend_from_slice(&base);

        let m_base = manifest(&base);
        let m_pre = manifest(&prepended);

        let shared = m_base.iter().filter(|h| m_pre.contains(h)).count();
        // The leading chunk shifts, but the majority downstream resynchronize.
        assert!(
            shared * 2 > m_base.len(),
            "expected majority of chunks preserved, {shared} of {}",
            m_base.len()
        );
    }

    /// Deterministic pseudo-random bytes via a linear congruential generator —
    /// stands in for real, non-periodic file content in CDC tests.
    fn pseudo_random(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                u8::try_from((state >> 33) & 0xff).unwrap_or(0)
            })
            .collect()
    }

    #[test]
    fn merge_fast_forwards_single_sided_change() {
        let base = vec!["a".to_string(), "b".to_string()];
        let local = base.clone();
        let remote = vec!["a".to_string(), "c".to_string()];
        assert_eq!(
            merge_manifests(&base, &local, &remote, BinaryConflictPolicy::KeepLocal),
            BinaryMerge::FastForward(remote)
        );
    }

    #[test]
    fn merge_conflict_keep_both_flags_copy() {
        let base = vec!["a".to_string()];
        let local = vec!["b".to_string()];
        let remote = vec!["c".to_string()];
        let merged = merge_manifests(&base, &local, &remote, BinaryConflictPolicy::KeepBoth);
        assert_eq!(
            merged,
            BinaryMerge::Conflict {
                resolved: local,
                needs_copy: true
            }
        );
    }

    #[test]
    fn merge_identical_changes_are_not_conflicts() {
        let base = vec!["a".to_string()];
        let both = vec!["b".to_string()];
        assert_eq!(
            merge_manifests(&base, &both, &both, BinaryConflictPolicy::KeepLocal),
            BinaryMerge::Unchanged(both)
        );
    }

    #[test]
    fn build_packs_groups_and_addresses() {
        // Many small chunks group into few packs bounded by TARGET_PACK_SIZE.
        let data = pseudo_random(TARGET_PACK_SIZE + 300_000);
        let chunks = chunk_data(&data);
        assert!(chunks.len() > 1);
        let packs = build_packs(&chunks);
        // At least two packs (data exceeds one target) and each within bounds
        // (a pack may exceed the target by at most one final chunk ≤ MAX_SIZE).
        assert!(packs.len() >= 2, "expected split, got {}", packs.len());
        for (pack_id, bytes, index) in &packs {
            assert!(bytes.len() <= TARGET_PACK_SIZE + MAX_SIZE);
            assert_eq!(*pack_id, index.pack_id);
            // Offsets/lengths locate each chunk's bytes exactly within the pack.
            for c in &index.chunks {
                let start = usize::try_from(c.offset).unwrap();
                let slice = &bytes[start..start + c.length as usize];
                assert_eq!(blake3::hash(slice).to_hex().to_string(), c.hash);
            }
        }
    }

    #[test]
    fn build_packs_dedupes_within_batch() {
        // The same chunk offered twice is packed once.
        let one = chunk_data(&pseudo_random(20_000));
        let mut doubled = one.clone();
        doubled.extend(one.clone());
        let packed: usize = build_packs(&doubled)
            .iter()
            .map(|(_, _, idx)| idx.chunks.len())
            .sum();
        let unique = one.len();
        assert_eq!(packed, unique, "duplicates must be packed once");
    }

    #[test]
    fn pack_index_serde_round_trips() {
        let index = PackIndex {
            pack_id: "pid".into(),
            chunks: vec![PackedChunk {
                hash: "h".into(),
                offset: 7,
                length: 42,
            }],
        };
        let bytes = serde_json::to_vec(&index).unwrap();
        let back: PackIndex = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(index, back);
    }

    /// A pack of the given per-chunk `(hash, length)` byte sizes.
    fn pack_of(chunks: &[(&str, u32)]) -> Vec<PackedChunk> {
        let mut offset = 0;
        chunks
            .iter()
            .map(|(h, len)| {
                let c = PackedChunk {
                    hash: (*h).to_string(),
                    offset,
                    length: *len,
                };
                offset += u64::from(*len);
                c
            })
            .collect()
    }

    fn live_set(hashes: &[&str]) -> HashSet<String> {
        hashes.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn classify_keeps_all_live_and_empty_packs() {
        let pack = pack_of(&[("a", 10), ("b", 10)]);
        assert_eq!(classify_pack(&pack, &live_set(&["a", "b"])), PackGc::Keep);
        assert_eq!(classify_pack(&[], &live_set(&[])), PackGc::Keep);
    }

    #[test]
    fn classify_deletes_fully_dead_pack() {
        let pack = pack_of(&[("a", 10), ("b", 10)]);
        assert_eq!(classify_pack(&pack, &live_set(&["x"])), PackGc::Delete);
    }

    #[test]
    fn classify_repacks_when_majority_dead() {
        // 30 of 40 bytes dead (75%) -> repack, keeping only the live hash.
        let pack = pack_of(&[("live", 10), ("dead1", 20), ("dead2", 10)]);
        assert_eq!(
            classify_pack(&pack, &live_set(&["live"])),
            PackGc::Repack {
                live: vec!["live".to_string()]
            }
        );
    }

    #[test]
    fn classify_keeps_mixed_when_mostly_live() {
        // 10 of 40 bytes dead (25%) -> below threshold, keep mixed.
        let pack = pack_of(&[("a", 15), ("b", 15), ("dead", 10)]);
        assert_eq!(
            classify_pack(&pack, &live_set(&["a", "b"])),
            PackGc::KeepMixed
        );
    }

    #[test]
    fn classify_repacks_exactly_at_threshold() {
        // Exactly 50% dead -> repack (threshold is inclusive).
        let pack = pack_of(&[("live", 10), ("dead", 10)]);
        assert_eq!(
            classify_pack(&pack, &live_set(&["live"])),
            PackGc::Repack {
                live: vec!["live".to_string()]
            }
        );
    }
}
