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

use fastcdc::v2020::FastCDC;

use crate::types::{BinaryConflictPolicy, ChunkInfo};

/// Minimum chunk size (bytes).
const MIN_SIZE: usize = 4 * 1024;
/// Average (target) chunk size (bytes).
const AVG_SIZE: usize = 16 * 1024;
/// Maximum chunk size (bytes).
const MAX_SIZE: usize = 64 * 1024;

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

/// Reassemble file bytes from an ordered manifest, given a chunk fetcher.
/// The fetcher returns the bytes for a hash (e.g. `remote.get_chunk`).
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

/// Given the set of live manifests and every stored chunk hash, return the
/// orphaned hashes safe to garbage-collect (present in storage, referenced by
/// no live manifest).
pub fn orphaned_chunks(live_manifests: &[Vec<String>], stored: &[String]) -> Vec<String> {
    use std::collections::HashSet;
    let live: HashSet<&String> = live_manifests.iter().flatten().collect();
    stored
        .iter()
        .filter(|h| !live.contains(h))
        .cloned()
        .collect()
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
    fn gc_finds_only_orphans() {
        let live = vec![vec!["a".to_string(), "b".to_string()]];
        let stored = vec!["a".to_string(), "b".to_string(), "orphan".to_string()];
        assert_eq!(orphaned_chunks(&live, &stored), vec!["orphan".to_string()]);
    }
}
