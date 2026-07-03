//! Text CRDT wrapper over yrs (y-crdt).
//!
//! Each local edit is turned into an *incremental* yrs update (the delta since
//! the pre-edit state vector) which becomes a [`ChangeType::TextDelta`]. Deltas
//! merge order-independently via `apply_update`, so concurrent edits from
//! multiple clients converge to the same document — no manual conflict logic.
//!
//! `set_text` diffs the current content against the new content by common
//! prefix/suffix and edits only the changed middle, so non-overlapping
//! concurrent edits (e.g. two clients appending in different places) survive
//! the merge instead of clobbering each other.

use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, TextRef, Transact, Update};

use crate::types::SyncError;

/// Root text field name inside the yrs document.
const TEXT_FIELD: &str = "content";

/// A CRDT-backed text document.
pub struct TextDoc {
    /// The underlying yrs document.
    doc: Doc,
    /// Cached handle to the root text type.
    text: TextRef,
}

/// Wrap a yrs error as [`SyncError::SerdeError`].
fn crdt_err<E: std::fmt::Display>(e: E) -> SyncError {
    SyncError::SerdeError { msg: e.to_string() }
}

impl TextDoc {
    /// Create an empty text document.
    pub fn new() -> Self {
        let doc = Doc::new();
        let text = doc.get_or_insert_text(TEXT_FIELD);
        Self { doc, text }
    }

    /// Current document content as a `String`.
    pub fn content(&self) -> String {
        let txn = self.doc.transact();
        self.text.get_string(&txn)
    }

    /// Replace the document content with `new`, returning the incremental yrs
    /// update (v1-encoded) that expresses the change. Empty edits yield an
    /// update with no new content, which is harmless to apply anywhere.
    pub fn set_text(&mut self, new: &str) -> Vec<u8> {
        let before = self.doc.transact().state_vector();
        {
            let old = self.content();
            let (start, del_len, insert) = diff_middle(&old, new);
            let mut txn = self.doc.transact_mut();
            if del_len > 0 {
                self.text.remove_range(&mut txn, start, del_len);
            }
            if !insert.is_empty() {
                self.text.insert(&mut txn, start, insert);
            }
        }
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&before)
    }

    /// Apply a remote delta (v1-encoded yrs update). Idempotent and
    /// order-independent — the CRDT converges regardless of application order.
    pub fn apply_delta(&mut self, delta: &[u8]) -> Result<(), SyncError> {
        let update = Update::decode_v1(delta).map_err(crdt_err)?;
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update).map_err(crdt_err)
    }

    /// Encode the entire document state as a single v1 update — used as a
    /// truncation baseline that replaces the deltas it subsumes.
    pub fn full_update(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }

    /// Rebuild a document from a baseline full-update plus an ordered list of
    /// deltas applied on top.
    pub fn from_baseline_and_deltas(
        baseline: Option<&[u8]>,
        deltas: &[Vec<u8>],
    ) -> Result<Self, SyncError> {
        let mut doc = TextDoc::new();
        if let Some(b) = baseline {
            doc.apply_delta(b)?;
        }
        for d in deltas {
            doc.apply_delta(d)?;
        }
        Ok(doc)
    }
}

impl Default for TextDoc {
    fn default() -> Self {
        Self::new()
    }
}

/// Diff `old` against `new` by common prefix/suffix (in UTF-16 code units, the
/// unit yrs indexes by). Returns `(start, delete_len, inserted_slice)` where
/// `start` is the code-unit offset at which `delete_len` units are removed and
/// `inserted_slice` is inserted.
// Called once (by `set_text`) but kept separate: it is a self-contained,
// individually-tested UTF-16 diff whose inlining would bloat `set_text`.
#[allow(clippy::single_call_fn)]
fn diff_middle<'a>(old: &str, new: &'a str) -> (u32, u32, &'a str) {
    let old_u16: Vec<u16> = old.encode_utf16().collect();
    let new_u16: Vec<u16> = new.encode_utf16().collect();

    // Common prefix length in code units.
    let mut prefix = 0usize;
    while prefix < old_u16.len() && prefix < new_u16.len() && old_u16[prefix] == new_u16[prefix] {
        prefix += 1;
    }

    // Common suffix length, not overlapping the prefix.
    let mut suffix = 0usize;
    while suffix < old_u16.len() - prefix
        && suffix < new_u16.len() - prefix
        && old_u16[old_u16.len() - 1 - suffix] == new_u16[new_u16.len() - 1 - suffix]
    {
        suffix += 1;
    }

    let del_len = old_u16.len() - prefix - suffix;

    // Map the inserted middle `[prefix, new_u16.len() - suffix)` back to a byte
    // slice on `new`. Walk chars accumulating UTF-16 code units until each
    // endpoint is reached; both fall on char boundaries because the scan stops
    // at the first differing code unit and surrogate pairs stay within one char.
    let insert_end_cu = new_u16.len() - suffix;
    let mut cu = 0usize;
    let mut byte_start = new.len();
    let mut byte_end = new.len();
    let mut start_set = false;
    for (byte_idx, ch) in new.char_indices() {
        if !start_set && cu >= prefix {
            byte_start = byte_idx;
            start_set = true;
        }
        if cu >= insert_end_cu {
            byte_end = byte_idx;
            break;
        }
        cu += ch.len_utf16();
    }

    (
        u32::try_from(prefix).unwrap_or(u32::MAX),
        u32::try_from(del_len).unwrap_or(u32::MAX),
        &new[byte_start..byte_end],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_read_back() {
        let mut doc = TextDoc::new();
        doc.set_text("hello world");
        assert_eq!(doc.content(), "hello world");
        doc.set_text("hello brave world");
        assert_eq!(doc.content(), "hello brave world");
    }

    #[test]
    fn delta_reconstructs_on_fresh_doc() {
        let mut a = TextDoc::new();
        let d1 = a.set_text("hello");
        let d2 = a.set_text("hello world");

        let rebuilt = TextDoc::from_baseline_and_deltas(None, &[d1, d2]).unwrap();
        assert_eq!(rebuilt.content(), "hello world");
    }

    #[test]
    fn concurrent_edits_converge() {
        // Shared base.
        let mut base = TextDoc::new();
        let base_delta = base.set_text("the quick fox");

        // Two clients start from the same base.
        let mut a =
            TextDoc::from_baseline_and_deltas(None, std::slice::from_ref(&base_delta)).unwrap();
        let mut b = TextDoc::from_baseline_and_deltas(None, &[base_delta]).unwrap();

        // A edits the front, B edits the back — non-overlapping.
        let da = a.set_text("the quick brown fox");
        let db = b.set_text("the quick fox jumps");

        // Cross-apply each other's delta.
        a.apply_delta(&db).unwrap();
        b.apply_delta(&da).unwrap();

        // Both converge to identical content containing both edits.
        assert_eq!(a.content(), b.content());
        assert!(a.content().contains("brown"));
        assert!(a.content().contains("jumps"));
    }

    #[test]
    fn baseline_full_update_subsumes_history() {
        let mut a = TextDoc::new();
        a.set_text("v1");
        a.set_text("v1 plus v2");
        let baseline = a.full_update();

        // A fresh doc restored from the baseline alone equals the original.
        let restored = TextDoc::from_baseline_and_deltas(Some(&baseline), &[]).unwrap();
        assert_eq!(restored.content(), "v1 plus v2");
    }

    #[test]
    fn diff_middle_prefix_suffix() {
        // Insert in the middle: prefix "ab", suffix "ef".
        assert_eq!(diff_middle("abef", "abcdef"), (2, 0, "cd"));
        // Pure delete of middle.
        assert_eq!(diff_middle("abcdef", "abef"), (2, 2, ""));
        // Append.
        assert_eq!(diff_middle("ab", "abc"), (2, 0, "c"));
        // Full replace.
        assert_eq!(diff_middle("abc", "xyz"), (0, 3, "xyz"));
    }
}
