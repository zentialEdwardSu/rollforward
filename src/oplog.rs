//! OpLog filename encoding and fork detection over the append-only remote log.
//!
//! Remote oplog objects are named `{sequence}_{client_id}.oplog`. Because the
//! remote is dumb and append-only, two clients can independently create the
//! same sequence with different client ids — that duplicate-sequence condition
//! is a *fork*, detected here and resolved by the engine's merge path.

use crate::types::{RemoteLogItem, SyncError};

/// File extension used for remote oplog objects.
pub const OPLOG_EXT: &str = "oplog";

/// Format the remote object name for an oplog entry: `{sequence}_{client_id}.oplog`.
pub fn format_oplog_name(sequence: u64, client_id: &str) -> String {
    format!("{sequence}_{client_id}.{OPLOG_EXT}")
}

/// Parse `{sequence}_{client_id}.oplog` back into its parts.
///
/// The client id may itself contain underscores; only the leading numeric
/// segment before the first `_` is the sequence.
pub fn parse_oplog_name(name: &str) -> Result<(u64, String), SyncError> {
    let stem = name
        .strip_suffix(&format!(".{OPLOG_EXT}"))
        .ok_or_else(|| SyncError::IoError {
            msg: format!("not an oplog name: {name}"),
        })?;
    let (seq_str, client) = stem.split_once('_').ok_or_else(|| SyncError::IoError {
        msg: format!("malformed oplog name: {name}"),
    })?;
    let sequence = seq_str.parse::<u64>().map_err(|_| SyncError::IoError {
        msg: format!("bad sequence in oplog name: {name}"),
    })?;
    if client.is_empty() {
        return Err(SyncError::IoError {
            msg: format!("empty client id in oplog name: {name}"),
        });
    }
    Ok((sequence, client.to_string()))
}

/// Sort remote items ascending by sequence, tie-broken by client id so the
/// ordering is deterministic across clients.
pub fn sort_items(items: &mut [RemoteLogItem]) {
    items.sort_by(|a, b| {
        a.sequence
            .cmp(&b.sequence)
            .then_with(|| a.client_id.cmp(&b.client_id))
    });
}

/// Return true if any sequence number is claimed by more than one client — a
/// concurrent fork that must be merged rather than linearly replayed.
///
/// Assumes `items` is sorted (see [`sort_items`]).
pub fn has_fork(items: &[RemoteLogItem]) -> bool {
    items
        .windows(2)
        .any(|w| w[0].sequence == w[1].sequence && w[0].client_id != w[1].client_id)
}

/// The sequence numbers at which a fork occurs.
pub fn forked_sequences(items: &[RemoteLogItem]) -> Vec<u64> {
    let mut out = Vec::new();
    for w in items.windows(2) {
        if w[0].sequence == w[1].sequence
            && w[0].client_id != w[1].client_id
            && out.last() != Some(&w[0].sequence)
        {
            out.push(w[0].sequence);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(seq: u64, client: &str) -> RemoteLogItem {
        RemoteLogItem {
            sequence: seq,
            client_id: client.to_string(),
            remote_path: format_oplog_name(seq, client),
        }
    }

    #[test]
    fn name_round_trips() {
        let name = format_oplog_name(42, "clientA");
        assert_eq!(name, "42_clientA.oplog");
        assert_eq!(
            parse_oplog_name(&name).unwrap(),
            (42, "clientA".to_string())
        );
    }

    #[test]
    fn client_id_with_underscore_round_trips() {
        // The sequence is only the leading numeric segment; the rest is the id.
        let name = format_oplog_name(7, "client_A_1");
        assert_eq!(
            parse_oplog_name(&name).unwrap(),
            (7, "client_A_1".to_string())
        );
    }

    #[test]
    fn rejects_malformed_names() {
        assert!(parse_oplog_name("nope.txt").is_err());
        assert!(parse_oplog_name("nounderscore.oplog").is_err());
        assert!(parse_oplog_name("x_client.oplog").is_err());
        assert!(parse_oplog_name("5_.oplog").is_err());
    }

    #[test]
    fn detects_fork_on_duplicate_sequence() {
        let mut items = vec![item(1, "a"), item(2, "b"), item(2, "a"), item(3, "a")];
        sort_items(&mut items);
        assert!(has_fork(&items));
        assert_eq!(forked_sequences(&items), vec![2]);
    }

    #[test]
    fn no_fork_when_sequences_unique() {
        let mut items = vec![item(3, "a"), item(1, "a"), item(2, "b")];
        sort_items(&mut items);
        assert!(!has_fork(&items));
        assert_eq!(items[0].sequence, 1);
        assert_eq!(items[2].sequence, 3);
    }

    #[test]
    fn same_sequence_same_client_is_not_a_fork() {
        // Idempotent re-listing of the same object must not read as a fork.
        let mut items = vec![item(2, "a"), item(2, "a")];
        sort_items(&mut items);
        assert!(!has_fork(&items));
    }
}
