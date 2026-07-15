//! Pure, backend-independent synchronization planner.

use std::collections::{BTreeMap, BTreeSet};

use crate::v2_types::{
    BaselineState, ConflictKind, ConflictRecord, PlanOperation, RemoteResourceState, ReplicaState,
    ResourceKey, SyncPlan,
};

/// Plan one resource from the local, acknowledged baseline, and remote states.
pub fn plan_resource(
    key: ResourceKey,
    local: ReplicaState,
    baseline: Option<BaselineState>,
    remote: RemoteResourceState,
) -> PlanOperation {
    use BaselineState::{Deleted as BaselineDeleted, Present as BaselinePresent};
    use PlanOperation::*;
    use RemoteResourceState::{
        Deleted as RemoteDeleted, Forked as RemoteForked, Missing as RemoteMissing,
        Present as RemotePresent,
    };
    use ReplicaState::{Deleted as LocalDeleted, Missing as LocalMissing, Present as LocalPresent};

    let conflict = |kind| Conflict {
        record: ConflictRecord::new(key.clone(), kind, &local, &remote),
    };

    match (&local, &remote, baseline.as_ref()) {
        (_, RemoteForked { .. }, _) => conflict(ConflictKind::RemoteFork),
        (
            LocalPresent {
                content_id: local_id,
                ..
            },
            RemotePresent {
                content_id: remote_id,
                heads,
                ..
            },
            _,
        ) if local_id == remote_id => EstablishBaseline {
            key,
            local,
            remote_heads: heads.clone(),
        },
        (LocalPresent { .. }, RemoteMissing, _) => Upload { key, local },
        (LocalMissing | LocalDeleted { .. }, RemotePresent { .. }, None) => {
            Download { key, remote }
        }
        (
            LocalMissing | LocalDeleted { .. },
            RemotePresent { .. },
            Some(BaselineDeleted { .. }),
        ) => Download { key, remote },
        (LocalPresent { .. }, RemotePresent { .. }, None) => {
            conflict(ConflictKind::InitialDivergence)
        }
        (
            LocalPresent { content_id, .. },
            RemotePresent { heads, .. },
            Some(BaselinePresent {
                content_id: base, ..
            }),
        ) if content_id == base => Download {
            key,
            remote: remote.clone(),
        },
        (
            LocalPresent { .. },
            RemotePresent { content_id, .. },
            Some(BaselinePresent {
                content_id: base, ..
            }),
        ) if content_id == base => Upload { key, local },
        (
            LocalPresent { .. },
            RemotePresent { .. },
            Some(BaselinePresent { .. } | BaselineDeleted { .. }),
        ) => conflict(ConflictKind::ContentVsContent),
        (
            LocalMissing | LocalDeleted { .. },
            RemotePresent { content_id, .. },
            Some(BaselinePresent {
                content_id: base, ..
            }),
        ) if content_id == base => PublishDelete { key },
        (
            LocalMissing | LocalDeleted { .. },
            RemotePresent { .. },
            Some(BaselinePresent { .. }),
        ) => conflict(ConflictKind::DeleteVsModify),
        (
            LocalPresent { content_id, .. },
            RemoteDeleted { heads },
            Some(BaselinePresent {
                content_id: base, ..
            }),
        ) if content_id == base => ApplyDelete {
            key,
            local,
            remote_heads: heads.clone(),
        },
        (LocalPresent { .. }, RemoteDeleted { .. }, Some(BaselineDeleted { .. })) => {
            Upload { key, local }
        }
        (LocalPresent { .. }, RemoteDeleted { .. }, _) => conflict(ConflictKind::ModifyVsDelete),
        (LocalMissing | LocalDeleted { .. }, RemoteDeleted { heads }, _) => {
            EstablishDeletedBaseline {
                key,
                remote_heads: heads.clone(),
            }
        }
        (LocalMissing | LocalDeleted { .. }, RemoteMissing, _) => Noop { key },
    }
}

/// Plan a deterministic union of local, baseline, and remote resources.
pub fn plan_inventory(
    local: Vec<(ResourceKey, ReplicaState)>,
    baselines: Vec<(ResourceKey, BaselineState)>,
    remote: Vec<(ResourceKey, RemoteResourceState)>,
) -> SyncPlan {
    let local: BTreeMap<_, _> = local.into_iter().collect();
    let baselines: BTreeMap<_, _> = baselines.into_iter().collect();
    let remote: BTreeMap<_, _> = remote.into_iter().collect();
    let keys: BTreeSet<_> = local
        .keys()
        .chain(baselines.keys())
        .chain(remote.keys())
        .cloned()
        .collect();
    SyncPlan {
        operations: keys
            .into_iter()
            .map(|key| {
                plan_resource(
                    key.clone(),
                    local.get(&key).cloned().unwrap_or(ReplicaState::Missing),
                    baselines.get(&key).cloned(),
                    remote
                        .get(&key)
                        .cloned()
                        .unwrap_or(RemoteResourceState::Missing),
                )
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ResourceKey {
        ResourceKey::new("item", "file.bin")
    }
    fn local(id: &str) -> ReplicaState {
        ReplicaState::present(id, 1, "v1")
    }
    fn remote(id: &str) -> RemoteResourceState {
        RemoteResourceState::present(id, 1, vec!["h1".into()])
    }

    #[test]
    fn initial_divergence_never_overwrites() {
        assert!(
            matches!(plan_resource(key(), local("a"), None, remote("b")), PlanOperation::Conflict { record } if record.kind == ConflictKind::InitialDivergence)
        );
    }

    #[test]
    fn unchanged_local_pulls_remote_change() {
        let baseline = BaselineState::present("a", "v1", vec!["old".into()]);
        assert!(matches!(
            plan_resource(key(), local("a"), Some(baseline), remote("b")),
            PlanOperation::Download { .. }
        ));
    }

    #[test]
    fn local_delete_publishes_tombstone() {
        let baseline = BaselineState::present("a", "v1", vec!["old".into()]);
        assert!(matches!(
            plan_resource(key(), ReplicaState::Missing, Some(baseline), remote("a")),
            PlanOperation::PublishDelete { .. }
        ));
    }
}
