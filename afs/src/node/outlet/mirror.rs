//! 收敛循环的纯函数核心。
//!
//! 这里不碰文件系统：输入是"Meta 期望的 root 集合"与"`/public` 当前内容"，
//! 输出是要执行的动作列表。IO 与循环在 `mirror` 的上层。把这个边界画清楚，
//! 是为了让"给定期望态与该有状态，应该做什么"能被表驱动测试穷举——包括那些
//! 在真机上极难复现的中间态（半发布、ghost 占位、换代瞬间）。

use std::collections::{BTreeMap, BTreeSet};

use crate::node::vfs::ownerfs::root::OutletRoot;

/// One filesystem action the mirror must perform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    /// Atomically publish `name -> target`. `target` is the symlink body.
    Publish { name: String, target: String },
    /// Remove `name` from the public directory. Idempotent.
    Remove { name: String },
    /// Move a non-symlink entry out of the way so the name can be published.
    ///
    /// Only produced for entries the mirror did not create; a consumer that
    /// mkdir'd over a name would otherwise make publishing fail with EEXIST
    /// forever, because `symlinkat` cannot replace a directory.
    Quarantine { name: String },
}

/// A `/public` entry as observed on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PresentEntry {
    /// A symlink whose body is `target`. This is the only shape the mirror owns.
    Symlink { target: String },
    /// A real file or directory: a consumer violated the contract.
    Foreign,
}

/// Where a published name points.
///
/// Rendered from `(home, root_epoch)` and the name-derived root id. Kept as a
/// function rather than a stored string so the path formula exists exactly once.
///
/// The target carries the generation (`-e<epoch>`): a same-name rebuild is a
/// different path, so old and new generations cannot be confused, and a
/// generation change is a single rename on `/public`.
///
/// **Assumes `root_id == "root-" + hex(name)`**, which is what OwnerFs enforces
/// (`root_id_from_name`: `format!("root-{}", hex(bytes))`). Concatenating
/// `-e<epoch>` onto the id therefore reproduces `root_data_dir` byte for byte.
/// `target_matches_ownerfs_data_dir_naming` pins that equivalence; if the
/// encoding ever changes, that test fails instead of the mirror silently
/// pointing at paths that do not exist.
pub fn target_path(import_root: &str, root: &OutletRoot) -> String {
    format!(
        "{}/{}/users/{}-e{}",
        import_root.trim_end_matches('/'),
        root.home_node_id,
        root.root_id.0,
        root.root_epoch
    )
}

/// Names the mirror reserves for its own bookkeeping and never publishes.
const RESERVED: [&str; 2] = [".pub", ".quarantine"];

/// Compute the actions that converge `present` onto `active`.
///
/// Contract:
/// - Pure: no IO, no clocks, no ambient state.
/// - Idempotent: applying the result and recomputing yields no actions, because
///   every surviving entry is then either an exactly-matching symlink or absent.
/// - Ordering is deterministic so logs and tests are stable.
///
/// `active` must already reflect the caller's cached snapshot (see
/// [`ActiveOutletRoots::merge_into`]); this function has no notion of
/// `unchanged` and must never be called with an empty slice that merely means
/// "the reply carried no payload" — that would remove everything (spec H14).
pub fn plan(
    import_root: &str,
    active: &[OutletRoot],
    present: &BTreeMap<String, PresentEntry>,
) -> Vec<Action> {
    let mut actions = Vec::new();

    // Expectation keyed by name. Later duplicates cannot occur: Meta returns at
    // most one Active record per root_id, and the name is derived from it.
    let mut wanted: BTreeMap<&str, &OutletRoot> = BTreeMap::new();
    for root in active {
        wanted.insert(root.root_id.0.as_str(), root);
    }

    for (name, root) in &wanted {
        let target = target_path(import_root, root);
        match present.get(*name) {
            Some(PresentEntry::Symlink { target: current }) if current == &target => {}
            // A wrong target is republished in place: stale after a generation
            // change, or left over from a previous import layout.
            Some(PresentEntry::Symlink { .. }) => actions.push(Action::Publish {
                name: (*name).to_owned(),
                target,
            }),
            Some(PresentEntry::Foreign) => {
                // Move it aside first; publishing over a real directory fails.
                actions.push(Action::Quarantine {
                    name: (*name).to_owned(),
                });
                actions.push(Action::Publish {
                    name: (*name).to_owned(),
                    target,
                });
            }
            None => actions.push(Action::Publish {
                name: (*name).to_owned(),
                target,
            }),
        }
    }

    for name in present.keys() {
        if RESERVED.contains(&name.as_str()) {
            continue;
        }
        if wanted.contains_key(name.as_str()) {
            continue;
        }
        // Not in the snapshot: either deactivated, or a ghost from a consumer.
        // Both cases must leave the name gone, and `unlink` handles a foreign
        // file too, so no Quarantine is needed for the removal direction.
        actions.push(Action::Remove { name: name.clone() });
    }

    actions
}

/// Classify one `/public` directory entry.
///
/// `read_link` is only called for entries the caller already identified as
/// symlinks; the mirror never stats a target, because a target may live on a
/// remote mount whose metadata call can block for the whole NFS timeout
/// (spec H4). Only `lstat` + `readlinkat` are safe there.
pub fn classify(lstat: LstatKind, target: Option<String>) -> PresentEntry {
    match (lstat, target) {
        (LstatKind::Symlink, Some(target)) => PresentEntry::Symlink { target },
        // A symlink whose body could not be read is treated as foreign: the
        // mirror cannot prove it owns it, so it must not silently delete it.
        _ => PresentEntry::Foreign,
    }
}

/// The subset of `lstat` output the mirror needs. Never follows symlinks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LstatKind {
    Symlink,
    Other,
}

/// Names in `present` that the mirror owns (symlinks) — used for logging only.
pub fn owned_names(present: &BTreeMap<String, PresentEntry>) -> BTreeSet<&str> {
    present
        .iter()
        .filter(|(_, entry)| matches!(entry, PresentEntry::Symlink { .. }))
        .map(|(name, _)| name.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::vfs::ownerfs::root::RootId;

    /// `id` is the already-encoded root id (`root-<hex(name)>`), matching what
    /// Meta returns; the helper must not prepend the prefix again.
    fn root(id: &str, epoch: u64, home: &str) -> OutletRoot {
        OutletRoot {
            root_id: RootId(id.to_owned()),
            root_epoch: epoch,
            home_node_id: home.to_owned(),
        }
    }

    fn present(entries: &[(&str, PresentEntry)]) -> BTreeMap<String, PresentEntry> {
        entries
            .iter()
            .map(|(name, entry)| ((*name).to_owned(), entry.clone()))
            .collect()
    }

    fn symlink(target: &str) -> PresentEntry {
        PresentEntry::Symlink {
            target: target.to_owned(),
        }
    }

    #[test]
    fn empty_expectation_and_empty_public_yields_no_actions() {
        let actions = plan("/imports", &[], &present(&[]));
        assert!(actions.is_empty());
    }

    #[test]
    fn new_root_is_published_with_generation_in_target() {
        let actions = plan("/imports", &[root("root-61", 7, "node-a")], &present(&[]));
        assert_eq!(
            actions,
            vec![Action::Publish {
                name: "root-61".into(),
                target: "/imports/node-a/users/root-61-e7".into(),
            }]
        );
    }

    #[test]
    fn matching_symlink_is_left_alone() {
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &present(&[("root-61", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert!(actions.is_empty(), "converged state must be a no-op");
    }

    /// 换代：同一名字的 target 文本变化，必须原子替换而不是摘除后重建。
    #[test]
    fn generation_change_republishes_the_same_name() {
        let actions = plan(
            "/imports",
            &[root("root-61", 8, "node-a")],
            &present(&[("root-61", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert_eq!(
            actions,
            vec![Action::Publish {
                name: "root-61".into(),
                target: "/imports/node-a/users/root-61-e8".into(),
            }],
            "a generation bump must be one publish, not remove+publish"
        );
    }

    #[test]
    fn removed_root_is_unlinked() {
        let actions = plan(
            "/imports",
            &[],
            &present(&[("root-61", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert_eq!(
            actions,
            vec![Action::Remove {
                name: "root-61".into()
            }]
        );
    }

    /// 消费方违约（真实目录占位）必须先隔离再发布，否则 symlinkat 永远 EEXIST。
    #[test]
    fn foreign_entry_is_quarantined_before_publish() {
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &present(&[("root-61", PresentEntry::Foreign)]),
        );
        assert_eq!(
            actions,
            vec![
                Action::Quarantine {
                    name: "root-61".into()
                },
                Action::Publish {
                    name: "root-61".into(),
                    target: "/imports/node-a/users/root-61-e7".into(),
                },
            ],
            "quarantine must precede publish, in that order"
        );
    }

    /// 保留目录既不发布也不删除，避免镜像清掉自己的 staging 与隔离区。
    #[test]
    fn reserved_entries_are_never_touched() {
        let actions = plan(
            "/imports",
            &[],
            &present(&[
                (".pub", PresentEntry::Foreign),
                (".quarantine", PresentEntry::Foreign),
            ]),
        );
        assert!(actions.is_empty());
    }

    /// plan 必须是幂等的：执行结果再算一次不应产生动作。
    #[test]
    fn applying_the_plan_leaves_nothing_to_do() {
        let active = vec![root("root-61", 7, "node-a"), root("root-62", 3, "node-b")];
        let start = present(&[
            ("root-61", symlink("/stale/target")),
            ("root-63", symlink("/imports/node-c/users/root-63-e1")),
            ("root-64", PresentEntry::Foreign),
        ]);
        let actions = plan("/imports", &active, &start);

        // 用动作构造"执行后"的现状态：Publish 覆盖，Quarantine 后紧跟 Publish，
        // Remove 清除。
        let mut after = start.clone();
        for action in &actions {
            match action {
                Action::Publish { name, target } => {
                    after.insert(name.clone(), symlink(target));
                }
                Action::Remove { name } => {
                    after.remove(name);
                }
                Action::Quarantine { name } => {
                    after.remove(name);
                }
            }
        }
        assert!(
            plan("/imports", &active, &after).is_empty(),
            "second pass must be a no-op, got {:?}",
            plan("/imports", &active, &after)
        );
    }

    /// 确定性顺序：日志与测试都依赖它稳定。
    #[test]
    fn actions_are_ordered_deterministically() {
        let active = vec![
            root("root-63", 1, "node-a"),
            root("root-61", 1, "node-a"),
            root("root-62", 1, "node-a"),
        ];
        let first = plan("/imports", &active, &present(&[]));
        let second = plan("/imports", &active, &present(&[]));
        assert_eq!(first, second);
        let names: Vec<_> = first
            .iter()
            .map(|action| match action {
                Action::Publish { name, .. } => name.as_str(),
                Action::Remove { name } => name.as_str(),
                Action::Quarantine { name } => name.as_str(),
            })
            .collect();
        assert_eq!(names, vec!["root-61", "root-62", "root-63"]);
    }

    #[test]
    fn unreadable_symlink_is_treated_as_foreign() {
        // 读不到 body 时不能假定"是自己建的"，否则会误删他人的同名条目。
        assert_eq!(classify(LstatKind::Symlink, None), PresentEntry::Foreign);
        assert_eq!(classify(LstatKind::Other, None), PresentEntry::Foreign);
        assert_eq!(
            classify(LstatKind::Symlink, Some("/x".into())),
            PresentEntry::Symlink {
                target: "/x".into()
            }
        );
    }

    /// 名字面 target 必须与 OwnerFs 的物理目录命名逐字节一致。
    ///
    /// 这里不复制 hex 算法，而是用 OwnerFs 的权威函数交叉验证——若命名规则
    /// 变更，本测试先失败，而不是让镜像在运行期指向不存在的路径。
    #[test]
    fn target_matches_ownerfs_data_dir_naming() {
        use crate::node::vfs::ownerfs::root::{root_data_dir, root_id_from_name};
        use std::ffi::OsStr;

        for (name, epoch) in [
            ("agent1", 7u64),
            ("workspace", 1),
            ("a", 12),
            ("名字-中文", 3),
            ("with space", 2),
        ] {
            let id = root_id_from_name(OsStr::new(name)).unwrap();
            let dir = root_data_dir(OsStr::new(name), epoch).unwrap();
            let node = OutletRoot {
                root_id: id,
                root_epoch: epoch,
                home_node_id: "node-a".into(),
            };
            assert_eq!(
                target_path("/imports", &node),
                format!("/imports/node-a/users/{}", dir.as_path().display()),
                "mirror target must equal the OwnerFs data directory for {name}"
            );
        }
    }

    #[test]
    fn target_path_normalizes_trailing_slash() {
        let node = root("root-61", 7, "node-a");
        assert_eq!(
            target_path("/imports/", &node),
            target_path("/imports", &node)
        );
    }
}
