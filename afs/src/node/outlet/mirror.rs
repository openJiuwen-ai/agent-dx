//! 收敛循环的纯函数核心。
//!
//! 这里不碰文件系统：输入是"Meta 期望的 root 集合"与"`/public` 当前内容"，
//! 输出是要执行的动作列表。IO 与循环在 `mirror` 的上层。把这个边界画清楚，
//! 是为了让"给定期望态与该有状态，应该做什么"能被表驱动测试穷举——包括那些
//! 在真机上极难复现的中间态（半发布、ghost 占位、换代瞬间）。

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
};

use crate::node::vfs::ownerfs::root::{OutletRoot, RESERVED_ROOT_NAMES, root_name_from_id};

/// One filesystem action the mirror must perform.
///
/// Names are [`OsString`], not `String`: a workspace name is an arbitrary byte
/// string (Linux path components are bytes, not UTF-8), and OwnerFs encodes it
/// losslessly as `root-<hex(bytes)>`. Converting the decoded name back to
/// `String` would replace every invalid byte with U+FFFD, which both renames the
/// entry the caller created and — since distinct byte sequences can collapse to
/// the same replacement string — silently merges two workspaces into one name.
/// The whole path from `root_id` to `symlinkat` therefore stays in bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    /// Atomically publish `name -> target`. `target` is the symlink body.
    Publish { name: OsString, target: String },
    /// Remove `name` from the public directory. Idempotent.
    Remove { name: OsString },
    /// Move a non-symlink entry out of the way so the name can be published.
    ///
    /// Only produced for entries the mirror did not create; a consumer that
    /// mkdir'd over a name would otherwise make publishing fail with EEXIST
    /// forever, because `symlinkat` cannot replace a directory.
    Quarantine { name: OsString },
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
///
/// Defined by OwnerFs because the same set is rejected at workspace creation:
/// a name the mirror would refuse to publish must never become a root in the
/// first place. Keeping one definition is what stops the two from drifting.
const RESERVED: [&str; 2] = RESERVED_ROOT_NAMES;

/// 保留名判定按字节比较，与 `validate_root_name` 用的是同一组字面量。
fn is_reserved(name: &OsStr) -> bool {
    RESERVED.iter().any(|reserved| OsStr::new(reserved) == name)
}

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
///
/// `ready_homes` gates **publishing new names** only. A root whose home is not
/// in the set keeps whatever entry it already has: newly created names are not
/// published at all, while an entry published before the home broke stays put.
///
/// The distinction matters because the two directions have opposite failure
/// modes. Publishing a name whose import is not mounted yields a dangling
/// symlink that `ls` cannot distinguish from a working one — silent breakage.
/// Withdrawing an already published name would instead make downstream
/// containers fail to bind during a transient outage (spec H7). So: never
/// publish into a hole, never retract a name that worked.
pub fn plan(
    import_root: &str,
    active: &[OutletRoot],
    ready_homes: &BTreeSet<String>,
    present: &BTreeMap<OsString, PresentEntry>,
) -> Vec<Action> {
    let mut actions = Vec::new();

    // Expectation keyed by the workspace name. Meta returns at most one Active
    // record per root_id and the id is an injective encoding of the name, so
    // two roots can never claim the same entry here — provided the key keeps
    // the name's bytes. `OsString`'s ordering is byte order on Unix, so this is
    // also the deterministic ordering the logs and tests rely on.
    //
    // Publishing under the workspace name is the point of the layer: a caller
    // binds the name they created, and `root-<hex(name)>` is not something a
    // caller ever wrote. An id that fails to decode cannot be published under
    // any name a caller could use, so it is skipped rather than guessed at.
    let mut wanted: BTreeMap<OsString, &OutletRoot> = BTreeMap::new();
    let mut undecodable: Vec<&str> = Vec::new();
    for root in active {
        match root_name_from_id(&root.root_id) {
            Ok(name) => {
                wanted.insert(name, root);
            }
            Err(_) => undecodable.push(root.root_id.0.as_str()),
        }
    }
    for id in &undecodable {
        afs_logging::warn!(
            "outlet.mirror.undecodable_root_id";
            "root_id" => *id
        );
    }

    for (name, root) in &wanted {
        // 保留名对镜像来说是透明的：不发布、不隔离、不删除。
        //
        // 创建路径已经拒了这些名字，但存量 root（旧版本建的）和别的节点建的
        // 都可能还在快照里。若在这里放行，`Quarantine` 会把它挪进隔离区、发布
        // 又把同名目录建回来（暂存目录就在 `<name>/` 里），每轮循环一次。
        if is_reserved(name) {
            continue;
        }
        // Every name in `wanted` counts as "should exist" for the removal pass
        // below, so an unready home never loses an entry it already has.
        let publishable = ready_homes.contains(&root.home_node_id);
        let target = target_path(import_root, root);
        match present.get(name) {
            Some(PresentEntry::Symlink { target: current }) if current == &target => {}
            // A wrong target is republished in place: stale after a generation
            // change, or left over from a previous import layout.
            Some(PresentEntry::Symlink { .. }) => {
                if publishable {
                    actions.push(Action::Publish {
                        name: name.clone(),
                        target,
                    });
                }
            }
            // A foreign entry occupies the name. Leave it alone while the home
            // is unready: quarantining it would destroy a caller's directory
            // for a name we cannot publish anyway.
            Some(PresentEntry::Foreign) => {
                if publishable {
                    // Move it aside first; publishing over a real directory fails.
                    actions.push(Action::Quarantine { name: name.clone() });
                    actions.push(Action::Publish {
                        name: name.clone(),
                        target,
                    });
                }
            }
            None => {
                if publishable {
                    actions.push(Action::Publish {
                        name: name.clone(),
                        target,
                    });
                }
            }
        }
    }

    let published: BTreeSet<&OsStr> = wanted.keys().map(OsString::as_os_str).collect();
    for name in present.keys() {
        if is_reserved(name) {
            continue;
        }
        if published.contains(name.as_os_str()) {
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
            home_nfs_addr: String::new(),
        }
    }

    fn present(entries: &[(&str, PresentEntry)]) -> BTreeMap<OsString, PresentEntry> {
        entries
            .iter()
            .map(|(name, entry)| (OsString::from(name), entry.clone()))
            .collect()
    }

    /// 所有 home 都就绪。现有用例默认用这个，保持它们原本的语义
    /// （发布门禁是后加的，不该改变既有行为断言）。
    fn all_ready(homes: &[&str]) -> BTreeSet<String> {
        homes.iter().map(|h| (*h).to_owned()).collect()
    }

    fn symlink(target: &str) -> PresentEntry {
        PresentEntry::Symlink {
            target: target.to_owned(),
        }
    }

    #[test]
    fn unready_home_publishes_nothing() {
        // home 从未挂上（未 Ready）→ 不发布新名字。
        // 发布出去会是断链，而 `ls` 无法把它和正常链接区分开，属于静默失败。
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&[]),
            &present(&[]),
        );
        assert!(
            actions.is_empty(),
            "an unready home must not publish: {actions:?}"
        );
    }

    #[test]
    fn unready_home_keeps_an_already_published_name() {
        // 已发布的名字在 home 掉线后必须保留（spec H7）：撤掉会让下游容器
        // 在对端抖动期间集体 bind 失败，而保留则在对端恢复后自动可用。
        let target = "/imports/node-a/users/root-61-e7";
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&[]),
            &present(&[("a", symlink(target))]),
        );
        assert!(
            actions.is_empty(),
            "an already published name must survive: {actions:?}"
        );
    }

    #[test]
    fn unready_home_is_not_removed_as_a_ghost() {
        // 未就绪 home 的名字仍算"应当存在"，不能在收尾的 Remove 阶段被清掉。
        // 这条防的是把 ready_homes 误用成 wanted 的过滤条件。
        let target = "/imports/node-a/users/root-61-e7";
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&[]),
            &present(&[("a", symlink(target)), ("ghost", symlink("/elsewhere"))]),
        );
        assert_eq!(
            actions,
            vec![Action::Remove {
                name: "ghost".into()
            }],
            "only the true ghost is removed"
        );
    }

    #[test]
    fn unready_home_does_not_quarantine_a_foreign_entry() {
        // 未就绪时不碰占据名字的非符号链接：既发不出去，又要毁掉调用方的目录，
        // 没有收益。
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&[]),
            &present(&[("a", PresentEntry::Foreign)]),
        );
        assert!(actions.is_empty(), "must not quarantine: {actions:?}");
    }

    #[test]
    fn ready_home_publishes_after_recovery() {
        // 同一份输入，home 恢复 Ready 后必须发布：这是"就绪之后挂载链接"的
        // 正向验证。
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[]),
        );
        assert_eq!(
            actions,
            vec![Action::Publish {
                name: "a".into(),
                target: "/imports/node-a/users/root-61-e7".into(),
            }]
        );
    }

    #[test]
    fn ready_home_repairs_a_stale_target_but_unready_does_not() {
        let stale = "/imports/node-a/users/root-61-e6";
        let roots = [root("root-61", 7, "node-a")];
        let present = present(&[("a", symlink(stale))]);

        assert!(
            plan("/imports", &roots, &all_ready(&[]), &present).is_empty(),
            "unready home must not touch a stale link either"
        );
        assert_eq!(
            plan("/imports", &roots, &all_ready(&["node-a"]), &present),
            vec![Action::Publish {
                name: "a".into(),
                target: "/imports/node-a/users/root-61-e7".into(),
            }],
            "ready home republishes with the current generation"
        );
    }

    #[test]
    fn empty_expectation_and_empty_public_yields_no_actions() {
        let actions = plan("/imports", &[], &all_ready(&[]), &present(&[]));
        assert!(actions.is_empty());
    }

    #[test]
    fn new_root_is_published_with_generation_in_target() {
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[]),
        );
        assert_eq!(
            actions,
            vec![Action::Publish {
                name: "a".into(),
                target: "/imports/node-a/users/root-61-e7".into(),
            }]
        );
    }

    #[test]
    fn matching_symlink_is_left_alone() {
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[("a", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert!(actions.is_empty(), "converged state must be a no-op");
    }

    /// 换代：同一名字的 target 文本变化，必须原子替换而不是摘除后重建。
    #[test]
    fn generation_change_republishes_the_same_name() {
        let actions = plan(
            "/imports",
            &[root("root-61", 8, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[("a", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert_eq!(
            actions,
            vec![Action::Publish {
                name: "a".into(),
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
            &all_ready(&[]),
            &present(&[("a", symlink("/imports/node-a/users/root-61-e7"))]),
        );
        assert_eq!(actions, vec![Action::Remove { name: "a".into() }]);
    }

    /// 消费方违约（真实目录占位）必须先隔离再发布，否则 symlinkat 永远 EEXIST。
    #[test]
    fn foreign_entry_is_quarantined_before_publish() {
        let actions = plan(
            "/imports",
            &[root("root-61", 7, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[("a", PresentEntry::Foreign)]),
        );
        assert_eq!(
            actions,
            vec![
                Action::Quarantine { name: "a".into() },
                Action::Publish {
                    name: "a".into(),
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
            &all_ready(&[]),
            &present(&[
                (".pub", PresentEntry::Foreign),
                (".quarantine", PresentEntry::Foreign),
            ]),
        );
        assert!(actions.is_empty());
    }

    /// Meta 里存在与内部目录同名的 root 时，绝不发布它。
    ///
    /// 旧版本允许创建 `.pub`，这类 root 会一直留在 Meta 里。镜像若继续尝试发布，
    /// 每轮都会"隔离 → 发布时把暂存目录建回来 → 再隔离"，在 tmpfs 上无限堆积
    /// 隔离目录。保留名对镜像来说是透明的：不发布、不删除、不隔离。
    #[test]
    fn a_root_named_like_an_internal_directory_is_never_published() {
        // hex(".pub") = 2e707562，即 OwnerFs 给这个名字生成的 root id。
        let actions = plan(
            "/imports",
            &[root("root-2e707562", 3, "node-a")],
            &all_ready(&["node-a"]),
            &present(&[(".pub", PresentEntry::Foreign)]),
        );
        assert!(actions.is_empty(), "保留名不得产生任何动作: {actions:?}");
    }

    /// plan 必须是幂等的：执行结果再算一次不应产生动作。
    #[test]
    fn applying_the_plan_leaves_nothing_to_do() {
        let active = vec![root("root-61", 7, "node-a"), root("root-62", 3, "node-b")];
        let start = present(&[
            ("a", symlink("/stale/target")),
            ("c", symlink("/imports/node-c/users/root-63-e1")),
            ("d", PresentEntry::Foreign),
        ]);
        let actions = plan("/imports", &active, &all_ready(&["node-a"]), &start);

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
            plan("/imports", &active, &all_ready(&["node-a"]), &after).is_empty(),
            "second pass must be a no-op, got {:?}",
            plan("/imports", &active, &all_ready(&["node-a"]), &after)
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
        let first = plan("/imports", &active, &all_ready(&["node-a"]), &present(&[]));
        let second = plan("/imports", &active, &all_ready(&["node-a"]), &present(&[]));
        assert_eq!(first, second);
        let names: Vec<_> = first
            .iter()
            .map(|action| match action {
                Action::Publish { name, .. } => name.to_str().unwrap(),
                Action::Remove { name } => name.to_str().unwrap(),
                Action::Quarantine { name } => name.to_str().unwrap(),
            })
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
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
                home_nfs_addr: String::new(),
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

    /// 非 UTF-8 名字必须逐字节发布，两个仅差非法字节的名字不得合并。
    ///
    /// 旧实现用 `to_string_lossy()` 把解码后的名字转成 `String` 做键：`\xe9`
    /// 与 `\xff` 都会变成 U+FFFD，于是两个 workspace 塌成一条（`BTreeMap`
    /// 的 insert 覆盖其一），发布出去的也不是调用方创建的名字。真机上已实测
    /// 该现象（两个 workspace、`/public` 只有一个 `utf8cafe\u{FFFD}`）。
    #[test]
    fn non_utf8_names_keep_their_bytes_and_do_not_collide() {
        use crate::node::vfs::ownerfs::root::root_id_from_name;
        use std::os::unix::ffi::OsStrExt;

        let first = b"utf8cafe\xe9".to_vec();
        let second = b"utf8cafe\xff".to_vec();
        let roots: Vec<OutletRoot> = [first.clone(), second.clone()]
            .iter()
            .map(|name| OutletRoot {
                root_id: root_id_from_name(OsStr::from_bytes(name))
                    .expect("arbitrary bytes are a legal workspace name"),
                root_epoch: 1,
                home_node_id: "node-a".to_owned(),
                home_nfs_addr: String::new(),
            })
            .collect();

        let actions = plan("/imports", &roots, &all_ready(&["node-a"]), &present(&[]));
        assert_eq!(actions.len(), 2, "两个名字必须是两条动作: {actions:?}");

        let names: Vec<Vec<u8>> = actions
            .iter()
            .map(|action| match action {
                Action::Publish { name, .. } => name.as_bytes().to_vec(),
                other => panic!("expected a publish, got {other:?}"),
            })
            .collect();
        // OsString 在 Unix 上按字节排序，`\xe9` < `\xff`，顺序确定。
        assert_eq!(
            names,
            vec![first, second],
            "published names must equal the bytes inside the root id"
        );
    }

    /// 收敛判据也得按字节比：`/public` 里存在同字节的链接时不得重发。
    #[test]
    fn a_matching_non_utf8_symlink_is_left_alone() {
        use crate::node::vfs::ownerfs::root::root_id_from_name;
        use std::os::unix::ffi::OsStrExt;

        let raw = b"utf8cafe\xe9".to_vec();
        let roots = [OutletRoot {
            root_id: root_id_from_name(OsStr::from_bytes(&raw)).unwrap(),
            root_epoch: 4,
            home_node_id: "node-a".to_owned(),
            home_nfs_addr: String::new(),
        }];
        let mut disk = BTreeMap::new();
        disk.insert(
            OsString::from(OsStr::from_bytes(&raw)),
            symlink("/imports/node-a/users/root-7574663863616665e9-e4"),
        );

        let actions = plan("/imports", &roots, &all_ready(&["node-a"]), &disk);
        assert!(actions.is_empty(), "同字节链接必须视为已收敛: {actions:?}");
    }
}
