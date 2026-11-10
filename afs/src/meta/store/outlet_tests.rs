//! outlet 快照与摘要的存储层合同测试。
//!
//! 从 store.rs 拆分而来：拆分后 `super::*` 取不到 store.rs 的私有导入，
//! 因此改为从 `crate::meta::store` 显式导入。

/// outlet 镜像面：全局 Active root 快照。
///
/// 合同：快照只包含 Active（= Root 实体存在）的 root；DeactivateRoot 之后
/// 必须消失；返回顺序稳定（按 root_id 升序）以便镜像 diff 可复现。
mod outlet_active_roots_tests {
    use crate::meta::owner_roots::*;
    use crate::meta::store::journal::MemoryLog;
    use crate::meta::store::*;
    use std::sync::Arc;
    use std::time::Duration;

    async fn fixture() -> (Arc<Store>, StoreOwnerRootAuthority) {
        let log = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(log).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("home", "register"),
                crate::meta::store::NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let authority = StoreOwnerRootAuthority::new(store.clone());
        (store, authority)
    }

    async fn reserve_and_activate(
        authority: &StoreOwnerRootAuthority,
        root_id: &str,
        intent: &str,
    ) -> RootAccessGrant {
        let reservation = authority
            .reserve_root(ReserveRootInput {
                request_id: format!("reserve-{intent}"),
                root_id: root_id.into(),
                preferred_home_node_id: "home".into(),
                session_id: "s".into(),
                rights: vec![RootRight::Read],
                expected_root_epoch: 0,
                create_intent_id: intent.into(),
            })
            .await
            .unwrap()
            .reservation;
        authority
            .activate_root(ActivateRootInput {
                request_id: format!("activate-{intent}"),
                local_prepare_id: "prepared".into(),
                reservation: WireRootReservation {
                    root_id: reservation.root_id.clone(),
                    root_epoch: reservation.root_epoch,
                    home_node_id: reservation.home_node_id.clone(),
                    session_id: reservation.home_session_id.clone(),
                    create_intent_id: reservation.create_intent_id.clone(),
                    prepare_token: reservation.prepare_token.clone(),
                    requester_node_id: reservation.home_node_id.clone(),
                    requester_session_id: reservation.home_session_id.clone(),
                },
            })
            .await
            .unwrap()
    }

    async fn active_ids(authority: &StoreOwnerRootAuthority) -> Vec<String> {
        authority
            .list_active_outlet_roots(ListActiveOutletRootsInput { known_digest: None })
            .await
            .unwrap()
            .roots
            .into_iter()
            .map(|root| root.root_id)
            .collect()
    }

    #[tokio::test]
    async fn empty_store_yields_empty_snapshot() {
        let (_store, authority) = fixture().await;
        assert!(active_ids(&authority).await.is_empty());
    }

    #[tokio::test]
    async fn activated_root_appears_with_home_and_epoch() {
        let (_store, authority) = fixture().await;
        let grant = reserve_and_activate(&authority, "root-6167", "agent1").await;
        let output = authority
            .list_active_outlet_roots(ListActiveOutletRootsInput { known_digest: None })
            .await
            .unwrap();
        assert_eq!(output.roots.len(), 1);
        let root = &output.roots[0];
        assert_eq!(root.root_id, "root-6167");
        assert_eq!(root.home_node_id, "home");
        assert_eq!(root.root_epoch, grant.root_epoch);
    }

    #[tokio::test]
    async fn snapshot_excludes_reservation_before_activation() {
        let (_store, authority) = fixture().await;
        authority
            .reserve_root(ReserveRootInput {
                request_id: "reserve-only".into(),
                root_id: "root-70656e64696e67".into(),
                preferred_home_node_id: "home".into(),
                session_id: "s".into(),
                rights: vec![RootRight::Read],
                expected_root_epoch: 0,
                create_intent_id: "pending".into(),
            })
            .await
            .unwrap();
        assert!(
            active_ids(&authority).await.is_empty(),
            "reservation must not leak into the outlet view"
        );
    }

    #[tokio::test]
    async fn deactivated_root_disappears_from_snapshot() {
        let (_store, authority) = fixture().await;
        let grant = reserve_and_activate(&authority, "root-6167", "agent1").await;
        assert_eq!(active_ids(&authority).await, vec!["root-6167".to_string()]);
        authority
            .deactivate_root(DeactivateRootInput {
                request_id: "deactivate".into(),
                root_id: "root-6167".into(),
                root_epoch: grant.root_epoch,
                home_node_id: "home".into(),
                home_session_id: "s".into(),
                reason: "finished".into(),
                authenticated_home_node_id: Some("home".into()),
            })
            .await
            .unwrap();
        assert!(
            active_ids(&authority).await.is_empty(),
            "deactivated root must vanish: outlet mirror relies on absence"
        );
    }

    #[tokio::test]
    async fn snapshot_is_ordered_by_root_id_for_stable_diff() {
        let (_store, authority) = fixture().await;
        for (root_id, intent) in [("root-63", "c"), ("root-61", "a"), ("root-62", "b")] {
            reserve_and_activate(&authority, root_id, intent).await;
        }
        assert_eq!(
            active_ids(&authority).await,
            vec![
                "root-61".to_string(),
                "root-62".to_string(),
                "root-63".to_string()
            ],
            "stable ordering keeps mirror diff reproducible across runs"
        );
    }

    #[tokio::test]
    async fn snapshot_reports_authority_revision() {
        let (store, authority) = fixture().await;
        reserve_and_activate(&authority, "root-6167", "agent1").await;
        let output = authority
            .list_active_outlet_roots(ListActiveOutletRootsInput { known_digest: None })
            .await
            .unwrap();
        let direct = store.read(MetaRead::ActiveRoots).await.unwrap();
        assert_eq!(output.revision, direct.revision);
        assert_eq!(output.roots.len(), direct.entities.len());
    }

    #[tokio::test]
    async fn active_roots_read_is_independent_per_store() {
        let (store, authority) = fixture().await;
        reserve_and_activate(&authority, "root-6167", "agent1").await;
        let before = active_ids(&authority).await;
        // 独立空 store 的读取为空，且不影响原 store 视图：扫描只读本地状态。
        let empty = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        let empty_authority = StoreOwnerRootAuthority::new(empty);
        assert!(active_ids(&empty_authority).await.is_empty());
        assert_eq!(active_ids(&authority).await, before);
        assert_eq!(
            store
                .read(MetaRead::ActiveRoots)
                .await
                .unwrap()
                .entities
                .len(),
            1
        );
    }
}

/// outlet 摘要的语义：TC15（DFS 事件不改变摘要）与 TC17（unchanged 判定）。
mod outlet_digest_tests {
    use crate::meta::owner_roots::*;
    use crate::meta::store::journal::MemoryLog;
    use crate::meta::store::*;
    use std::sync::Arc;
    use std::time::Duration;

    async fn fixture() -> (Arc<Store>, StoreOwnerRootAuthority) {
        let log = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(log).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("home", "register"),
                crate::meta::store::NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let authority = StoreOwnerRootAuthority::new(store.clone());
        (store, authority)
    }

    async fn snapshot(
        authority: &StoreOwnerRootAuthority,
        known: Option<&str>,
    ) -> ListActiveOutletRootsOutput {
        authority
            .list_active_outlet_roots(ListActiveOutletRootsInput {
                known_digest: known.map(str::to_string),
            })
            .await
            .unwrap()
    }

    async fn activate(authority: &StoreOwnerRootAuthority, root_id: &str, intent: &str) -> u64 {
        let reservation = authority
            .reserve_root(ReserveRootInput {
                request_id: format!("reserve-{intent}"),
                root_id: root_id.into(),
                preferred_home_node_id: "home".into(),
                session_id: "s".into(),
                rights: vec![RootRight::Read],
                expected_root_epoch: 0,
                create_intent_id: intent.into(),
            })
            .await
            .unwrap()
            .reservation;
        authority
            .activate_root(ActivateRootInput {
                request_id: format!("activate-{intent}"),
                local_prepare_id: "prepared".into(),
                reservation: WireRootReservation {
                    root_id: reservation.root_id.clone(),
                    root_epoch: reservation.root_epoch,
                    home_node_id: reservation.home_node_id.clone(),
                    session_id: reservation.home_session_id.clone(),
                    create_intent_id: reservation.create_intent_id.clone(),
                    prepare_token: reservation.prepare_token.clone(),
                    requester_node_id: reservation.home_node_id.clone(),
                    requester_session_id: reservation.home_session_id.clone(),
                },
            })
            .await
            .unwrap()
            .root_epoch
    }

    #[tokio::test]
    async fn digest_is_stable_across_repeated_reads() {
        let (_store, authority) = fixture().await;
        activate(&authority, "root-6167", "agent1").await;
        let first = snapshot(&authority, None).await;
        let second = snapshot(&authority, None).await;
        assert_eq!(first.digest, second.digest);
        assert!(!first.digest.is_empty());
    }

    #[tokio::test]
    async fn digest_changes_when_a_root_is_activated() {
        let (_store, authority) = fixture().await;
        let empty = snapshot(&authority, None).await;
        activate(&authority, "root-6167", "agent1").await;
        let populated = snapshot(&authority, None).await;
        assert_ne!(empty.digest, populated.digest);
    }

    #[tokio::test]
    async fn digest_changes_when_a_root_is_deactivated() {
        let (_store, authority) = fixture().await;
        let epoch = activate(&authority, "root-6167", "agent1").await;
        let before = snapshot(&authority, None).await;
        authority
            .deactivate_root(DeactivateRootInput {
                request_id: "deactivate".into(),
                root_id: "root-6167".into(),
                root_epoch: epoch,
                home_node_id: "home".into(),
                home_session_id: "s".into(),
                reason: "finished".into(),
                authenticated_home_node_id: Some("home".into()),
            })
            .await
            .unwrap();
        let after = snapshot(&authority, None).await;
        assert_ne!(
            before.digest, after.digest,
            "removing a root must invalidate the mirror snapshot"
        );
    }

    /// TC15：DFS 写入推进全局 revision，但绝不能改变 outlet 摘要。
    #[tokio::test]
    async fn dfs_writes_do_not_change_digest() {
        let (store, authority) = fixture().await;
        activate(&authority, "root-6167", "agent1").await;
        let before = snapshot(&authority, None).await;

        // 写一个非 root 实体（新增节点会话）：全局 revision 前进，
        // root 集合不变。这正是 DFS 流量在真实集群里的效果。
        store
            .register_node_session(
                RequestKey::new("other", "register-2"),
                crate::meta::store::NodeSessionLease {
                    node_id: "other".into(),
                    session_id: "s2".into(),
                    grpc_addr: "g2".into(),
                    data_addr: "d2".into(),
                    rest_addr: "r2".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();

        let after = snapshot(&authority, None).await;
        assert!(
            after.revision > before.revision,
            "the write must advance the global revision, otherwise this test proves nothing"
        );
        assert_eq!(
            before.digest, after.digest,
            "non-root writes must not invalidate the outlet snapshot"
        );
    }

    /// TC17：known_digest 命中时返回 unchanged 且不带 payload。
    #[tokio::test]
    async fn matching_digest_returns_unchanged_without_roots() {
        let (_store, authority) = fixture().await;
        activate(&authority, "root-6167", "agent1").await;
        let full = snapshot(&authority, None).await;
        assert!(!full.unchanged);
        assert_eq!(full.roots.len(), 1);

        let cached = snapshot(&authority, Some(&full.digest)).await;
        assert!(cached.unchanged);
        assert!(
            cached.roots.is_empty(),
            "unchanged replies must not carry a payload"
        );
        assert_eq!(cached.digest, full.digest);
    }

    /// TC17：known_digest 失配时必须回全量，调用方据此重建。
    #[tokio::test]
    async fn stale_digest_forces_a_full_snapshot() {
        let (_store, authority) = fixture().await;
        activate(&authority, "root-6167", "agent1").await;
        let full = snapshot(&authority, None).await;
        activate(&authority, "root-6262", "agent2").await;

        let refreshed = snapshot(&authority, Some(&full.digest)).await;
        assert!(!refreshed.unchanged);
        assert_eq!(refreshed.roots.len(), 2);
        assert_ne!(refreshed.digest, full.digest);
    }

    /// 重启后摘要必须能复现：否则客户端缓存会与新的 Meta 实例错配。
    #[tokio::test]
    async fn digest_is_reproducible_after_reopen() {
        let log = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("home", "register"),
                crate::meta::store::NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let authority = StoreOwnerRootAuthority::new(store.clone());
        activate(&authority, "root-6167", "agent1").await;
        let before = snapshot(&authority, None).await;

        let reopened = Arc::new(Store::open_log(log).await.unwrap());
        let reopened_authority = StoreOwnerRootAuthority::new(reopened);
        let after = snapshot(&reopened_authority, None).await;
        assert_eq!(
            before.digest, after.digest,
            "digest must be a pure function of the recovered root set"
        );
        assert_eq!(before.roots.len(), after.roots.len());
    }
}
