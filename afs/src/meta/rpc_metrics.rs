//! Meta 服务端按 RPC 方法计数，作为结构成本口径：一次文件操作花了几次 Meta 往返。
//!
//! 标签只取自下面的静态方法表，未知路径一律记为 `other`，客户端无法制造新标签。
//! 单测核对该表与 `meta.proto` 一致，新增 RPC 时漏登记会直接失败。

use std::task::{Context, Poll};

use tonic::codegen::http::Request;
use tower::{Layer, Service};

const META_RPCS: &[&str] = &[
    "afs.meta.v1.Meta/Ping",
    "afs.meta.v1.Meta/RegisterNode",
    "afs.meta.v1.Meta/LookupNode",
    "afs.meta.v1.OwnerRoots/ReserveRoot",
    "afs.meta.v1.OwnerRoots/ActivateRoot",
    "afs.meta.v1.OwnerRoots/AbortRoot",
    "afs.meta.v1.OwnerRoots/DeactivateRoot",
    "afs.meta.v1.OwnerRoots/LookupRoot",
    "afs.meta.v1.OwnerRoots/ListOwnerRoots",
    "afs.meta.v1.OwnerRoots/AcquireRoot",
    "afs.meta.v1.OwnerRoots/ValidateRootAccess",
    "afs.meta.v1.OwnerRoots/WatchRootCommands",
    "afs.meta.v1.OwnerRoots/PollRootCommandBatch",
    "afs.meta.v1.OwnerRoots/AckRevocation",
    "afs.meta.v1.OwnerRoots/RecoverRoot",
    "afs.meta.v1.DfsMeta/UpdateGcReference",
    "afs.meta.v1.DfsMeta/ReleaseGcReferences",
    "afs.meta.v1.DfsMeta/CollectGarbage",
    "afs.meta.v1.DfsMeta/Lookup",
    "afs.meta.v1.DfsMeta/Create",
    "afs.meta.v1.DfsMeta/Mkdir",
    "afs.meta.v1.DfsMeta/ReadDir",
    "afs.meta.v1.DfsMeta/Unlink",
    "afs.meta.v1.DfsMeta/Rmdir",
    "afs.meta.v1.DfsMeta/Rename",
    "afs.meta.v1.DfsMeta/Link",
    "afs.meta.v1.DfsMeta/Symlink",
    "afs.meta.v1.DfsMeta/Mknod",
    "afs.meta.v1.DfsMeta/ReadLink",
    "afs.meta.v1.DfsMeta/SetInodeAttributes",
    "afs.meta.v1.DfsMeta/GetXattr",
    "afs.meta.v1.DfsMeta/ListXattr",
    "afs.meta.v1.DfsMeta/SetXattr",
    "afs.meta.v1.DfsMeta/RemoveXattr",
    "afs.meta.v1.DfsMeta/OpenWrite",
    "afs.meta.v1.DfsMeta/ResolveLockAuthority",
    "afs.meta.v1.DfsMeta/ResolveWriteAuthority",
    "afs.meta.v1.DfsMeta/RenewWriteLease",
    "afs.meta.v1.DfsMeta/GetInode",
    "afs.meta.v1.DfsMeta/GetFileVersion",
    "afs.meta.v1.DfsMeta/GetPlacementSnapshot",
    "afs.meta.v1.DfsMeta/ValidateReplicaWrite",
    "afs.meta.v1.DfsMeta/ClaimReplicationTask",
    "afs.meta.v1.DfsMeta/ReportReplicationTask",
    "afs.meta.v1.DfsMeta/ReportChunkCorruption",
    "afs.meta.v1.DfsMeta/GetChunkSources",
    "afs.meta.v1.DfsMeta/ValidateReadGrants",
    "afs.meta.v1.DfsMeta/ValidateExternalReadGrants",
    "afs.meta.v1.DfsMeta/AuthorizeCurrentExternalRead",
    "afs.meta.v1.DfsMeta/AuthorizeExternalVersionRead",
    "afs.meta.v1.DfsMeta/SyncInodeMetadata",
    "afs.meta.v1.DfsMeta/CommitFileVersion",
];

fn method_label(path: &str) -> &'static str {
    let method = path.strip_prefix('/').unwrap_or(path);
    META_RPCS
        .iter()
        .copied()
        .find(|known| *known == method)
        .unwrap_or("other")
}

/// 每个请求在进入服务实现之前计数一次（含之后失败的请求）。
#[derive(Clone)]
pub(crate) struct MetaRpcCountLayer {
    calls: afs_metrics::IntCounterVec,
}

impl MetaRpcCountLayer {
    pub(crate) fn register(
        registry: &afs_metrics::Registry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let calls = afs_metrics::IntCounterVec::new(
            afs_metrics::Opts::new(
                "afs_meta_rpc_requests_total",
                "Meta gRPC requests received, by method; unknown paths count as \"other\".",
            ),
            &["method"],
        )?;
        afs_metrics::register_collector(registry, &calls)?;
        Ok(Self { calls })
    }
}

impl<S> Layer<S> for MetaRpcCountLayer {
    type Service = MetaRpcCountService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetaRpcCountService {
            inner,
            calls: self.calls.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MetaRpcCountService<S> {
    inner: S,
    calls: afs_metrics::IntCounterVec,
}

impl<S, B> Service<Request<B>> for MetaRpcCountService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.calls
            .with_label_values(&[method_label(request.uri().path())])
            .inc();
        self.inner.call(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_table_matches_meta_proto() {
        let proto = include_str!("../../crates/protocol/proto/meta.proto");
        let mut service = "";
        let mut declared = Vec::new();
        for line in proto.lines() {
            if let Some(rest) = line.strip_prefix("service ") {
                service = rest.split_whitespace().next().unwrap();
            } else if let Some(rest) = line.trim_start().strip_prefix("rpc ") {
                let name = rest.split('(').next().unwrap().trim();
                declared.push(format!("afs.meta.v1.{service}/{name}"));
            }
        }
        let table: Vec<String> = META_RPCS.iter().map(|m| (*m).to_owned()).collect();
        assert_eq!(declared, table);
    }

    #[test]
    fn unknown_paths_share_one_label() {
        assert_eq!(
            method_label("/afs.meta.v1.DfsMeta/GetInode"),
            "afs.meta.v1.DfsMeta/GetInode"
        );
        assert_eq!(method_label("/afs.meta.v1.DfsMeta/NoSuchMethod"), "other");
        assert_eq!(method_label("/random/path"), "other");
    }

    #[test]
    fn layer_counts_each_request_before_the_service() {
        let registry = afs_metrics::Registry::new();
        let layer = MetaRpcCountLayer::register(&registry).unwrap();
        let mut service = layer.layer(tower::service_fn(|_request: Request<()>| async {
            Ok::<_, std::convert::Infallible>(())
        }));
        for path in [
            "/afs.meta.v1.DfsMeta/GetInode",
            "/afs.meta.v1.DfsMeta/GetInode",
            "/bogus",
        ] {
            let request = Request::builder().uri(path).body(()).unwrap();
            drop(service.call(request));
        }
        let calls = layer.calls.clone();
        assert_eq!(
            calls
                .with_label_values(&["afs.meta.v1.DfsMeta/GetInode"])
                .get(),
            2
        );
        assert_eq!(calls.with_label_values(&["other"]).get(), 1);
    }
}
