//! UDS `LocalData` 诊断服务：同机 SDK 经 memfd/FD 传递读写诊断对象存储。
//!
//! 写入：SDK 给 source grant -> node 通过 fd broker 取 memfd fd -> `read_fd_at`
//! 从 fd copy bytes -> 写入诊断对象存储。读取：node 从诊断对象存储读 bytes ->
//! 通过 target grant 取 fd -> `write_fd_at` copy 到 SDK memfd。服务装配与 socket
//! 生命周期仍在 `node::api::local`。

use std::sync::Arc;

use afs_protocol::local_api::{
    LocalReadReply, LocalReadRequest, LocalWriteReply, LocalWriteRequest,
    local_data_server::LocalData,
};
use afs_transport::{
    grpc::error_status::{coded_status, error_to_status},
    shm::{read_fd_at, write_fd_at},
};
use tonic::{Request, Response, Status};

use super::probe_store::{DiagnosticStorage, DiagnosticStorageError};
use crate::node::api::local::{LocalApiMetrics, request_grant_fd, shm_status};

#[derive(Clone)]
pub(in crate::node) struct DiagnosticLocalDataService {
    storage: Arc<DiagnosticStorage>,
    metrics: Option<LocalApiMetrics>,
}

impl DiagnosticLocalDataService {
    pub(in crate::node) fn new(
        storage: Arc<DiagnosticStorage>,
        metrics: Option<LocalApiMetrics>,
    ) -> Self {
        Self { storage, metrics }
    }

    fn record(&self, operation: &'static str, ok: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.record(operation, ok);
        }
    }
}

#[tonic::async_trait]
impl LocalData for DiagnosticLocalDataService {
    async fn write(
        &self,
        request: Request<LocalWriteRequest>,
    ) -> Result<Response<LocalWriteReply>, Status> {
        let outcome = self.write_inner(request.into_inner()).await;
        self.record("diagnostic_write", outcome.is_ok());
        match &outcome {
            Ok(reply) => {
                afs_logging::info!("local_api.diagnostic_write";"result"=>"ok","written"=>reply.written);
            }
            Err(error) => {
                afs_logging::warn!("local_api.diagnostic_write";"result"=>"error","error"=>error.to_string());
            }
        }
        outcome.map(Response::new)
    }

    async fn read(
        &self,
        request: Request<LocalReadRequest>,
    ) -> Result<Response<LocalReadReply>, Status> {
        let outcome = self.read_inner(request.into_inner()).await;
        self.record("diagnostic_read", outcome.is_ok());
        match &outcome {
            Ok(reply) => {
                afs_logging::info!("local_api.diagnostic_read";"result"=>"ok","length"=>reply.length);
            }
            Err(error) => {
                afs_logging::warn!("local_api.diagnostic_read";"result"=>"error","error"=>error.to_string());
            }
        }
        outcome.map(Response::new)
    }
}

impl DiagnosticLocalDataService {
    async fn write_inner(&self, request: LocalWriteRequest) -> Result<LocalWriteReply, Status> {
        // 控制面必须带 source grant；没有 grant 就直接失败，不能退化为 gRPC payload 写入。
        let grant = request
            .source
            .ok_or_else(|| coded_status(afs_error::NODE_SHM_INVALID, "missing SHM source grant"))?;
        if request.length != grant.length {
            return Err(coded_status(
                afs_error::NODE_SHM_INVALID,
                "request length does not match SHM grant",
            ));
        }
        let (fd, offset) = request_grant_fd(grant).await?;
        // 这里会从 memfd copy 出 Vec；第一版不是 zero-copy。seal 只校验 fd 大小稳定。
        let data = read_fd_at(fd, offset, request.length as usize).map_err(shm_status)?;
        let written = self
            .storage
            .write(&request.name, request.file_offset, data)
            .await
            .map_err(storage_status)?;
        Ok(LocalWriteReply {
            written: written as u32,
        })
    }

    async fn read_inner(&self, request: LocalReadRequest) -> Result<LocalReadReply, Status> {
        // 读取也必须带 target grant；node 把 Storage 读出的 bytes copy 进 SDK memfd。
        let grant = request
            .target
            .ok_or_else(|| coded_status(afs_error::NODE_SHM_INVALID, "missing SHM target grant"))?;
        if request.length != grant.length {
            return Err(coded_status(
                afs_error::NODE_SHM_INVALID,
                "request length does not match SHM grant",
            ));
        }
        let data = self
            .storage
            .read(&request.name, request.file_offset, request.length)
            .await
            .map_err(storage_status)?;
        let length = u32::try_from(data.len()).map_err(|_| {
            coded_status(
                afs_error::NODE_SHM_INTERNAL,
                "storage returned too many bytes",
            )
        })?;
        let (fd, offset) = request_grant_fd(grant).await?;
        write_fd_at(fd, offset, &data).map_err(shm_status)?;
        Ok(LocalReadReply { length })
    }
}

fn storage_status(error: DiagnosticStorageError) -> Status {
    error_to_status(error.into())
}
