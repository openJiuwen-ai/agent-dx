use crate::pb::process_server::Process;
use crate::pb::{ExecRequest, ExecResponse};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct ProcessSvc;

#[tonic::async_trait]
impl Process for ProcessSvc {
    async fn exec(&self, req: Request<ExecRequest>) -> Result<Response<ExecResponse>, Status> {
        let r = req.into_inner();
        if r.cmd.is_empty() {
            return Err(Status::invalid_argument("cmd is empty"));
        }
        let mut cmd = tokio::process::Command::new(&r.cmd);
        cmd.args(&r.args);
        crate::runtime::child_env::apply_tokio(&mut cmd);
        if !r.cwd.is_empty() {
            cmd.current_dir(&r.cwd);
        }
        for (k, v) in &r.env {
            crate::runtime::child_env::validate_override(k).map_err(Status::invalid_argument)?;
            cmd.env(k, v);
        }
        let out = cmd
            .output()
            .await
            .map_err(|e| Status::internal(format!("spawn failed: {e}")))?;
        Ok(Response::new(ExecResponse {
            exit_code: out.status.code().unwrap_or(-1),
            stdout: out.stdout,
            stderr: out.stderr,
        }))
    }
}
