use crate::pb::filesystem_server::Filesystem;
use crate::pb::*;
use std::path::Path;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct FilesystemSvc;

fn map_io<T>(r: std::io::Result<T>) -> Result<T, Status> {
    r.map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => Status::not_found(e.to_string()),
        std::io::ErrorKind::PermissionDenied => Status::permission_denied(e.to_string()),
        _ => Status::internal(e.to_string()),
    })
}

#[tonic::async_trait]
impl Filesystem for FilesystemSvc {
    async fn stat(&self, req: Request<StatRequest>) -> Result<Response<StatResponse>, Status> {
        let p = req.into_inner().path;
        match tokio::fs::metadata(&p).await {
            Ok(m) => {
                use std::os::unix::fs::MetadataExt;
                Ok(Response::new(StatResponse {
                    exists: true,
                    is_dir: m.is_dir(),
                    size: m.len(),
                    mode: m.mode(),
                    mtime_unix: m.mtime(),
                }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Response::new(StatResponse {
                exists: false,
                ..Default::default()
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn list_dir(
        &self,
        req: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let p = req.into_inner().path;
        let mut rd = map_io(tokio::fs::read_dir(&p).await)?;
        let mut entries = Vec::new();
        while let Some(e) = map_io(rd.next_entry().await)? {
            let md = map_io(e.metadata().await)?;
            entries.push(DirEntry {
                name: e.file_name().to_string_lossy().into_owned(),
                is_dir: md.is_dir(),
                size: md.len(),
            });
        }
        Ok(Response::new(ListDirResponse { entries }))
    }

    async fn make_dir(&self, req: Request<MakeDirRequest>) -> Result<Response<Ack>, Status> {
        let r = req.into_inner();
        if r.parents {
            map_io(tokio::fs::create_dir_all(&r.path).await)?;
        } else {
            map_io(tokio::fs::create_dir(&r.path).await)?;
        }
        Ok(Response::new(Ack {}))
    }

    async fn r#move(&self, req: Request<MoveRequest>) -> Result<Response<Ack>, Status> {
        let r = req.into_inner();
        map_io(tokio::fs::rename(&r.from, &r.to).await)?;
        Ok(Response::new(Ack {}))
    }

    async fn remove(&self, req: Request<RemoveRequest>) -> Result<Response<Ack>, Status> {
        let r = req.into_inner();
        let meta = map_io(tokio::fs::metadata(&r.path).await)?;
        if meta.is_dir() {
            if r.recursive {
                map_io(tokio::fs::remove_dir_all(&r.path).await)?;
            } else {
                map_io(tokio::fs::remove_dir(&r.path).await)?;
            }
        } else {
            map_io(tokio::fs::remove_file(&r.path).await)?;
        }
        Ok(Response::new(Ack {}))
    }

    async fn read_file(
        &self,
        req: Request<ReadFileRequest>,
    ) -> Result<Response<ReadFileResponse>, Status> {
        let p = req.into_inner().path;
        let content = map_io(tokio::fs::read(&p).await)?;
        Ok(Response::new(ReadFileResponse { content }))
    }

    async fn write_file(&self, req: Request<WriteFileRequest>) -> Result<Response<Ack>, Status> {
        let r = req.into_inner();
        if r.create_parents {
            if let Some(parent) = Path::new(&r.path).parent() {
                map_io(tokio::fs::create_dir_all(parent).await)?;
            }
        }
        map_io(tokio::fs::write(&r.path, &r.content).await)?;
        Ok(Response::new(Ack {}))
    }
}
