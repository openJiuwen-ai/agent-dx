use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

pub mod pb {
    tonic::include_proto!("rrt.v1");
}
/// RuntimeRPC bindings between runtime and functionsystem.
pub mod posix {
    pub mod common {
        tonic::include_proto!("common");
    }
    pub mod affinity {
        tonic::include_proto!("affinity");
    }
    pub mod core_service {
        tonic::include_proto!("core_service");
    }
    pub mod runtime_service {
        tonic::include_proto!("runtime_service");
    }
    pub mod runtime_rpc {
        tonic::include_proto!("runtime_rpc");
    }
    pub mod resources {
        tonic::include_proto!("resources");
    }
}
pub mod filesystem;
pub mod health;
pub mod port;
pub mod process;
pub mod pyval;
pub mod runtime;
pub mod startup;

/// Serve on an already-bound listener; tests use this to obtain an ephemeral port.
pub async fn serve_with_listener(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(pb::process_server::ProcessServer::new(
            process::ProcessSvc::default(),
        ))
        .add_service(pb::health_server::HealthServer::new(
            health::HealthSvc::default(),
        ))
        .add_service(pb::filesystem_server::FilesystemServer::new(
            filesystem::FilesystemSvc::default(),
        ))
        .add_service(pb::port_server::PortServer::new(port::PortSvc::default()))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

/// Serve on an address; binaries use this helper.
pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    serve_with_listener(listener).await
}
