//! Per-request immutable mount context resolution for console file operations.
use crate::{clients::Clients,config::{ConsoleConfig,Tenant},files::{StorageMappings,SafeRoot}};
use serde_json::{Value,json};
use sha2::{Digest,Sha256};

pub struct Context {
    pub binding:adx_agent_core::AgentBinding,
    pub mounts:Vec<adx_agent_core::mounts::Mount>,
    pub mapping:StorageMappings,
}
pub async fn context(clients:&Clients,config:&ConsoleConfig,tenant:&Tenant,scope:&adx_agent_core::Scope,generation:&str,slot:&str,environment:&str)->Result<Context,&'static str> {
    let kind=match scope.template_ref {adx_agent_core::TemplateRef::Version(_)=>"versions",_=>"aliases"};
    let value=clients.agent(config,tenant,reqwest::Method::GET,&["templates",&scope.template,kind,scope.template_ref.name(),"bindings",&scope.binding_id],"",None).await.map_err(|_|"binding_query_failed")?;
    let binding:adx_agent_core::AgentBinding=serde_json::from_value(value["binding"].clone()).map_err(|_|"binding_query_failed")?;
    crate::agent_view::check_context(&binding,scope,generation,Some((slot,environment)))?;
    let version=if slot=="current" {&binding.current.version} else {&binding.secondary.as_ref().ok_or("environment_context_conflict")?.environment.version};
    let value=clients.agent(config,tenant,reqwest::Method::GET,&["templates",&scope.template,"versions",version],"",None).await.map_err(|_|"mount_resolution_failed")?;
    let snapshot:adx_agent_core::TemplateVersion=serde_json::from_value(value["template"].clone()).map_err(|_|"mount_resolution_failed")?;
    if snapshot.name!=scope.template || snapshot.version!=*version {return Err("mount_resolution_failed");}
    let mounts=snapshot.mounts.iter().map(|mount|mount.render(&binding.parameters).map_err(|_|"mount_resolution_failed")).collect::<Result<Vec<_>,_>>()?;
    let path=&config.shared_files.as_ref().ok_or("shared_mount_unsupported")?.mappings_file;
    let mapping=serde_json::from_slice(&tokio::fs::read(path).await.map_err(|_|"shared_storage_unavailable")?).map_err(|_|"shared_storage_unavailable")?;
    Ok(Context {binding,mounts,mapping})
}
pub struct Resolved {pub id:String,pub target:String,pub root:String,pub path:String,pub source:String,pub identity:Value}
pub async fn resolve(clients:&Clients,config:&ConsoleConfig,tenant:&Tenant,ctx:&Context,index:usize,slot:&str)->Result<Resolved,&'static str> {
    let mount=ctx.mounts.get(index).ok_or("mount_context_conflict")?;
    let id=mount.volume_id.as_deref().ok_or("shared_mount_unsupported")?;
    let uid=ctx.binding.volumes.get(id).ok_or("volume_identity_conflict")?;
    if mount.volume_uid.as_ref().is_some_and(|value|value!=uid) {return Err("volume_identity_conflict");}
    let url=Clients::url(&config.apiserver.base_url,&["api","environment","v1","volumes",id]).map_err(|_|"volume_resolution_unavailable")?;
    let volume=clients.json(tenant,reqwest::Method::GET,url,None,config.apiserver.timeout_ms).await.map_err(|error|if error.status==reqwest::StatusCode::NOT_FOUND {"volume_not_found"} else {"volume_resolution_unavailable"})?;
    // The public Platform API wraps successful JSON in a base64 data envelope.
    use base64::Engine;
    let volume = if volume.get("code").is_some() {
        let encoded = volume.get("data").and_then(Value::as_str).ok_or("volume_resolution_unavailable")?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_| "volume_resolution_unavailable")?;
        serde_json::from_slice::<Value>(&bytes).map_err(|_| "volume_resolution_unavailable")?
    } else { volume };
    let volume=volume.get("volume").cloned().unwrap_or(volume);
    let parsed=crate::files::resolve_volume(&volume,&tenant.tenant_id,id,uid,&ctx.mapping)?;
    let domain=ctx.mapping.domains.iter().find(|domain|domain.read_only_root==parsed.read_only_root && domain.backend_id==parsed.backend_id).ok_or("mount_context_conflict")?;
    let source=domain.mount_source.clone().ok_or("shared_storage_unavailable")?;
    let identity=json!([ctx.binding.scope,ctx.binding.generation,slot,index,mount,volume,ctx.mapping.revision,parsed.backend_id,source,parsed.read_only_root]);
    let token=hex::encode(Sha256::digest(serde_json::to_vec(&identity).map_err(|_|"mount_context_conflict")?));
    Ok(Resolved {id:token,target:mount.target.clone(),root:parsed.read_only_root,path:parsed.relative_path,source,identity})
}
pub fn open(resolved:&Resolved,path:&str)->Result<std::fs::File,&'static str> {
    // SAFETY: geteuid has no pointer or lifetime preconditions.
    if unsafe {libc::geteuid()}==0 {return Err("shared_storage_unavailable");}
    let info=std::fs::read_to_string("/proc/self/mountinfo").map_err(|_|"shared_storage_unavailable")?;
    crate::files::verify_mount(&info,&resolved.root,&resolved.source)?;
    let root=SafeRoot::open(std::path::Path::new(&resolved.root))?;
    let directory=root.open_relative(&resolved.path)?;
    let root=SafeRoot::from_directory(directory)?;
    root.open_relative(path)
}
