use adx_control_cli::{config::Deployment, supervisor, Result};
use std::{path::Path, time::Duration};
#[tokio::main]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    match a.get(1).map(String::as_str) {
  Some("validate") if a.len()==4&&a[2]=="--config"=>{Deployment::load(Path::new(&a[3]))?;println!("configuration valid");}
  Some("render") if a.len()==6&&a[2]=="--config"&&a[4]=="--output"=>{let d=Deployment::load(Path::new(&a[3]))?;let p=d.render(Path::new(&a[5]))?;println!("rendered {} services",p.len());}
  Some("run"|"start") if a.len()==4&&a[2]=="--config"=>supervisor::run(Deployment::load(Path::new(&a[3]))?).await?,
  Some(action @ ("status"|"stop")) if a.len()==4&&a[2]=="--config"=>{let d=Deployment::load(Path::new(&a[3]))?;let timeout=Duration::from_secs(d.stop_timeout_seconds.saturating_mul((d.services.len()*2+1)as u64));println!("{}",supervisor::request(&d.state_dir,action,timeout).await?);}
  _=>return Err("usage: adxctl validate|run|start|status|stop --config deployment.json; adxctl render --config deployment.json --output new-directory".into())
 }
    Ok(())
}
