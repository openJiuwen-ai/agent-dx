//! Deployment-owned terminal identity and protocol adaptation.
use serde::Deserialize;

#[derive(Default)]
pub struct HelperDecoder { pending:Vec<u8> }
impl HelperDecoder {
    pub fn push(&mut self,chunk: &[u8]) -> Result<Vec<serde_json::Value>,&'static str> {
        const LIMIT:usize = 262144;
        let mut frames = Vec::new();
        for byte in chunk {
            if self.pending.len() >= LIMIT { self.pending.clear(); return Err("terminal_protocol_invalid"); }
            self.pending.push(*byte);
            if *byte == b'\n' {
                let frame: serde_json::Value = serde_json::from_slice(&self.pending).map_err(|_| "terminal_protocol_invalid")?;
                self.pending.clear();
                if frame["v"].as_u64() != Some(1) || !matches!(frame["type"].as_str(),Some("transport_ready"|"ready"|"output"|"exited"|"error")) {
                    return Err("terminal_protocol_invalid");
                }
                frames.push(frame);
            }
        }
        Ok(frames)
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub tenant:String,
    pub template:String,
    pub version:String,
    pub revision:String,
    pub backend:String,
    pub uid:u32,
    pub gid:u32,
    pub supplementary_groups:Vec<u32>,
    pub home:String,
    pub shell:String,
    pub adapter:String,
    pub cwd_mount_target:Option<String>,
    pub cwd_directory:Option<String>,
}
fn absolute(path: &str) -> bool {
    path.starts_with('/') && !path.chars().any(char::is_control)
        && !path.split('/').skip(1).any(|part| matches!(part,"."|".."))
}
impl Profile {
    pub fn validate(&self) -> Result<(),&'static str> {
        adx_agent_core::identifier(&self.tenant,"tenant").map_err(|_|"terminal_profile_invalid")?;
        adx_agent_core::identifier(&self.template,"template").map_err(|_|"terminal_profile_invalid")?;
        adx_agent_core::version_number(&self.version).map_err(|_|"terminal_profile_invalid")?;
        if self.revision.is_empty() || self.backend != "pty_adapter"
            || self.supplementary_groups.len()>256
            || self.uid == u32::MAX || self.gid == u32::MAX || self.supplementary_groups.contains(&u32::MAX)
            || ![&self.home,&self.shell,&self.adapter].iter().all(|path| absolute(path))
            || self.cwd_directory.as_ref().is_some_and(|path|!absolute(path))
            || self.cwd_mount_target.as_ref().is_some_and(|path|!absolute(path))
            || self.cwd_mount_target.is_some() == self.cwd_directory.is_some() {
            return Err("terminal_profile_invalid");
        }
        Ok(())
    }
}

pub fn pty_url(ingress: &crate::config::Ingress, environment: &str, adapter: &str, cols: u32, rows: u32) -> Result<url::Url, &'static str> {
    native_resize(cols, rows)?;
    if environment.is_empty() || environment.chars().any(|c| c.is_control() || matches!(c,'/'|'\\')) || !absolute(adapter) {
        return Err("terminal_configuration_invalid");
    }
    // Direct access lets Gateway authorize the Environment and inject its private
    // Execd credential; ordinary port forwarding cannot authenticate to Execd.
    let mut url = crate::clients::Clients::url(&ingress.base_url,&["direct",environment,"pty"])
        .map_err(|_| "terminal_configuration_invalid")?;
    let scheme = match url.scheme() { "https" => "wss", "http" => "ws", _ => return Err("terminal_configuration_invalid") };
    url.set_scheme(scheme).map_err(|_| "terminal_configuration_invalid")?;
    url.query_pairs_mut().append_pair("command",adapter).append_pair("command","--pty")
        .append_pair("cols",&cols.to_string()).append_pair("rows",&rows.to_string());
    Ok(url)
}

pub struct PtyProtocol {
    init: serde_json::Value,
    state: u8,
    utf8: Vec<u8>,
}
impl PtyProtocol {
    pub fn new(profile: &Profile, cwd: &str, cols: u32, rows: u32) -> Result<Self,&'static str> {
        profile.validate()?;
        native_resize(cols,rows)?;
        if !absolute(cwd) { return Err("terminal_cwd_unavailable"); }
        Ok(Self { init:serde_json::json!({"v":1,"type":"init","uid":profile.uid,"gid":profile.gid,
            "groups":profile.supplementary_groups,"home":profile.home,"cwd":cwd,"shell":profile.shell,"cols":cols,"rows":rows}),state:0,utf8:Vec::new() })
    }
    pub fn ready(&self) -> bool { self.state == 2 }
    pub fn frame(&mut self, frame: serde_json::Value) -> Result<(Option<serde_json::Value>,Option<serde_json::Value>),&'static str> {
        use base64::Engine;
        use serde_json::json;
        if frame["v"].as_u64() != Some(1) { return Err("terminal_protocol_invalid"); }
        match (self.state,frame["type"].as_str()) {
            (0,Some("transport_ready")) => { self.state=1; Ok((Some(self.init.clone()),None)) },
            (1,Some("ready")) => {
                for (actual,expected) in [("uid","uid"),("gid","gid"),("home","home"),("cwd","cwd")] {
                    if frame[actual] != self.init[expected] { return Err("terminal_identity_mismatch"); }
                }
                let groups = |value: &serde_json::Value| -> Result<std::collections::BTreeSet<u64>,&'static str> {
                    value.as_array().ok_or("terminal_identity_mismatch")?.iter()
                        .map(|v|v.as_u64().ok_or("terminal_identity_mismatch")).collect()
                };
                if groups(&frame["groups"])? != groups(&self.init["groups"])? { return Err("terminal_identity_mismatch"); }
                self.state=2;
                Ok((None,Some(json!({"type":"ready"}))))
            },
            (2,Some("output")) => {
                let data=frame["data"].as_str().ok_or("terminal_protocol_invalid")?;
                self.utf8.extend(base64::engine::general_purpose::STANDARD.decode(data).map_err(|_|"terminal_protocol_invalid")?);
                let mut text=String::new();
                loop {
                    match std::str::from_utf8(&self.utf8) {
                        Ok(valid) => { text.push_str(valid);self.utf8.clear();break; },
                        Err(error) => {
                            let valid=error.valid_up_to();
                            text.push_str(std::str::from_utf8(&self.utf8[..valid]).map_err(|_|"terminal_protocol_invalid")?);
                            self.utf8.drain(..valid);
                            if let Some(length)=error.error_len() { text.push('\u{fffd}');self.utf8.drain(..length); } else { break; }
                        }
                    }
                }
                Ok((None,(!text.is_empty()).then(||json!({"type":"output","data":text}))))
            },
            (2,Some("exited")) => {
                self.state=3;
                Ok((None,Some(json!({"type":"closed","reason":"exited","exit_code":frame["exit_code"]}))))
            },
            (_,Some("error")) => Err(match frame["code"].as_str() {
                Some("terminal_home_unavailable") => "terminal_home_unavailable",
                Some("terminal_cwd_unavailable") => "terminal_cwd_unavailable",
                Some("terminal_identity_mismatch") => "terminal_identity_mismatch",
                _ => "terminal_adapter_unavailable",
            }),
            _ => Err("terminal_protocol_invalid"),
        }
    }
}
pub fn terminal_cwd(profile: &Profile,tenant: &str,snapshot: &adx_agent_core::TemplateVersion,parameters: &adx_agent_core::mounts::Parameters) -> Result<String,&'static str> {
    profile.validate()?;
    if profile.tenant != tenant || profile.template != snapshot.name || profile.version != snapshot.version { return Err("terminal_profile_unavailable"); }
    if let Some(directory) = &profile.cwd_directory {
        if !absolute(directory) { return Err("terminal_cwd_unavailable"); }
        return Ok(directory.clone());
    }
    let target = profile.cwd_mount_target.as_deref().ok_or("terminal_cwd_unavailable")?;
    let mount = snapshot.mounts.iter().find(|mount| mount.target == target).ok_or("terminal_cwd_unavailable")?;
    let rendered = mount.render(parameters).map_err(|_| "mount_resolution_failed")?;
    Ok(rendered.target)
}
pub fn native_resize(cols: u32, rows: u32) -> Result<String,&'static str> {
    if cols == 0 || rows == 0 || u16::try_from(cols).is_err() || u16::try_from(rows).is_err() { return Err("invalid_terminal_size"); }
    Ok(format!("RESIZE:{cols}:{rows}"))
}

pub struct BridgeLimits { pub connect_ms:u64, pub idle_seconds:u64, pub lost_seconds:u64, pub tls: Option<std::sync::Arc<rustls::ClientConfig>> }
/// Ask the adapter to reap its child before closing the outer PTY transport.
pub async fn shutdown<S>(upstream:&mut tokio_tungstenite::WebSocketStream<S>, timeout:std::time::Duration)
where S:tokio::io::AsyncRead+tokio::io::AsyncWrite+Unpin {
    use futures_util::{SinkExt,StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let _=tokio::time::timeout(timeout,async {
        upstream.send(Message::Binary(b"{\"v\":1,\"type\":\"close\"}\n".to_vec().into())).await?;
        while let Some(message)=upstream.next().await {
            match message? {
                Message::Text(text) if serde_json::from_str::<serde_json::Value>(&text).ok().is_some_and(|frame|frame["type"]=="exited")=>break,
                Message::Close(_)=>break,
                Message::Ping(data)=>upstream.send(Message::Pong(data)).await?,
                _=>{},
            }
        }
        Ok::<(),tokio_tungstenite::tungstenite::Error>(())
    }).await;
    let _=tokio::time::timeout(timeout,upstream.close(None)).await;
}
async fn bounded_send<S,T>(sink:&mut S,message:T,timeout:std::time::Duration) -> Result<(),&'static str>
where S:futures_util::Sink<T>+Unpin {
    use futures_util::SinkExt;
    tokio::time::timeout(timeout,sink.send(message)).await.map_err(|_|"terminal_send_timeout")?.map_err(|_|"terminal_connection_closed")
}
pub async fn bridge(mut browser:axum::extract::ws::WebSocket,url:url::Url,token:String,mut protocol:PtyProtocol,meta:serde_json::Value,limits:BridgeLimits,active:impl Fn()->bool+Send) -> Result<(),&'static str> {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest,Message};
    use axum::extract::ws::Message as Browser;
    use base64::Engine;
    use serde_json::json;
    use std::time::Duration;
    let mut request=url.as_str().into_client_request().map_err(|_|"terminal_configuration_invalid")?;
    request.headers_mut().insert("authorization",format!("Bearer {token}").parse().map_err(|_|"upstream_credential_invalid")?);
    let config=tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default().max_message_size(Some(262144)).max_frame_size(Some(262144));
    let connector = limits.tls.map(tokio_tungstenite::Connector::Rustls);
    let (mut upstream,_)=tokio::time::timeout(Duration::from_millis(limits.connect_ms),tokio_tungstenite::connect_async_tls_with_config(request,Some(config),false,connector)).await.map_err(|_|"terminal_connect_timeout")?.map_err(|_|"terminal_connect_failed")?;
    let send_timeout=Duration::from_secs(limits.lost_seconds);
    let mut decoder=HelperDecoder::default();
    let started=tokio::time::Instant::now();
    let mut activity=started;
    let mut heartbeat=started;
    let mut tick=tokio::time::interval(Duration::from_secs(1));
    let result=async {
        bounded_send(&mut browser,Browser::Text(meta.to_string().into()),send_timeout).await?;
        loop {
            tokio::select! {
                _=tick.tick()=> {
                    if !active() { return Err("terminal_session_revoked"); }
                    if !protocol.ready() && started.elapsed()>send_timeout { return Err("terminal_startup_timeout"); }
                    if activity.elapsed()>Duration::from_secs(limits.idle_seconds) { return Err("terminal_idle_timeout"); }
                    if heartbeat.elapsed()>send_timeout { return Err("terminal_connection_lost"); }
                    bounded_send(&mut browser,Browser::Ping(Vec::new().into()),send_timeout).await?;
                },
                message=browser.next()=> {
                    match message {
                        Some(Ok(Browser::Pong(_)))=>heartbeat=tokio::time::Instant::now(),
                        Some(Ok(Browser::Ping(data)))=> { heartbeat=tokio::time::Instant::now(); bounded_send(&mut browser,Browser::Pong(data),send_timeout).await?; },
                        Some(Ok(Browser::Text(text)))=> {
                            heartbeat=tokio::time::Instant::now();
                            let frame:serde_json::Value=serde_json::from_str(&text).map_err(|_|"terminal_protocol_invalid")?;
                            if frame["type"]=="close" { return Ok(()); }
                            if !protocol.ready() { return Err("terminal_not_ready"); }
                            activity=tokio::time::Instant::now();
                            let helper=match frame["type"].as_str() {
                                Some("input")=>json!({"v":1,"type":"input","data":base64::engine::general_purpose::STANDARD.encode(frame["data"].as_str().ok_or("terminal_protocol_invalid")?.as_bytes())}),
                                Some("resize")=> {
                                    let cols=frame["cols"].as_u64().and_then(|v|u32::try_from(v).ok()).ok_or("invalid_terminal_size")?;
                                    let rows=frame["rows"].as_u64().and_then(|v|u32::try_from(v).ok()).ok_or("invalid_terminal_size")?;
                                    bounded_send(&mut upstream,Message::Text(native_resize(cols,rows)?.into()),send_timeout).await?;
                                    json!({"v":1,"type":"resize","cols":cols,"rows":rows})
                                },
                                Some("eof")=>json!({"v":1,"type":"eof"}),
                                _=>return Err("terminal_protocol_invalid"),
                            };
                            bounded_send(&mut upstream,Message::Binary(format!("{helper}\n").into_bytes().into()),send_timeout).await?;
                        },
                        _=>return Ok(()),
                    }
                },
                message=upstream.next()=> {
                    match message {
                        Some(Ok(Message::Binary(data)))=>for frame in decoder.push(&data)? {
                            let (init,event)=protocol.frame(frame)?;
                            if let Some(init)=init { bounded_send(&mut upstream,Message::Binary(format!("{init}\n").into_bytes().into()),send_timeout).await?; }
                            if let Some(event)=event {
                                bounded_send(&mut browser,Browser::Text(event.to_string().into()),send_timeout).await?;
                                if event["type"]=="closed" { return Ok(()); }
                            }
                        },
                        Some(Ok(Message::Text(text)))=> {
                            let frame:serde_json::Value=serde_json::from_str(&text).map_err(|_|"terminal_protocol_invalid")?;
                            if frame["type"]=="exited" { return Err("terminal_adapter_exited"); }
                            if frame["type"]!="started" { return Err("terminal_protocol_invalid"); }
                        },
                        Some(Ok(Message::Ping(data)))=>bounded_send(&mut upstream,Message::Pong(data),send_timeout).await?,
                        Some(Ok(Message::Pong(_)))=>{},
                        _=>return Err("terminal_upstream_closed"),
                    }
                }
            }
        }
    };
    let outcome=result.await;
    shutdown(&mut upstream,send_timeout).await;
    let reason=outcome.as_ref().err().copied().unwrap_or("closed");
    let _=tokio::time::timeout(send_timeout,browser.send(Browser::Text(json!({"type":"closed","reason":reason}).to_string().into()))).await;
    outcome
}
