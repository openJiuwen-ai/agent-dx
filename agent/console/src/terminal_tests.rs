use crate::terminal::{Profile,terminal_cwd,native_resize};

#[tokio::test]
async fn revoked_terminal_session_closes_helper_and_reports_reason() {
    use futures_util::{SinkExt,StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use serde_json::{json,Value};
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=listener.local_addr().unwrap();
    let worker=tokio::spawn(async move {
        let (socket,_)=listener.accept().await.unwrap();
        let mut socket=tokio_tungstenite::accept_async(socket).await.unwrap();
        let Message::Binary(data)=socket.next().await.unwrap().unwrap() else { panic!("expected helper close") };
        assert_eq!(serde_json::from_slice::<Value>(&data).unwrap()["type"],"close");
        socket.send(Message::Text(json!({"version":1,"type":"exited","exit_code":0}).to_string().into())).await.unwrap();
        assert!(matches!(socket.next().await.unwrap().unwrap(),Message::Close(_)));
    });
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let console=listener.local_addr().unwrap();
    let router=axum::Router::new().route("/terminal",axum::routing::get(move |ws:axum::extract::WebSocketUpgrade|async move {
        ws.on_upgrade(move |socket|async move {
            let profile:Profile=serde_json::from_value(json!({"tenant":"t","template":"app","version":"1","revision":"p1","backend":"pty_adapter","uid":1000,"gid":1000,"supplementary_groups":[],"home":"/tmp","shell":"/bin/sh","adapter":"/helper","cwd_directory":"/tmp"})).unwrap();
            let outcome=crate::terminal::bridge(socket,format!("ws://{address}/pty").parse().unwrap(),"token".into(),crate::terminal::PtyProtocol::new(&profile,"/tmp",80,24).unwrap(),json!({"type":"meta"}),crate::terminal::BridgeLimits {connect_ms:1000,idle_seconds:10,lost_seconds:2,tls:None},||false).await;
            assert_eq!(outcome,Err("terminal_session_revoked"));
        })
    }));
    let server=tokio::spawn(async move {axum::serve(listener,router).await.unwrap();});
    let (mut client,_)=tokio_tungstenite::connect_async(format!("ws://{console}/terminal")).await.unwrap();
    let reason=tokio::time::timeout(std::time::Duration::from_secs(4),async {
        while let Some(Ok(Message::Text(text)))=client.next().await {
            let frame:Value=serde_json::from_str(&text).unwrap();
            if frame["type"]=="closed" {return frame["reason"].as_str().unwrap().to_owned();}
        }
        panic!("missing closed event");
    }).await.unwrap();
    assert_eq!(reason,"terminal_session_revoked");
    worker.await.unwrap();
    server.abort();
}

#[tokio::test]
async fn terminal_shutdown_waits_for_adapter_exit_before_ws_close() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        let Message::Binary(command) = socket.next().await.unwrap().unwrap() else { panic!("expected helper close") };
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&command).unwrap()["type"], "close");
        assert!(tokio::time::timeout(std::time::Duration::from_millis(80), socket.next()).await.is_err(), "WS closed before helper cleanup completed");
        socket.send(Message::Text(r#"{"version":1,"type":"exited","exit_code":0}"#.into())).await.unwrap();
        assert!(matches!(socket.next().await.unwrap().unwrap(), Message::Close(_)));
    });
    let (mut upstream, _) = tokio_tungstenite::connect_async(format!("ws://{address}")).await.unwrap();
    crate::terminal::shutdown(&mut upstream, std::time::Duration::from_secs(1)).await;
    server.await.unwrap();
}

#[test]
fn native_resize_uses_execd_text_protocol_and_validates_dimensions() {
    assert_eq!(native_resize(100,40).unwrap(),"RESIZE:100:40");
    assert!(native_resize(0,40).is_err());
    assert!(native_resize(80,65536).is_err());
}

#[test]
fn terminal_profile_rejects_invalid_identity_scope_and_cwd_at_startup() {
    let base=serde_json::json!({"tenant":"t","template":"app","version":"1","revision":"p1","backend":"pty_adapter","uid":1000,"gid":1000,"supplementary_groups":[],"home":"/tmp","shell":"/bin/sh","adapter":"/helper","cwd_directory":"/tmp"});
    for (key,value) in [("tenant",serde_json::json!("")),("template",serde_json::json!("bad\ntemplate")),("version",serde_json::json!("latest")),("cwd_directory",serde_json::json!("relative")),("cwd_mount_target",serde_json::json!("/data/../other")),("supplementary_groups",serde_json::json!(vec![1;257]))] {
        let mut invalid=base.clone();
        if key=="cwd_mount_target" {invalid.as_object_mut().unwrap().remove("cwd_directory");}
        invalid[key]=value;
        let profile:Profile=serde_json::from_value(invalid).unwrap();
        assert!(profile.validate().is_err(),"accepted invalid {key}");
    }
}

#[test]
fn terminal_profile_uses_slot_actual_version_and_historical_mount_target() {
    let profile: Profile = serde_json::from_value(serde_json::json!({"tenant":"t","template":"app","version":"1","revision":"p1","backend":"pty_adapter","uid":1000,"gid":1000,"supplementary_groups":[1001],"home":"/home/app","shell":"/bin/sh","adapter":"/opt/adx/terminal-helper","cwd_mount_target":"/data/state"})).unwrap();
    let snapshot: adx_agent_core::TemplateVersion = serde_json::from_value(serde_json::json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runc","resources":{"cpu_millis":100,"memory_mib":128},"mounts":[{"type":"bind","host_path":"/mnt/work/${workspace}","target":"/data/state"}]})).unwrap();
    let parameters = std::collections::BTreeMap::from([("workspace".into(),"old-user".into())]);
    assert_eq!(terminal_cwd(&profile,"t",&snapshot,&parameters).unwrap(),"/data/state");
    assert!(terminal_cwd(&profile,"other",&snapshot,&parameters).is_err());
    assert!(terminal_cwd(&profile,"t",&snapshot,&Default::default()).is_err());
    let mut wrong = snapshot; wrong.version = "2".into();
    assert!(terminal_cwd(&profile,"t",&wrong,&parameters).is_err());
}

#[test]
fn pty_url_pins_environment_and_encodes_argv_without_shell() {
    let ingress = crate::config::Ingress { base_url:"https://ingress.example".into(),execd_port:44772,connect_timeout_ms:1000 };
    let url = crate::terminal::pty_url(&ingress,"env-secondary","/opt/adx/helper",80,24).unwrap();
    assert_eq!(url.scheme(),"wss");
    assert_eq!(url.path(),"/direct/env-secondary/pty");
    assert_eq!(url.query_pairs().filter(|(k,_)| k == "command").map(|(_,v)|v.into_owned()).collect::<Vec<_>>(),vec!["/opt/adx/helper","--pty"]);
    assert!(crate::terminal::pty_url(&ingress,"bad/env","/helper",80,24).is_err());
}

#[test]
fn pty_handshake_checks_ready_identity_and_incremental_utf8() {
    use serde_json::json;
    let profile: Profile = serde_json::from_value(json!({"tenant":"t","template":"app","version":"1","revision":"p1","backend":"pty_adapter","uid":1000,"gid":1000,"supplementary_groups":[1001],"home":"/home/app","shell":"/bin/sh","adapter":"/helper","cwd_directory":"/tmp"})).unwrap();
    let mut protocol = crate::terminal::PtyProtocol::new(&profile,"/tmp",80,24).unwrap();
    assert!(protocol.frame(json!({"v":1,"type":"output","data":"YQ=="})).is_err());
    let mut protocol = crate::terminal::PtyProtocol::new(&profile,"/tmp",80,24).unwrap();
    let (init, browser) = protocol.frame(json!({"v":1,"type":"transport_ready"})).unwrap();
    assert!(browser.is_none());
    assert_eq!(init.unwrap()["type"],"init");
    assert!(protocol.frame(json!({"v":1,"type":"ready","uid":0,"gid":1000,"groups":[1001],"home":"/home/app","cwd":"/tmp"})).is_err());
    let mut protocol = crate::terminal::PtyProtocol::new(&profile,"/tmp",80,24).unwrap();
    protocol.frame(json!({"v":1,"type":"transport_ready"})).unwrap();
    assert_eq!(protocol.frame(json!({"v":1,"type":"ready","uid":1000,"gid":1000,"groups":[1001],"home":"/home/app","cwd":"/tmp"})).unwrap().1.unwrap()["type"],"ready");
    assert!(protocol.frame(json!({"v":1,"type":"output","data":"5A=="})).unwrap().1.is_none());
    assert_eq!(protocol.frame(json!({"v":1,"type":"output","data":"uK0="})).unwrap().1.unwrap()["data"],"中");
}

#[test]
fn helper_decoder_retains_fragmented_frames_and_checks_version_and_bound() {
    let mut decoder = crate::terminal::HelperDecoder::default();
    assert!(decoder.push(b"{\"v\":1,\"type\":\"out").unwrap().is_empty());
    let frames = decoder.push(b"put\",\"data\":\"YQ==\"}\n{\"v\":1,\"type\":\"exited\",\"exit_code\":0}\n").unwrap();
    assert_eq!(frames.len(),2);
    assert_eq!(frames[0]["data"],"YQ==");
    assert_eq!(frames[1]["exit_code"],0);
    assert!(decoder.push(b"{\"v\":2,\"type\":\"ready\"}\n").is_err());
    let mut decoder = crate::terminal::HelperDecoder::default();
    assert!(decoder.push(&vec![b'x';262145]).is_err());
}

#[tokio::test]
async fn pty_bridge_uses_live_websocket_handshake_binary_input_and_dual_resize() {
    use futures_util::{SinkExt,StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    use serde_json::{json,Value};
    let upstream=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address=upstream.local_addr().unwrap();
    let worker=tokio::spawn(async move {
        let (socket,_)=upstream.accept().await.unwrap();
        // Tungstenite's callback signature fixes this large error type.
        #[allow(clippy::result_large_err)]
        fn check_header(request:&tokio_tungstenite::tungstenite::handshake::server::Request,response:tokio_tungstenite::tungstenite::handshake::server::Response) -> Result<tokio_tungstenite::tungstenite::handshake::server::Response,tokio_tungstenite::tungstenite::handshake::server::ErrorResponse> {
            assert_eq!(request.headers()["authorization"],"Bearer fixed-tenant-token"); Ok(response)
        }
        let mut ws=tokio_tungstenite::accept_hdr_async(socket,check_header).await.unwrap();
        ws.send(Message::Text(json!({"version":1,"type":"started"}).to_string().into())).await.unwrap();
        ws.send(Message::Binary(b"{\"v\":1,\"type\":\"transport_ready\"}\n".to_vec().into())).await.unwrap();
        let init=ws.next().await.unwrap().unwrap();
        let Message::Binary(init)=init else { panic!("init must be Binary") };
        let init:Value=serde_json::from_slice(&init).unwrap();
        assert_eq!(init["uid"],1000);
        ws.send(Message::Binary(format!("{}\n",json!({"v":1,"type":"ready","uid":1000,"gid":1000,"groups":[],"home":"/tmp","cwd":"/tmp"})).into_bytes().into())).await.unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap(),Message::Text("RESIZE:100:40".into()));
        let Message::Binary(resize)=ws.next().await.unwrap().unwrap() else { panic!("helper resize Binary") };
        assert_eq!(serde_json::from_slice::<Value>(&resize).unwrap()["cols"],100);
        let Message::Binary(input)=ws.next().await.unwrap().unwrap() else { panic!("helper input Binary") };
        assert_eq!(serde_json::from_slice::<Value>(&input).unwrap()["data"],"aGk=");
        ws.send(Message::Binary(b"{\"v\":1,\"type\":\"output\",\"data\":\"aGk=\"}\n{\"v\":1,\"type\":\"exited\",\"exit_code\":0}\n".to_vec().into())).await.unwrap();
        while let Some(Ok(message))=ws.next().await { if matches!(message,Message::Close(_)) { break; } }
    });
    let profile:Profile=serde_json::from_value(json!({"tenant":"t","template":"app","version":"1","revision":"p1","backend":"pty_adapter","uid":1000,"gid":1000,"supplementary_groups":[],"home":"/tmp","shell":"/bin/sh","adapter":"/helper","cwd_directory":"/tmp"})).unwrap();
    let server=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let console=server.local_addr().unwrap();
    let router=axum::Router::new().route("/terminal",axum::routing::get(move |ws:axum::extract::WebSocketUpgrade| async move {
        ws.on_upgrade(move |socket| async move {
            crate::terminal::bridge(socket,format!("ws://{address}/pty").parse().unwrap(),"fixed-tenant-token".into(),crate::terminal::PtyProtocol::new(&profile,"/tmp",80,24).unwrap(),json!({"type":"meta"}),crate::terminal::BridgeLimits {connect_ms:1000,idle_seconds:10,lost_seconds:3,tls:None},||true).await.unwrap();
        })
    }));
    let server=tokio::spawn(async move { axum::serve(server,router).await.unwrap(); });
    let (mut client,_)=tokio_tungstenite::connect_async(format!("ws://{console}/terminal")).await.unwrap();
    let mut output=String::new();
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        while let Some(Ok(message))=client.next().await {
            match message {
                Message::Ping(data)=>client.send(Message::Pong(data)).await.unwrap(),
                Message::Text(text)=> {
                    let frame:Value=serde_json::from_str(&text).unwrap();
                    if frame["type"]=="ready" {
                        client.send(Message::Text(json!({"type":"resize","cols":100,"rows":40}).to_string().into())).await.unwrap();
                        client.send(Message::Text(json!({"type":"input","data":"hi"}).to_string().into())).await.unwrap();
                    }
                    if frame["type"]=="output" { output.push_str(frame["data"].as_str().unwrap()); }
                    if frame["type"]=="closed" { break; }
                },_=>{}
            }
        }
    }).await.unwrap();
    assert_eq!(output,"hi");
    worker.await.unwrap();
    server.abort();
}
