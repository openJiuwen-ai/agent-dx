//! Linux acceptance bridge: JSON lines, production runtime driver and host.
use adxlet::{
    jiuwenbox::{Config, Jiuwenbox},
    sandboxd::Config as LaunchConfig,
    RuntimeDriver,
};
use serde_json::{json, Value};
use std::{
    io::{self, BufRead, Write},
    time::Duration,
};
use tokio::time::Instant;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("config path required")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    let driver = Jiuwenbox::connect(
        config,
        LaunchConfig {
            command: vec![],
            rpc_timeout: Duration::from_secs(90),
            ..Default::default()
        },
    )
    .await?;
    println!("{}", json!({"ready":true}));
    io::stdout().flush()?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(8);
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines().map_while(Result::ok) {
            if tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    while let Some(line) = rx.recv().await {
        let input: Value = serde_json::from_str(&line)?;
        let id = input["id"].as_str().unwrap_or("");
        let result: adx_core::Result<Value> = match input["action"].as_str().unwrap_or("") {
            "start" => {
                let spec: adx_core::EnvironmentSpec = serde_json::from_value(input["spec"].clone())?;
                driver.start_with_deadline(&spec,id,1,&[],Instant::now()+Duration::from_millis(input["deadline_ms"].as_u64().unwrap_or(90000))).await.map(|ip|json!({"ip":ip}))
            }
            "remove" => driver.remove(id).await.map(|_|json!({"deleted":true})),
            "reconcile" => {
                let catalog: Vec<adx_core::EnvironmentRecord> = serde_json::from_value(input.get("catalog").cloned().unwrap_or(json!([])))?;
                driver.reconcile_inventory(&catalog).await.map(|r|json!({"count":r.len(),"ids":r.into_iter().map(|r|r.runtime_id).collect::<Vec<_>>()}))
            },
            "inventory" => driver.inventory().await.map(|r|json!({"count":r.len(),"ids":r.into_iter().map(|r|r.runtime_id).collect::<Vec<_>>()})),
            "classes" => driver.available_runtime_classes().await.map(|c|json!({"classes":c})),
            "observe" => driver.is_running(id).await.map(|r|json!({"running":r})),
            _ => Err(adx_core::Error::Invalid("unknown acceptance action".into())),
        };
        let output = match result {
            Ok(value) => json!({"ok":true,"value":value}),
            Err(error) => {
                json!({"ok":false,"error":error.to_string(),"debug":format!("{error:?}")})
            }
        };
        println!("{output}");
        io::stdout().flush()?;
    }
    Ok(())
}
