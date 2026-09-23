//! Legacy operation result adaptation. Gateway owns HTTP validation and Execd transport.
use crate::{Error, Result};
use serde_json::{json, Value};

/// Preserve a shell string or execute literal argv without interpreting its metacharacters.
pub fn command(value: &Value) -> Result<String> {
    if let Some(command) = value
        .as_str()
        .filter(|s| !s.trim().is_empty() && !s.contains('\0'))
    {
        return Ok(command.into());
    }
    let argv = value
        .as_array()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::Invalid("command must be a nonempty string or argv".into()))?;
    let args = argv
        .iter()
        .map(|arg| {
            let arg = arg.as_str().filter(|v| !v.contains('\0')).ok_or_else(|| {
                Error::Invalid("command argv must contain strings without NUL".into())
            })?;
            Ok(format!("'{}'", arg.replace('\'', "'\"'\"'")))
        })
        .collect::<Result<Vec<_>>>()?;
    if argv[0].as_str().is_none_or(str::is_empty) {
        return Err(Error::Invalid("empty executable".into()));
    }
    Ok(format!("exec {}", args.join(" ")))
}
pub fn checked(value: Value) -> Result<Value> {
    if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
        return Err(Error::Unavailable(format!(
            "Sandbox operation failed: {error}"
        )));
    }
    Ok(value)
}
pub fn exec_result(value: Value) -> Result<Value> {
    let value = checked(value)?;
    let code = value["exit_code"]
        .as_i64()
        .ok_or_else(|| Error::Unavailable("invalid command result".into()))?;
    let stdout = value["stdout"]
        .as_str()
        .ok_or_else(|| Error::Unavailable("invalid command stdout".into()))?;
    let stderr = value["stderr"]
        .as_str()
        .ok_or_else(|| Error::Unavailable("invalid command stderr".into()))?;
    Ok(json!({"returncode":code,"stdout":stdout,"stderr":stderr}))
}
pub fn list_result(value: Value) -> Result<Value> {
    let value = checked(value)?;
    let entries = value["entries"]
        .as_array()
        .ok_or_else(|| Error::Unavailable("invalid file listing".into()))?;
    let items = entries.iter().map(|entry| {
        let directory = entry["type"] == "dir";
        let seconds = entry["modified_time"].as_f64().filter(|s| s.is_finite() && *s >= 0.0 && *s < i64::MAX as f64)
            .ok_or_else(|| Error::Unavailable("invalid file modification time".into()))?;
        let date = chrono::DateTime::from_timestamp(seconds as i64, ((seconds.fract()) * 1e9) as u32)
            .ok_or_else(|| Error::Unavailable("invalid file modification time".into()))?;
        Ok(json!({"name":entry["name"],"path":entry["path"],"size":entry["size"],"is_directory":directory,"type":if directory {"directory"} else {"file"},"modified_time":date.to_rfc3339()}))
    }).collect::<Result<Vec<_>>>()?;
    Ok(json!({"items":items}))
}
