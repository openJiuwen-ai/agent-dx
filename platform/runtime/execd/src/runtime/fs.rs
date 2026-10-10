//! Rust implementation of akernel `fs_*` methods. Return dicts strictly match `akernel_sdk/environment.py` and always include `error`.

use super::codec::{kw_str, map_value};
use rmpv::Value;
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

fn nil() -> Value {
    Value::Nil
}
fn err(msg: String) -> Value {
    Value::from(msg)
}

fn kw_i64(kw: &BTreeMap<String, Value>, key: &str) -> Option<i64> {
    kw.get(key).and_then(|v| v.as_i64())
}

fn decode_hex(data: &str) -> Result<Vec<u8>, String> {
    if !data.len().is_multiple_of(2) {
        return Err("hex data length must be even".to_string());
    }
    (0..data.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&data[i..i + 2], 16).map_err(|e| format!("invalid hex data: {e}"))
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// EntryInfo has six fields: name/path/type/size/permissions/modified_time.
fn entry_fields(path: &str) -> Vec<(&'static str, Value)> {
    let p = Path::new(path);
    let name = p
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    let (typ, size, perms, mtime) = match fs::symlink_metadata(p) {
        Ok(m) => {
            let typ = if m.is_dir() {
                "dir"
            } else if m.file_type().is_symlink() {
                "symlink"
            } else {
                "file"
            };
            #[cfg(unix)]
            let perms = {
                use std::os::unix::fs::PermissionsExt;
                format!("{:o}", m.permissions().mode() & 0o777)
            };
            #[cfg(not(unix))]
            let perms = String::from("0");
            let mtime = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            (typ, m.len() as i64, perms, mtime)
        }
        Err(_) => ("file", 0, String::from("0"), 0.0),
    };
    vec![
        ("name", Value::from(name)),
        ("path", Value::from(path)),
        ("type", Value::from(typ)),
        ("size", Value::from(size)),
        ("permissions", Value::from(perms)),
        ("modified_time", Value::F64(mtime)),
    ]
}

/// fs_read returns {data, error}; data is hex when binary=true.
pub fn fs_read(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let binary = kw.get("binary").and_then(|v| v.as_bool()).unwrap_or(false);
    match fs::read(&path) {
        Ok(bytes) => {
            let data = if binary {
                bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
            } else {
                String::from_utf8_lossy(&bytes).into_owned()
            };
            map_value(vec![("data", Value::from(data)), ("error", nil())])
        }
        Err(e) => map_value(vec![("data", nil()), ("error", err(e.to_string()))]),
    }
}

/// fs_write → {path, name, type, size, error}。
pub fn fs_write(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let binary = kw.get("binary").and_then(|v| v.as_bool()).unwrap_or(false);
    let data = kw_str(kw, "data").unwrap_or_default();
    let bytes: Vec<u8> = if binary {
        (0..data.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(data.get(i..i + 2)?, 16).ok())
            .collect()
    } else {
        data.into_bytes()
    };
    if let Some(parent) = Path::new(&path).parent() {
        let _ = fs::create_dir_all(parent);
    }
    match fs::write(&path, &bytes) {
        Ok(_) => {
            let mut f = entry_fields(&path);
            f.retain(|(k, _)| matches!(*k, "name" | "path" | "type" | "size"));
            f.push(("error", nil()));
            map_value(f)
        }
        Err(e) => map_value(vec![
            ("path", Value::from(path)),
            ("name", Value::from("")),
            ("type", Value::from("")),
            ("size", Value::from(0i64)),
            ("error", err(e.to_string())),
        ]),
    }
}

/// fs_write_chunk → {path, offset, bytes_written, error}.
/// Data is hex encoded so frontend WebSocket can stream binary frames without
/// holding a whole file in one invoke payload. The caller supplies the absolute
/// write offset; offset=0 truncates/creates the file for a new upload.
pub fn fs_write_chunk(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let offset = kw_i64(kw, "offset").unwrap_or(0).max(0) as u64;
    let data = kw_str(kw, "data").unwrap_or_default();
    let bytes = match decode_hex(&data) {
        Ok(bytes) => bytes,
        Err(e) => {
            return map_value(vec![
                ("path", Value::from(path)),
                ("offset", Value::from(offset as i64)),
                ("bytes_written", Value::from(0i64)),
                ("error", err(e)),
            ])
        }
    };
    if let Some(parent) = Path::new(&path).parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut open = OpenOptions::new();
    open.create(true).write(true);
    if offset == 0 {
        open.truncate(true);
    }
    match open.open(&path).and_then(|mut f| {
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(&bytes)?;
        Ok(())
    }) {
        Ok(_) => map_value(vec![
            ("path", Value::from(path)),
            ("offset", Value::from(offset as i64)),
            ("bytes_written", Value::from(bytes.len() as i64)),
            ("error", nil()),
        ]),
        Err(e) => map_value(vec![
            ("path", Value::from(path)),
            ("offset", Value::from(offset as i64)),
            ("bytes_written", Value::from(0i64)),
            ("error", err(e.to_string())),
        ]),
    }
}

/// fs_read_chunk → {path, offset, data, bytes_read, eof, error}.
/// Returned data is hex encoded.
pub fn fs_read_chunk(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let offset = kw_i64(kw, "offset").unwrap_or(0).max(0) as u64;
    let limit = kw_i64(kw, "limit")
        .unwrap_or(64 * 1024)
        .clamp(1, 4 * 1024 * 1024) as usize;

    match OpenOptions::new().read(true).open(&path).and_then(|mut f| {
        let size = f.metadata().map(|m| m.len()).unwrap_or(0);
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; limit];
        let n = f.read(&mut buf)?;
        buf.truncate(n);
        Ok((buf, offset + n as u64 >= size))
    }) {
        Ok((bytes, eof)) => map_value(vec![
            ("path", Value::from(path)),
            ("offset", Value::from(offset as i64)),
            ("data", Value::from(encode_hex(&bytes))),
            ("bytes_read", Value::from(bytes.len() as i64)),
            ("eof", Value::Boolean(eof)),
            ("error", nil()),
        ]),
        Err(e) => map_value(vec![
            ("path", Value::from(path)),
            ("offset", Value::from(offset as i64)),
            ("data", Value::from("")),
            ("bytes_read", Value::from(0i64)),
            ("eof", Value::Boolean(true)),
            ("error", err(e.to_string())),
        ]),
    }
}

/// fs_list → {entries: [EntryInfo], error}。
pub fn fs_list(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let depth = kw_i64(kw, "depth").unwrap_or(1).clamp(1, 64) as usize;
    let mut entries = Vec::new();
    match collect_entries(Path::new(&path), depth, &mut entries) {
        Ok(()) => map_value(vec![("entries", Value::Array(entries)), ("error", nil())]),
        Err(e) => map_value(vec![
            ("entries", Value::Array(vec![])),
            ("error", err(e.to_string())),
        ]),
    }
}

fn collect_entries(path: &Path, depth: usize, entries: &mut Vec<Value>) -> std::io::Result<()> {
    let mut children = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    children.sort_by_key(std::fs::DirEntry::file_name);
    for child in children {
        let child_path = child.path();
        let file_type = child.file_type()?;
        entries.push(map_value(entry_fields(&child_path.to_string_lossy())));
        if depth > 1 && file_type.is_dir() {
            collect_entries(&child_path, depth - 1, entries)?;
        }
    }
    Ok(())
}

/// fs_exists → {exists}。
pub fn fs_exists(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    map_value(vec![("exists", Value::Boolean(Path::new(&path).exists()))])
}

/// fs_remove → {error}。
pub fn fs_remove(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let p = Path::new(&path);
    let r = if p.is_dir() {
        fs::remove_dir_all(p)
    } else {
        fs::remove_file(p)
    };
    match r {
        Ok(_) => map_value(vec![("error", nil())]),
        Err(e) => map_value(vec![("error", err(e.to_string()))]),
    }
}

/// fs_rename → {…EntryInfo…, error}。
pub fn fs_rename(kw: &BTreeMap<String, Value>) -> Value {
    let old = kw_str(kw, "old_path").unwrap_or_default();
    let new = kw_str(kw, "new_path").unwrap_or_default();
    match fs::rename(&old, &new) {
        Ok(_) => {
            let mut f = entry_fields(&new);
            f.push(("error", nil()));
            map_value(f)
        }
        Err(e) => map_value(vec![("error", err(e.to_string()))]),
    }
}

/// fs_make_dir → {created, error}。
pub fn fs_make_dir(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    let existed = Path::new(&path).exists();
    match fs::create_dir_all(&path) {
        Ok(_) => map_value(vec![
            ("created", Value::Boolean(!existed)),
            ("error", nil()),
        ]),
        Err(e) => map_value(vec![
            ("created", Value::Boolean(false)),
            ("error", err(e.to_string())),
        ]),
    }
}

/// fs_get_info → {…EntryInfo…, error}。
pub fn fs_get_info(kw: &BTreeMap<String, Value>) -> Value {
    let path = kw_str(kw, "path").unwrap_or_default();
    if !Path::new(&path).exists() {
        return map_value(vec![("error", err(format!("path not found: {path}")))]);
    }
    let mut f = entry_fields(&path);
    f.push(("error", nil()));
    map_value(f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn args(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect()
    }

    fn listed_paths(value: &Value) -> Vec<String> {
        let Value::Map(fields) = value else {
            panic!("expected map: {value:?}");
        };
        let entries = fields
            .iter()
            .find_map(|(key, value)| (key.as_str() == Some("entries")).then_some(value));
        let Some(Value::Array(entries)) = entries else {
            panic!("expected entries: {value:?}");
        };
        entries
            .iter()
            .map(|entry| {
                let Value::Map(fields) = entry else {
                    panic!("expected entry map: {entry:?}");
                };
                fields
                    .iter()
                    .find_map(|(key, value)| {
                        (key.as_str() == Some("path"))
                            .then(|| value.as_str().expect("path string").to_string())
                    })
                    .expect("path field")
            })
            .collect()
    }

    #[test]
    fn list_honors_requested_depth() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("adx-execd-fs-list-{}-{suffix}", std::process::id()));
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create fixture");
        fs::write(nested.join("payload.bin"), b"payload").expect("write fixture");

        let shallow = fs_list(&args(&[
            ("path", Value::from(root.to_string_lossy().as_ref())),
            ("depth", Value::from(1)),
        ]));
        assert_eq!(listed_paths(&shallow), vec![nested.to_string_lossy()]);

        let recursive = fs_list(&args(&[
            ("path", Value::from(root.to_string_lossy().as_ref())),
            ("depth", Value::from(2u64)),
        ]));
        assert_eq!(
            listed_paths(&recursive),
            vec![
                nested.to_string_lossy().to_string(),
                nested.join("payload.bin").to_string_lossy().to_string(),
            ]
        );

        fs::remove_dir_all(root).expect("remove fixture");
    }
}
