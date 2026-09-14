//! Encoding/decoding for yr cross-language serialized values plus kwargs parsing.
//! Single-value format: `[8B metadata][8B msgpack_size][msgpack_data][optional cloudpickle]`.
//! akernel method args/returns are simple values (str/int/dict/None), so they use msgpack.

use crate::posix::common::Arg;
use rmpv::Value;
use std::collections::BTreeMap;

/// Decode one adx-serialized value into a msgpack Value.
pub fn adx_deserialize(buf: &[u8]) -> Option<Value> {
    if buf.len() < 16 {
        return None;
    }
    // The Go libruntime path may wrap inline values as
    // [24B internal header][8B little-endian msgpack_size][msgpack_data].
    // This is the shape observed from frontend InvokeByInstanceId for sandbox
    // v1; accept it before trying the older cross-language header variants.
    if buf.len() >= 32 {
        let mut size_bytes = [0u8; 8];
        size_bytes.copy_from_slice(&buf[24..32]);
        let size = u64::from_le_bytes(size_bytes) as usize;
        if size > 0 {
            if let Some(data) = buf.get(32..32 + size) {
                if let Ok(value) = rmpv::decode::read_value(&mut &data[..]) {
                    return Some(value);
                }
            }
        }
    }
    // size is the msgpack int stored in header[8:16].
    let mut size_slice = &buf[8..16];
    if let Ok(size) = rmp::decode::read_int::<u64, _>(&mut size_slice) {
        let size = size as usize;
        if size > 0 {
            if let Some(data) = buf.get(16..16 + size) {
                if let Ok(value) = rmpv::decode::read_value(&mut &data[..]) {
                    return Some(value);
                }
            }
        }
    }

    // Some callers (notably libruntime's raw DataObject path) can already
    // strip or re-wrap the size header. Accept the cross-language form
    // `[16B zero header][msgpack]` and a bare msgpack value as fallbacks so
    // sandbox_invoke remains robust across frontend/runtime transport paths.
    rmpv::decode::read_value(&mut &buf[16..])
        .or_else(|_| rmpv::decode::read_value(&mut &buf[..]))
        .ok()
        .and_then(unwrap_msgpack_binary)
}

fn unwrap_msgpack_binary(value: Value) -> Option<Value> {
    match value {
        Value::Binary(bytes) => adx_deserialize(&bytes).or_else(|| {
            rmpv::decode::read_value(&mut &bytes[..])
                .ok()
                .and_then(unwrap_msgpack_binary)
        }),
        other => Some(other),
    }
}

/// Encode a msgpack Value into a adx-serialized value: `[16B zero header][msgpack]`.
/// split_buffer sees metadata=0 && size=0 and infers CROSS_LANGUAGE, with msgpack_data = buf[16:].
pub fn adx_serialize_value(v: &Value) -> Vec<u8> {
    let mut mp = Vec::new();
    let _ = rmpv::encode::write_value(&mut mp, v);
    let mut buf = vec![0u8; 16];
    buf.extend_from_slice(&mp);
    buf
}

/// Extract kwargs from a CallRequest arg list.
///
/// The sandbox/RRT wire contract carries either one map value or alternating
/// key/value entries starting at args[0]. Transport metadata is not part of the
/// application argument list.
pub fn parse_kwargs(args: &[Arg]) -> BTreeMap<String, Value> {
    parse_kwargs_from(args, 0).unwrap_or_default()
}

fn parse_kwargs_from(args: &[Arg], start: usize) -> Option<BTreeMap<String, Value>> {
    if start >= args.len() {
        return None;
    }
    if let Some(Value::Map(kvs)) = adx_deserialize(&args[start].value) {
        let mut m = BTreeMap::new();
        for (k, v) in kvs {
            if let Some(key) = k.as_str() {
                m.insert(key.to_string(), v);
            }
        }
        if !m.is_empty() {
            return Some(m);
        }
    }

    let mut m = BTreeMap::new();
    let mut i = start;
    while i + 1 < args.len() {
        let key = adx_deserialize(&args[i].value).and_then(|v| v.as_str().map(String::from));
        let val = adx_deserialize(&args[i + 1].value);
        if let (Some(k), Some(v)) = (key, val) {
            m.insert(k, v);
        }
        i += 2;
    }
    if m.is_empty() {
        None
    } else {
        Some(m)
    }
}

/// Read a string from kwargs.
pub fn kw_str(kw: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    kw.get(key).and_then(|v| v.as_str().map(String::from))
}

/// Build a return dict as a msgpack map.
pub fn map_value(pairs: Vec<(&str, Value)>) -> Value {
    Value::Map(
        pairs
            .into_iter()
            .map(|(k, v)| (Value::from(k), v))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_sandbox_envelope_from_arg_zero() {
        let envelope = map_value(vec![
            ("sandbox_method", Value::from("sandbox_invoke")),
            ("action", Value::from("process.exec")),
            (
                "args",
                map_value(vec![("cmd", Value::from("printf envelope"))]),
            ),
        ]);
        let args = vec![Arg {
            value: adx_serialize_value(&envelope),
            ..Default::default()
        }];

        let parsed = parse_kwargs(&args);

        assert_eq!(
            kw_str(&parsed, "sandbox_method").as_deref(),
            Some("sandbox_invoke")
        );
        assert_eq!(kw_str(&parsed, "action").as_deref(), Some("process.exec"));
    }
}
