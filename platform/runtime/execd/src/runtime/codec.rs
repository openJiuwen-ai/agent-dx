//! Helpers for normalized HTTP operation arguments.
use rmpv::Value;
use std::collections::BTreeMap;

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
