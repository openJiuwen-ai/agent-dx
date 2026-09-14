/// pyval: cloudpickle ↔ Rust value codec for rrt runtime-mode.
///
/// For the simple types used by execute/tunnel (str, list[str], int, None, dict),
/// cloudpickle output == standard pickle (all in-band), so serde-pickle handles them.
use serde::{Deserialize, Serialize};

/// Decode a Python-pickled byte slice into a Rust value.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, serde_pickle::Error> {
    serde_pickle::from_slice(bytes, serde_pickle::DeOptions::new())
}

/// The result of an `execute` call: mirrors Python dict `{returncode, stdout, stderr}`.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecResult {
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Encode an execute result as a Python-pickle byte sequence.
pub fn encode_exec_result(
    returncode: i32,
    stdout: String,
    stderr: String,
) -> Result<Vec<u8>, serde_pickle::Error> {
    serde_pickle::to_vec(
        &ExecResult {
            returncode,
            stdout,
            stderr,
        },
        serde_pickle::SerOptions::new(),
    )
}
