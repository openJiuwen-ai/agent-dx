//! Transport-neutral template initialization, consumed only for a missing Binding.
use crate::mounts::{parameter_name, validate_parameters, Parameters};

pub const HEADER: &str = "x-adx-parameters";

pub fn parse(input: &[u8]) -> Result<Parameters, String> {
    if input.len() > 65536 {
        return Err("parameters exceed size limit".into());
    }
    let input = std::str::from_utf8(input).map_err(|_| "parameters must be UTF-8")?;
    let mut parameters = Parameters::new();
    if input.is_empty() {
        return Ok(parameters);
    }
    for pair in input.split('&') {
        let (key, value) = pair
            .split_once('=')
            .ok_or("parameters require key=value pairs")?;
        let key = decode(key)?;
        let value = decode(value)?;
        if parameters.insert(key, value).is_some() {
            return Err("duplicate parameter name".into());
        }
    }
    validate_parameters(&parameters)?;
    Ok(parameters)
}
fn decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    for index in 0..bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err("invalid parameter escape".into());
        }
    }
    percent_encoding::percent_decode_str(&value.replace('+', " "))
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| "parameters must be UTF-8".into())
}
pub fn encode(parameters: &Parameters) -> Vec<u8> {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(parameters)
        .finish()
        .into_bytes()
}
pub fn render(value: &str, parameters: Option<&Parameters>) -> Result<String, String> {
    let mut remaining = value;
    let mut output = String::new();
    while let Some(start) = remaining.find("${") {
        output.push_str(&remaining[..start]);
        let tail = &remaining[start + 2..];
        let end = tail.find('}').ok_or("unterminated template parameter")?;
        let name = &tail[..end];
        if !parameter_name(name) {
            return Err("invalid template parameter name".into());
        }
        output.push_str(match parameters {
            Some(parameters) => parameters
                .get(name)
                .ok_or_else(|| format!("missing template parameter: {name}"))?,
            None => "parameter",
        });
        remaining = &tail[end + 1..];
    }
    output.push_str(remaining);
    Ok(output)
}
