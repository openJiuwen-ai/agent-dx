use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket = std::env::args()
        .nth(1)
        .unwrap_or("/run/adx/execd.sock".into());
    let mut connection = UnixStream::connect(socket)?;
    connection.set_read_timeout(Some(Duration::from_secs(180)))?;
    let body = "{\"timeoutSeconds\":120}";
    write!(
        connection,
        "POST /checkpoint HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    )?;
    let mut result = Vec::new();
    connection.read_to_end(&mut result)?;
    let result = String::from_utf8(result)?;
    print!("{result}");
    if !result.starts_with("HTTP/1.1 200") {
        return Err("checkpoint rejected".into());
    }
    Ok(())
}
