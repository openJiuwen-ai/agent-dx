//! Explicitly built measurement helper; never a product or release binary.
//!
//! BLAKE3 uses one ordinary Hasher and a reused 1MiB input. TLS uses one
//! pre-handshaken loopback connection, with the client timing first payload
//! write through the server's complete-body acknowledgement. Certificate/key
//! loading and handshaking are outside that interval. TLS authenticates bytes;
//! this probe does not add application hashing to its transfer interval.

use std::{
    error::Error,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
};
use serde_json::{Value, json};

type ProbeError = Box<dyn Error + Send + Sync>;
const BLOCK: usize = 1024 * 1024;
const MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Ordinary, single-threaded standard BLAKE3 over repeated fixed input.
    Blake3 {
        #[arg(long, default_value_t = 1024 * 1024 * 1024)]
        bytes: u64,
    },
    /// Pre-handshaken local rustls transfer; all identity files must be DER.
    Tls {
        #[arg(long, default_value_t = 1024 * 1024 * 1024)]
        bytes: u64,
        #[arg(long)]
        certificate: PathBuf,
        #[arg(long)]
        private_key: PathBuf,
        #[arg(long)]
        ca: PathBuf,
        #[arg(long, default_value = "localhost")]
        server_name: String,
    },
}

fn main() -> Result<(), ProbeError> {
    if !cfg!(target_os = "linux") {
        return Err("performance denominators must run on Linux".into());
    }
    let result = match Args::parse().command {
        Command::Blake3 { bytes } => blake3_probe(bytes)?,
        Command::Tls {
            bytes,
            certificate,
            private_key,
            ca,
            server_name,
        } => tls_probe(bytes, certificate, private_key, ca, server_name)?,
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

fn validate_bytes(bytes: u64) -> Result<(), ProbeError> {
    if bytes == 0 || bytes > MAX_BYTES || !bytes.is_multiple_of(BLOCK as u64) {
        return Err("bytes must be a positive multiple of 1MiB and no greater than 8GiB".into());
    }
    Ok(())
}

fn report(mode: &str, bytes: u64, elapsed: Duration) -> Value {
    json!({
        "schema_version": 1,
        "mode": mode,
        "bytes": bytes,
        "block_bytes": BLOCK,
        "elapsed_ns": elapsed.as_nanos(),
        "bytes_per_second": bytes as f64 / elapsed.as_secs_f64(),
    })
}

fn blake3_probe(bytes: u64) -> Result<Value, ProbeError> {
    validate_bytes(bytes)?;
    let block = vec![0x5a; BLOCK];
    let mut check = blake3::Hasher::new();
    for part in block.chunks(4096) {
        check.update(part);
    }
    if check.finalize() != blake3::hash(&block) {
        return Err("standard BLAKE3 preflight root mismatch".into());
    }
    let mut hasher = blake3::Hasher::new();
    let start = Instant::now();
    for _ in 0..bytes / BLOCK as u64 {
        hasher.update(&block);
    }
    let root = hasher.finalize();
    let elapsed = start.elapsed();
    let mut result = report("blake3_single_thread", bytes, elapsed);
    result["root"] = json!(root.to_hex().to_string());
    result["standard_root_preflight"] = json!(true);
    result["timing_boundary"] =
        json!("first Hasher::update through finalize; input allocation excluded");
    Ok(result)
}

fn configure_socket(socket: &TcpStream) -> Result<(), ProbeError> {
    socket.set_read_timeout(Some(TIMEOUT))?;
    socket.set_write_timeout(Some(TIMEOUT))?;
    socket.set_nodelay(true)?;
    Ok(())
}

fn accept_with_timeout(
    listener: &TcpListener,
    timeout: Duration,
) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((socket, address)) => {
                socket.set_nonblocking(false)?;
                return Ok((socket, address));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "TLS loopback accept timed out",
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn tls_probe(
    bytes: u64,
    certificate: PathBuf,
    private_key: PathBuf,
    ca: PathBuf,
    server_name: String,
) -> Result<Value, ProbeError> {
    validate_bytes(bytes)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cert = CertificateDer::from(fs::read(certificate)?);
    let key = PrivateKeyDer::try_from(fs::read(private_key)?).map_err(std::io::Error::other)?;
    let server_config = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(fs::read(ca)?))?;
    let client_config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(server_name)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> Result<Value, ProbeError> {
        let (mut socket, _) = accept_with_timeout(&listener, TIMEOUT)?;
        configure_socket(&socket)?;
        let mut connection = ServerConnection::new(Arc::new(server_config))?;
        while connection.is_handshaking() {
            connection.complete_io(&mut socket)?;
        }
        let mut stream = StreamOwned::new(connection, socket);
        let mut header = [0_u8; 8];
        stream.read_exact(&mut header)?;
        let expected = u64::from_be_bytes(header);
        validate_bytes(expected)?;
        let mut buffer = vec![0_u8; BLOCK];
        stream.write_all(&[1])?;
        stream.flush()?;
        let start = Instant::now();
        let mut remaining = expected;
        while remaining > 0 {
            let count = usize::try_from(remaining.min(BLOCK as u64))?;
            stream.read_exact(&mut buffer[..count])?;
            remaining -= count as u64;
        }
        stream.write_all(&expected.to_be_bytes())?;
        stream.flush()?;
        Ok(report("rustls_server", expected, start.elapsed()))
    });
    let client_result = (|| -> Result<Value, ProbeError> {
        let mut socket = TcpStream::connect_timeout(&address, TIMEOUT)?;
        configure_socket(&socket)?;
        let mut connection = ClientConnection::new(Arc::new(client_config), name)?;
        while connection.is_handshaking() {
            connection.complete_io(&mut socket)?;
        }
        let protocol = format!("{:?}", connection.protocol_version());
        let cipher_suite = format!("{:?}", connection.negotiated_cipher_suite());
        let mut stream = StreamOwned::new(connection, socket);
        let buffer = vec![0x5a; BLOCK];
        stream.write_all(&bytes.to_be_bytes())?;
        stream.flush()?;
        let mut ready = [0_u8; 1];
        stream.read_exact(&mut ready)?;
        if ready != [1] {
            return Err("invalid TLS receiver ready acknowledgement".into());
        }
        let start = Instant::now();
        for _ in 0..bytes / BLOCK as u64 {
            stream.write_all(&buffer)?;
        }
        stream.flush()?;
        let mut ack = [0_u8; 8];
        stream.read_exact(&mut ack)?;
        let elapsed = start.elapsed();
        if u64::from_be_bytes(ack) != bytes {
            return Err("TLS receiver byte count mismatch".into());
        }
        let mut result = report("rustls_loopback", bytes, elapsed);
        result["protocol"] = json!(protocol);
        result["cipher_suite"] = json!(cipher_suite);
        result["receiver_ack_bytes"] = json!(bytes);
        result["payload_buffer_bytes"] = json!(2 * BLOCK);
        result["timing_boundary"] = json!(
            "first payload write through complete receiver ACK; handshake and allocation excluded"
        );
        Ok(result)
    })();
    let server_result = server.join().map_err(|_| "TLS receiver thread failed")?;
    let mut result = client_result?;
    result["server"] = server_result?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unbounded_and_partial_inputs() {
        for bytes in [0, 1, BLOCK as u64 - 1, MAX_BYTES + BLOCK as u64] {
            assert!(validate_bytes(bytes).is_err());
        }
        assert!(validate_bytes(BLOCK as u64).is_ok());
        assert!(validate_bytes(MAX_BYTES).is_ok());
    }

    #[test]
    fn probe_root_matches_standard_whole_input() {
        let result = blake3_probe(2 * BLOCK as u64).unwrap();
        let expected = blake3::hash(&vec![0x5a; 2 * BLOCK]);
        assert_eq!(result["root"], expected.to_hex().to_string());
        assert_eq!(result["bytes"], 2 * BLOCK);
        assert_eq!(result["standard_root_preflight"], true);
    }

    #[test]
    fn absent_client_has_a_bounded_accept_deadline() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let error = accept_with_timeout(&listener, Duration::ZERO).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn pending_client_can_connect_without_waiting_for_deadline() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_server, address) = accept_with_timeout(&listener, Duration::ZERO).unwrap();
        assert_eq!(address, client.local_addr().unwrap());
    }
}
