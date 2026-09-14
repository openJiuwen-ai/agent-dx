use crate::pb::port_server::Port;
use crate::pb::{ListPortsRequest, ListPortsResponse, ListenPort};
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct PortSvc;

// LISTEN state is 0A in /proc/net/tcp.
const TCP_LISTEN: &str = "0A";

/// Parse a /proc/net/tcp(6) file and collect LISTEN ports.
fn parse_proc_net_tcp(content: &str, ipv6: bool, out: &mut Vec<ListenPort>) {
    for line in content.lines().skip(1) {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 || f[3] != TCP_LISTEN {
            continue;
        }
        // local_address = "HEXIP:HEXPORT"
        let (hex_ip, hex_port) = match f[1].split_once(':') {
            Some(v) => v,
            None => continue,
        };
        let port = match u32::from_str_radix(hex_port, 16) {
            Ok(p) => p,
            Err(_) => continue,
        };
        out.push(ListenPort {
            port,
            address: decode_hex_ip(hex_ip, ipv6),
            ipv6,
        });
    }
}

/// /proc/net/tcp stores IPs as little-endian hex. tcp has 8 hex chars for IPv4; tcp6 has 32 for IPv6.
fn decode_hex_ip(hex: &str, ipv6: bool) -> String {
    if !ipv6 && hex.len() == 8 {
        let b: Vec<u8> = (0..4)
            .filter_map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
            .collect();
        if b.len() == 4 {
            // Little-endian: reverse bytes.
            return format!("{}.{}.{}.{}", b[3], b[2], b[1], b[0]);
        }
    }
    if ipv6 && hex.len() == 32 {
        // Group every 4 bytes (8 hex chars), little-endian within each group, and output colon-separated hex segments.
        let mut segs = Vec::new();
        for g in 0..4 {
            let grp = &hex[g * 8..g * 8 + 8];
            let bytes: Vec<u8> = (0..4)
                .filter_map(|i| u8::from_str_radix(&grp[i * 2..i * 2 + 2], 16).ok())
                .collect();
            if bytes.len() == 4 {
                segs.push(format!("{:02x}{:02x}", bytes[3], bytes[2]));
                segs.push(format!("{:02x}{:02x}", bytes[1], bytes[0]));
            }
        }
        return segs.join(":");
    }
    hex.to_string()
}

#[tonic::async_trait]
impl Port for PortSvc {
    async fn list_ports(
        &self,
        _: Request<ListPortsRequest>,
    ) -> Result<Response<ListPortsResponse>, Status> {
        let mut ports = Vec::new();
        if let Ok(c) = tokio::fs::read_to_string("/proc/net/tcp").await {
            parse_proc_net_tcp(&c, false, &mut ports);
        }
        if let Ok(c) = tokio::fs::read_to_string("/proc/net/tcp6").await {
            parse_proc_net_tcp(&c, true, &mut ports);
        }
        Ok(Response::new(ListPortsResponse { ports }))
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn parses_ipv4_listen() {
        // Port 0x1538=5432; address 0100007F=127.0.0.1 in little-endian form.
        let sample = "  sl  local_address rem_address   st ...\n   0: 0100007F:1538 00000000:0000 0A 00000000\n   1: 0100007F:0050 0100007F:9999 01 00000000\n";
        let mut out = Vec::new();
        parse_proc_net_tcp(sample, false, &mut out);
        assert_eq!(out.len(), 1); // Only the 0A (LISTEN) line remains.
        assert_eq!(out[0].port, 5432);
        assert_eq!(out[0].address, "127.0.0.1");
    }
}
