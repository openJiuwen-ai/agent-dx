use super::resolver::AccessKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDirectPath {
    pub instance_id: String,
    pub target_port: u16,
    pub stripped_path: String,
    pub access_kind: AccessKind,
}

pub fn parse_direct_path(
    path: &str,
    default_direct_port: u16,
    default_tunnel_port: u16,
) -> Option<ParsedDirectPath> {
    let (normalized, access_kind) = if let Some(rest) = path.strip_prefix("/direct/") {
        (
            normalize_alias_path(rest, default_direct_port)?,
            AccessKind::Direct,
        )
    } else if let Some(rest) = path.strip_prefix("/tunnel/") {
        if rest.is_empty() {
            return None;
        }
        let mut segments = rest.splitn(3, '/');
        let instance_id = segments.next()?;
        let second = segments.next();
        match second.and_then(|value| value.parse::<u16>().ok()) {
            Some(port) if port != 0 => {
                let tail = segments.next().unwrap_or("");
                (format!("/{instance_id}/{port}/{tail}"), AccessKind::Tunnel)
            }
            _ => {
                let tail = second.unwrap_or("");
                (
                    format!("/{instance_id}/{default_tunnel_port}/{tail}"),
                    AccessKind::Tunnel,
                )
            }
        }
    } else {
        (path.to_owned(), AccessKind::PortForwarding)
    };

    let rest = normalized.strip_prefix('/')?;
    let mut segments = rest.splitn(3, '/');
    let instance_id = segments.next()?.trim();
    let target_port = segments.next()?.parse::<u16>().ok()?;
    if instance_id.is_empty() || target_port == 0 {
        return None;
    }
    let tail = segments.next().unwrap_or("");
    Some(ParsedDirectPath {
        instance_id: instance_id.to_owned(),
        target_port,
        stripped_path: if tail.is_empty() {
            "/".to_owned()
        } else {
            format!("/{tail}")
        },
        access_kind,
    })
}

fn normalize_alias_path(rest: &str, default_port: u16) -> Option<String> {
    if rest.is_empty() || default_port == 0 {
        return None;
    }
    let mut segments = rest.splitn(2, '/');
    let instance_id = segments.next()?.trim();
    if instance_id.is_empty() {
        return None;
    }
    let tail = segments.next().unwrap_or("");
    Some(format!("/{instance_id}/{default_port}/{tail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_direct_and_tunnel_alias_paths() {
        assert_eq!(
            parse_direct_path("/instance-a/8080/api", 50090, 8765).unwrap(),
            ParsedDirectPath {
                instance_id: "instance-a".into(),
                target_port: 8080,
                stripped_path: "/api".into(),
                access_kind: AccessKind::PortForwarding,
            }
        );
        assert_eq!(
            parse_direct_path("/tunnel/instance-a", 50090, 8765)
                .unwrap()
                .target_port,
            8765
        );
        assert_eq!(
            parse_direct_path("/direct/instance-a/upload/status", 50090, 8765).unwrap(),
            ParsedDirectPath {
                instance_id: "instance-a".into(),
                target_port: 50090,
                stripped_path: "/upload/status".into(),
                access_kind: AccessKind::Direct,
            }
        );
        assert!(parse_direct_path("/direct/", 50090, 8765).is_none());
    }
}
