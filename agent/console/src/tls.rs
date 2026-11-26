//! Shared public roots plus deployment CA for HTTP and WebSocket clients.
use std::sync::Arc;

#[derive(Clone)]
pub struct Trust {
    certificates: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub websocket: Arc<rustls::ClientConfig>,
}

impl Trust {
    pub fn load(path: Option<&str>) -> Result<Self,String> {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut certificates = Vec::new();
        if let Some(path) = path {
            let pem = std::fs::read(path).map_err(|_|"Cannot read upstream CA file")?;
            certificates = rustls_pemfile::certs(&mut pem.as_slice()).collect::<Result<Vec<_>,_>>().map_err(|_|"Invalid upstream CA PEM")?;
            if certificates.is_empty() { return Err("Upstream CA file contains no certificates".into()); }
            for cert in &certificates {
                roots.add(cert.clone()).map_err(|_|"Invalid upstream CA certificate")?;
            }
        }
        let websocket = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions().map_err(|_|"Cannot initialize upstream TLS")?
            .with_root_certificates(roots).with_no_client_auth();
        Ok(Self { certificates,websocket:Arc::new(websocket) })
    }

    pub fn http_builder(&self) -> Result<reqwest::ClientBuilder,String> {
        let mut builder = reqwest::Client::builder();
        for cert in &self.certificates {
            builder = builder.add_root_certificate(reqwest::Certificate::from_der(cert.as_ref()).map_err(|_|"Invalid upstream HTTP CA")?);
        }
        Ok(builder)
    }
}
