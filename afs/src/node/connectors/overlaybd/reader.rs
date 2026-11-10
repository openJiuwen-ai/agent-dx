use super::lsmt::{Index, MAX_INDEX_BYTES, METADATA_SIZE, Mapping, Metadata, SECTOR_SIZE};
use reqwest::{Client, StatusCode, Url, header};
use serde::Deserialize;
use std::{io, path::Path, time::Duration};

const MAX_LAYERS: usize = 32;
const RANGE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImageConfig {
    #[serde(default)]
    repo_blob_url: String,
    lowers: Vec<LayerConfig>,
    #[serde(rename = "resultFile")]
    _result_file: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LayerConfig {
    digest: String,
    size: u64,
    #[serde(default)]
    repo_blob_url: String,
    #[serde(rename = "dir")]
    _dir: Option<String>,
}

struct Blob {
    url: Url,
    size: u64,
}

struct Layer {
    blob: Blob,
    index: Index,
}

pub(super) struct Image {
    client: Client,
    layers: Vec<Layer>,
    size: u64,
}

impl Blob {
    fn from_config(layer: LayerConfig, default_url: &str) -> io::Result<Self> {
        let digest = layer.digest.strip_prefix("sha256:").filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        });
        if digest.is_none() || layer.size < 2 * METADATA_SIZE as u64 {
            return Err(invalid(
                "expected a digest-pinned OverlayBD layer and its blob size",
            ));
        }
        let base = if layer.repo_blob_url.is_empty() {
            default_url
        } else {
            &layer.repo_blob_url
        };
        let mut url = Url::parse(base).map_err(|_| invalid("invalid OverlayBD blob URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid("invalid OverlayBD blob URL"));
        }
        url.path_segments_mut()
            .map_err(|_| invalid("invalid OverlayBD blob path"))?
            .pop_if_empty()
            .push(&layer.digest);
        Ok(Self {
            url,
            size: layer.size,
        })
    }

    async fn read(&self, client: &Client, offset: u64, output: &mut [u8]) -> io::Result<()> {
        if output.is_empty() {
            return Ok(());
        }
        let last = offset
            .checked_add(output.len() as u64 - 1)
            .filter(|last| *last < self.size)
            .ok_or_else(|| invalid("OverlayBD read exceeds blob size"))?;
        let mut response = client
            .get(self.url.clone())
            .header(header::RANGE, format!("bytes={offset}-{last}"))
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(io::Error::other)?;
        let expected = format!("bytes {offset}-{last}/{}", self.size);
        if response.status() != StatusCode::PARTIAL_CONTENT
            || response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                != Some(expected.as_str())
            || response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|value| value != "identity")
        {
            return Err(invalid("AFS did not serve the exact OverlayBD blob range"));
        }
        let mut remaining = output;
        while let Some(chunk) = response.chunk().await.map_err(io::Error::other)? {
            let (destination, tail) = remaining
                .split_at_mut_checked(chunk.len())
                .ok_or_else(|| invalid("oversized OverlayBD blob response"))?;
            destination.copy_from_slice(&chunk);
            remaining = tail;
        }
        if !remaining.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short OverlayBD blob response",
            ));
        }
        Ok(())
    }
}

impl Image {
    pub(super) async fn open(path: &Path) -> io::Result<Self> {
        let config: ImageConfig = serde_json::from_slice(&tokio::fs::read(path).await?)?;
        if config.lowers.is_empty() || config.lowers.len() > MAX_LAYERS {
            return Err(invalid("invalid number of OverlayBD layers"));
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(RANGE_TIMEOUT)
            .build()
            .map_err(io::Error::other)?;
        let mut layers = Vec::with_capacity(config.lowers.len());
        let mut size = 0;
        let mut index_bytes_remaining = MAX_INDEX_BYTES;
        for layer in config.lowers {
            let blob = Blob::from_config(layer, &config.repo_blob_url)?;
            let mut header = [0; METADATA_SIZE];
            let mut trailer = [0; METADATA_SIZE];
            blob.read(&client, 0, &mut header).await?;
            blob.read(&client, blob.size - METADATA_SIZE as u64, &mut trailer)
                .await?;
            let metadata = Metadata::parse(&header, &trailer, blob.size)?;
            if metadata.virtual_size < size {
                return Err(invalid("an OverlayBD layer shrinks the virtual disk"));
            }
            size = metadata.virtual_size;
            index_bytes_remaining = index_bytes_remaining
                .checked_sub(metadata.index_bytes)
                .ok_or_else(|| invalid("OverlayBD layer stack exceeds the index size limit"))?;
            let mut bytes = vec![0; metadata.index_bytes];
            blob.read(&client, metadata.index_offset, &mut bytes)
                .await?;
            layers.push(Layer {
                blob,
                index: metadata.parse_index(&bytes)?,
            });
        }
        Ok(Self {
            client,
            layers,
            size,
        })
    }

    pub(super) fn sectors(&self) -> u64 {
        self.size / SECTOR_SIZE
    }

    pub(super) async fn read_at(&self, mut offset: u64, mut output: &mut [u8]) -> io::Result<()> {
        let end = offset
            .checked_add(output.len() as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| invalid("read exceeds OverlayBD virtual disk size"))?;
        while !output.is_empty() {
            let mut next = end;
            let mut source = None;
            for layer in self.layers.iter().rev() {
                let (mapping, boundary) = layer.index.lookup(offset);
                next = next.min(boundary);
                if let Some(mapping) = mapping {
                    if let Mapping::Data(physical) = mapping {
                        source = Some((&layer.blob, physical));
                    }
                    break;
                }
            }
            let length = usize::try_from(next - offset).map_err(io::Error::other)?;
            let (current, rest) = output.split_at_mut(length);
            match source {
                Some((blob, physical)) => blob.read(&self.client, physical, current).await?,
                None => current.fill(0),
            }
            output = rest;
            offset = next;
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Path as RoutePath, State},
        http::HeaderMap,
        response::IntoResponse,
        routing::get,
    };
    use std::{collections::BTreeMap, sync::Arc};

    #[tokio::test]
    async fn reads_across_layers_and_never_fetches_explicit_zeroes() {
        let base = format!("sha256:{}", "a".repeat(64));
        let top = format!("sha256:{}", "b".repeat(64));
        let blobs = BTreeMap::from([
            (base.clone(), layer(&[(0, 4, 8, false)], 1)),
            (top.clone(), layer(&[(1, 1, 8, false), (2, 1, 0, true)], 2)),
        ]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/blobs", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/blobs/{digest}", get(range))
            .with_state(Arc::new(blobs));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = tempfile::NamedTempFile::new().unwrap();
        serde_json::to_writer(
            config.as_file(),
            &serde_json::json!({
                "repoBlobUrl": url,
                "lowers": [{"digest": base, "size": 16384}, {"digest": top, "size": 16384}],
            }),
        )
        .unwrap();

        let image = Image::open(config.path()).await.unwrap();
        assert_eq!(image.sectors(), 5);
        let mut data = vec![0xff; 5 * 512];
        image.read_at(0, &mut data).await.unwrap();
        assert_eq!(
            data,
            [[1; 512], [2; 512], [0; 512], [1; 512], [0; 512]].concat()
        );
        assert!(image.read_at(512, &mut data).await.is_err());
        server.abort();
    }

    fn layer(entries: &[(u64, u64, u64, bool)], contents: u8) -> Vec<u8> {
        let mut file = vec![0; 16384];
        let mut header = [0; METADATA_SIZE];
        header[..24].copy_from_slice(
            b"LSMT\0\x01\x02\0\x65\x7e\x63\xd2\x94\x44\x08\x4c\xa2\xd2\xc8\xec\x4f\xcf\xae\x8a",
        );
        header[24..28].copy_from_slice(&390u32.to_le_bytes());
        header[28..32].copy_from_slice(&7u32.to_le_bytes());
        header[132..134].copy_from_slice(&[1, 1]);
        file[..METADATA_SIZE].copy_from_slice(&header);
        header[28..32].copy_from_slice(&6u32.to_le_bytes());
        header[32..40].copy_from_slice(&8192u64.to_le_bytes());
        header[40..48].copy_from_slice(&(entries.len() as u64).to_le_bytes());
        header[48..56].copy_from_slice(&(5 * SECTOR_SIZE).to_le_bytes());
        file[12288..].copy_from_slice(&header);
        for (record, &(start, length, physical, zero)) in
            file[8192..].chunks_exact_mut(16).zip(entries)
        {
            record[..8].copy_from_slice(&(start | (length << 50)).to_le_bytes());
            record[8..].copy_from_slice(&(physical | (u64::from(zero) << 55)).to_le_bytes());
        }
        file[4096..8192].fill(contents);
        file
    }

    async fn range(
        State(blobs): State<Arc<BTreeMap<String, Vec<u8>>>>,
        RoutePath(digest): RoutePath<String>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let data = &blobs[&digest];
        let (start, end) = headers[header::RANGE]
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let (start, end) = (
            start.parse::<usize>().unwrap(),
            end.parse::<usize>().unwrap(),
        );
        // No layer payload at all is expected for the explicitly zeroed block.
        assert!(!(digest.ends_with('b') && start == 4096 && end > 4607));
        (
            StatusCode::PARTIAL_CONTENT,
            [(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", data.len()),
            )],
            data[start..=end].to_vec(),
        )
    }
}
