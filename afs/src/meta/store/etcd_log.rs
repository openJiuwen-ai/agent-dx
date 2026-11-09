//! etcd service for the shared-service Meta log (R16, `kv_log`).
//!
//! Every compare-and-write step is one etcd transaction. An acknowledged
//! transaction is committed by the Raft majority and synced to each member's
//! WAL; a single-member etcd therefore equals one local fsync. One
//! transaction, compares and writes included, must fit the server's
//! `--max-request-bytes` (etcd default 1.5 MiB); `max_request_bytes` must be
//! set to the server's value and bounds every append and checkpoint chunk.

use afs_error::{Error, Result};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, DeleteOptions, GetOptions, KvClient, Txn, TxnOp,
    TxnOpResponse,
};

use super::{
    journal::{invalid_log, unavailable},
    kv_log::{Head, KvFuture, KvService, Swap, chunk_id_of, chunk_key, frame_key},
};

/// Room for keys, compares and transaction framing inside one request.
const REQUEST_OVERHEAD: usize = 64 * 1024;
/// Frames per range request during recovery (each at most one request).
const FRAME_PAGE: i64 = 32;
/// Writer opens retried against concurrent openers.
const OPEN_ATTEMPTS: usize = 16;
/// gRPC message bound on replies (range pages of frames, chunks).
const MESSAGE_BYTES: usize = 128 * 1024 * 1024;

/// An etcd cluster holding one Meta log under `prefix`.
pub struct EtcdService {
    kv: KvClient,
    prefix: String,
    max_request_bytes: usize,
}

fn etcd_error(error: etcd_client::Error) -> Error {
    unavailable(format!("etcd Meta log: {error}"))
}

/// The exclusive end of a prefix range (`prefix` with its last byte + 1).
fn prefix_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

fn decimal(bytes: &[u8]) -> Result<u64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or_else(|| invalid_log("etcd Meta log term is not a number"))
}

impl EtcdService {
    /// Connect to `endpoints`. `max_request_bytes` is the server's
    /// `--max-request-bytes`.
    pub async fn connect(
        endpoints: &[String],
        prefix: &str,
        max_request_bytes: usize,
    ) -> Result<Self> {
        if max_request_bytes <= 2 * REQUEST_OVERHEAD {
            return Err(Error::coded(
                afs_error::CONFIG_INVALID,
                "meta_etcd_max_request_bytes is too small",
            ));
        }
        let options = ConnectOptions::new()
            .with_connect_timeout(std::time::Duration::from_secs(10))
            .with_timeout(super::kv_log::REQUEST_TIMEOUT);
        let client = Client::connect(endpoints, Some(options))
            .await
            .map_err(etcd_error)?;
        let kv = client
            .kv_client()
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(max_request_bytes);
        Ok(Self {
            kv,
            prefix: prefix.to_owned(),
            max_request_bytes,
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}/{name}", self.prefix)
    }

    fn kv(&self) -> KvClient {
        self.kv.clone()
    }

    async fn get(&self, key: &str) -> Result<Option<(Vec<u8>, i64)>> {
        let reply = self.kv().get(key, None).await.map_err(etcd_error)?;
        Ok(reply
            .kvs()
            .first()
            .map(|kv| (kv.value().to_vec(), kv.mod_revision())))
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.kv()
            .delete(
                prefix,
                Some(DeleteOptions::new().with_range(prefix_end(prefix))),
            )
            .await
            .map_err(etcd_error)?;
        Ok(())
    }

    /// Compare-and-put outcome: the else branch reads the term.
    fn swap(&self, term: u64, reply: etcd_client::TxnResponse) -> Result<Swap> {
        if reply.succeeded() {
            return Ok(Swap::Done);
        }
        let current = reply
            .op_responses()
            .into_iter()
            .find_map(|op| match op {
                TxnOpResponse::Get(get) => {
                    Some(get.kvs().first().map_or(Ok(0), |kv| decimal(kv.value())))
                }
                _ => None,
            })
            .transpose()?
            .unwrap_or(0);
        Ok(if current == term {
            Swap::Changed
        } else {
            Swap::Fenced(current)
        })
    }

    fn compare_value(key: String, expected: Option<&[u8]>) -> Compare {
        match expected {
            Some(value) => Compare::value(key, CompareOp::Equal, value.to_vec()),
            None => Compare::version(key, CompareOp::Equal, 0),
        }
    }
}

impl KvService for EtcdService {
    fn name(&self) -> &'static str {
        "etcd"
    }

    fn append_bound_bytes(&self) -> usize {
        self.max_request_bytes - REQUEST_OVERHEAD
    }

    fn chunk_bytes(&self) -> usize {
        (self.max_request_bytes - REQUEST_OVERHEAD).min(1024 * 1024)
    }

    fn open_writer<'a>(&'a self, layout: &'a [u8]) -> KvFuture<'a, (Vec<u8>, u64)> {
        Box::pin(async move {
            let (layout_key, term_key) = (self.key("layout"), self.key("term"));
            for _ in 0..OPEN_ATTEMPTS {
                match self.get(&layout_key).await? {
                    None => {
                        // Never adopt data: the prefix must be empty.
                        let any = self
                            .kv()
                            .get(
                                format!("{}/", self.prefix),
                                Some(
                                    GetOptions::new()
                                        .with_prefix()
                                        .with_keys_only()
                                        .with_limit(1),
                                ),
                            )
                            .await
                            .map_err(etcd_error)?;
                        if !any.kvs().is_empty() {
                            return Err(invalid_log(
                                "etcd Meta log prefix is not empty but has no layout key; data retained",
                            ));
                        }
                        let reply = self
                            .kv()
                            .txn(
                                Txn::new()
                                    .when(vec![
                                        Compare::version(layout_key.clone(), CompareOp::Equal, 0),
                                        Compare::version(term_key.clone(), CompareOp::Equal, 0),
                                    ])
                                    .and_then(vec![
                                        TxnOp::put(layout_key.clone(), layout.to_vec(), None),
                                        TxnOp::put(term_key.clone(), "1", None),
                                    ]),
                            )
                            .await
                            .map_err(etcd_error)?;
                        if reply.succeeded() {
                            return Ok((layout.to_vec(), 1));
                        }
                    }
                    Some((stored, _)) => {
                        let (term, revision) = match self.get(&term_key).await? {
                            Some((value, revision)) => (decimal(&value)?, revision),
                            None => return Err(invalid_log("etcd Meta log layout without term")),
                        };
                        let next = term
                            .checked_add(1)
                            .ok_or_else(|| invalid_log("etcd Meta log term exhausted"))?;
                        let reply = self
                            .kv()
                            .txn(
                                Txn::new()
                                    .when(vec![Compare::mod_revision(
                                        term_key.clone(),
                                        CompareOp::Equal,
                                        revision,
                                    )])
                                    .and_then(vec![TxnOp::put(
                                        term_key.clone(),
                                        next.to_string(),
                                        None,
                                    )]),
                            )
                            .await
                            .map_err(etcd_error)?;
                        if reply.succeeded() {
                            return Ok((stored, next));
                        }
                    }
                }
            }
            Err(unavailable(
                "etcd Meta log writer open lost every race with concurrent openers",
            ))
        })
    }

    fn term(&self) -> KvFuture<'_, u64> {
        Box::pin(async move {
            self.get(&self.key("term"))
                .await?
                .map_or(Ok(0), |(value, _)| decimal(&value))
        })
    }

    fn head(&self) -> KvFuture<'_, Head> {
        Box::pin(async move {
            // One transaction reads the three keys at one revision.
            let reply = self
                .kv()
                .txn(Txn::new().and_then(vec![
                    TxnOp::get(self.key("term"), None),
                    TxnOp::get(self.key("tip"), None),
                    TxnOp::get(self.key("checkpoint"), None),
                ]))
                .await
                .map_err(etcd_error)?;
            let mut values = reply.op_responses().into_iter().map(|op| match op {
                TxnOpResponse::Get(get) => get.kvs().first().map(|kv| kv.value().to_vec()),
                _ => None,
            });
            let term = values.next().flatten();
            Ok(Head {
                term: term.map_or(Ok(0), |value| decimal(&value))?,
                tip: values.next().flatten(),
                checkpoint: values.next().flatten(),
            })
        })
    }

    fn append<'a>(
        &'a self,
        term: u64,
        expected_tip: Option<&'a [u8]>,
        tip: &'a [u8],
        first_sequence: u64,
        frames: &'a [Vec<u8>],
    ) -> KvFuture<'a, Swap> {
        Box::pin(async move {
            let mut puts: Vec<TxnOp> = frames
                .iter()
                .enumerate()
                .map(|(offset, frame)| {
                    TxnOp::put(
                        frame_key(&self.prefix, first_sequence + offset as u64),
                        frame.clone(),
                        None,
                    )
                })
                .collect();
            puts.push(TxnOp::put(self.key("tip"), tip.to_vec(), None));
            let reply = self
                .kv()
                .txn(
                    Txn::new()
                        .when(vec![
                            Compare::value(self.key("term"), CompareOp::Equal, term.to_string()),
                            Self::compare_value(self.key("tip"), expected_tip),
                        ])
                        .and_then(puts)
                        .or_else(vec![TxnOp::get(self.key("term"), None)]),
                )
                .await
                .map_err(etcd_error)?;
            self.swap(term, reply)
        })
    }

    fn frames(&self, from: u64, through: u64) -> KvFuture<'_, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut frames = Vec::new();
            let mut next = from;
            while next <= through {
                let reply = self
                    .kv()
                    .get(
                        frame_key(&self.prefix, next),
                        Some(
                            GetOptions::new()
                                .with_range(frame_key(&self.prefix, through + 1))
                                .with_limit(FRAME_PAGE),
                        ),
                    )
                    .await
                    .map_err(etcd_error)?;
                if reply.kvs().is_empty() {
                    break;
                }
                for kv in reply.kvs() {
                    let key = kv.key_str().map_err(etcd_error)?;
                    let sequence = key
                        .rsplit('/')
                        .next()
                        .and_then(|sequence| sequence.parse::<u64>().ok())
                        .ok_or_else(|| invalid_log("etcd Meta log frame key"))?;
                    // Absent sequences in between stay `None`.
                    while next < sequence {
                        frames.push(None);
                        next += 1;
                    }
                    frames.push(Some(kv.value().to_vec()));
                    next = sequence + 1;
                }
                if !reply.more() {
                    break;
                }
            }
            while next <= through {
                frames.push(None);
                next += 1;
            }
            Ok(frames)
        })
    }

    fn put_chunk<'a>(&'a self, id: &'a str, index: u32, bytes: &'a [u8]) -> KvFuture<'a, ()> {
        Box::pin(async move {
            self.kv()
                .put(chunk_key(&self.prefix, id, index), bytes.to_vec(), None)
                .await
                .map_err(etcd_error)?;
            Ok(())
        })
    }

    fn chunk<'a>(&'a self, id: &'a str, index: u32) -> KvFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok(self
                .get(&chunk_key(&self.prefix, id, index))
                .await?
                .map(|(value, _)| value))
        })
    }

    fn switch_checkpoint<'a>(
        &'a self,
        term: u64,
        expected: Option<&'a [u8]>,
        pointer: &'a [u8],
    ) -> KvFuture<'a, Swap> {
        Box::pin(async move {
            let reply = self
                .kv()
                .txn(
                    Txn::new()
                        .when(vec![
                            Compare::value(self.key("term"), CompareOp::Equal, term.to_string()),
                            Self::compare_value(self.key("checkpoint"), expected),
                        ])
                        .and_then(vec![TxnOp::put(
                            self.key("checkpoint"),
                            pointer.to_vec(),
                            None,
                        )])
                        .or_else(vec![TxnOp::get(self.key("term"), None)]),
                )
                .await
                .map_err(etcd_error)?;
            self.swap(term, reply)
        })
    }

    fn delete_frames_through(&self, through: u64) -> KvFuture<'_, ()> {
        Box::pin(async move {
            self.kv()
                .delete(
                    frame_key(&self.prefix, 0),
                    Some(DeleteOptions::new().with_range(frame_key(&self.prefix, through + 1))),
                )
                .await
                .map_err(etcd_error)?;
            Ok(())
        })
    }

    fn chunk_ids(&self) -> KvFuture<'_, Vec<String>> {
        Box::pin(async move {
            let reply = self
                .kv()
                .get(
                    format!("{}/chunk/", self.prefix),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await
                .map_err(etcd_error)?;
            let mut ids = Vec::new();
            for kv in reply.kvs() {
                if let Some(id) = chunk_id_of(&self.prefix, kv.key_str().map_err(etcd_error)?) {
                    ids.push(id);
                }
            }
            ids.sort();
            ids.dedup();
            Ok(ids)
        })
    }

    fn delete_chunks<'a>(&'a self, id: &'a str) -> KvFuture<'a, ()> {
        Box::pin(async move {
            self.delete_prefix(&format!("{}/chunk/{id}/", self.prefix))
                .await
        })
    }

    #[cfg(test)]
    fn dump(&self) -> KvFuture<'_, Vec<(String, Vec<u8>)>> {
        Box::pin(async move {
            let reply = self
                .kv()
                .get(
                    format!("{}/", self.prefix),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .map_err(etcd_error)?;
            let mut out = Vec::new();
            for kv in reply.kvs() {
                out.push((
                    kv.key_str().map_err(etcd_error)?.to_owned(),
                    kv.value().to_vec(),
                ));
            }
            out.sort();
            Ok(out)
        })
    }
}
