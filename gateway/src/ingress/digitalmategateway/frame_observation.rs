//! HTR-C9: bounded passive WebSocket metadata capture; never a protocol executor.
use serde::Serialize;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const MAX_MESSAGE_METADATA_BYTES: usize = 64 * 1024;

#[derive(Debug, Default, Serialize)]
pub(super) struct Identity {
    pub rpc_id: Option<String>,
    pub message_id: Option<String>,
    pub submission_id: Option<String>,
    pub source_message_id: Option<String>,
    pub reply_event_id: Option<String>,
    pub task_ref: Option<String>,
    pub method: Option<String>,
    pub event: Option<String>,
    pub phase: Option<String>,
    pub trigger: Option<String>,
    pub final_reply: Option<bool>,
}

pub(super) struct MessageRecord {
    pub seq: u64,
    pub started_at_ms: u128,
    pub duration_ms: f64,
    pub bytes: u64,
    pub identity: Identity,
    pub coverage: &'static str,
}

struct Message {
    start: Instant,
    epoch: u128,
    bytes: u64,
    data: Vec<u8>,
    gap: Option<&'static str>,
}

struct Frame {
    remaining: u64,
    total: u64,
    mask: Option<[u8; 4]>,
    fin: bool,
    data: bool,
}

#[derive(Default)]
pub(super) struct Decoder {
    header: Vec<u8>,
    frame: Option<Frame>,
    message: Option<Message>,
    seq: u64,
    disabled: bool,
}

fn epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |v| v.as_millis())
}

fn token(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(serde_json::Value::as_str)
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 200
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._:/+-".contains(&b))
        })
        .map(str::to_owned)
}

fn identity(bytes: &[u8]) -> Option<Identity> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let object = value.as_object()?;
    let params = value.get("params");
    Some(Identity {
        rpc_id: token(object.get("id")),
        message_id: token(object.get("message_id"))
            .or_else(|| token(params.and_then(|p| p.get("message_id")))),
        submission_id: token(params.and_then(|p| p.get("submission_id"))),
        source_message_id: token(object.get("source_message_id")),
        reply_event_id: token(object.get("event_id")),
        task_ref: token(object.get("task_ref")),
        method: token(object.get("method")),
        event: token(object.get("event")),
        phase: token(object.get("phase")),
        trigger: token(object.get("trigger")),
        final_reply: object.get("final").and_then(serde_json::Value::as_bool),
    })
}

impl Decoder {
    pub(super) fn feed(&mut self, mut bytes: &[u8], mut emit: impl FnMut(MessageRecord)) {
        if self.disabled {
            return;
        }
        while !bytes.is_empty() || self.frame.as_ref().is_some_and(|f| f.remaining == 0) {
            if self.frame.is_none() {
                let need = if self.header.len() < 2 {
                    2
                } else {
                    2 + match self.header[1] & 127 {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    } + if self.header[1] & 128 != 0 { 4 } else { 0 }
                };
                let n = (need - self.header.len()).min(bytes.len());
                self.header.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if self.header.len() < need {
                    break;
                }
                if need == 2 && ((self.header[1] & 127) >= 126 || self.header[1] & 128 != 0) {
                    continue;
                }
                let first = self.header[0];
                let second = self.header[1];
                let (length, offset) = match second & 127 {
                    126 => (
                        u64::from(u16::from_be_bytes([self.header[2], self.header[3]])),
                        4,
                    ),
                    127 => {
                        let mut b = [0; 8];
                        b.copy_from_slice(&self.header[2..10]);
                        let length = u64::from_be_bytes(b);
                        if length >> 63 != 0 {
                            self.disabled = true;
                            return;
                        }
                        (length, 10)
                    }
                    n => (u64::from(n), 2),
                };
                let opcode = first & 15;
                if opcode == 1 || opcode == 2 {
                    self.message = Some(Message {
                        start: Instant::now(),
                        epoch: epoch(),
                        bytes: 0,
                        data: Vec::new(),
                        gap: if opcode == 2 {
                            Some("binary_message")
                        } else {
                            None
                        },
                    });
                }
                if let Some(message) = &mut self.message {
                    if first & 112 != 0 {
                        message.gap = Some("extension_not_decoded");
                    }
                }
                let mask = if second & 128 != 0 {
                    let mut key = [0; 4];
                    key.copy_from_slice(&self.header[offset..offset + 4]);
                    Some(key)
                } else {
                    None
                };
                self.frame = Some(Frame {
                    remaining: length,
                    total: length,
                    mask,
                    fin: first & 128 != 0,
                    data: opcode <= 2,
                });
                self.header.clear();
            }
            if let Some(frame) = &mut self.frame {
                let n =
                    usize::try_from(frame.remaining.min(bytes.len() as u64)).unwrap_or(bytes.len());
                if frame.data {
                    if let Some(message) = &mut self.message {
                        message.bytes = message.bytes.saturating_add(n as u64);
                        if message.gap.is_none() {
                            if n > MAX_MESSAGE_METADATA_BYTES.saturating_sub(message.data.len()) {
                                message.data.clear();
                                message.gap = Some("message_metadata_limit");
                            } else {
                                let offset = (frame.total - frame.remaining) % 4;
                                for (i, byte) in bytes[..n].iter().enumerate() {
                                    message.data.push(
                                        *byte
                                            ^ frame
                                                .mask
                                                .map_or(0, |mask| mask[(offset as usize + i) % 4]),
                                    );
                                }
                            }
                        }
                    }
                }
                frame.remaining -= n as u64;
                bytes = &bytes[n..];
                if frame.remaining != 0 {
                    break;
                }
            }
            if let Some(frame) = self.frame.take() {
                if frame.data && frame.fin {
                    if let Some(message) = self.message.take() {
                        self.seq = self.seq.saturating_add(1);
                        let parsed = if message.gap.is_none() {
                            identity(&message.data)
                        } else {
                            None
                        };
                        let coverage = message.gap.unwrap_or(if parsed.is_some() {
                            "metadata_observed"
                        } else {
                            "non_json_message"
                        });
                        emit(MessageRecord {
                            seq: self.seq,
                            started_at_ms: message.epoch,
                            duration_ms: message.start.elapsed().as_secs_f64() * 1000.0,
                            bytes: message.bytes,
                            identity: parsed.unwrap_or_default(),
                            coverage,
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(data: &[u8], opcode: u8, fin: bool, masked: bool) -> Vec<u8> {
        let mut out = vec![opcode | if fin { 128 } else { 0 }];
        let mask = if masked { 128 } else { 0 };
        match data.len() {
            n if n < 126 => out.push(mask | n as u8),
            n if n < 65536 => {
                out.push(mask | 126);
                out.extend((n as u16).to_be_bytes());
            }
            n => {
                out.push(mask | 127);
                out.extend((n as u64).to_be_bytes());
            }
        }
        let key = [3, 17, 99, 201];
        if masked {
            out.extend(key);
        }
        out.extend(
            data.iter()
                .enumerate()
                .map(|(i, b)| *b ^ if masked { key[i % 4] } else { 0 }),
        );
        out
    }
    #[test]
    fn fragmented_masked_message_with_interleaved_ping_has_one_identity() {
        let data=br#"{"id":"rpc-1","method":"hive.message.send","params":{"message_id":"m-1","text":"PRIVATE"}}"#;
        let mut wire = frame(&data[..30], 1, false, true);
        wire.extend(frame(b"ping", 9, true, true));
        wire.extend(frame(&data[30..], 0, true, true));
        let mut decoder = Decoder::default();
        let mut records = vec![];
        for byte in wire {
            decoder.feed(&[byte], |r| records.push(r));
        }
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].identity.rpc_id.as_deref(), Some("rpc-1"));
        assert_eq!(records[0].identity.message_id.as_deref(), Some("m-1"));
        assert!(!serde_json::to_string(&records[0].identity)
            .unwrap()
            .contains("PRIVATE"));
    }
    #[test]
    fn oversized_message_does_not_allocate_its_length_and_next_message_is_observed() {
        let data = vec![b'x'; MAX_MESSAGE_METADATA_BYTES + 1];
        let mut wire = frame(&data, 1, true, false);
        wire.extend(frame(
            br#"{"event":"hive.reply","message_id":"m-2"}"#,
            1,
            true,
            false,
        ));
        let mut decoder = Decoder::default();
        let mut records = vec![];
        for chunk in wire.chunks(333) {
            decoder.feed(chunk, |r| records.push(r));
        }
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].coverage, "message_metadata_limit");
        assert_eq!(records[1].identity.message_id.as_deref(), Some("m-2"));
        assert_eq!(records[1].seq, 2);
    }
    #[test]
    fn malformed_and_compressed_input_is_never_reported_as_complete_identity() {
        let mut decoder = Decoder::default();
        let mut records = vec![];
        let mut wire = frame(b"PRIVATE BAD JSON", 1, true, false);
        let mut compressed = frame(b"PRIVATE", 1, true, false);
        compressed[0] |= 64;
        wire.extend(compressed);
        decoder.feed(&wire, |r| records.push(r));
        assert_eq!(records[0].coverage, "non_json_message");
        assert_eq!(records[1].coverage, "extension_not_decoded");
        assert!(records.iter().all(|r| r.identity.rpc_id.is_none()));
    }
}
