//! Passive connection/message observations on the DigitalMate application entry.
//! Local transport acceptance is never reported as remote receipt. No retries or routing changes.
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |v| u64::try_from(v.as_millis()).unwrap_or(u64::MAX))
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Off,
    Summary,
    Trace,
}

impl Mode {
    fn parse(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some("summary") => Self::Summary,
            Some("trace") => Self::Trace,
            _ => Self::Off,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Summary => "summary",
            Self::Trace => "trace",
        }
    }
}

pub(super) struct ConnectionObservation {
    mode: Mode,
    connection_id: Uuid,
    started: Instant,
}

impl ConnectionObservation {
    pub(super) fn from_environment(application: &str) -> Self {
        let value = std::env::var("DIGITALMATE_PROFILING_MODE").ok();
        Self {
            mode: if application == "digitalmate" {
                Mode::parse(value.as_deref())
            } else {
                Mode::Off
            },
            connection_id: Uuid::new_v4(),
            started: Instant::now(),
        }
    }

    pub(super) fn stage(&self, phase: &'static str) -> Stage<'_> {
        let stage = Stage {
            observation: self,
            phase,
            started: Instant::now(),
            finished: false,
        };
        if self.mode == Mode::Trace {
            stage.emit("started", "in_progress");
        }
        stage
    }

    pub(super) fn upgrade_observation(
        &self,
    ) -> Option<crate::ingress::upgrade_observation::UpgradeObservation> {
        use crate::ingress::upgrade_observation::{ByteObserver, UpgradeObservation};
        use std::sync::{Arc, Mutex};
        if self.mode == Mode::Off {
            return None;
        }
        let connection_id = self.connection_id;
        let connection_started = self.started;
        let mode = self.mode.label();
        let observer = |boundary: &'static str| {
            let decoder = Mutex::new(super::frame_observation::Decoder::default());
            ByteObserver(Arc::new(move |bytes| {
                let Ok(mut decoder) = decoder.try_lock() else {
                    return;
                };
                decoder.feed(bytes, |record| {
                    let id = &record.identity;
                    tracing::info!(target: "digitalmate.performance",
                        schema_version = 1, event_type = "digitalmate_gateway_message_boundary",
                        event_id = %Uuid::new_v4(), connection_id = %connection_id,
                        profiling_mode = mode, boundary, frame_seq = record.seq,
                        connection_elapsed_ms = connection_started.elapsed().as_secs_f64() * 1000.0,
                        at_ms = epoch_millis(), started_at_ms = %record.started_at_ms,
                        duration_ms = record.duration_ms, byte_count = record.bytes,
                        rpc_id = id.rpc_id.as_deref(), message_id = id.message_id.as_deref(),
                        submission_id = id.submission_id.as_deref(), source_message_id = id.source_message_id.as_deref(),
                        reply_event_id = id.reply_event_id.as_deref(), task_ref = id.task_ref.as_deref(),
                        method = id.method.as_deref(), reply_event = id.event.as_deref(),
                        phase = id.phase.as_deref(), trigger = id.trigger.as_deref(),
                        final_reply = id.final_reply, coverage = record.coverage,
                        evidence_scope = "accepted_local_transport_bytes",
                        "DigitalMate message boundary");
                });
            }))
        };
        Some(UpgradeObservation {
            downstream_read: observer("gateway.client_message_received"),
            upstream_write: observer("gateway.sandbox_write_accepted"),
            upstream_read: observer("gateway.sandbox_message_received"),
            downstream_write: observer("gateway.client_write_accepted"),
        })
    }

    pub(super) fn annotate_response<B>(&self, response: &mut http::Response<B>) {
        if self.mode == Mode::Off {
            return;
        }
        // A generated observation identifier has no routing or authorization meaning.
        if let Ok(value) = http::HeaderValue::from_str(&self.connection_id.to_string()) {
            response
                .headers_mut()
                .insert("x-digitalmate-observation-id", value);
        }
    }
}

pub(super) struct Stage<'a> {
    observation: &'a ConnectionObservation,
    phase: &'static str,
    started: Instant,
    finished: bool,
}

impl Stage<'_> {
    pub(super) fn finish(mut self, status: &'static str) {
        self.emit("finished", status);
        self.finished = true;
    }

    fn emit(&self, transition: &'static str, status: &'static str) {
        if self.observation.mode == Mode::Off {
            return;
        }
        let at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|value| value.as_millis().to_string());
        let (trace_id, span_id) = adx_observability::trace::log_context();
        tracing::info!(
            target: "digitalmate.performance",
            schema_version = 1,
            event_type = "digitalmate_gateway_connection_stage",
            event_id = %Uuid::new_v4(),
            connection_id = %self.observation.connection_id,
            trace_id,
            span_id,
            profiling_mode = self.observation.mode.label(),
            phase = self.phase,
            transition,
            status,
            at_ms = at_ms.as_deref(),
            duration_ms = self.started.elapsed().as_secs_f64() * 1000.0,
            connection_elapsed_ms = self.observation.started.elapsed().as_secs_f64() * 1000.0,
            evidence_scope = "authorized_application_connection",
            frame_observation = "passive_bounded_metadata",
            "DigitalMate connection timing"
        );
    }
}

impl Drop for Stage<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Early returns, cancellation and panic cannot be distinguished here.
            self.emit("finished", "ended_without_observed_result");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn captured(mode: Mode, finish: bool) -> String {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer = Writer(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let observation = ConnectionObservation {
                mode,
                connection_id: Uuid::new_v4(),
                started: Instant::now(),
            };
            let stage = observation.stage("gateway.selection");
            if finish {
                stage.finish("ok");
            }
        });
        let bytes = buffer.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn summary_has_one_terminal_trace_has_two_events_and_off_has_none() {
        assert_eq!(captured(Mode::Summary, true).lines().count(), 1);
        let trace = captured(Mode::Trace, true);
        assert_eq!(trace.lines().count(), 2);
        assert!(!trace.contains("ended_without_observed_result"));
        assert!(captured(Mode::Off, true).is_empty());
    }

    #[test]
    fn unobserved_result_never_claims_success() {
        let output = captured(Mode::Summary, false);
        assert_eq!(output.lines().count(), 1);
        assert!(output.contains("ended_without_observed_result"));
        assert!(output.contains("passive_bounded_metadata"));
    }

    #[test]
    fn invalid_or_unset_mode_is_disabled() {
        assert!(Mode::parse(None) == Mode::Off);
        assert!(Mode::parse(Some("typo")) == Mode::Off);
    }

    #[test]
    fn response_observation_does_not_change_body_or_status_and_is_opt_in() {
        let mut response = http::Response::builder()
            .status(101)
            .body("unchanged")
            .unwrap();
        let observation = ConnectionObservation {
            mode: Mode::Trace,
            connection_id: Uuid::new_v4(),
            started: Instant::now(),
        };
        observation.annotate_response(&mut response);
        assert_eq!(response.status(), 101);
        assert_eq!(response.body(), &"unchanged");
        assert_eq!(
            response.headers()["x-digitalmate-observation-id"],
            observation.connection_id.to_string()
        );
        let mut disabled = http::Response::new("unchanged");
        ConnectionObservation {
            mode: Mode::Off,
            connection_id: Uuid::new_v4(),
            started: Instant::now(),
        }
        .annotate_response(&mut disabled);
        assert!(!disabled
            .headers()
            .contains_key("x-digitalmate-observation-id"));
    }
}
