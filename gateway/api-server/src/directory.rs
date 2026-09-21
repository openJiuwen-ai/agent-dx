//! Complete node views expire independently of the watch transport.
use adx_protocol::control as pb;
use std::time::{Duration, Instant};
#[derive(Default)]
pub(crate) struct Directory {
    nodes: Vec<pb::NodeEndpoint>,
    valid_until: Option<Instant>,
    next: usize,
}
impl Directory {
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.valid_until = None;
    }
    pub fn update(&mut self, frame: pb::NodeDirectory) -> Result<(), tonic::Status> {
        let mut ids = std::collections::BTreeSet::new();
        if frame.epoch == 0
            || frame.valid_for_millis == 0
            || frame.nodes.iter().any(|n| {
                n.node_id.is_empty()
                    || n.address.is_empty()
                    || n.session_id.is_empty()
                    || !ids.insert(n.node_id.clone())
            })
        {
            self.clear();
            return Err(tonic::Status::data_loss("invalid node directory"));
        }
        self.nodes = frame.nodes;
        self.valid_until =
            Some(Instant::now() + Duration::from_millis(frame.valid_for_millis.min(5000)));
        Ok(())
    }
    pub fn select(&mut self) -> Option<pb::NodeEndpoint> {
        if self.valid_until.is_none_or(|t| Instant::now() >= t) || self.nodes.is_empty() {
            return None;
        }
        self.next %= self.nodes.len();
        let node = self.nodes[self.next].clone();
        self.next += 1;
        Some(node)
    }
    pub fn snapshot(&self) -> Option<Vec<pb::NodeEndpoint>> {
        if self.valid_until.is_none_or(|t| Instant::now() >= t) {
            return None;
        }
        Some(self.nodes.clone())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotates_only_fresh_full_views_and_replaces_sessions() {
        let mut d = Directory::default();
        assert!(d.select().is_none());
        let node = |id: &str| pb::NodeEndpoint {
            node_id: id.into(),
            address: format!("{id}:9000"),
            session_id: "boot".into(),
            ..Default::default()
        };
        d.update(pb::NodeDirectory {
            epoch: 1,
            nodes: vec![node("a"), node("b")],
            valid_for_millis: 1000,
        })
        .unwrap();
        assert_eq!(
            d.snapshot()
                .unwrap()
                .into_iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert_eq!(
            (
                d.select().unwrap().node_id,
                d.select().unwrap().node_id,
                d.select().unwrap().node_id
            ),
            ("a".into(), "b".into(), "a".into())
        );
        d.valid_until = Some(Instant::now());
        assert!(d.select().is_none());
        assert!(d.snapshot().is_none());
        d.update(pb::NodeDirectory {
            epoch: 2,
            nodes: vec![pb::NodeEndpoint {
                session_id: "new".into(),
                ..node("a")
            }],
            valid_for_millis: 1000,
        })
        .unwrap();
        assert_eq!(d.select().unwrap().session_id, "new");
        d.clear();
        assert!(d.select().is_none());
        assert!(d.snapshot().is_none());
    }
}
