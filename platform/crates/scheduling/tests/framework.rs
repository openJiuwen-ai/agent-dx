use adx_core::{Error, InstanceSpec, Resources, Result};
use adx_scheduling::{
    Candidate, Filter, Framework, Node, Placement, Score, WeightedScore, MAX_SCORE,
};
use std::sync::Arc;

struct Constant(&'static str, u32);
impl Score for Constant {
    fn name(&self) -> &'static str {
        self.0
    }
    fn score(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<u32> {
        Ok(self.1)
    }
}
fn weighted(name: &'static str, value: u32, weight: u32) -> WeightedScore {
    WeightedScore::new(Arc::new(Constant(name, value)), weight).unwrap()
}
fn request() -> InstanceSpec {
    InstanceSpec {
        env: Default::default(),
        scheduling: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runsc".into(),
        priority: 0,
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 1,
        },
    }
}
#[test]
fn invalid_profiles_and_out_of_range_scores_are_rejected() {
    assert!(WeightedScore::new(Arc::new(Constant("zero", 1)), 0).is_err());
    assert!(Framework::new(vec![], vec![weighted("", 1, 1)]).is_err());
    assert!(Framework::new(
        vec![],
        vec![weighted("duplicate", 1, 1), weighted("duplicate", 2, 2)]
    )
    .is_err());
    let framework = Framework::new(vec![], vec![weighted("bad", MAX_SCORE + 1, 1)]).unwrap();
    let request = request();
    let node = Node {
        labels: Default::default(),
        devices: vec![],
        id: "n".into(),
        capacity: request.resources,
        available: true,
    };
    assert!(matches!(
        framework.select(
            &request,
            [Candidate {
                prepared: None,
                devices: &[],
                snapshot: &Default::default(),
                node: &node,
                available: node.capacity
            }]
        ),
        Err(Error::Invalid(_))
    ));
}
#[test]
fn builtin_profile_exposes_actual_static_registration() {
    let framework = Framework::builtin(Placement::Pack);
    assert_eq!(
        framework.filter_names(),
        [
            "node-available",
            "resource-fit",
            "device-fit",
            "node-affinity",
            "instance-affinity",
            "topology-spread"
        ]
    );
    assert_eq!(
        framework.score_weights(),
        [
            ("resource-balance", 1),
            ("node-preference", 1),
            ("instance-preference", 1),
            ("topology-preference", 1)
        ]
    );
}
struct FailingFilter;
impl Filter for FailingFilter {
    fn name(&self) -> &'static str {
        "failing"
    }
    fn filter(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<bool> {
        Err(Error::Unavailable("filter observation missing".into()))
    }
}
#[test]
fn filter_failure_is_an_error_not_an_eligible_candidate() {
    let request = request();
    let node = Node {
        labels: Default::default(),
        devices: vec![],
        id: "n".into(),
        capacity: request.resources,
        available: true,
    };
    let framework = Framework::new(vec![Arc::new(FailingFilter)], vec![]).unwrap();
    assert!(matches!(
        framework.select(
            &request,
            [Candidate {
                prepared: None,
                devices: &[],
                snapshot: &Default::default(),
                node: &node,
                available: node.capacity
            }]
        ),
        Err(Error::Unavailable(_))
    ));
}

#[test]
fn high_weights_accumulate_without_wrapping_and_zero_disk_is_supported() {
    let mut request = request();
    request.resources.disk_bytes = 0;
    let node = Node {
        labels: Default::default(),
        devices: vec![],
        id: "n".into(),
        capacity: request.resources,
        available: true,
    };
    let framework = Framework::new(
        vec![],
        vec![
            weighted("a", MAX_SCORE, u32::MAX),
            weighted("b", MAX_SCORE, u32::MAX),
        ],
    )
    .unwrap();
    assert_eq!(
        framework
            .select(
                &request,
                [Candidate {
                    prepared: None,
                    devices: &[],
                    snapshot: &Default::default(),
                    node: &node,
                    available: node.capacity
                }]
            )
            .unwrap(),
        Some("n")
    );
    for placement in [Placement::Pack, Placement::Spread] {
        assert_eq!(
            Framework::builtin(placement)
                .select(
                    &request,
                    [Candidate {
                        prepared: None,
                        devices: &[],
                        snapshot: &Default::default(),
                        node: &node,
                        available: node.capacity
                    }]
                )
                .unwrap(),
            Some("n")
        );
    }
}
