//! Node read-path observability: fixed-cardinality work counters (`work`) and the
//! run-time-switched DFS read-timing journal (`read_timing`). Neither grants
//! budget or authority.

pub(crate) mod read_timing;
pub mod work;
