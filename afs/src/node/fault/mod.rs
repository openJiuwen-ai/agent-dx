//! Node fault layer: classification today; repair, reaper and lease keeper
//! loops move here in later S1c steps. Normal paths only record and report;
//! the decisions those records feed live here.

pub(crate) mod classify;
