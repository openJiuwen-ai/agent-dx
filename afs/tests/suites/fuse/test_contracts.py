from fixtures.common import Case
from suites.operations import rust_contract


# DFS front-gate ordering (S3b, narrowed by the namespace-gate removal): same
# inode callbacks order; path-addressed callbacks order only by kernel locks.
FRONT_GATE_ORDERING = (
    "read_after_an_acknowledged_write_waits_for_its_body",
    "fsync_waits_for_an_acknowledged_write",
    "truncate_waits_for_an_acknowledged_write",
    "path_lookup_waits_for_acknowledged_writes",
    "path_metadata_under_kernel_ttl_waits_for_acknowledged_writes",
    "namespace_change_does_not_wait_for_another_files_acknowledged_write",
    "renaming_a_file_with_an_acknowledged_write_keeps_the_write",
    "read_waits_for_an_in_flight_truncate_of_the_same_file",
    "write_waits_for_an_in_flight_read_of_the_same_file",
    "namespace_change_overlaps_in_flight_file_callbacks",
    "file_callbacks_overlap_an_in_flight_namespace_change",
    "write_then_rename_over_a_target_before_close",
)


def ordering(c):
    rust_contract(
        c,
        [
            "fuse_ftruncate_on_same_handle_waits_for_prior_write",
            "dfs_buffered_reopen_refreshes_growth_and_truncation_with_new_heads",
        ]
        + ["front_gate_ordering::" + name for name in FRONT_GATE_ORDERING],
    )


def real_dfs(c):
    rust_contract(c, ["real_dfs_keep_cache_contract::dfs_real_nodes_cross_handle_keep_cache_and_permission"])


def busy(c):
    rust_contract(c, ["fuse_normal_join_rejects_busy_mount_without_detaching"])


CASES = (
    Case("fuse.contracts.ordering", ("local-basic",), "dfs", ("LB06",), ordering, rust=True),
    Case("fuse.contracts.real_dfs_cache", ("local-extended",), "dfs", ("LB06",), real_dfs, rust=True),
    Case("fuse.contracts.busy_unmount", ("local-basic",), "dfs", ("LB08",), busy, rust=True),
)
