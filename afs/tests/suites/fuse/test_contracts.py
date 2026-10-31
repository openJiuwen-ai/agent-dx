from fixtures.common import Case
from suites.operations import rust_contract


def ordering(c):
    rust_contract(
        c,
        [
            "fuse_ftruncate_on_same_handle_waits_for_prior_write",
            "dfs_buffered_reopen_refreshes_growth_and_truncation_with_new_heads",
        ],
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
