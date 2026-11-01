"""Declared CI Full functional scope; adding a case requires explicit registration."""

CASE_IDS = (
    'cluster.topology_cleanup', 'dfs.basic.one_writer_many_readers', 'dfs.basic.namespace',
    'dfs.cluster.read_write', 'dfs.cluster.visibility', 'dfs.cluster.restart', 'dfs.cluster.many_readers',
    'dfs.recovery.normal_restart', 'dfs.visibility.new_open_permissions', 'dfs.visibility.real_cross_handle_cache',
    'dfs.cache.same_version_reopen', 'dfs.cache.warm_permission_change', 'fuse.contracts.ordering',
    'fuse.contracts.real_dfs_cache', 'fuse.contracts.busy_unmount', 'ownerfs.basic.read_write',
    'ownerfs.extended.namespace_identity', 'ownerfs.basic.namespace', 'ownerfs.cluster.read_write',
    'ownerfs.cluster.namespace', 'ownerfs.cluster.permissions', 'ownerfs.cluster.restart',
    'ownerfs.permissions.deny_other_uid', 'ownerfs.recovery.normal_restart',
)
