from fixtures.common import Case
from suites.operations import visibility
from suites.dfs.real_cache import run as real_cache
from suites.dfs.real_cache import reuse as cache_reuse, permission as cache_permission

CASES = (
    Case("dfs.visibility.new_open_permissions", ("local-basic",), "dfs", ("LB04",), visibility),
    Case("dfs.visibility.real_cross_handle_cache", ("local-basic",), "dfs", ("LB06",), real_cache,
         observe_cache=True),
    Case("dfs.cache.same_version_reopen", ("local-basic",), "dfs", ("LB06",), cache_reuse,
         observe_cache=True),
    Case("dfs.cache.warm_permission_change", ("local-basic",), "dfs", ("LB04",), cache_permission,
         observe_cache=True),
)
