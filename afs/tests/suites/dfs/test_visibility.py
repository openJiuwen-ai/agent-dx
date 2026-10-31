from fixtures.common import Case
from suites.operations import visibility
from suites.dfs.real_cache import run as real_cache

CASES = (
    Case("dfs.visibility.new_open_permissions", ("local-basic",), "dfs", ("LB04",), visibility),
    Case("dfs.visibility.real_cross_handle_cache", ("local-basic",), "dfs", ("LB06",), real_cache),
)
