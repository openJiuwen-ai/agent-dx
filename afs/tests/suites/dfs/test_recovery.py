from fixtures.common import Case
from suites.operations import recovery

CASES = (Case("dfs.recovery.normal_restart", ("local-basic",), "dfs", ("LB07",), recovery),)
