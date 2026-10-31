from fixtures.common import Case
from suites.operations import recovery

CASES = (Case("ownerfs.recovery.normal_restart", ("local-basic",), "ownerfs", ("LB07",), recovery),)
