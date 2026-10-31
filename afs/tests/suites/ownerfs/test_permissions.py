from fixtures.common import Case
from suites.operations import permissions

CASES = (Case("ownerfs.permissions.deny_other_uid", ("local-basic",), "ownerfs", ("LB04",), permissions),)
