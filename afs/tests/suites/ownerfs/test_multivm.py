from fixtures.common import Case
from suites.operations import owner_namespace, permissions, read_write, recovery

TAGS = ("cluster-basic", "cluster-manyread")
CASES = (
    Case("ownerfs.cluster.read_write", TAGS, "ownerfs", ("MV02",), read_write),
    Case("ownerfs.cluster.namespace", TAGS, "ownerfs", ("MV03",), owner_namespace),
    Case("ownerfs.cluster.permissions", TAGS, "ownerfs", ("MV05",), permissions),
    Case("ownerfs.cluster.restart", TAGS, "ownerfs", ("MV06",), recovery),
)
