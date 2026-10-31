from fixtures.common import Case
from suites.operations import read_write, recovery, visibility

TAGS = ("cluster-basic", "cluster-manyread")
CASES = (
    Case("dfs.cluster.read_write", TAGS, "dfs", ("MV04",), read_write),
    Case("dfs.cluster.visibility", TAGS, "dfs", ("MV05",), visibility),
    Case("dfs.cluster.restart", TAGS, "dfs", ("MV06",), recovery),
    Case("dfs.cluster.many_readers", ("cluster-manyread",), "dfs", ("MV08",), read_write),
)
