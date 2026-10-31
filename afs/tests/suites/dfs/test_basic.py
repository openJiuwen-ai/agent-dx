from fixtures.common import Case
from suites.operations import namespace, read_write

CASES = (
    Case("dfs.basic.one_writer_many_readers", ("local-basic",), "dfs", ("LB01", "LB05", "LB08"), read_write),
    Case("dfs.basic.namespace", ("local-basic",), "dfs", ("LB02",), namespace),
)
