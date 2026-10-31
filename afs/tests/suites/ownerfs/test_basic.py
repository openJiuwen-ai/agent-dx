from fixtures.common import Case
from suites.operations import namespace, owner_namespace, read_write

CASES = (
    Case("ownerfs.basic.read_write", ("local-basic",), "ownerfs", ("LB01", "LB03", "LB08"), read_write),
    Case("ownerfs.extended.namespace_identity", ("local-extended",), "ownerfs", (), owner_namespace),
    Case("ownerfs.basic.namespace", ("local-basic",), "ownerfs", ("LB02",), namespace),
)
