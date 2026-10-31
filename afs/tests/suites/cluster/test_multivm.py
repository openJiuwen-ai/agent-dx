from fixtures.common import Case


def topology(c):
    c.topology()


# Cleanup is a required assertion of every case, not just a printed finally message.
CASES = (
    Case("cluster.topology_cleanup", ("cluster-basic", "cluster-manyread"), "ownerfs", ("MV01", "MV07"), topology),
)
