"""Targeted component resource regressions, independent of default CI selections."""

from fixtures.common import Case, DfsProfile
from suites.dfs.fd_budget import run

CASES = (
    Case('dfs.resources.reader_fd_budget', ('local-resources',), 'dfs', (), run,
         profile=DfsProfile(node_count=2)),
)
