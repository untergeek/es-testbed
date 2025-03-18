"""Unit test initialization file."""

import typing as t

ALIAS: str = 'test-alias'
"""Default alias name for testing."""

INDEX1: str = 'index1'
"""Default index name for testing."""

INDEX2: str = 'index2'
"""Additional index name for testing."""

INDICES: t.Sequence[str] = [INDEX1, INDEX2]
"""Default index list for testing."""

REPO: str = 'repo'
"""Default snapshot repository for testing."""

TIERS: t.Sequence[str] = ['hot', 'warm', 'cold', 'frozen', 'delete']
"""Default ILM tiers for testing."""

TREPO: t.Dict[str, t.Union[str, None]] = {
    'hot': None,
    'warm': None,
    'cold': REPO,
    'frozen': REPO,
    'delete': None,
}
"""Default ILM tiers and their corresponding repositories for testing."""


def my_retval(kind, kwargs) -> t.Any:
    """Function to return all kinds of return values"""
    retval = None
    if kind == 'alias_body':
        retval = {
            'name': kwargs.get('alias', ALIAS),
            'indices': kwargs.get('indices', INDICES),
        }
    return retval


def forcemerge(
    fm: bool = False, mns: int = 1
) -> t.Union[t.Dict[str, t.Dict[str, str]], None]:
    """Return forcemerge action."""
    if fm:
        return {'forcemerge': {'max_num_segments': mns}}
    return {}


def searchable(
    repository: str = None, fm: bool = None
) -> t.Union[t.Dict[str, t.Dict[str, str]], None]:
    """Return searchable snapshot action."""
    if repository:
        return {
            'searchable_snapshot': {
                'snapshot_repository': repository,
                'force_merge_index': fm,
            }
        }
    return {}
