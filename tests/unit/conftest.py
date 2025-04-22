"""Pytest configuration for unit tests."""

# pylint: disable=redefined-outer-name,C0116,W0212
import typing as t
import logging
from unittest.mock import MagicMock, Mock, patch
import datetime
import pytest
from elastic_transport import ApiResponseMeta
from elasticsearch8.exceptions import NotFoundError
from es_testbed._base import TestBed
from es_testbed._plan import PlanBuilder
from es_testbed.debug import debug
from es_testbed.defaults import (
    ilmhot,
    ilmwarm,
    ilmcold,
    ilmfrozen,
    ilmdelete,
)
from es_testbed.entities import Alias, Index
from es_testbed.utils import build_ilm_phase
from es_testbed.ilm import IlmTracker
from . import forcemerge, searchable, ALIAS, INDEX1, INDICES, REPO, TIERS, TREPO

logger = logging.getLogger(__name__)

debug.level = 5  # Set the debug level to 5 for all tests

FMAP: t.Dict[str, t.Dict] = {
    "hot": ilmhot(),
    "warm": ilmwarm(),
    "cold": ilmcold(),
    "frozen": ilmfrozen(),
    "delete": ilmdelete(),
}
"""Default ILM phase map for testing."""

RESOLVER: t.Dict[str, t.Sequence] = {
    "indices": [{"name": INDEX1}],
    "aliases": [],
    "data_streams": [],
}


@pytest.fixture(scope="function")
def alias(request):
    return request.param


@pytest.fixture
def alias_body_params(alias, idx_list):
    return {"name": alias, "indices": idx_list}


@pytest.fixture(scope="session")
def alias_body():
    return {"name": ALIAS, "indices": INDICES}


@pytest.fixture(scope="function")
def alias_cls(client, alias):
    return Alias(client=client, name=alias)


@pytest.fixture(scope="function")
def client():
    """Return a mock Elasticsearch client."""
    return MagicMock()


@pytest.fixture
def dt(request):
    # request should be a tuple of (year, month, day, hour, minute, second)
    return request.param


@pytest.fixture
def tz(request):
    return (
        datetime.timezone.utc
        if request.param == "UTC"
        else datetime.timezone(datetime.timedelta(hours=request.param))
    )


@pytest.fixture
def expected(request):
    return request.param


@pytest.fixture
def fake_now(dt, tz):
    return datetime.datetime(*dt, tzinfo=tz)


@pytest.fixture
def idx(request) -> str:
    return request.param


@pytest.fixture
def idx_list(request) -> t.Sequence[str]:
    return request.param


@pytest.fixture
def index_cls(client):
    index = Index(client=client, name=INDEX1)
    index.ilm_tracker = MagicMock()
    return index


@pytest.fixture
def logmsg(request) -> str:
    return request.param


@pytest.fixture
def meta():
    return ApiResponseMeta(200, "1.1", {}, 0.01, None)


@pytest.fixture()
def mock_datetime_now(monkeypatch, fake_now):
    datetime_mock = MagicMock(wraps=datetime.datetime)
    datetime_mock.now.return_value = fake_now
    monkeypatch.setattr(datetime, "datetime", datetime_mock)


@pytest.fixture
def notfound(meta):
    return NotFoundError("error", meta, "error")


@pytest.fixture
def phase(request) -> str:
    return request.param


@pytest.fixture
def phases(request) -> t.Sequence[str]:
    return request.param


@pytest.fixture
def policy(request):
    return request.param


@pytest.fixture
def retval(request):
    """Parametrize any return value."""
    return request.param


@pytest.fixture
def settings():
    """Return the PlanBuilder test settings."""
    return {
        "type": "indices",
        "prefix": "es-testbed",
        "rollover_alias": False,
        "uniq": "my-unique-str",
        "repository": "test-repo",
        "ilm": {
            "enabled": False,
            "phases": ["hot", "delete"],
            "readonly": "PHASE",
            "forcemerge": False,
            "max_num_segments": 1,
            "policy": {},
        },
        "index_buildlist": [],
    }


@pytest.fixture
def tier(request) -> str:
    return request.param


@pytest.fixture
def tiered_debug(request) -> int:
    """Fixture to set the tiered debug level for tests"""
    retval = int(request.param)
    logger.debug(f"Tiered debug level set to {retval}")
    if not 1 <= retval <= 5:
        retval = 1
    return retval


# Fixture to create a basic IlmTracker instance with mocked dependencies
@pytest.fixture
def tracker(client):
    with patch("es_testbed.ilm.resolver", return_value=RESOLVER), patch(
        "es_testbed.ilm.ilm_explain",
        return_value={"phase": "hot", "action": "complete", "step": "complete"},
    ), patch(
        "es_testbed.ilm.get_ilm_phases",
        return_value={"hot": {}, "warm": {}, "delete": {}},
    ):
        return IlmTracker(client, INDEX1)


@pytest.fixture
def tracker_adv(tracker):
    _ = tracker
    _._explain = Mock(phase=None)  # Mock the phase attribute
    with patch("es_testbed.ilm.IlmTracker.next_phase", return_value=None), patch(
        "es_testbed.ilm.IlmTracker.next_step",
        return_value={"phase": "mock_phase", "action": "mock_action"},
    ):
        return _


@pytest.fixture
def plan_builder(settings):
    """Return a PlanBuilder instance."""
    return PlanBuilder(settings=settings, autobuild=False)


@pytest.fixture(scope="session")
def repo_val():
    """Return the snapshot repository."""
    return REPO


@pytest.fixture(scope="function")
def testbed(client):
    """Return a TestBed instance."""
    _ = TestBed(
        client=client,
        builtin="searchable_test",
        path="path",
        ref="ref",
        url="url",
        scenario="hot",
    )
    _.plan = MagicMock()
    return _


@pytest.fixture
def testbed_fodder(testbed):
    """Return a TestBed instance for testing the _erase_all method."""
    testbed.plan.repository = "test-repo"
    testbed.plan.prefix = "test-prefix"
    testbed.plan.uniq = "test-uniq"
    return testbed


@pytest.fixture(scope="session")
def tiers():
    """Return the ILM tiers."""
    return TIERS


@pytest.fixture(scope="session")
def trepo():
    """Return the ILM tiers and their corresponding repositories."""
    return TREPO


@pytest.fixture(scope="function")
def tiertestval():
    """Return the ILM policy for a given tier."""

    def _tiertestval(tier: str, repo: str = None, fm: bool = False, mns: int = 1):
        retval = {tier: FMAP[tier]}
        retval[tier]["actions"].update(searchable(repository=repo, fm=fm))
        kwargs = {"fm": fm, "mns": mns} if tier in ["hot", "warm"] else {}
        retval[tier]["actions"].update(forcemerge(**kwargs))
        return retval

    return _tiertestval


@pytest.fixture(scope="function")
def builtphase():
    """Return the built ILM phase."""

    def _builtphase(tier: str, repo: str = None, fm: bool = False, mns: int = 1):
        return build_ilm_phase(tier, actions=forcemerge(fm=fm, mns=mns), repo=repo)

    return _builtphase
