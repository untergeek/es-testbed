"""Top-level conftest.py"""

# pylint: disable=C0115,missing-function-docstring,redefined-outer-name,R0913
import typing as t
from os import environ, path
from datetime import datetime, timezone
import random
import string
import pytest
from dotenv import load_dotenv
from elasticsearch8.exceptions import NotFoundError
from es_client import Builder
from es_client.logging import set_logging
from es_wait.debug import debug as wait_debug
from es_testbed.debug import debug
from es_testbed.defaults import NAMEMAPPER
from es_testbed.es_api import get_ds_current, get_write_index

LOGLEVEL = "DEBUG"
LOCALREPO = "testing"
PROJ = path.abspath(path.join(path.dirname(__file__), ".."))
ENVPATH = path.join(PROJ, ".env")
print(f"ENVPATH: {ENVPATH}")

debug.level = 5  # Set the debug level to 5 for all tests
wait_debug.level = debug.level  # Set the wait_debug level to match the debug level


class Index4Test:

    def __init__(self, client, idx_name, idx_settings):
        self.client = client
        self.name = idx_name
        self.settings = idx_settings

    def setup(self):
        self.client.indices.create(index=self.name, settings=self.settings)

    def teardown(self):
        self.client.indices.delete(index=self.name)


@pytest.fixture(scope="class")
def actual_index(entitymgr):
    def _actual_index(tb, which):
        if tb.plan.type == "data_stream":
            return entitymgr(tb).ds.backing_indices[which]
        return entitymgr(tb).entity_list[which].name  # implied else

    return _actual_index


@pytest.fixture(scope="class")
def actual_rollover(entitymgr):
    def _actual_rollover(tb):
        if tb.plan.type == "data_stream":
            return entitymgr(tb).last
        if tb.plan.rollover_alias:
            if entitymgr(tb).alias.name is not None:
                return entitymgr(tb).alias.name
        return ""  # implied else

    return _actual_rollover


@pytest.fixture(scope="class")
def actual_write_index(actual_rollover):
    def _actual_write_index(tb):
        name = actual_rollover(tb)
        if not name:
            return name
        func = get_write_index
        if tb.plan.type == "data_stream":
            func = get_ds_current
        return func(tb.client, name)

    return _actual_write_index


@pytest.fixture(scope="session")
def client():
    """Return an Elasticsearch client"""
    load_dotenv(dotenv_path=ENVPATH)
    host = environ.get("TEST_ES_SERVER")
    user = environ.get("TEST_USER")
    pswd = environ.get("TEST_PASS")
    cacrt = environ.get("CA_CRT")
    file = environ.get("ES_CLIENT_FILE", None)  # Path to es_client YAML config
    repo = environ.get("TEST_ES_REPO", "found-snapshots")
    if file:
        kwargs = {"configfile": file}
    else:
        kwargs = {
            "configdict": {
                "elasticsearch": {
                    "client": {"hosts": host, "ca_certs": cacrt},
                    "other_settings": {"username": user, "password": pswd},
                }
            }
        }
    set_logging({"loglevel": LOGLEVEL, "blacklist": ["elastic_transport", "urllib3"]})
    builder = Builder(**kwargs)
    builder.connect()
    # This is a contradiction that cannot exist...
    if repo == "found-snapshots" and host == "https://127.0.0.1:9200" and not file:
        # Reset the env var
        environ["TEST_ES_REPO"] = LOCALREPO
    return builder.client


@pytest.fixture(scope="class")
def cold():
    """Return the prefix for cold indices"""
    return "restored-"


@pytest.fixture(scope="class")
def components(namecore):
    """Return the component names in a list"""
    components = []
    components.append(f'{namecore("component")}-000001')
    components.append(f'{namecore("component")}-000002')
    return components


@pytest.fixture
def name(request):
    return request.param


@pytest.fixture
def idxcfg(request):
    return request.param


@pytest.fixture
def create_idx(client, name, idxcfg):
    # Setup
    print(f"Creating index {name} with settings {idxcfg}")
    index = Index4Test(client, name, idxcfg)
    index.setup()
    yield index

    # Teardown
    print(f"Teardown: Deleting index {name}")
    index.teardown()


def create_repository(client, name: str) -> None:
    """
    PUT _snapshot/REPO_NAME
    {
        "type": "fs",
        "settings": {
            "location": "RELATIVE_PATH"
        }
    }
    """
    repobody = {"type": "fs", "settings": {"location": "/media"}}
    client.snapshot.create_repository(name=name, repository=repobody, verify=False)


@pytest.fixture(scope="class")
def entity_count(defaults):
    def _entity_count(kind):
        if kind == "data_stream":
            return 1
        return defaults()["count"]

    return _entity_count


@pytest.fixture(scope="class")
def defaults() -> t.Dict:
    def _defaults(sstier: str = "hot") -> t.Dict:
        retval = {"count": 3, "docs": 10, "match": True, "searchable": None}
        if sstier in ["cold", "frozen"]:
            retval["searchable"] = sstier
        return retval

    return _defaults


@pytest.fixture(scope="class")
def entitymgr():
    def _entitymgr(tb):
        if tb.plan.type == "data_stream":
            return tb.data_streammgr
        return tb.indexmgr  # implied else

    return _entitymgr


@pytest.fixture(scope="class")
def first():
    return 0


@pytest.fixture(scope="class")
def frozen():
    """Return the prefix for frozen indices"""
    return "partial-"


@pytest.fixture(scope="class")
def get_template(template):
    def _get_template(client):
        return client.indices.get_index_template(name=template)["index_templates"]

    return _get_template


@pytest.fixture(scope="class")
def idxmain(namecore, ymd):
    def _idxmain(kind):
        result = f"{namecore(kind)}"
        if kind == "data_stream":
            return f".ds-{result}-{ymd}"
        return result

    return _idxmain


@pytest.fixture(scope="class")
def idxss(first, ssprefix, rollable):
    def _idxss(tier, which, plan):
        if which != first:
            if rollable(plan):
                return ""  # No searchable prefix
        return ssprefix(tier)

    return _idxss


@pytest.fixture(scope="class")
def idxtail(first, last):
    def _idxtail(which):
        if which == first:
            return "-000001"
        if which == last:
            return "-000003"
        return "-000002"  # implied else

    return _idxtail


@pytest.fixture(scope="class")
def index_name(first, idxmain, idxss, idxtail):
    def _index_name(which=first, plan=None, tier: str = "hot"):
        prefix = idxss(tier, which, plan)
        main = idxmain(plan.type)
        suffix = idxtail(which)
        return f"{prefix}{main}{suffix}"

    return _index_name


@pytest.fixture(scope="class")
def last():
    return -1


@pytest.fixture(scope="class")
def namecore(prefix, uniq):
    def _namecore(kind):
        return f"{prefix}-{NAMEMAPPER[kind]}-{uniq}"

    return _namecore


@pytest.fixture(scope="class")
def prefix():
    """Return a random prefix"""
    return randomstr(length=8, lowercase=True)


def randomstr(length: int = 16, lowercase: bool = False):
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str("".join(random.choices(letters + string.digits, k=length)))


@pytest.fixture(scope="class")
def repo(client):
    """Return the elasticsearch repository"""
    load_dotenv(dotenv_path=ENVPATH)
    name = environ.get("TEST_ES_REPO", "found-snapshots")  # Going with Cloud default
    if not repo:
        return False
    try:
        client.snapshot.get_repository(name=name)
    except NotFoundError:
        return False
    return name  # Return the repo name if it's online


@pytest.fixture(scope="class")
def rollable():
    def _rollable(plan):
        if plan.type == "data_stream":
            return True
        if plan.rollover_alias:
            return True
        return False

    return _rollable


@pytest.fixture(scope="class")
def rollovername(namecore, rollable):
    def _rollovername(plan):
        if rollable(plan):
            return namecore(plan.type)
        return ""

    return _rollovername


@pytest.fixture(scope="class")
def settings(defaults, prefix, repo, uniq):
    def _settings(
        plan_type: t.Literal["data_stream", "index"] = "data_stream",
        rollover_alias: bool = False,
        ilm: t.Union[t.Dict, False] = False,
        sstier: str = "hot",
    ):
        return {
            "type": plan_type,
            "prefix": prefix,
            "rollover_alias": rollover_alias,
            "repository": repo,
            "uniq": uniq,
            "ilm": ilm,
            "defaults": defaults(sstier),
        }

    return _settings


@pytest.fixture(scope="class")
def skip_no_repo(repo) -> None:
    def _skip_no_repo(skip_it: bool) -> None:
        if skip_it:
            if not repo:
                pytest.skip("No snapshot repository", allow_module_level=True)

    return _skip_no_repo


@pytest.fixture(scope="class")
def skip_localhost() -> None:
    def _skip_localhost(skip_it: bool) -> None:
        if skip_it:
            load_dotenv(dotenv_path=ENVPATH)
            host = environ.get("TEST_ES_SERVER")
            file = environ.get("ES_CLIENT_FILE", None)  # Path to es_client YAML config
            repo = environ.get("TEST_ES_REPO")
            if repo == LOCALREPO and host == "https://127.0.0.1:9200" and not file:
                pytest.skip(
                    "Local Docker test does not work with this test",
                    allow_module_level=False,
                )

    return _skip_localhost


@pytest.fixture(scope="class")
def ssprefix(cold, frozen):
    def _ssprefix(tier):
        retval = ""  # hot or warm
        if tier == "cold":
            retval = cold
        if tier == "frozen":
            retval = frozen
        return retval

    return _ssprefix


@pytest.fixture(scope="class")
def template(namecore):
    """Return the name of the index template"""
    return f'{namecore("template")}-000001'


@pytest.fixture(scope="class")
def uniq():
    """Return a random uniq value"""
    return randomstr(length=8, lowercase=True)


@pytest.fixture(scope="class")
def write_index_name(last, idxmain, idxss, idxtail, rollable):
    def _write_index_name(which=last, plan=None, tier: str = "hot"):
        if not rollable(plan):
            return ""
        prefix = idxss(tier, which, plan)
        main = idxmain(plan.type)
        suffix = idxtail(which)
        return f"{prefix}{main}{suffix}"

    return _write_index_name


@pytest.fixture(scope="class")
def ymd():
    return datetime.now(timezone.utc).strftime("%Y.%m.%d")
