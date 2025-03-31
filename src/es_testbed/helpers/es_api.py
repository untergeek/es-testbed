"""Functions that make Elasticsearch API Calls"""

# pylint: disable=R0913,R0917,W0707
import typing as t
import logging
from os import getenv
import tiered_debug as debug
from elasticsearch8.exceptions import NotFoundError, TransportError
from es_wait import Exists, Snapshot
from es_wait.exceptions import EsWaitFatal, EsWaitTimeout
from ..defaults import MAPPING, PAUSE_DEFAULT, PAUSE_ENVVAR
from ..exceptions import (
    NameChanged,
    ResultNotExpected,
    TestbedFailure,
    TestbedMisconfig,
)
from ..helpers.utils import (
    get_routing,
    mounted_name,
    prettystr,
    storage_type,
)

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))

logger = logging.getLogger(__name__)


def emap(kind: str, es: 'Elasticsearch', value=None) -> t.Dict[str, t.Any]:
    """Return a value from a dictionary"""
    _ = {
        'alias': {
            'delete': es.indices.delete_alias,
            'exists': es.indices.exists_alias,
            'get': es.indices.get_alias,
            'kwargs': {'index': value, 'expand_wildcards': ['open', 'closed']},
            'plural': 'alias(es)',
        },
        'data_stream': {
            'delete': es.indices.delete_data_stream,
            'exists': es.indices.exists,
            'get': es.indices.get_data_stream,
            'kwargs': {'name': value, 'expand_wildcards': ['open', 'closed']},
            'plural': 'data_stream(s)',
            'key': 'data_streams',
        },
        'index': {
            'delete': es.indices.delete,
            'exists': es.indices.exists,
            'get': es.indices.get,
            'kwargs': {'index': value, 'expand_wildcards': ['open', 'closed']},
            'plural': 'index(es)',
        },
        'template': {
            'delete': es.indices.delete_index_template,
            'exists': es.indices.exists_index_template,
            'get': es.indices.get_index_template,
            'kwargs': {'name': value},
            'plural': 'index template(s)',
            'key': 'index_templates',
        },
        'ilm': {
            'delete': es.ilm.delete_lifecycle,
            'exists': es.ilm.get_lifecycle,
            'get': es.ilm.get_lifecycle,
            'kwargs': {'name': value},
            'plural': 'ilm policy(ies)',
        },
        'component': {
            'delete': es.cluster.delete_component_template,
            'exists': es.cluster.exists_component_template,
            'get': es.cluster.get_component_template,
            'kwargs': {'name': value},
            'plural': 'component template(s)',
            'key': 'component_templates',
        },
        'snapshot': {
            'delete': es.snapshot.delete,
            'exists': es.snapshot.get,
            'get': es.snapshot.get,
            'kwargs': {'snapshot': value},
            'plural': 'snapshot(s)',
        },
    }
    return _[kind]


def change_ds(client: 'Elasticsearch', actions: t.Optional[str] = None) -> None:
    """Change/Modify/Update a data_stream"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: client.indices.modify_data_stream')
        debug.lv5(f'modify_data_stream actions: {actions}')
        res = client.indices.modify_data_stream(actions=actions, body=None)
        debug.lv5(f'modify_data_stream response: {res}')
    except Exception as err:
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise ResultNotExpected(
            f'Unable to modify data_streams. {prettystr(err)}'
        ) from err
    debug.lv3('Exiting function')


def wait_wrapper(
    client: 'Elasticsearch',
    wait_cls: t.Callable,
    wait_kwargs: t.Dict,
    func: t.Callable,
    f_kwargs: t.Dict,
) -> None:
    """Wrapper function for waiting on an object to be created"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: func()')
        debug.lv5(f'func kwargs: {f_kwargs}')
        func(**f_kwargs)
        debug.lv4('TRY: wait_cls')
        debug.lv5(f'wait_cls kwargs: {wait_kwargs}')
        test = wait_cls(client, **wait_kwargs)
        debug.lv4('TRY: wait()')
        test.wait()
    except EsWaitFatal as wait:
        msg = f'{wait.message}. Elapsed time: {wait.elapsed}. Errors: {wait.errors}'
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(wait)}')
        raise TestbedFailure(msg) from wait
    except EsWaitTimeout as wait:
        msg = f'{wait.message}. Elapsed time: {wait.elapsed}. Timeout: {wait.timeout}'
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(wait)}')
        raise TestbedFailure(msg) from wait
    except TransportError as err:
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise TestbedFailure(
            f'Elasticsearch TransportError class exception encountered:'
            f'{prettystr(err)}'
        ) from err
    except Exception as err:
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise TestbedFailure(f'General Exception caught: {prettystr(err)}') from err
    debug.lv3('Exiting function')


def create_data_stream(client: 'Elasticsearch', name: str) -> None:
    """Create a data_stream"""
    debug.lv2('Starting function...')
    wait_kwargs = {'name': name, 'kind': 'data_stream', 'pause': PAUSE_VALUE}
    debug.lv5(f'wait_kwargs: {wait_kwargs}')
    f_kwargs = {'name': name}
    debug.lv5(f'f_kwargs: {f_kwargs}')
    debug.lv5(f'Creating data_stream {name} and waiting for it to exist')
    wait_wrapper(
        client, Exists, wait_kwargs, client.indices.create_data_stream, f_kwargs
    )
    debug.lv3('Exiting function')


def create_index(
    client: 'Elasticsearch',
    name: str,
    aliases: t.Union[t.Dict, None] = None,
    settings: t.Union[t.Dict, None] = None,
    tier: str = 'hot',
) -> None:
    """Create named index"""
    debug.lv2('Starting function...')
    if not settings:
        settings = get_routing(tier=tier)
    else:
        settings.update(get_routing(tier=tier))
    debug.lv5(f'settings: {settings}')
    wait_kwargs = {'name': name, 'kind': 'index', 'pause': PAUSE_VALUE}
    debug.lv5(f'wait_kwargs: {wait_kwargs}')
    f_kwargs = {
        'index': name,
        'aliases': aliases,
        'mappings': MAPPING,
        'settings': settings,
    }
    debug.lv5(f'f_kwargs: {f_kwargs}')
    debug.lv5(f'Creating index {name} and waiting for it to exist')
    wait_wrapper(client, Exists, wait_kwargs, client.indices.create, f_kwargs)
    retval = exists(client, 'index', name)
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def verify(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Optional[str] = None,
) -> bool:
    """Verify that whatever was deleted is actually deleted"""
    debug.lv2('Starting function...')
    success = True
    items = name.split(',')
    for item in items:
        result = exists(client, kind, item, repository=repository)
        if result:  # That means it's still in the cluster
            success = False
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {success}')
    return success


def delete(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Optional[str] = None,
) -> bool:
    """Delete the named object of type kind"""
    debug.lv2('Starting function...')
    which = emap(kind, client)
    func = which['delete']
    success = False
    if name is not None:  # Typically only with ilm
        try:
            debug.lv4('TRY: func')
            if kind == 'snapshot':
                debug.lv5(f'Deleting snapshot {name} from repository {repository}')
                res = func(snapshot=name, repository=repository)
            elif kind == 'index':
                debug.lv5(f'Deleting index {name}')
                res = func(index=name)
            else:
                debug.lv5(f'Deleting {kind} {name}')
                res = func(name=name)
        except NotFoundError as err:
            debug.lv5(f'{kind} named {name} not found: {prettystr(err)}')
            debug.lv3('Exiting function, returning value')
            debug.lv5('Value = True')
            return True
        except Exception as err:
            debug.lv3('Exiting function, raising exception')
            debug.lv5(f'Exception: {prettystr(err)}')
            raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
        if 'acknowledged' in res and res['acknowledged']:
            success = True
            debug.lv3(f'Deleted {which["plural"]}: "{name}"')
        else:
            debug.lv5('Verifying deletion manually')
            success = verify(client, kind, name, repository=repository)
    else:
        debug.lv3(f'"{kind}" has a None value for name')
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {success}')
    return success


def do_snap(
    client: 'Elasticsearch', repo: str, snap: str, idx: str, tier: str = 'cold'
) -> None:
    """Perform a snapshot"""
    debug.lv2('Starting function...')
    wait_kwargs = {'snapshot': snap, 'repository': repo, 'pause': 1, 'timeout': 60}
    debug.lv5(f'wait_kwargs: {wait_kwargs}')
    f_kwargs = {'repository': repo, 'snapshot': snap, 'indices': idx}
    debug.lv5(f'f_kwargs: {f_kwargs}')
    debug.lv5(f'Creating snapshot {snap} and waiting for it to complete')
    wait_wrapper(client, Snapshot, wait_kwargs, client.snapshot.create, f_kwargs)

    # Mount the index accordingly
    debug.lv5(
        f'Mounting index {idx} from snapshot {snap} as searchable snapshot '
        f'with mounted name: {mounted_name(idx, tier)}'
    )
    client.searchable_snapshots.mount(
        repository=repo,
        snapshot=snap,
        index=idx,
        index_settings=get_routing(tier=tier),
        renamed_index=mounted_name(idx, tier),
        storage=storage_type(tier),
        wait_for_completion=True,
    )
    # Fix aliases
    debug.lv5(f'Fixing aliases for {idx} to point to {mounted_name(idx, tier)}')
    fix_aliases(client, idx, mounted_name(idx, tier))
    debug.lv3('Exiting function')


def exists(
    client: 'Elasticsearch', kind: str, name: str, repository: t.Union[str, None] = None
) -> bool:
    """Return boolean existence of the named kind of object"""
    debug.lv2('Starting function...')
    if name is None:
        return False
    retval = True
    func = emap(kind, client)['exists']
    try:
        debug.lv4('TRY: func')
        if kind == 'snapshot':
            # Expected response: {'snapshots': [{'snapshot': name, ...}]}
            # Since we are specifying by name, there should only be one returned
            debug.lv5(f'Checking for snapshot {name} in repository {repository}')
            res = func(snapshot=name, repository=repository)
            debug.lv3(f'Snapshot response: {res}')
            # If there are no entries, load a default None value for the check
            _ = dict(res['snapshots'][0]) if res else {'snapshot': None}
            # Since there should be only 1 snapshot with this name, we can check it
            retval = bool(_['snapshot'] == name)
        elif kind == 'ilm':
            # There is no true 'exists' method for ILM, so we have to get the policy
            # and check for a NotFoundError
            debug.lv5(f'Checking for ILM policy {name}')
            retval = bool(name in dict(func(name=name)))
        elif kind in ['index', 'data_stream']:
            debug.lv5(f'Checking for {kind} {name}')
            retval = func(index=name)
        else:
            debug.lv5(f'Checking for {kind} {name}')
            retval = func(name=name)
    except NotFoundError:
        debug.lv5(f'{kind} named {name} not found')
        retval = False
    except Exception as err:
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def fill_index(
    client: 'Elasticsearch',
    name: t.Optional[str] = None,
    doc_generator: t.Optional[t.Generator[t.Dict, None, None]] = None,
    options: t.Optional[t.Dict] = None,
) -> None:
    """
    Create and fill the named index with mappings and settings as directed

    :param client: ES client
    :param name: Index name
    :param doc_generator: The generator function

    :returns: No return value
    """
    debug.lv2('Starting function...')
    if not options:
        options = {}
    for doc in doc_generator(**options):
        client.index(index=name, document=doc)
    client.indices.flush(index=name)
    client.indices.refresh(index=name)
    debug.lv3('Exiting function')


def find_write_index(client: 'Elasticsearch', name: str) -> t.AnyStr:
    """Find the write_index for an alias by searching any index the alias points to"""
    debug.lv2('Starting function...')
    retval = None
    for alias in get_aliases(client, name):
        debug.lv5(f'Inspecting alias: {alias}')
        retval = get_write_index(client, alias)
        debug.lv5(f'find_write_index response: {retval}')
        if retval:
            debug.lv5(f'Found write index: {retval}')
            break
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def fix_aliases(client: 'Elasticsearch', oldidx: str, newidx: str) -> None:
    """Fix aliases using the new and old index names as data"""
    debug.lv2('Starting function...')
    # Delete the original index
    debug.lv5(f'Deleting index {oldidx}')
    client.indices.delete(index=oldidx)
    # Add the original index name as an alias to the mounted index
    debug.lv5(f'Adding alias {oldidx} to index {newidx}')
    client.indices.put_alias(index=f'{newidx}', name=oldidx)
    debug.lv3('Exiting function')


def get(
    client: 'Elasticsearch',
    kind: str,
    pattern: str,
    repository: t.Optional[str] = None,
) -> t.Sequence[str]:
    """get any/all objects of type kind matching pattern"""
    debug.lv2('Starting function...')
    if pattern is None:
        msg = f'"{kind}" has a None value for pattern'
        logger.error(msg)
        raise TestbedMisconfig(msg)
    which = emap(kind, client, value=pattern)
    func = which['get']
    kwargs = which['kwargs']
    if kind == 'snapshot':
        kwargs['repository'] = repository
    try:
        debug.lv4('TRY: func')
        debug.lv5(f'func kwargs: {kwargs}')
        result = func(**kwargs)
    except NotFoundError:
        debug.lv3(f'{kind} pattern "{pattern}" had zero matches')
        return []
    except Exception as err:
        raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
    if kind == 'snapshot':
        debug.lv5('Checking for snapshot')
        retval = [x['snapshot'] for x in result['snapshots']]
    elif kind in ['data_stream', 'template', 'component']:
        debug.lv5('Checking for data_stream/template/component')
        retval = [x['name'] for x in result[which['key']]]
    else:
        debug.lv5('Checking for alias/ilm/index')
        retval = list(result.keys())
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_aliases(client: 'Elasticsearch', name: str) -> t.Sequence[str]:
    """Get aliases from index 'name'"""
    debug.lv2('Starting function...')
    res = client.indices.get(index=name)
    debug.lv5(f'get_aliases response: {res}')
    try:
        debug.lv4('TRY: getting aliases')
        retval = list(res[name]['aliases'].keys())
        debug.lv5(f"list(res[name]['aliases'].keys()) = {retval}")
    except KeyError:
        retval = None
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_backing_indices(client: 'Elasticsearch', name: str) -> t.Sequence[str]:
    """Get the backing indices from the named data_stream"""
    debug.lv2('Starting function...')
    resp = resolver(client, name)
    data_streams = resp['data_streams']
    retval = []
    if data_streams:
        debug.lv5('Checking for backing indices...')
        if len(data_streams) > 1:
            debug.lv3('Exiting function, raising exception')
            debug.lv5(f'ResultNotExpected: More than 1 found {data_streams}')
            raise ResultNotExpected(
                f'Expected only a single data_stream matching {name}'
            )
        retval = data_streams[0]['backing_indices']
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_ds_current(client: 'Elasticsearch', name: str) -> str:
    """
    Find which index is the current 'write' index of the data_stream
    This is best accomplished by grabbing the last backing_index
    """
    debug.lv2('Starting function...')
    backers = get_backing_indices(client, name)
    retval = None
    if backers:
        retval = backers[-1]
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_ilm(client: 'Elasticsearch', pattern: str) -> t.Union[t.Dict[str, str], None]:
    """Get any ILM entity in ES that matches pattern"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: ilm.get_lifecycle')
        retval = client.ilm.get_lifecycle(name=pattern)
    except Exception as err:
        msg = f'Unable to get ILM lifecycle matching {pattern}. Error: {prettystr(err)}'
        logger.critical(msg)
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise ResultNotExpected(msg) from err
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_ilm_phases(client: 'Elasticsearch', name: str) -> t.Dict:
    """Return the policy/phases part of the ILM policy identified by 'name'"""
    debug.lv2('Starting function...')
    ilm = get_ilm(client, name)
    try:
        debug.lv4('TRY: get ILM phases')
        retval = ilm[name]['policy']['phases']
    except KeyError as err:
        msg = f'Unable to get ILM lifecycle named {name}. Error: {prettystr(err)}'
        logger.critical(msg)
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise ResultNotExpected(msg) from err
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def get_write_index(client: 'Elasticsearch', name: str) -> str:
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param name: An alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: The the index name associated with the alias that is designated
    ``is_write_index``
    """
    debug.lv2('Starting function...')
    response = client.indices.get_alias(index=name)
    debug.lv5(f'get_alias response: {response}')
    retval = None
    for index in list(response.keys()):
        try:
            debug.lv4('TRY: get write index')
            if response[index]['aliases'][name]['is_write_index']:
                retval = index
                break
        except KeyError:
            continue
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def ilm_explain(client: 'Elasticsearch', name: str) -> t.Union[t.Dict, None]:
    """Return the results from the ILM Explain API call for the named index"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: ilm.explain_lifecycle')
        retval = client.ilm.explain_lifecycle(index=name)['indices'][name]
    except KeyError:
        debug.lv5('Index name changed')
        new = list(client.ilm.explain_lifecycle(index=name)['indices'].keys())[0]
        debug.lv5(f'ilm.explain_lifecycle response: {new}')
        retval = client.ilm.explain_lifecycle(index=new)['indices'][new]
    except NotFoundError as err:
        logger.warning(f'Datastream/Index Name changed. {name} was not found')
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise NameChanged(f'{name} was not found, likely due to a name change') from err
    except Exception as err:
        msg = f'Unable to get ILM information for index {name}'
        logger.critical(msg)
        debug.lv3('Exiting function, raising exception')
        debug.lv5(f'Exception: {prettystr(err)}')
        raise ResultNotExpected(f'{msg}. Exception: {prettystr(err)}') from err
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval


def ilm_move(
    client: 'Elasticsearch', name: str, current_step: t.Dict, next_step: t.Dict
) -> None:
    """Move index 'name' from the current step to the next step"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: ilm.move_to_step')
        res = client.ilm.move_to_step(
            index=name, current_step=current_step, next_step=next_step
        )
        debug.lv5(f'ilm.move_to_step response: {res}')
    except Exception as err:
        msg = (
            f'Unable to move index {name} to ILM next step: {next_step}. '
            f'Error: {prettystr(err)}'
        )
        logger.critical(msg)
        raise ResultNotExpected(msg, (err,))
    debug.lv3('Exiting function')


def put_comp_tmpl(client: 'Elasticsearch', name: str, component: t.Dict) -> None:
    """Publish a component template"""
    debug.lv2('Starting function...')
    wait_kwargs = {'name': name, 'kind': 'component_template', 'pause': PAUSE_VALUE}
    f_kwargs = {'name': name, 'template': component, 'create': True}
    wait_wrapper(
        client,
        Exists,
        wait_kwargs,
        client.cluster.put_component_template,
        f_kwargs,
    )
    debug.lv3('Exiting function')


def put_idx_tmpl(
    client: 'Elasticsearch',
    name: str,
    index_patterns: t.List[str],
    components: t.List[str],
    data_stream: t.Optional[t.Dict] = None,
) -> None:
    """Publish an index template"""
    debug.lv2('Starting function...')
    wait_kwargs = {'name': name, 'kind': 'index_template', 'pause': PAUSE_VALUE}
    f_kwargs = {
        'name': name,
        'composed_of': components,
        'data_stream': data_stream,
        'index_patterns': index_patterns,
        'create': True,
    }
    wait_wrapper(
        client,
        Exists,
        wait_kwargs,
        client.indices.put_index_template,
        f_kwargs,
    )
    debug.lv3('Exiting function')


def put_ilm(
    client: 'Elasticsearch', name: str, policy: t.Union[t.Dict, None] = None
) -> None:
    """Publish an ILM Policy"""
    debug.lv2('Starting function...')
    try:
        debug.lv4('TRY: ilm.put_lifecycle')
        debug.lv5(f'ilm.put_lifecycle name: {name}, policy: {policy}')
        res = client.ilm.put_lifecycle(name=name, policy=policy)
        debug.lv5(f'ilm.put_lifecycle response: {res}')
    except Exception as err:
        msg = f'Unable to put ILM policy {name}. Error: {prettystr(err)}'
        logger.error(msg)
        raise TestbedFailure(msg) from err
    debug.lv3('Exiting function')


def resolver(client: 'Elasticsearch', name: str) -> dict:
    """
    Resolve details about the entity, be it an index, alias, or data_stream

    Because you can pass search patterns and aliases as name, each element comes back
    as an array:

    {'indices': [], 'aliases': [], 'data_streams': []}

    If you only resolve a single index or data stream, you will still have a 1-element
    list
    """
    debug.lv2('Starting function...')
    _ = client.indices.resolve_index(name=name, expand_wildcards=['open', 'closed'])
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {_}')
    return _


def rollover(client: 'Elasticsearch', name: str) -> None:
    """Rollover alias or data_stream identified by name"""
    debug.lv2('Starting function...')
    res = client.indices.rollover(alias=name, wait_for_active_shards='all')
    debug.lv5(f'rollover response: {res}')
    debug.lv3('Exiting function')


def snapshot_name(client: 'Elasticsearch', name: str) -> t.Union[t.AnyStr, None]:
    """Get the name of the snapshot behind the mounted index data"""
    debug.lv2('Starting function...')
    res = {}
    if exists(client, 'index', name):  # Can jump straight to nested keys if it exists
        res = client.indices.get(index=name)[name]['settings']['index']
        debug.lv5(f'indices.get response: {res}')
    try:
        debug.lv4("TRY: retval = res['store']['snapshot']['snapshot_name']")
        retval = res['store']['snapshot']['snapshot_name']
    except KeyError:
        logger.error(f'{name} is not a searchable snapshot')
        retval = None
    debug.lv3('Exiting function, returning value')
    debug.lv5(f'Value = {retval}')
    return retval
