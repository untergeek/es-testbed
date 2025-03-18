"""Functions that make Elasticsearch API Calls"""

import typing as t
import logging
from os import getenv
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

# pylint: disable=W0707


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
    try:
        client.indices.modify_data_stream(actions=actions, body=None)
    except Exception as err:
        raise ResultNotExpected(
            f'Unable to modify data_streams. {prettystr(err)}'
        ) from err


# pylint: disable=R0913,R0917
def wait_wrapper(
    client: 'Elasticsearch',
    wait_cls: t.Callable,
    wait_kwargs: t.Dict,
    func: t.Callable,
    f_kwargs: t.Dict,
) -> None:
    """Wrapper function for waiting on an object to be created"""
    try:
        func(**f_kwargs)
        test = wait_cls(client, **wait_kwargs)
        test.wait()
    except EsWaitFatal as wait:
        msg = f'{wait.message}. Elapsed time: {wait.elapsed}. Errors: {wait.errors}'
        raise TestbedFailure(msg) from wait
    except EsWaitTimeout as wait:
        msg = f'{wait.message}. Elapsed time: {wait.elapsed}. Timeout: {wait.timeout}'
        raise TestbedFailure(msg) from wait
    except TransportError as err:
        raise TestbedFailure(
            f'Elasticsearch TransportError class exception encountered:'
            f'{prettystr(err)}'
        ) from err
    except Exception as err:
        raise TestbedFailure(f'General Exception caught: {prettystr(err)}') from err


def create_data_stream(client: 'Elasticsearch', name: str) -> None:
    """Create a data_stream"""
    wait_kwargs = {'name': name, 'kind': 'data_stream', 'pause': PAUSE_VALUE}
    f_kwargs = {'name': name}
    wait_wrapper(
        client, Exists, wait_kwargs, client.indices.create_data_stream, f_kwargs
    )


def create_index(
    client: 'Elasticsearch',
    name: str,
    aliases: t.Union[t.Dict, None] = None,
    settings: t.Union[t.Dict, None] = None,
    tier: str = 'hot',
) -> None:
    """Create named index"""
    if not settings:
        settings = get_routing(tier=tier)
    else:
        settings.update(get_routing(tier=tier))
    wait_kwargs = {'name': name, 'kind': 'index', 'pause': PAUSE_VALUE}
    f_kwargs = {
        'index': name,
        'aliases': aliases,
        'mappings': MAPPING,
        'settings': settings,
    }
    wait_wrapper(client, Exists, wait_kwargs, client.indices.create, f_kwargs)
    return exists(client, 'index', name)


def verify(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Optional[str] = None,
) -> bool:
    """Verify that whatever was deleted is actually deleted"""
    success = True
    items = name.split(',')
    for item in items:
        result = exists(client, kind, item, repository=repository)
        if result:  # That means it's still in the cluster
            success = False
    return success


def delete(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Optional[str] = None,
) -> bool:
    """Delete the named object of type kind"""
    which = emap(kind, client)
    func = which['delete']
    success = False
    if name is not None:  # Typically only with ilm
        try:
            if kind == 'snapshot':
                res = func(snapshot=name, repository=repository)
            elif kind == 'index':
                res = func(index=name)
            else:
                res = func(name=name)
        except NotFoundError as err:
            logger.warning(f'{kind} named {name} not found: {prettystr(err)}')
            return True
        except Exception as err:
            raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
        if 'acknowledged' in res and res['acknowledged']:
            success = True
            logger.info(f'Deleted {which["plural"]}: "{name}"')
        else:
            success = verify(client, kind, name, repository=repository)
    else:
        logger.debug(f'"{kind}" has a None value for name')
    return success


def do_snap(
    client: 'Elasticsearch', repo: str, snap: str, idx: str, tier: str = 'cold'
) -> None:
    """Perform a snapshot"""
    wait_kwargs = {'snapshot': snap, 'repository': repo, 'pause': 1, 'timeout': 60}
    f_kwargs = {'repository': repo, 'snapshot': snap, 'indices': idx}
    wait_wrapper(client, Snapshot, wait_kwargs, client.snapshot.create, f_kwargs)

    # Mount the index accordingly
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
    fix_aliases(client, idx, mounted_name(idx, tier))


def exists(
    client: 'Elasticsearch', kind: str, name: str, repository: t.Union[str, None] = None
) -> bool:
    """Return boolean existence of the named kind of object"""
    if name is None:
        return False
    retval = True
    func = emap(kind, client)['exists']
    try:
        if kind == 'snapshot':
            # Expected response: {'snapshots': [{'snapshot': name, ...}]}
            # Since we are specifying by name, there should only be one returned
            res = func(snapshot=name, repository=repository)
            logger.debug(f'Snapshot response: {res}')
            # If there are no entries, load a default None value for the check
            _ = dict(res['snapshots'][0]) if res else {'snapshot': None}
            # Since there should be only 1 snapshot with this name, we can check it
            retval = bool(_['snapshot'] == name)
        elif kind == 'ilm':
            # There is no true 'exists' method for ILM, so we have to get the policy
            # and check for a NotFoundError
            retval = bool(name in dict(func(name=name)))
        elif kind in ['index', 'data_stream']:
            retval = func(index=name)
        else:
            retval = func(name=name)
    except NotFoundError:
        retval = False
    except Exception as err:
        raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
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
    if not options:
        options = {}
    for doc in doc_generator(**options):
        client.index(index=name, document=doc)
    client.indices.flush(index=name)
    client.indices.refresh(index=name)


def find_write_index(client: 'Elasticsearch', name: str) -> t.AnyStr:
    """Find the write_index for an alias by searching any index the alias points to"""
    retval = None
    for alias in get_aliases(client, name):
        retval = get_write_index(client, alias)
        if retval:
            break
    return retval


def fix_aliases(client: 'Elasticsearch', oldidx: str, newidx: str) -> None:
    """Fix aliases using the new and old index names as data"""
    # Delete the original index
    client.indices.delete(index=oldidx)
    # Add the original index name as an alias to the mounted index
    client.indices.put_alias(index=f'{newidx}', name=oldidx)


def get(
    client: 'Elasticsearch',
    kind: str,
    pattern: str,
    repository: t.Optional[str] = None,
) -> t.Sequence[str]:
    """get any/all objects of type kind matching pattern"""
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
        result = func(**kwargs)
    except NotFoundError:
        logger.debug(f'{kind} pattern "{pattern}" had zero matches')
        return []
    except Exception as err:
        raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
    if kind == 'snapshot':
        retval = [x['snapshot'] for x in result['snapshots']]
    elif kind in ['data_stream', 'template', 'component']:
        retval = [x['name'] for x in result[which['key']]]
    else:
        # ['alias', 'ilm', 'index']
        retval = list(result.keys())
    return retval


def get_aliases(client: 'Elasticsearch', name: str) -> t.Sequence[str]:
    """Get aliases from index 'name'"""
    res = client.indices.get(index=name)
    try:
        retval = list(res[name]['aliases'].keys())
    except KeyError:
        retval = None
    return retval


def get_backing_indices(client: 'Elasticsearch', name: str) -> t.Sequence[str]:
    """Get the backing indices from the named data_stream"""
    resp = resolver(client, name)
    data_streams = resp['data_streams']
    retval = []
    if data_streams:
        if len(data_streams) > 1:
            raise ResultNotExpected(
                f'Expected only a single data_stream matching {name}'
            )
        retval = data_streams[0]['backing_indices']
    return retval


def get_ds_current(client: 'Elasticsearch', name: str) -> str:
    """
    Find which index is the current 'write' index of the data_stream
    This is best accomplished by grabbing the last backing_index
    """
    backers = get_backing_indices(client, name)
    retval = None
    if backers:
        retval = backers[-1]
    return retval


def get_ilm(client: 'Elasticsearch', pattern: str) -> t.Union[t.Dict[str, str], None]:
    """Get any ILM entity in ES that matches pattern"""
    try:
        return client.ilm.get_lifecycle(name=pattern)
    except Exception as err:
        msg = f'Unable to get ILM lifecycle matching {pattern}. Error: {prettystr(err)}'
        logger.critical(msg)
        raise ResultNotExpected(msg) from err


def get_ilm_phases(client: 'Elasticsearch', name: str) -> t.Dict:
    """Return the policy/phases part of the ILM policy identified by 'name'"""
    ilm = get_ilm(client, name)
    try:
        return ilm[name]['policy']['phases']
    except KeyError as err:
        msg = f'Unable to get ILM lifecycle named {name}. Error: {prettystr(err)}'
        logger.critical(msg)
        raise ResultNotExpected(msg) from err


def get_write_index(client: 'Elasticsearch', name: str) -> str:
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param name: An alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`

    :returns: The the index name associated with the alias that is designated
    ``is_write_index``
    """
    response = client.indices.get_alias(index=name)
    retval = None
    for index in list(response.keys()):
        try:
            if response[index]['aliases'][name]['is_write_index']:
                retval = index
                break
        except KeyError:
            continue
    return retval


def ilm_explain(client: 'Elasticsearch', name: str) -> t.Union[t.Dict, None]:
    """Return the results from the ILM Explain API call for the named index"""
    try:
        retval = client.ilm.explain_lifecycle(index=name)['indices'][name]
    except KeyError:
        logger.debug('Index name changed')
        new = list(client.ilm.explain_lifecycle(index=name)['indices'].keys())[0]
        retval = client.ilm.explain_lifecycle(index=new)['indices'][new]
    except NotFoundError as err:
        logger.warning(f'Datastream/Index Name changed. {name} was not found')
        raise NameChanged(f'{name} was not found, likely due to a name change') from err
    except Exception as err:
        msg = f'Unable to get ILM information for index {name}'
        logger.critical(msg)
        raise ResultNotExpected(f'{msg}. Exception: {prettystr(err)}') from err
    return retval


def ilm_move(
    client: 'Elasticsearch', name: str, current_step: t.Dict, next_step: t.Dict
) -> None:
    """Move index 'name' from the current step to the next step"""
    try:
        client.ilm.move_to_step(
            index=name, current_step=current_step, next_step=next_step
        )
    except Exception as err:
        msg = (
            f'Unable to move index {name} to ILM next step: {next_step}. '
            f'Error: {prettystr(err)}'
        )
        logger.critical(msg)
        raise ResultNotExpected(msg, (err,))


def put_comp_tmpl(client: 'Elasticsearch', name: str, component: t.Dict) -> None:
    """Publish a component template"""
    wait_kwargs = {'name': name, 'kind': 'component_template', 'pause': PAUSE_VALUE}
    f_kwargs = {'name': name, 'template': component, 'create': True}
    wait_wrapper(
        client,
        Exists,
        wait_kwargs,
        client.cluster.put_component_template,
        f_kwargs,
    )


def put_idx_tmpl(
    client: 'Elasticsearch',
    name: str,
    index_patterns: t.List[str],
    components: t.List[str],
    data_stream: t.Optional[t.Dict] = None,
) -> None:
    """Publish an index template"""
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


def put_ilm(
    client: 'Elasticsearch', name: str, policy: t.Union[t.Dict, None] = None
) -> None:
    """Publish an ILM Policy"""
    try:
        client.ilm.put_lifecycle(name=name, policy=policy)
    except Exception as err:
        raise TestbedFailure(
            f'Unable to put index lifecycle policy {name}. Error: {prettystr(err)}'
        ) from err


def resolver(client: 'Elasticsearch', name: str) -> dict:
    """
    Resolve details about the entity, be it an index, alias, or data_stream

    Because you can pass search patterns and aliases as name, each element comes back
    as an array:

    {'indices': [], 'aliases': [], 'data_streams': []}

    If you only resolve a single index or data stream, you will still have a 1-element
    list
    """
    return client.indices.resolve_index(name=name, expand_wildcards=['open', 'closed'])


def rollover(client: 'Elasticsearch', name: str) -> None:
    """Rollover alias or data_stream identified by name"""
    client.indices.rollover(alias=name, wait_for_active_shards='all')


def snapshot_name(client: 'Elasticsearch', name: str) -> t.Union[t.AnyStr, None]:
    """Get the name of the snapshot behind the mounted index data"""
    res = {}
    if exists(client, 'index', name):  # Can jump straight to nested keys if it exists
        res = client.indices.get(index=name)[name]['settings']['index']
    try:
        retval = res['store']['snapshot']['snapshot_name']
    except KeyError:
        logger.error(f'{name} is not a searchable snapshot')
        retval = None
    return retval
