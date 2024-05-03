"""Functions that make Elasticsearch API Calls"""

import typing as t
import logging
from os import getenv
from elasticsearch8.exceptions import NotFoundError
from es_wait import Exists, Snapshot
from ..defaults import MAPPING, PAUSE_DEFAULT, PAUSE_ENVVAR
from ..exceptions import (
    NameChanged,
    ResultNotExpected,
    TestbedFailure,
    TestbedMisconfig,
    TimeoutException,
)
from ..helpers.utils import (
    doc_gen,
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


def change_ds(client: 'Elasticsearch', actions: t.Union[str, None] = None) -> None:
    """Change/Modify/Update a datastream"""
    try:
        client.indices.modify_data_stream(actions=actions, body=None)
    except Exception as err:
        raise ResultNotExpected(
            f'Unable to modify datastreams. {prettystr(err)}'
        ) from err


def create_data_stream(client: 'Elasticsearch', name: str) -> None:
    """Create a datastream"""
    try:
        client.indices.create_data_stream(name=name)
        test = Exists(client, name=name, kind='datastream', pause=PAUSE_VALUE)
        test.wait()
    except Exception as err:
        raise TestbedFailure(
            f'Unable to create datastream {name}. Error: {prettystr(err)}'
        ) from err


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
    client.indices.create(
        index=name, aliases=aliases, mappings=MAPPING, settings=settings
    )
    try:
        test = Exists(client, name=name, kind='index', pause=PAUSE_VALUE)
        test.wait()
    except TimeoutException as err:
        raise ResultNotExpected(f'Failed to create index {name}') from err
    return exists(client, 'index', name)


def verify(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Union[str, None] = None,
) -> bool:
    """Verify that whatever was deleted is actually deleted"""
    success = True
    items = ','.split(name)
    for item in items:
        result = exists(client, kind, item, repository=repository)
        if result:  # That means it's still in the cluster
            success = False
    return success


def delete(
    client: 'Elasticsearch',
    kind: str,
    name: str,
    repository: t.Union[str, None] = None,
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
            logger.warning('%s named %s not found: %s', kind, name, prettystr(err))
            success = True
        except Exception as err:
            raise ResultNotExpected(f'Unexpected result: {prettystr(err)}') from err
        if 'acknowledged' in res and res['acknowledged']:
            success = True
            logger.info('Deleted %s: "%s"', which['plural'], name)
        else:
            success = verify(client, kind, name, repository=repository)
    else:
        logger.debug('"%s" has a None value for name', kind)
    return success


def do_snap(
    client: 'Elasticsearch', repo: str, snap: str, idx: str, tier: str = 'cold'
) -> None:
    """Perform a snapshot"""
    client.snapshot.create(repository=repo, snapshot=snap, indices=idx)
    test = Snapshot(client, snapshot=snap, repository=repo, pause=1, timeout=60)
    test.wait()

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
            retval = func(snapshot=name, repository=repository)
        elif kind == 'ilm':
            retval = func(name=name)
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
    name: t.Union[str, None] = None,
    count: t.Union[int, None] = None,
    start_num: t.Union[int, None] = None,
    match: bool = True,
) -> None:
    """
    Create and fill the named index with mappings and settings as directed

    :param client: ES client
    :param name: Index name
    :param count: The number of docs to create
    :param start_number: Where to start the incrementing number
    :param match: Whether to use the default values for key (True) or random strings
        (False)

    :type client: es
    :type name: str
    :type count: int
    :type start_number: int
    :type match: bool

    :rtype: None
    :returns: No return value
    """
    for doc in doc_gen(count=count, start_at=start_num, match=match):
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
    repository: t.Union[str, None] = None,
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
        logger.debug('%s pattern "%s" had zero matches', kind, pattern)
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
    Find which index is the current 'write' index of the datastream
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
        logger.warning('Datastream/Index Name changed. %s was not found', name)
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
        raise ResultNotExpected(msg, err)


def put_comp_tmpl(client: 'Elasticsearch', name: str, component: t.Dict) -> None:
    """Publish a component template"""
    try:
        client.cluster.put_component_template(
            name=name, template=component, create=True
        )
        test = Exists(client, name=name, kind='component', pause=PAUSE_VALUE)
        test.wait()
    except Exception as err:
        raise TestbedFailure(
            f'Unable to create component template {name}. Error: {prettystr(err)}'
        ) from err


def put_idx_tmpl(
    client,
    name: str,
    index_patterns: list,
    components: list,
    data_stream: t.Union[t.Dict, None] = None,
) -> None:
    """Publish an index template"""
    try:
        client.indices.put_index_template(
            name=name,
            composed_of=components,
            data_stream=data_stream,
            index_patterns=index_patterns,
            create=True,
        )
        test = Exists(client, name=name, kind='template', pause=PAUSE_VALUE)
        test.wait()
    except Exception as err:
        raise TestbedFailure(
            f'Unable to create index template {name}. Error: {prettystr(err)}'
        ) from err


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
    """Rollover alias or datastream identified by name"""
    client.indices.rollover(alias=name, wait_for_active_shards='all')


def snapshot_name(client: 'Elasticsearch', name: str) -> t.Union[t.AnyStr, None]:
    """Get the name of the snapshot behind the mounted index data"""
    res = {}
    if exists(client, 'index', name):  # Can jump straight to nested keys if it exists
        res = client.indices.get(index=name)[name]['settings']['index']
    try:
        retval = res['store']['snapshot']['snapshot_name']
    except KeyError:
        logger.error('%s is not a searchable snapshot')
        retval = None
    return retval
