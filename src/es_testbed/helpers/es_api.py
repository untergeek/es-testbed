"""Functions that make Elasticsearch API Calls"""
from elasticsearch8 import Elasticsearch
from es_testbed.defaults import MAPPING
from es_testbed import exceptions as exc
from es_testbed.helpers.waiters import wait_for_it
from es_testbed.helpers.utils import doc_gen, get_routing, mounted_name, storage_type

# pylint: disable=broad-except

def change_ds(client: Elasticsearch, actions: dict=None) -> None:
    """Change/Modify/Update a datastream"""
    try:
        client.indices.modify_data_stream(actions=actions, body=None)
    except Exception as err:
        raise exc.ResultNotExpected(f'Unable to modify datastreams. {err}') from err

def create_datastream(client: Elasticsearch, name: str) -> None:
    """Create a datastream"""
    try:
        client.create_data_stream(name)
        wait_for_it(client, 'datastream', name=name)
    except Exception as err:
        raise exc.TestbedMisconfig(
            f'Unable to create datastream {name}. Error: {err}') from err

def create_index(
        client: Elasticsearch,
        name: str,
        aliases: dict=None,
        settings: dict=None,
        tier: str='hot'
    ) -> None:
    """Create named index"""
    if not settings:
        settings = get_routing(tier=tier)
    else:
        settings.update(get_routing(tier=tier))
    # As a preventative measure, pre-delete anything we are going to create
    delete_index(client, name)
    client.indices.create(
        index=name,
        aliases=aliases,
        mappings=MAPPING,
        settings=settings
    )
    try:
        wait_for_it(client, 'index', name=name)
    except exc.TimeoutException as err:
        raise exc.ResultNotExpected(f'Failed to create index {name}') from err

def delete_component(client: Elasticsearch, name: str) -> None:
    """Delete named component template"""
    try:
        client.cluster.delete_component_template(name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_ds(client: Elasticsearch, name: str) -> None:
    """Delete named datastream and all backing indices"""
    try:
        client.indices.delete_data_stream(name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_ilm(client: Elasticsearch, name: str) -> None:
    """Delete named ILM policy"""
    try:
        client.ilm.delete_lifecycle(name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_index(client: Elasticsearch, name: str) -> None:
    """Delete named index"""
    try:
        client.indices.delete(index=name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_snapshot(client: Elasticsearch, repo: str, name: str) -> None:
    """Delete named snapshot from repository"""
    try:
        client.snapshot.delete(repository=repo, snapshot=name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_template(client: Elasticsearch, name: str) -> None:
    """Delete named ILM policy"""
    try:
        client.indices.delete_index_template(name)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def do_snap(client: Elasticsearch, repo: str, snap: str, idx: str, tier: str='cold') -> None:
    """Perform a snapshot"""
    delete_snapshot(client, repo, snap)
    client.snapshot.create(repository=repo, snapshot=snap, indices=idx)
    wait_for_it(client, 'snapshot', snapshot=snap, repository=repo, wait_interval=1, max_wait=60)

    # Mount the index accordingly
    client.searchable_snapshots.mount(
        repository=repo, snapshot=snap, index=idx, index_settings=get_routing(tier=tier),
        renamed_index=mounted_name(idx, tier), storage=storage_type(tier),
        wait_for_completion=True)

def fill_index(
        client: Elasticsearch,
        name: str=None,
        count: int=None,
        start_num: int=None,
        match: bool=True
    ) -> None:
    """
    Create and fill the named index with mappings and settings as directed

    :param client: ES client
    :param name: Index name
    :param count: The number of docs to create
    :param start_number: Where to start the incrementing number
    :param match: Whether to use the default values for key (True) or random strings (False)

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

def fix_aliases(client: Elasticsearch, oldidx: str, newidx: str) -> None:
    """Fix aliases using the new and old index names as data"""
    # Delete the original index
    client.indices.delete(index=oldidx)
    # Add the original index name as an alias to the mounted index
    client.indices.put_alias(index=f'{newidx}', name=oldidx)

def get_backing_indices(client: Elasticsearch, name: str) -> list:
    """Get the backing indices from the named data_stream"""
    resp = resolver(client, name)
    data_streams = resp['data_streams']
    if len(data_streams) > 1:
        raise exc.ResultNotExpected(f'Expected only a single data_stream matching {name}')
    return data_streams[0]['backing_indices']

def get_ds_generation(client: Elasticsearch, name: str) -> dict:
    """Get information for the named data_stream"""
    response = client.indices.get_data_stream(name=name)['data_streams']
    retval = None
    for entry in response:
        if entry['name'] == name:
            retval = entry['generation']
            break
    return retval

def get_ds_current(client: Elasticsearch, name: str) -> str:
    """Find which index is the current 'write' index of the datastream"""
    backers = get_backing_indices(client, name)
    generation = get_ds_generation(client, name)
    retval = None
    for idx in backers:
        if idx[-1] == generation:
            retval = idx
            break
    return retval

def get_write_index(client: Elasticsearch, name: str) -> str:
    """
    Calls :py:meth:`~.elasticsearch.client.IndicesClient.get_alias`

    :param client: A client connection object
    :param name: An alias name

    :type client: :py:class:`~.elasticsearch.Elasticsearch`
    :type name: str

    :returns: The the index name associated with the alias that is designated ``is_write_index``
    :rtype: str
    """
    response = client.indices.get_alias(index=name)
    retval = None
    for index in list(response.keys()):
        if response[index]['aliases'][name]['is_write_index']:
            retval = index
            break
    return retval

def put_comp_tmpl(client: Elasticsearch, name: str, component: dict) -> None:
    """Publish a component template"""
    try:
        client.cluster.put_component_template(name=name, template=component, create=True)
        wait_for_it(client, 'component', name=name)
    except Exception as err:
        raise exc.TestbedMisconfig(
            f'Unable to create component template {name}. Error: {err}') from err

def put_idx_tmpl(
        client, name: str, index_patterns: list, components: list,
        data_stream: dict=None) -> None:
    """Publish an index template"""
    ds = None
    if data_stream:
        ds = {}
    try:
        client.indices.put_index_template(
            name=name,
            composed_of=components,
            data_stream=ds,
            index_patterns=index_patterns,
            create=True,
        )
        wait_for_it(client, 'template', name=name)
    except Exception as err:
        raise exc.TestbedMisconfig(
            f'Unable to create index template {name}. Error: {err}') from err

def put_ilm(client: Elasticsearch, name: str, policy: dict=None) -> None:
    """Publish an ILM Policy"""
    client.ilm.put_lifecycle(name=name, policy=policy)

def resolver(client: Elasticsearch, name: str) -> dict:
    """
    Resolve details about the entity, be it an index, alias, or data_stream
    
    Because you can pass search patterns and aliases as name, each element comes back as an array:
    
    {'indices': [], 'aliases': [], 'data_streams': []}
    
    If you only resolve a single index or data stream, you will still have a 1-element list
    """
    return client.indices.resolve_index(name=name, expand_wildcards=['open', 'closed'])

def rollover(client: Elasticsearch, name: str) -> None:
    """Rollover alias or datastream identified by name"""
    client.indices.rollover(alias=name, wait_for_active_shards=True)
