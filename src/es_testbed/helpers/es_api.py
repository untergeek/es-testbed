"""Functions that make Elasticsearch API Calls"""
from es_testbed.defaults import MAPPING, TIER
from es_testbed.exceptions import TestbedMisconfig
from es_testbed.helpers import config
from es_testbed.helpers.utils import doc_gen

# pylint: disable=broad-except

def get_routing(tier='hot'):
    """Return the routing allocation tier preference"""
    try:
        pref = TIER[tier]['pref']
    except KeyError:
        # Fallback value
        pref = 'data_content'
    return {
        'index': {
            'routing': {
                'allocation': {
                    'include': {
                        '_tier_preference': pref
                    }
                }
            }
        }
    }

def create_index(client, idx, tier='hot'):
    """Create named index"""
    # As a preventative measure, pre-delete anything we are going to create
    delete_index(client, idx)
    client.indices.create(index=idx, mappings=MAPPING, settings=get_routing(tier=tier))

def delete_index(client, idx):
    """Delete named index"""
    try:
        client.indices.delete(index=idx)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def delete_snapshot(client, repo, snap):
    """Delete named snapshot from repository"""
    try:
        client.snapshot.delete(repository=repo, snapshot=snap)
    except Exception:
        # We don't care if it fails. Delete it if it's there.
        pass

def fill_index(client, idx, count, start_num, match=True):
    """
    Create and fill the named index with mappings and settings as directed

    :param client: ES client
    :param idx: Index name
    :param count: The number of docs to create
    :param start_number: Where to start the incrementing number
    :param match: Whether to use the default values for key (True) or random strings (False)

    :type client: es
    :type idx: str
    :type count: int
    :type start_number: int
    :type match: bool

    :rtype: None
    :returns: No return value
    """
    for doc in doc_gen(count=count, start_at=start_num, match=match):
        client.index(index=idx, document=doc)
    client.indices.flush(index=idx)
    client.indices.refresh(index=idx)

def do_snap(client, repo, snap, idx, tier='cold'):
    """Perform a snapshot"""
    delete_snapshot(client, repo, snap)
    client.snapshot.create(repository=repo, snapshot=snap, indices=idx, wait_for_completion=True)

    # Mount the index accordingly
    client.searchable_snapshots.mount(
        repository=repo, snapshot=snap, index=idx, index_settings=get_routing(tier=tier),
        renamed_index=f'{TIER[tier]["pfx"]}-{idx}', storage=TIER[tier]["sto"],
        wait_for_completion=True)

def fix_aliases(client, oldidx, newidx):
    """Fix aliases using the new and old index names as data"""
    # Delete the original index
    client.indices.delete(index=oldidx)
    # Add the original index name as an alias to the mounted index
    client.indices.put_alias(index=f'{newidx}', name=oldidx)

def resolver(client, entity):
    """Resolve details about the entity, be it an index or a datastream"""
    resp = client.indices.resolve_index(entity, expand_wildcards='all')
    return resp

def set_component_template(client, name, kind, ilm_policy=None) -> None:
    """Put out a component template"""
    template = {'template':{}}
    if kind == 'settings':
        template['template'][kind] = config.index_settings(ilm_policy=ilm_policy)
    elif kind == 'mapping':
        template['template'][kind] = config.index_mapping()
    try:
        client.cluster.put_component_template(name=name, template=template, create=True)
    except Exception as exc:
        raise TestbedMisconfig(f'Unable to create component template {name}. Error: {exc}') from exc

def set_index_template(
        client, name, index_patterns, data_stream: bool=True, components: list=None) -> None:
    """Put out an index template"""
    try:
        client.indices.put_index_template(
            name=name,
            composed_of=components,
            data_stream=data_stream,
            index_patterns=index_patterns,
            create=True,
        )
    except Exception as exc:
        raise TestbedMisconfig(f'Unable to create index template {name}. Error: {exc}') from exc

def create_datastream(client, datastream):
    """Create a datastream"""
    client.info()
    print(f'datastream = {datastream}')
