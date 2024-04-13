"""Configuration bits returned by function"""
from es_testbed.defaults import MAPPING

SETTINGS: dict = {
    'index.lifecycle.name': 'my-lifecycle-policy'
}

COMP_MAPPINGS: dict = {
    "template": {
        "settings": SETTINGS
    }
}

COMP_SETTINGS: dict = {
    "template": {
        "mappings": MAPPING
    }
}

def hot() -> dict:
    """Return the hot ILM phase"""
    return {
        "actions": {
            "rollover": {
                "max_primary_shard_size": "1gb"
            }
        }
    }

def warm() -> dict:
    """Return the warm ILM phase"""
    return {
        "min_age": "30m",
        "actions": {}
    }

def cold(repository='found-snapshots') -> dict:
    """Return the cold ILM phase"""
    return {
        "min_age": "1h",
        "actions": {
            "searchable_snapshot": {
            "snapshot_repository": repository
            }
        }
    }

def frozen(repository='found-snapshots') -> dict:
    """Return the frozen ILM phase"""
    return {
        "min_age": "2h",
        "actions": {
            "searchable_snapshot": {
            "snapshot_repository": repository
            }
        }
    }

def delete() -> dict:
    """Return the delete ILM phase"""
    return {
        "min_age": "2d",
        "actions": {
            "delete": {}
        }
    }

def generate_ilm(phases: list=None, repository=None) -> dict:
    """Generate a full ILM policy based on which phases are passed"""
    policy = {"policy": {"phases": {}}}
    # We always have at least a hot and a delete phase
    policy['policy']['phases']['hot'] = hot()
    if 'warm' in phases:
        policy['policy']['phases']['warm'] = warm()
    if repository:
        if 'cold' in phases:
            policy['policy']['phases']['cold'] = cold(repository=repository)
        if 'frozen' in phases:
            policy['policy']['phases']['frozen'] = frozen(repository=repository)
    # We always have at least a hot and a delete phase
    policy['policy']['phases']['delete'] = delete()
    return policy

def index_settings(ilm_policy=None) -> dict:
    """Generate index settings suitable for an index template"""
    settings = {}
    if ilm_policy:
        settings = {'index.lifecycle.name': ilm_policy}
    return settings

def index_mapping() -> dict:
    """Generate index mappings suitable for an index template"""
    return MAPPING

def index_template(index_patterns: list, data_stream: bool=True, components: list=None) -> dict:
    """Generate the body of an index template"""
    template = {"index_patterns": index_patterns}
    if data_stream:
        template['data_stream'] = { }
    if components:
        template['composed_of'] = components
    return template
