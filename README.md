# es-testbed
A way to create indices, datastreams, and snapshots to facilitate testing.

# Preliminary Documentation

## 1. Create a TestPlan

```
from es_testbed import TestPlan

plan = {
    'type': 'indices',
    'prefix': 'es-testbed',
    'uniq': 'mytest',
    'ilm': False,
    'defaults': {
        'entity_count': 3,
        'docs': 10,
        'match': True,
        'searchable': None,
    }
}

tp = TestPlan(settings=plan)
```

Save this for step 2.

## 2. Create a TestBed

**Must have an Elasticsearch client established to proceed**

```
from es_testbed import TestBed

tb = TestBed(client, plan=tp)
```

### 2.1 Index Template creation (behind the scenes)

Based on the settings in step 1, you will have 2 component templates and one index template that
references them:

#### Component Template 1

```
es-testbed-cmp-mytest-000001
```

Settings:

```
{'index.number_of_replicas': 0}
```

If using a `rollover_alias` or ILM Policy, then additional values will automatically be added.

#### Component Template 2

```
es-testbed-cmp-mytest-000002
```

Mappings:

```
{
    'properties': {
        '@timestamp': {'type': 'date'},
        'message': {'type': 'keyword'},
        'number': {'type': 'long'},
        'nested': {'properties': {'key': {'type': 'keyword'}}},
        'deep': {'properties': {'l1': {'properties': {'l2': {
            'properties': {'l3': {'type': 'keyword'}}}}}}
        }
    }
}
```

#### Index Template
```
es-testbed-tmpl-mytest-000001
```

### 2.2 You have indices

Based on what was provided in step 1, you will have 3 indices with a basic mapping, and 10 documents each:

```
es-testbed-idx-mytest-000001
es-testbed-idx-mytest-000002
es-testbed-idx-mytest-000003
```

Documents will have been added per the TestPlan settings. The orthography for these documents is in
`es_testbed.helpers.utils.doc_gen()`. Counts are preserved and continue to grow from one index to
the next.

## 3. Perform your tests.

This is where the testing can be performed.

## 4. Teardown

```
tb.teardown()
```

Barring anything unusual happening, all indices, data_streams, ILM policies, index & component
templates, and snapshots (if an index is promoted to searchable snapshots) will be deleted as part
of the `teardown()` method.