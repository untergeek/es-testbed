# es-testbed
A way to create indices, datastreams, and snapshots to facilitate testing.

# Preliminary Documentation

## 1. Create a Preset

Create a preset directory. An example preset directory is in
`src/es_testbed/presets/searchable_test`.

Your preset directory must include the following files:

- A plan YAML file, e.g. `plan.yml`
- A buildlist YAML file, e.g. `buildlist.yml`
- A `functions.py` file (the actual python code), which must contain a
  function named `doc_generator()`. This function must accept all kwargs from
  the buildlist's `options`
- A `definitions.py` file, which is a Python variable file that helps find
  the path to the module, etc., as well as import the plan, the buildlist,
  the mappings and settings, etc. This must at least include a `get_plan()`
  function that returns a dictionary of a built/configured plan.
- A `mappings.json` file (contains the index mappings your docs need)
- A `settings.json` file (contains the index settings)
Any other files can be included to help your doc_generator function, e.g.
Faker definitions and classes, etc. Once the preset module is imported,
relative imports should work.

**Note:** If `ilm['enabled'] == False`, the other subkeys will be ignored. In fact, `ilm: False` is also acceptable.

Acceptable values for `readonly` are the tier names where `readonly` is acceptable: `hot`, `warm`, or `cold`.

Save this for step 2.

## 2. Create a TestBed

**Must have an Elasticsearch client established to proceed**

```
from es_testbed import TestBed

tb = TestBed(client, **kwargs)
```

For a builtin preset, like `searchable_test`, this is:

```
tb = TestBed(client, builtin='searchable_test', scenario=None)
```

`scenario` can be one of many, if configured.

For using your own preset in a filesystem path:

```
tb = TestBed(client, path='/path/to/preset/dir')
```

`path` _must_ be a directory.

For importing from a Git repository:

```
tb = TestBed(client, url='user:token@https://github.com/GITUSER/reponame.git', ref='main', path='subpath/to/preset')
```

Note that `user:token@` is only necessary if it's a protected repository.
In this case `path` must be a subdirectory of the repository. `depth=1` is manually set, so only the most recent
bits will be pulled into a tmpdir, which will be destroyed as part of `teardown()`.

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