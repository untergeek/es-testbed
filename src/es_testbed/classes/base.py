"""Base TestBed Class"""
from datetime import datetime, timezone
from es_testbed.exceptions import TestbedException, TestbedMisconfig
from es_testbed.helpers import es_api, utils

# Hot will be a child of TestBed
# Hot will not need to extend this much
# Cold will also be a child of TestBed
# Cold will extend the TestBed class to add repository and snapshot bits.
# Frozen will be a child of the Cold class, and extend/rewrite a few bits is all

# The actual rub is that in order to populate setup() (we should be able to manage teardown)
# properly, the actual testbed will be a child of the _actual_ class named for the tier they want
# to use, e.g. class TestHot(Hot), TestCold(Cold), or TestFrozen(Frozen). Then doing your super()
# and adding the test buildup steps to setup()
#

class Entity:
    """Object definining an entity used by Tracker"""
    def __init__(self):
        """Initialize"""
        self.entity_list = []

    def add(self, value):
        """Add/Append the value to the list"""
        self.entity_list.append(value)

    def get(self):
        """Return the entire list"""
        return self.entity_list

    def generator(self):
        """Generator object to return self.entity_list"""
        for entity in self.entity_list:
            yield entity

class Tracker:
    """Object for tracking entities created in TestBed"""
    ENTITIES = ['components', 'datastreams', 'ilm_policies', 'indices', 'snapshots', 'templates']
    def __init__(self):
        """Initialize"""
        self.start_time = self.timestamp()
        self.build_entities()

    @property
    def components(self):
        """Getter Decorator"""
        return self.components
    @property
    def datastreams(self):
        """Getter Decorator"""
        return self.datastreams
    @property
    def ilm_policies(self):
        """Getter Decorator"""
        return self.ilm_policies
    @property
    def indices(self):
        """Getter Decorator"""
        return self.indices
    @property
    def snapshots(self):
        """Getter Decorator"""
        return self.snapshots
    @property
    def templates(self):
        """Getter Decorator"""
        return self.templates

    def build_entities(self):
        """Build object attributes from entities"""
        for entity in self.ENTITIES:
            setattr(self, entity, Entity())

    def timestamp(self):
        """Get the timestamp right now"""
        return datetime.now(timezone.utc)


class TestBed:
    """Base TestBed Class"""
    PFXMAP = {
        'index': 'idx',
        'datastream': 'ds',
        'component': 'cmp',
        'ilm': 'ilm',
        'template': 'tmpl',
        'snapshot': 'snp',
    }
    def __init__(self, client, tier: str='hot', prefix='es-testbed'):
        """Initialize"""
        self.client = client
        self.tier = tier
        self.prefix = prefix
        self.rand = utils.randomstr(length=8, lowercase=True)
        self.patterns = []
        self.patterns.append(f"{prefix}-{self.PFXMAP['index']}-{self.rand}*")
        self.patterns.append(f"{prefix}-{self.PFXMAP['datastream']}-{self.rand}*")
        self.tracker = Tracker()

    @property
    def repository(self):
        """repository getter"""
        return self._repository

    @repository.setter
    def repository(self, value):
        """repository setter"""
        self._repository = value

    def namer(self, prefix, suffix=None):
        """
        Use self.rand as the core of all of our naming so as not to stomp on anything

        Prepend {prefix}- to {self.rand}. Append -{suffix:06} if it exists
        """
        if suffix:
            return f'{prefix}-{self.rand}-{suffix:06}'
        return f'{prefix}-{self.rand}'

    def setup(self):
        """Setup the instance"""
        # Each child class will need to extend setup for itself
        # self.add_ilm()
        # self.add_component_template() 2x, ostensibly
        # self.add_template()
        # self.add_index() or self.add_datastream()
        #   then...
        # es_api.fill_index(self.client, name, count, start_num, match=match)
        #   then...
        # self.add_index() again or self.ds_rollover()
        #   then...
        # es_api.fill_index(self.client, name, count, start_num, match=match)
        #   ...
        # You get the idea...

    def teardown(self):
        """Tear down anything we created"""
        # Each child class will need to extend teardown for itself
        # Teardown will need to clean up each of the lists
        # self.client.delete_datastream(name)
        # self.client.delete_index_template(name)
        # self.client.cluster.delete_component_template(name)
        # self.client.ilm.delete_lifecycle(name)

    def add_index(self):
        """Add an index to ES and to self.tracker.indices"""
        prefix = f"{self.prefix}-{self.PFXMAP['index']}"
        name = self.namer(prefix, suffix=len(self.tracker.indices) + 1)
        es_api.create_index(self.client, name)
        self.tracker.indices.add(name)

    def add_datastream(self):
        """Add a datastream to ES and self.tracker.datastreams"""
        # Be sure to track component and index templates for cleanup.
        name = self.namer(f"{self.prefix}-{self.PFXMAP['datastream']}")
        es_api.create_datastream(self.client, name)
        self.tracker.datastreams.add(name)

    def add_component_template(self, template: dict=None):
        """Add a component template"""
        prefix = f"{self.prefix}-{self.PFXMAP['component']}"
        name = self.namer(prefix, suffix=len(self.tracker.components) + 1)
        self.client.cluster.put_component_template(
            name, template=template, allow_auto_create=False, create=True, meta=None, body=None)
        self.tracker.components.add(name)

    def add_ilm(self):
        """Add an ILM policy"""
        # Have a policy builder helper that adds tiers based on a value at class instantiation
        # policy = utils.build_ilm(self.tier, self.pattern)
        prefix = f"{self.prefix}-{self.PFXMAP['ilm']}"
        name = self.namer(prefix, suffix=len(self.tracker.ilm_policies) + 1)
        policy = {}
        self.client.ilm.put_lifecycle(name, policy=policy, body=None)
        self.tracker.ilm_policies.add(name)

    def add_template(self, for_datastream=True):
        """Add an index template"""
        ds = None
        if for_datastream:
            ds = {}
        prefix = f"{self.prefix}-{self.PFXMAP['template']}"
        name = self.namer(prefix, suffix=len(self.tracker.templates) + 1)
        # data_stream needs to be an empty dict to make the template work with datastreams
        patterns = ','.join(self.patterns) # This may also just work as self.patterns
        self.client.indices.put_index_template(
            name, composed_of=self.tracker.components, create=True, data_stream=ds,
            index_patterns=patterns, template=None, body=None)
        self.tracker.templates.add(name)

    def ds_rollover(self, datastream=None):
        """Trigger an index rollover in the datastream"""
        if not datastream:
            if len(self.tracker.datastreams) == 1:
                datastream = self.tracker.datastreams[0]
            else:
                raise TestbedException('Unable to rollover datastream. More than one in tracker.')
        if datastream:
            self.client.indices.rollover(alias=datastream, wait_for_active_shards=True)
        else:
            raise TestbedMisconfig('No datastream available.')

    def ds_action_generator(self, datastream: str, index: str, action: str=None):
        """Generate a single add or remove backing index action for a datastream"""
        if not action or action not in ['add', 'remove']:
            raise TestbedMisconfig('action must be "add" or "remove"')
        return {
            f'{action}_backing_index': {
                'data_stream': datastream,
                'index': index
            }
        }

    def ds_modify(self, actions):
        """Modify a datastream with the provided actions"""
        try:
            self.client.indices.modify_data_stream(actions=actions, body=None)
        except Exception as exc:
            raise TestbedException(f'Unable to apply datastream modifications {actions}') from exc

    def create_test_indices(self, count=3, fill=True, docs=10):
        """Create 'count' indices using the default settings"""
        # Create ILM Policy, component templates, index template, and count indices
        self.add_ilm()
        self.add_component_template({})
        self.add_component_template({})
        self.add_template(for_datastream=False)
        incr = 0
        for _ in range(0, count):
            self.add_index()
            if fill:
                es_api.fill_index(self.client, self.tracker.indices[-1], docs, incr, match=True)
            incr += docs

    def create_test_ds(self, count=3, fill=True, docs=10):
        """Create datastream with count indices using the default settings"""
        # Create ILM Policy, component templates, index template, and count indices
        self.add_ilm()
        self.add_component_template({})
        self.add_component_template({})
        self.add_template()
        incr = 0
        for _ in range(0, count):
            if not self.tracker.datastreams:
                self.add_datastream()
            else:
                self.ds_rollover()
            if fill:
                es_api.fill_index(self.client, self.tracker.datastreams[-1], docs, incr, match=True)
            incr += docs
