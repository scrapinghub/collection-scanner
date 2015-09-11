"""
utils for mocking hubstorage collection
"""
from operator import itemgetter
from copy import deepcopy

from mock import patch

class FakeCollection(object):
    def __init__(self, name, samples):
        """
        samples is a list of tuples (key, record dict)
        """
        self.colname = name
        self.samples = sorted(samples, key=itemgetter(0))
        self.base_time = 1441940400000 # 2015-09-11
    
    def _must_issue_record(self, key, **kwargs):
        prefixes = kwargs.get('prefix')
        retval = prefixes is None
        if not retval:
            for prefix in prefixes:
                if key.startswith(prefix):
                    retval = True
                    break
        startafter = kwargs.get('startafter') or ''
        if isinstance(startafter, list):
            startafter = startafter[0]
        endts = kwargs.get('endts')
        retval = retval and key > startafter and (not endts or self.base_time < endts)
        return retval


    def get(self, **kwargs):
        include_key = '_key' in kwargs.get('meta', {})
        include_ts = '_ts' in kwargs.get('meta', {})
        count = kwargs.get('count') or None
        if isinstance(count, list):
            count = count[0]
        for key, value in self.samples:
            rvalue = deepcopy(value)
            if self._must_issue_record(key, **kwargs):
                if include_key:
                    rvalue['_key'] = key
                if include_ts:
                    rvalue['_ts'] = self.base_time
                yield rvalue
                count -= 1
                if count == 0:
                    break
            self.base_time += 3600000 # each record separated by one hour

class FakeCollections(object):
    def __init__(self, project):
        self.project = project

    def new_store(self, name):
        return FakeCollection(name, self.project.client.samples[name])

class FakeProject(object):
    def __init__(self, client):
        self.client = client
        self.collections = FakeCollections(self)

class FakeClient(object):
    def __init__(self, samples):
        self.samples = samples

    def get_project(self, *args):
        return FakeProject(self)
