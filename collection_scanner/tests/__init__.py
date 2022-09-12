"""
utils for mocking hubstorage collection
"""
from operator import itemgetter
from copy import deepcopy


class FakeCollection(object):
    def __init__(self, name, samples, return_less=0):
        """
        name is the collection name
        samples is a list of tuples (key, record dict)
        return_less is a parameter to simulate situation in which HS server returns less records than requested, even
            if end of collection hasn't been reached. It just returns the given number less records than requested by
            count.
        """
        self.colname = name
        self.samples = sorted(samples, key=itemgetter(0))
        self.return_less = return_less
        self.base_time = 1441940400000 # 2015-09-11
        self.timestamps = {}
        for key, _ in self.samples:
            self._get_basetime(key) # populate timestamps

    def _must_issue_record(self, key, **kwargs):
        prefix = kwargs.get('prefix')
        retval = prefix is None or key.startswith(tuple(prefix))
        startafter = kwargs.get('startafter') or ''
        start = kwargs.get('start') or ''
        if isinstance(startafter, list):
            startafter = startafter[0] or ''
        if isinstance(start, list):
            start = start[0]
        # start nulifies startafter
        if start:
            startafter = ''
        endts = kwargs.get('endts')
        retval = retval and key >= start and key > startafter and (not endts or self._get_basetime(key) < endts)
        return retval

    def _get_basetime(self, key):
        if key not in self.timestamps:
            self.timestamps[key] = self.base_time
            self.base_time += 3600000 # each record separated by one hour
        return self.timestamps[key]

    def get(self, **kwargs):
        if not self.samples:
            raise KeyError(None)
        include_key = '_key' in kwargs.get('meta', {})
        include_ts = '_ts' in kwargs.get('meta', {})
        count = kwargs.get('count') or None
        if isinstance(count, list):
            count = count[0] or None
        for key, value in self.samples:
            rvalue = deepcopy(value)
            if self._must_issue_record(key, **kwargs):
                if include_key:
                    rvalue['_key'] = key
                if include_ts:
                    rvalue['_ts'] = self._get_basetime(key)
                yield rvalue
                if count is not None:
                    count -= 1
                    if count == self.return_less or count == 0:
                        break

class FakeCollections(object):
    def __init__(self, project, **kwargs):
        self.project = project
        self.kwargs = kwargs
        self.collection_list = [{'name': n, 'type': 's'} for n in self.project.client.samples.keys()]

    def new_store(self, name):
        return FakeCollection(name, self.project.client.samples[name], **self.kwargs)

    def apiget(self, call):
        if call == 'list':
            return self.collection_list
        raise NotImplementedError('Not supported in FakeCollections')

class FakeProject(object):
    def __init__(self, client, **kwargs):
        self.client = client
        self.collections = FakeCollections(self, **kwargs)

class FakeClient(object):
    def __init__(self, samples, **kwargs):
        self.samples = samples
        self.kwargs = kwargs

    def get_project(self, *args):
        return FakeProject(self, **self.kwargs)
