"""
High level hubstorage collection scanner

Basic usage:

from collection_scanner import CollectionScanner

scanner = CollectionScanner(<apikey>, <project id>, <collection name>, **kwargs)
batches = scanner.scan_collection_batches()
batch = next(batches)
for record in batch:
    ...

Before getting a new batch you can set a new startafter value with set_startafter() method.

"""
import time
import re
from dateutil import parser
import logging
from collections import defaultdict
import resource
from operator import itemgetter

from retrying import retry

import hubstorage

from .utils import retry_on_exception


__all__ = ['CollectionScanner']

DEFAULT_BATCHSIZE = 10000
LIMIT_KEY_CHAR = '~'

log = logging.getLogger(__name__)


class _CachedBlocksCollection(object):
    """
    Gets blocks of records and cache them for fast future gets.
    """
    def __init__(self, hsp, colname, partitions=None):
        self.hsp = hsp
        self.colname = colname
        self.collections = []
        self.cache = []

        if not partitions:
            self.collections.append(hsp.collections.new_store(colname))
        else:
            for p in range(partitions):
                self.collections.append(hsp.collections.new_store("{}_{}".format(colname, p)))

    def get(self, **kwargs):
        initial_count = kwargs.pop('count')[0] # must always be used with count parameter
        assert initial_count
        requested_startafter = initial_startafter = kwargs.pop('startafter', None)
        if isinstance(requested_startafter, list):
            requested_startafter = initial_startafter = requested_startafter[0]

        if requested_startafter == None:
            self.cache = []
        else:
            index = 0
            for key, _ in self.cache:
                if key > requested_startafter:
                    self.cache = self.cache[index:]
                    break
                index += 1


        cache = []
        if len(self.cache) < initial_count * len(self.collections):
            if self.cache:
                initial_startafter = max(requested_startafter, self.cache[-1][0])
            startafter = {col.colname: [initial_startafter] for col in self.collections}
            for col in self.collections:
                data = True
                count = 0
                while data and count < initial_count:
                    data = False
                    for record in self._read_from_collection(col, count=[initial_count - count], startafter=startafter[col.colname], **kwargs):
                        count += 1
                        data = True
                        cache.append((record['_key'], record))
                        startafter[col.colname] = [record['_key']]
        returned = 0
        cache = sorted(cache, key=itemgetter(0))
        if not self.cache:
            self.cache = cache
        else:
            while cache:
                self.cache.append(cache.pop(0))
        while self.cache:
            key, record = self.cache.pop(0)
            yield record
            returned += 1
            if returned == initial_count:
                return

    @retry(wait_fixed=120000, retry_on_exception=retry_on_exception, stop_max_attempt_number=10)
    def _read_from_collection(self, collection, **kwargs):
        return collection.get(**kwargs)


class CollectionScanner(object):
    """
    Base class for all collection scanners
    """
    # a list of names of complementary collection which shares same keys to the principal,
    # which its data will be merged in the output
    # for optimization purposes, it is made the assumption that secondary collections does not
    # have keys that are not present in principal. That is, key set of secondary collections
    # are always a subset of key set of principal.
    # TODO: logic does not work with startts
    secondary_collections = []
    has_many_collections = {}

    def __init__(self, apikey, project_id, collection_name, endpoint=None, batchsize=DEFAULT_BATCHSIZE, count=0,
                 max_next_records=10000, startafter=None, stopbefore=None, exclude_prefixes=None,
                 secondary_collections=None, has_many_collections=None,
                 autodetect_partitions=True, **kwargs):
        """
        apikey - hubstorage apikey with access to given project
        project_id - target project id
        collection_name - target collection
        endpoint - hubstorage server endpoint (defaults to python-hubstorage default)
        batchsize - size of each batch in number of records
        count - total count of records to retrieve
        max_next_records - how many records get on each call to hubstorage server
        startafter - start to scan after given hs key prefix
        stopbefore - stop once found given hs key prefix
        exclude_prefix - a list of key prefixes to exclude from scanning
        secondary_collections - a list of secondary collections that updates the class default one.
        has_many_collections - a dict of ('property_name', 'collection') pairs. Each collection can contain zero or many
                               items that will be added to 'property_name' property (a list)
        autodetect_partitions - If provided, autodetect partitioned collection. By default is True. If you want instead to force to read a non-partitioned
                collection when partitioned version also exists, use value False.
        **kwargs - other extras arguments you want to pass to hubstorage collection, i.e.:
                - prefix (list of key prefixes to include in the scan)
                - startts and endts, either in epoch millisecs (as accepted by hubstorage) or a date string (support is added here)
                - meta (a list with either '_ts' and/or '_key')
                etc (see husbtorage documentation)
        """
        self.hsc = hubstorage.HubstorageClient(apikey, endpoint=endpoint)
        self.hsp = self.hsc.get_project(project_id)

        num_partitions = None
        if autodetect_partitions:
            partitions = []
            partitions_re = re.compile(r'%s_(\d+)' % collection_name)
            for entry in self.hsp.collections.apiget('list'):
                m = partitions_re.match(entry['name'])
                if m:
                    partitions.append(int(m.groups()[0]))
            if partitions:
                if len(partitions) == max(partitions) + 1:
                    num_partitions = len(partitions)
                    log.info("Partitioned collection detected: %d total partitions.", num_partitions)
                else:
                    raise ValueError('Collection seems to be partitioned but not all partitions are available.')

        self.col = _CachedBlocksCollection(self.hsp, collection_name, num_partitions)
        self.__scanned_count = 0
        self.__totalcount = count
        self.lastkey = None
        self.__startafter = startafter
        self.__stopbefore = stopbefore
        self.__exclude_prefixes = exclude_prefixes or []
        self.secondary_collections.extend(secondary_collections or [])
        self.secondary = [_CachedBlocksCollection(self.hsp, name) for name in self.secondary_collections]
        self.__secondary_is_empty = defaultdict(bool)
        self.has_many_collections.update(has_many_collections or {})
        self.has_many = {prop: _CachedBlocksCollection(self.hsp, col) for prop, col in self.has_many_collections.items()}
        self.__has_many_is_empty = defaultdict(bool)
        self.__batchsize = batchsize
        self.__max_next_records = max_next_records
        self.__enabled = True

        kwargs = kwargs.copy()
        self.__endts = self.convert_ts(kwargs.get('endts', None))
        kwargs['endts'] = self.__endts
        kwargs['startts'] = self.convert_ts(kwargs.get('startts', None))
        self.__get_kwargs = kwargs

    def reset(self):
        """
        Resets the scanner state variables in order to start again to scan collection
        """
        self.__scanned_count = 0
        self.__totalcount = 0
        self.lastkey = None
        self.__startafter = None
        self.__secondary_is_empty = defaultdict(bool)
        self.__enabled = True

    def get_secondary_data(self, start, meta):
        secondary_data = defaultdict(dict)
        last = None
        for col in self.secondary:
            if not self.__secondary_is_empty[col.colname]:
                count = 0
                try:
                    for r in col.get(count=[self.__max_next_records], start=start, meta=meta):
                        count += 1
                        last = key = r.pop('_key')
                        ts = r.pop('_ts')
                        secondary_data[key].update(r)
                        if '_ts' not in secondary_data[key] or ts > secondary_data[key]['_ts']:
                            secondary_data[key]['_ts'] = ts
                except KeyError:
                    pass
                if count < self.__max_next_records:
                    self.__secondary_is_empty[col.colname] = True
                    log.info('Secondary collection {} is depleted'.format(col.colname))
        return last, dict(secondary_data)

    def get_additional_column_data(self, start, meta):
        secondary_data = defaultdict(dict)
        last = None
        for prop, col in self.has_many.items():
            if not self.__has_many_is_empty[prop]:
                count = 0
                try:
                    for r in col.get(count=[self.__max_next_records], start=start, meta=meta):
                        count += 1
                        last = key = r.pop('_key').split("_")[0]
                        ts = r.pop('_ts')
                        secondary_data[key][prop] = [r] if prop not in secondary_data[key] else secondary_data[key][
                                                                                                    prop] + [r]
                        if '_ts' not in secondary_data[key] or ts > secondary_data[key]['_ts']:
                            secondary_data[key]['_ts'] = ts
                except KeyError:
                    pass
                if count < self.__max_next_records:
                    self.__has_many_is_empty[prop] = True
                    log.info('Additional collection {} is depleted'.format(prop))
        return last, dict(secondary_data)

    def convert_ts(self, timestamp):
        """
        Read a timestamp in diverse formats and return milisecs epoch
        """
        if hasattr(timestamp, '__iter__'):
            timestamp = timestamp[0]
        if isinstance(timestamp, basestring):
            timestamp = self.str_to_msecs(timestamp)
        return timestamp

    def get_new_batch(self):
        """
        Convenient way for scanning a collection in batches
        """
        kwargs = self.__get_kwargs.copy()
        original_meta = kwargs.pop('meta', [])
        meta = {'_key', '_ts'}.union(original_meta)
        last_secondary_key = None
        last_additional_key = None
        additional_data = {}
        batchcount = self.__batchsize
        max_next_records = self._get_max_next_records(batchcount)
        while max_next_records and self.__enabled:
            count = 0
            jump_prefix = False
            for r in self.col.get(count=[max_next_records], startafter=[self.__startafter], meta=meta, **kwargs):
                if self.__stopbefore is not None and r['_key'].startswith(self.__stopbefore):
                    self.__enabled = False
                    break
                count += 1
                for exclude in self.__exclude_prefixes:
                    if r['_key'].startswith(exclude):
                        self.__startafter = exclude + LIMIT_KEY_CHAR
                        jump_prefix = True
                        break
                if jump_prefix:
                    break
                self.__startafter = self.lastkey = r['_key']
                if last_secondary_key is None or self.__startafter > last_secondary_key:
                    last_secondary_key, secondary_data = self.get_secondary_data(start=self.__startafter, meta=meta)
                if last_additional_key is None or self.__startafter > last_additional_key:
                    last_additional_key, additional_data_temp = self.get_additional_column_data(start=self.__startafter,
                                                                                                meta=meta)
                    additional_data.update(additional_data_temp)
                srecord = secondary_data.pop(r['_key'], None)
                if srecord is not None:
                    ts = srecord['_ts']
                    r.update(srecord)
                    if ts > r['_ts']:
                        r['_ts'] = ts
                if r['_key'] in additional_data:
                    ts = additional_data[r['_key']]['_ts']
                    for prop, data in additional_data[r['_key']].items():
                        if r.get(prop):
                            log.error(
                                "Items of has-many relationship can't be assigned to property %s, it's already defined on item %s")
                        else:
                            r[prop] = data
                    if ts > r['_ts']:
                        r['_ts'] = ts
                    del additional_data[r['_key']]

                if self.__endts and r['_ts'] > self.__endts:
                    continue

                for m in ['_key', '_ts']:
                    if m not in original_meta:
                        r.pop(m)

                self.__scanned_count += 1
                batchcount -= 1
                if self.__scanned_count % 10000 == 0:
                    log.info("Last key: {}, Scanned {}".format(self.lastkey, self.__scanned_count))
                yield r
            self.__enabled = count >= max_next_records and (
                not self.__totalcount or self.__scanned_count < self.__totalcount) or jump_prefix
            max_next_records = self._get_max_next_records(batchcount)

    def _get_max_next_records(self, batchcount):
        max_next_records = min(self.__max_next_records, batchcount)
        if self.__totalcount:
            max_next_records = min(max_next_records, self.__totalcount - self.__scanned_count)
        return max_next_records

    def scan_collection_batches(self):
        while self.__enabled:
            batch = list(self.get_new_batch())
            if batch:
                yield batch

    def close(self):
        log.info("Total scanned: %d" % self.__scanned_count)
        self.hsc.close()

    def scan_prefixes(self, codelen):
        """
        Generates all prefixes up to the given length
        """
        data = True
        lastkey = self.__startafter
        while data:
            data = False
            for r in self.col.get(nodata=1, meta=['_key'], startafter=lastkey, count=1):
                data = True
                code = r['_key'][:codelen]
                lastkey = code + LIMIT_KEY_CHAR
                yield code

    def set_startafter(self, startafter):
        self.__startafter = startafter

    @staticmethod
    def str_to_msecs(strtime):
        """
        Converts from '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d' format to epoch milisecs,
        which is the time representation used by hubstorage
        """
        if isinstance(strtime, int):
            return strtime
        if isinstance(strtime, basestring):
            d = parser.parse(strtime)
            return int(time.mktime(d.timetuple()) - time.timezone) * 1000
        return 0

    @property
    def scanned_count(self):
        return self.__scanned_count

    @property
    def is_enabled(self):
        return self.__enabled
