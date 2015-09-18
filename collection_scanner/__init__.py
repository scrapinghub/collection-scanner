"""
Any class that scans a collection should be a subclass of this
"""
import time
import logging
from collections import defaultdict

from retrying import retry

import hubstorage

from .utils import retry_on_exception


__all__ = ['CollectionScanner']


DEFAULT_BATCHSIZE = 1000000
LIMIT_KEY_CHAR = '~'


log = logging.getLogger(__name__)


@retry(wait_fixed=120000, retry_on_exception=retry_on_exception)
def _read_from_collection(collection, **kwargs):
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

    def __init__(self, apikey, project_id, collection_name, endpoint=None, batchsize=DEFAULT_BATCHSIZE, count=0,
                max_next_records=10000, startafter=None, exclude_prefixes=None, **kwargs):
        self.hsc = hubstorage.HubstorageClient(apikey, endpoint=endpoint)
        self.hsp = self.hsc.get_project(project_id)
        self.col = self.hsp.collections.new_store(collection_name)
        self.__scanned_count = 0
        self.__totalcount = count
        self.lastkey = None
        self.__startafter = startafter
        self.__exclude_prefixes = exclude_prefixes or []
        self.secondary = [self.hsp.collections.new_store(name) for name in self.secondary_collections]
        self.__secondary_is_empty = defaultdict(bool)
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
                    for r in _read_from_collection(col, count=[self.__max_next_records], start=start, meta=meta):
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
        batchcount = self.__batchsize
        max_next_records = self._get_max_next_records(batchcount)
        while max_next_records and self.__enabled:
            count = 0
            jump_prefix = False
            for r in _read_from_collection(self.col, count=[max_next_records], startafter=[self.__startafter], meta=meta, **kwargs):
                count += 1
                for exclude in self.__exclude_prefixes:
                    if r['_key'].startswith(exclude):
                        self.__startafter = exclude + LIMIT_KEY_CHAR
                        jump_prefix = True
                        break
                if jump_prefix:
                    break
                self.__startafter = self.lastkey = r['_key']
                if last_secondary_key is None or r['_key'] > last_secondary_key:
                    last_secondary_key, secondary_data = self.get_secondary_data(start=self.__startafter, meta=meta)
                if r['_key'] in secondary_data:
                    ts = secondary_data[r['_key']]['_ts']
                    r.update(secondary_data[r['_key']])
                    if ts > r['_ts']:
                        r['_ts'] = ts
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
            self.__enabled = count >= max_next_records and (not self.__totalcount or self.__scanned_count < self.__totalcount) or jump_prefix
            max_next_records = self._get_max_next_records(batchcount)

    def _get_max_next_records(self, batchcount):
        max_next_records = min(self.__max_next_records, batchcount)
        if self.__totalcount:
            max_next_records = min(max_next_records, self.__totalcount - self.__scanned_count)
        return max_next_records

    def scan_collection_batches(self):
        while self.__enabled:
            yield self.get_new_batch()

    def close(self):
        log.info("Total scanned: %d" % self.__scanned_count)

    def scan_prefixes(self, codelen):
        """
        Generates all prefixes up to the given length
        """
        data = True
        lastkey = self.__startafter
        while data:
            data = False
            for r in _read_from_collection(self.col, nodata=1, meta=['_key'], startafter=lastkey, count=1):
                data = True
                code = r['_key'][:codelen]
                lastkey = code + LIMIT_KEY_CHAR
                yield code

    @staticmethod
    def str_to_msecs(strtime):
        """
        Converts from '%Y-%m-%d %H:%M:%S' or '%Y-%m-%d' format to epoch milisecs,
        which is the time representation used by hubstorage
        """
        if strtime:
            try:
                return int(strtime)
            except ValueError:
                strtime = strtime.replace('T', ' ')
                if ':' in strtime:
                    ttime = time.strptime(strtime, '%Y-%m-%d %H:%M:%S')
                else:
                    ttime = time.strptime(strtime, '%Y-%m-%d')
                return int(time.mktime(ttime) - time.timezone) * 1000
        return 0

    @property
    def scanned_count(self):
        return self.__scanned_count
