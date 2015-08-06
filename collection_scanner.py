"""
Any class that scans a collection should be a subclass of this
"""
import time
from collections import defaultdict

from hubstorage import HubstorageClient

import logging


__all__ = ['CollectionScanner']


DEFAULT_BATCHSIZE = 10000
LIMIT_KEY_CHAR = '~'


log = logging.getLogger(__name__)


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

    def __init__(self, apikey, project_id, collection_name, endpoint=None):
        self.hsc = HubstorageClient(apikey, endpoint=endpoint)
        self.hsp = self.hsc.get_project(project_id)
        self.col = self.hsp.collections.new_store(collection_name)
        self.count = 0
        self.__totalcount = 0
        self.lastkey = None
        self.__startafter = None
        self.secondary = [self.hsp.collections.new_store(name) for name in self.secondary_collections]
        self.__secondary_is_empty = defaultdict(bool)

    def reset(self):
        """
        Resets the scanner state variables in order to start again to scan collection
        """
        self.count = 0
        self.__totalcount = 0
        self.lastkey = None
        self.__startafter = None
        self.__secondary_is_empty = defaultdict(bool)

    def get_secondary_data(self, start, meta):
        secondary_data = defaultdict(dict)
        last = None
        for col in self.secondary:
            if not self.__secondary_is_empty[col.colname]:
                count = 0
                try:
                    for r in col.get(count=[DEFAULT_BATCHSIZE], start=start, meta=meta):
                        count += 1
                        last = key = r.pop('_key')
                        ts = r.pop('_ts')
                        secondary_data[key].update(r)
                        if '_ts' not in secondary_data[key] or ts > secondary_data[key]['_ts']:
                            secondary_data[key]['_ts'] = ts
                except KeyError:
                    pass
                if count < DEFAULT_BATCHSIZE:
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
        if timestamp is not None:
            timestamp = int(timestamp)
        return timestamp

    def scan_collection(self, batchsize=DEFAULT_BATCHSIZE, exclude_prefixes=None, **kwargs):
        """
        Convenient way for scanning a collection in batches
        kwargs are the collection get() parameters
        """
        exclude_prefixes = exclude_prefixes or []
        data = True
        totalcount = kwargs.pop('count', 0)
        self.__totalcount = self.__totalcount or totalcount
        startafter = kwargs.pop('startafter', None)
        self.__startafter = self.__startafter or startafter
        original_meta = kwargs.pop('meta', [])
        meta = {'_key', '_ts'}.union(original_meta)
        last_secondary_key = None
        endts = self.convert_ts(kwargs.get('endts', None))
        kwargs['endts'] = endts
        kwargs['startts'] = self.convert_ts(kwargs.get('startts', None))

        while data:
            data = False
            if self.__totalcount:
                batchsize = min(batchsize, self.__totalcount - self.count)
            for r in self.col.get(count=[batchsize], startafter=[self.__startafter], meta=meta, **kwargs):
                data = True
                jump_prefix = False
                for exclude in exclude_prefixes:
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
                if endts and r['_ts'] > endts:
                    continue

                for m in ['_key', '_ts']:
                    if m not in original_meta:
                        r.pop(m)

                self.count += 1
                if self.count % 10000 == 0:
                    log.info("Last key: {}, Scanned {}".format(self.lastkey, self.count))
                yield self.process_record(r)
                if self.count == self.__totalcount:
                    data = False
                    break

    def process_record(self, record):
        return record

    def close(self):
        log.info("Total scanned: %d" % self.count)

    def on_kbd_interrupt(self):
        pass

    def scan_prefixes(self, codelen, **kwargs):
        """
        Generates all prefixes up to the given length
        """
        data = True
        for karg in ['count', 'meta', 'nodata']:
            kwargs.pop(karg, None)
        lastkey = kwargs.pop('startafter', None)
        while data:
            data = False
            for r in self.col.get(nodata=1, meta=['_key'], startafter=lastkey, count=1):
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
                return repr(int(strtime))
            except ValueError:
                if ':' in strtime:
                    ttime = time.strptime(strtime, '%Y-%m-%d %H:%M:%S')
                else:
                    ttime = time.strptime(strtime, '%Y-%m-%d')
                return repr(int(time.mktime(ttime) * 1000))
