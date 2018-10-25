"""
High level hubstorage collection scanner

Basic usage:

from collection_scanner import CollectionScanner

scanner = CollectionScanner(<project id>, <collection name>, **kwargs)
batches = scanner.scan_collection_batches()
batch = next(batches)
for record in batch:
    ...

Before getting a new batch you can set a new startafter value with set_startafter() method.

"""
import time
import random
import logging
from collections import defaultdict
from operator import itemgetter

import dateparser

from retrying import retry

from scrapinghub import ScrapinghubClient

from .utils import (
    retry_on_exception,
    get_num_partitions,
    filter_collections_exist,
    LIMIT_KEY_CHAR,
    get_project_id,
)


__all__ = ['CollectionScanner']

DEFAULT_BATCHSIZE = 10000

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class _CachedBlocksCollection(object):
    """
    - Gets blocks of records and cache them for fast future gets.
    - Hides partitioning
    """
    def __init__(self, hsp, colname, partitions=None):
        self.hsp = hsp
        self.colname = colname
        self.collections = []
        self.cache = defaultdict(list)
        self.return_cache = []
        self.max_in_return_cache = ''
        self.__last_requested_startafter = ''

        if not partitions:
            self.collections.append(hsp.collections.new_store(colname))
        else:
            for p in range(partitions):
                self.collections.append(hsp.collections.new_store("{}_{}".format(colname, p)))

    def get(self, random_mode=False, **kwargs):
        """
        if random_mode is True, optimize for random generation of samples.
        """
        collections = set([random.choice(self.collections)] if random_mode else self.collections)
        max_next_records = kwargs.pop('count')[0] # must always be used with count parameter
        assert max_next_records
        requested_startafter = kwargs.pop('startafter', None)
        if isinstance(requested_startafter, list):
            requested_startafter = requested_startafter[0]

        if not requested_startafter:
            self.cache = defaultdict(list)
            self.return_cache = []
        else: # remove all entries in cache below the given startafter
            assert requested_startafter > self.__last_requested_startafter, \
                   'startafter series must be strictly increasing. Previous startafter: %s Last startafter: %s' \
                   % (self.__last_requested_startafter, requested_startafter)
            self.__last_requested_startafter = requested_startafter
            for col in self.cache.keys():
                index = -1
                for index, (key, _) in enumerate(self.cache[col]):
                    if key > requested_startafter:
                        break
                else:
                    index += 1
                self.cache[col] = self.cache[col][index:]

            index = -1
            for index, (key, _) in enumerate(self.return_cache):
                if key > requested_startafter:
                    break
            else:
                index += 1
            self.return_cache = self.return_cache[index:]

        finished_collections = set()
        if self.return_cache:
            requested_startafter = self.return_cache[-1][0]

        startafter = {}
        for col in collections:
            startafter[col] = max(requested_startafter, self.cache[col][-1][0]) if self.cache[col] else requested_startafter

        if self.return_cache:
            self.return_cache[0][0], self.return_cache[-1][0]

        while collections.difference(finished_collections):
            for col in collections.difference(finished_collections):
                pcache = self.cache[col]
                if not pcache:
                    data = False
                    for record in self._read_from_collection(col, count=[max_next_records], startafter=[startafter[col]], **kwargs):
                        data = True
                        startafter[col] = record['_key']
                        pcache.append((record['_key'], record))
                    if not data:
                        finished_collections.add(col)
                if pcache and (len(self.return_cache) < max_next_records or pcache[0][0] < self.max_in_return_cache):
                    key, record = pcache.pop(0)
                    self.return_cache.append((key, record))
                    self.max_in_return_cache = max(self.max_in_return_cache, key)
                else:
                    finished_collections.add(col)
        self.return_cache = sorted(self.return_cache, key=itemgetter(0))
        to_return_now, self.return_cache = self.return_cache[:max_next_records], self.return_cache[max_next_records:]
        for key, record in to_return_now:
            yield record

    @retry(wait_fixed=120000, retry_on_exception=retry_on_exception, stop_max_attempt_number=10)
    def _read_from_collection(self, collection, **kwargs):
        try:
            for record in collection.get(**kwargs):
                yield record
        except KeyError: # HS raises KeyError on empty collections
            return


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

    def __init__(self, collection_name, project_id=None, apikey=None, batchsize=DEFAULT_BATCHSIZE, count=0,
                 max_next_records=1000, startafter=None, stopbefore=None, exclude_prefixes=None,
                 secondary_collections=None,
                 autodetect_partitions=True, **kwargs):
        """
        collection_name - target collection
        project_id - target project id. If none, autodetect from SHUB_JOBKEY environment variable.
        apikey - hubstorage apikey with access to given project. If None, get from SH_APIKEY environment variable
                 (delegated to scrapinghub library).
        batchsize - size of each batch in number of records
        count - total count of records to retrieve
        max_next_records - how many records get on each call to hubstorage server
        startafter - start to scan after given hs key prefix
        stopbefore - stop once found given hs key prefix
        exclude_prefix - a list of key prefixes to exclude from scanning
        secondary_collections - a list of secondary collections that updates the class default one.
        autodetect_partitions - If provided, autodetect partitioned collection. By default is True. If you want instead to force to read a non-partitioned
                collection when partitioned version also exists under the same name, use False.
        **kwargs - other extras arguments you want to pass to hubstorage collection, i.e.:
                - prefix (list of key prefixes to include in the scan)
                - startts and endts, either in epoch millisecs (as accepted by hubstorage) or a date string (support is added here)
                - meta (a list with either '_ts' and/or '_key')
                etc (see husbtorage documentation)
        """
        self.hsc = ScrapinghubClient(apikey)._hsclient
        project_id = project_id or get_project_id()
        self.hsp = self.hsc.get_project(project_id)

        num_partitions = None
        if autodetect_partitions:
            num_partitions = get_num_partitions(self.hsp, collection_name)
            if num_partitions:
                log.info("Partitioned collection detected: %d total partitions.", num_partitions)

        self.col = _CachedBlocksCollection(self.hsp, collection_name, num_partitions)
        self.__scanned_count = 0
        self.__totalcount = count
        self.lastkey = None
        self.__startafter = startafter
        self.__stopbefore = stopbefore
        self.__exclude_prefixes = exclude_prefixes or []
        self.secondary_collections.extend(secondary_collections or [])
        self.secondary = [_CachedBlocksCollection(self.hsp, name) for name in filter_collections_exist(self.hsp, self.secondary_collections)]
        self.__secondary_is_empty = defaultdict(bool)
        self.__batchsize = batchsize
        self.__max_next_records = max_next_records
        self.__enabled = True

        self.__start = kwargs.pop('start', '')
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
        max_next_records = self._get_max_next_records(self.__batchsize)
        for col in self.secondary:
            if not self.__secondary_is_empty[col.colname]:
                count = 0
                try:
                    for r in col.get(count=[max_next_records], start=start, meta=meta):
                        count += 1
                        last = key = r.pop('_key')
                        ts = r.pop('_ts')
                        secondary_data[key].update(r)
                        if '_ts' not in secondary_data[key] or ts > secondary_data[key]['_ts']:
                            secondary_data[key]['_ts'] = ts
                except KeyError:
                    pass
                if count < max_next_records:
                    self.__secondary_is_empty[col.colname] = True
                    log.info('Secondary collection %s is depleted', col.colname)
        return last, dict(secondary_data)

    def convert_ts(self, timestamp):
        """
        Read a timestamp in diverse formats and return milisecs epoch
        """
        if isinstance(timestamp, (list, tuple)):
            timestamp = timestamp[0]
        if isinstance(timestamp, str):
            timestamp = self.str_to_msecs(timestamp)
        return timestamp

    def get_new_batch(self, random_mode=False):
        """
        Convenient way for scanning a collection in batches
        """
        kwargs = self.__get_kwargs.copy()
        original_meta = kwargs.pop('meta', [])
        meta = {'_key', '_ts'}.union(original_meta)
        last_secondary_key = None
        batchcount = self.__batchsize
        max_next_records = self._get_max_next_records(batchcount)
        # start used only once, as HS nulifies startafter if start is given
        start = self.__start
        self.__start = ''

        while max_next_records and self.__enabled:
            count = 0
            jump_prefix = False
            for r in self.col.get(random_mode, count=[max_next_records], startafter=[self.__startafter], start=start, meta=meta, **kwargs):
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
                srecord = secondary_data.pop(r['_key'], None)
                if srecord is not None:
                    ts = srecord['_ts']
                    r.update(srecord)
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
                    log.info("Last key: %s, Scanned %d", self.lastkey, self.__scanned_count)
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
        log.info("Total scanned: %d", self.__scanned_count)
        self.hsc.close()

    def set_startafter(self, startafter):
        self.__startafter = startafter

    @staticmethod
    def str_to_msecs(strtime):
        """
        Converts from any format supported by dateparser to epoch milisecs,
        which is the time representation used by hubstorage
        """
        if isinstance(strtime, int):
            return strtime
        if isinstance(strtime, str):
            d = dateparser.parse(strtime)
            return int(time.mktime(d.timetuple()) - time.timezone) * 1000
        return 0

    @property
    def scanned_count(self):
        return self.__scanned_count

    @property
    def is_enabled(self):
        return self.__enabled
