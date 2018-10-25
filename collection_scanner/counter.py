"""
Allow to count on partitioned collections
"""
import logging
from scrapinghub import ScrapinghubClient
import random
from .utils import get_num_partitions, generate_prefixes, get_project_id


__all__ = ['CollectionCounter']


log = logging.getLogger(__name__)


class CollectionCounter(object):
    def __init__(self, collection_name, project_id=None, apikey=None, autodetect_partitions=True):
        """
        collection_name - target collection
        project_id - target project id
        apikey - hubstorage apikey with access to given project. If None, delegate to scrapinghub lib.
        autodetect_partitions - If provided, autodetect partitioned collection. By default is True. If you want instead to force to read a non-partitioned
                collection when partitioned version also exists under the same name, use False.
        """
        self.hsc = ScrapinghubClient(apikey)._hsclient
        project_id = project_id or get_project_id()
        self.hsp = self.hsc.get_project(project_id)

        num_partitions = None
        if autodetect_partitions:
            num_partitions = get_num_partitions(self.hsp, collection_name)
            if num_partitions:
                log.info("Partitioned collection detected: %d total partitions.", num_partitions)

        self.collections = []

        if num_partitions:
            for p in range(num_partitions):
                self.collections.append(self.hsp.collections.new_store("{}_{}".format(collection_name, p)))
        else:
            self.collections.append(self.hsp.collections.new_store(collection_name))

    def count(self, *args, **kwargs):
        """
        Real count: iterates over all partitions, count on each one, and sum
        """
        return sum(col.count(*args, **kwargs) for col in self.collections)

    def fast_count(self, *args, **kwargs):
        """
        Fast count: pick a random partition, count on it, and multiply by number of partitions
        Result is more precise as records are better homogeneously distributed among partitions
        """
        col = random.choice(self.collections)
        return col.count(*args, **kwargs) * len(self.collections)

    def get_prefixes(self, codelen, fast=False, **kwargs):
        """
        Generate all prefixes of given codelen. If fast is True, it will pick only
        one partition. Otherwise will generate prefixes using all ones.
        """
        cols = [random.choice(self.collections)] if fast else self.collections
        gens = [generate_prefixes(col, codelen, **kwargs) for col in cols]
        prefixes = set()
        while gens:
            for g in list(gens):
                try:
                    prefix = next(g)
                    if prefix not in prefixes:
                        prefixes.add(prefix)
                        yield prefix
                except StopIteration:
                    gens.remove(g)
                    continue
