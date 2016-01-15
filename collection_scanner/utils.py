import re
import traceback


def retry_on_exception(exception):
    print "Retried: {}".format(traceback.format_exc())
    return not isinstance(exception, KeyboardInterrupt)


def get_num_partitions(hsp, collection_name):
    """Gets number of partitions of a partitioned collection."""
    partitions = []
    partitions_re = re.compile(r'%s_(\d+)' % collection_name)
    for entry in hsp.collections.apiget('list'):
        m = partitions_re.match(entry['name'])
        if m:
            partitions.append(int(m.groups()[0]))
    if partitions:
        if len(partitions) == max(partitions) + 1:
            return len(partitions)
        else:
            raise ValueError('Collection seems to be partitioned but not all partitions are available.')
