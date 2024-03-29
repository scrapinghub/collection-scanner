import os

from unittest import TestCase
from hashlib import sha256

from unittest.mock import patch


from collection_scanner import CollectionScanner
from collection_scanner.tests import FakeClient


class BaseCollectionScannerTest(TestCase):

    samples = {
        # collection name -> records
        'test': [('AD%.3d' % i, {'field1': 'value 1-%.3d' % i, 'field2': 'value 2-%.3d' % i}) for i in range(1000)],
        'test2': [('AD%.3d' % i, {'field3': 'value 1-%.3d' % i}) for i in range(1000)],
        'test_many_collections': [('AD%.3d_%d' % (i, j), {'field3': 'value 1-%.3d' % i, 'field4': 'value 1-%.3d' % j})
                                  for i in range(1000) for j in
                                  range(3)],
        'empty': [],
    }

    scanner_class = CollectionScanner

    def setUp(self):
        self.prev_env = os.environ
        os.environ['SH_APIKEY'] = 'apikey'
        os.environ['SHUB_JOBKEY'] = '10/1/1'

    def _get_scanner_records(self, client_mock, startafter_list=None, **kwargs):
        client_mock.return_value._hsclient = FakeClient(self.samples, return_less=kwargs.get('return_less', 0))
        scanner = self.scanner_class(**kwargs)
        records = []
        keys = set()
        batch_count = 0
        if startafter_list:
            scanner.set_startafter(startafter_list.pop(0))
        for batch in scanner.scan_collection_batches():
            batch_count += 1
            for record in batch:
                records.append(record)
                if '_key' in record:
                    keys.add(record['_key'])
            if startafter_list:
                scanner.set_startafter(startafter_list.pop(0))
        return scanner, records, sorted(keys), batch_count

    def tearDown(self):
        os.environ = self.prev_env


@patch('collection_scanner.scanner.ScrapinghubClient')
class CollectionScannerTest(BaseCollectionScannerTest):
    def test_get(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', meta=['_key'])
        self.assertEqual(len(keys), 1000)
        self.assertEqual(batch_count, 1)
        for record in records:
            self.assertTrue('_ts' not in record)

    def test_prefix(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', prefix=['AD1', 'AD4'], meta=['_key'])
        self.assertEqual(len(keys), 200)
        self.assertEqual(batch_count, 1)

    def test_exclude_prefixes(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', exclude_prefixes=['AD1', 'AD4'],
                                      meta=['_key'])
        self.assertEqual(len(keys), 800)
        self.assertEqual(batch_count, 1)

    def test_startafter(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', startafter='AD8', meta=['_key'])
        self.assertEqual(len(keys), 200)
        self.assertEqual(batch_count, 1)

    def test_stopbefore(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', stopbefore='AD3', meta=['_key'])
        self.assertEqual(len(keys), 300)
        self.assertEqual(batch_count, 1)

    def test_startafter_stopbefore(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', startafter='AD3', stopbefore='AD8',
                                      meta=['_key'])
        self.assertEqual(len(keys), 500)
        self.assertEqual(batch_count, 1)

    def test_batchsize(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', batchsize=501, meta=['_key'])
        self.assertEqual(len(keys), 1000)
        self.assertEqual(batch_count, 2)

    def test_no_key(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test')
        self.assertEqual(len(keys), 0)
        self.assertEqual(len(records), 1000)

    def test_count(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', count=150, meta=['_key'])
        self.assertEqual(len(keys), 150)

    def test_endts(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', meta=['_key'], endts='2015-10-01 23:00:00')
        self.assertEqual(len(keys), 500)

    def test_startafter_per_batch(self, client_mock):
        # expected batches:
        # from AD100 to AD199
        # from AD400 to AD499
        # from AD800 to AD899
        # from AD900 to AD999
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', startafter_list=['AD099', 'AD399', 'AD799'],
                                      meta=['_key'], batchsize=100)
        self.assertEqual(len(keys), 400)
        self.assertEqual(batch_count, 4)
        self.assertEqual(records[0]['_key'], 'AD100')
        self.assertEqual(records[100]['_key'], 'AD400')
        self.assertEqual(records[200]['_key'], 'AD800')
        self.assertEqual(records[300]['_key'], 'AD900')
        self.assertEqual(records[-1]['_key'], 'AD999')

    def test_startafter_per_batch_unsorted_startafter(self, client_mock):
        # expected batches:
        # from AD100 to AD199
        # from AD800 to AD899
        # from AD900 to AD999
        # from AD400 to AD499
        with self.assertRaisesRegexp(AssertionError,
            'startafter series must be strictly increasing. Previous startafter: AD799 Last startafter: AD399'):
            scanner, records, keys, batch_count = \
                self._get_scanner_records(client_mock, collection_name='test', startafter_list=['AD099', 'AD799', 'AD399'],
                                      meta=['_key'], batchsize=100)

    def test_server_returns_less_records_than_requested(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', meta=['_key'], batchsize=100, return_less=20)
        self.assertEqual(len(keys), 1000)
        self.assertEqual(batch_count, 10)

    def test_start(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', start='AD3', meta=['_key'], batchsize=100)
        self.assertEqual(len(set(keys)), 700)
        self.assertEqual(len(keys), 700)

    def test_get_from_empty_collection(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='empty', meta=['_key'])
        self.assertEqual(len(keys), 0)
        self.assertEqual(batch_count, 0)


@patch('collection_scanner.scanner.ScrapinghubClient')
class CollectionScannerPartitionedTest(BaseCollectionScannerTest):
    samples = {}
    for partition in range(4):
        samples['testp_%d' % partition] = []
    for i in range(4000):
        partition = i % 4
        samples['testp_%d' % partition].append(('AD%.4d' % i, {'field1': 'value 1-%.4d' % i}))

    for partition in range(8):
        samples['bigtestp_%d' % partition] = []
    unsorted_samples = set()
    for i in range(40000):
        keyhash = sha256()
        keyhash.update(str(i).encode())
        unsorted_samples.add(keyhash.hexdigest()[-16:])
    for keyhash in sorted(unsorted_samples):
        key = 'AD' + keyhash
        partition = int(keyhash[0], base=16) % 8
        samples['bigtestp_%d' % partition].append((key, {'field1': keyhash}))

    def test_partitioned(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='testp', meta=['_key'], batchsize=100)
        self.assertEqual(batch_count, 40)
        self.assertEqual(len(records), 4000)
        self.assertEqual(len(keys), 4000)

    def test_partitioned_realistic(self, client_mock):
        """
        A more realistic test with thoysands of records, not consecutive keys, and different number of records per partition
        """
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='bigtestp', meta=['_key'], batchsize=1000)
        self.assertEqual(batch_count, 40)
        self.assertEqual(len(records), 40000)
        self.assertEqual(len(keys), 40000)

    def test_partitioned_startafter(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='testp', meta=['_key'], batchsize=100,
                                      startafter='AD2499')
        self.assertEqual(batch_count, 15)
        self.assertEqual(len(records), 1500)
        self.assertEqual(len(keys), 1500)
        self.assertEqual(sorted(keys), [r['_key'] for r in records])
        self.assertEqual(records[0]['_key'], 'AD2500')
        self.assertEqual(records[-1]['_key'], 'AD3999')

    def test_partitioned_stopbefore(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='testp', meta=['_key'], batchsize=100,
                                      stopbefore='AD2500')
        self.assertEqual(batch_count, 25)
        self.assertEqual(len(records), 2500)
        self.assertEqual(len(keys), 2500)
        self.assertEqual(sorted(keys), [r['_key'] for r in records])
        self.assertEqual(records[0]['_key'], 'AD0000')
        self.assertEqual(records[-1]['_key'], 'AD2499')

    def test_partitioned_count(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='testp', meta=['_key'], batchsize=100,
                                      startafter='AD2199', count=500)
        self.assertEqual(batch_count, 5)
        self.assertEqual(len(records), 500)
        self.assertEqual(len(keys), 500)
        self.assertEqual(sorted(keys), [r['_key'] for r in records])
        self.assertEqual(records[0]['_key'], 'AD2200')
        self.assertEqual(records[-1]['_key'], 'AD2699')


@patch('collection_scanner.scanner.ScrapinghubClient')
class CollectionScannerPartitionedTestIncomplete(BaseCollectionScannerTest):
    samples = {}
    for partition in [1, 2, 3]:
        samples['testp_%d' % partition] = []
    for i in range(4000):
        partition = i % 4
        if partition != 0:
            samples['testp_%d' % partition].append(('AD%.4d' % i, {'field1': 'value 1-%.4d' % i}))

    def test_partitioned(self, client_mock):
        self.assertRaisesRegexp(KeyError, r'\'testp\'', self._get_scanner_records, client_mock, collection_name='testp', meta=['_key'],
                          batchsize=100)


@patch('collection_scanner.scanner.ScrapinghubClient')
class SecondaryCollectionScannerTest(BaseCollectionScannerTest):
    class MyCollectionScanner(CollectionScanner):
        secondary_collections = ['test2', 'test3'] # test3 does not exist, must be filtered

    scanner_class = MyCollectionScanner

    def test_get(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', meta=['_key'])
        self.assertEqual(len(keys), 1000)
        for record in records:
            self.assertEqual(record['field1'], record['field3'])

    def test_endts(self, client_mock):
        scanner, records, keys, batch_count = \
            self._get_scanner_records(client_mock, collection_name='test', meta=['_key'], endts='2015-10-01 23:00:00')
        self.assertEqual(len(keys), 500)
        for record in records:
            self.assertEqual(record['field1'], record['field3'])


class MiscelaneousTest(TestCase):
    def test_str_to_msecs(self):
        self.assertEqual(CollectionScanner.str_to_msecs(100), 100)
        self.assertEqual(CollectionScanner.str_to_msecs(0), 0)
        self.assertEqual(CollectionScanner.str_to_msecs('2015-09-08'), 1441670400000)
        self.assertEqual(CollectionScanner.str_to_msecs('2015-09-08 20:00:00'), 1441742400000)
        self.assertEqual(CollectionScanner.str_to_msecs('2015-09-08T20:00:00'), 1441742400000)
        self.assertEqual(CollectionScanner.str_to_msecs(None), 0)
