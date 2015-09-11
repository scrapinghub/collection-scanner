from unittest import TestCase

from mock import patch

from collection_scanner import CollectionScanner
from collection_scanner.tests import FakeClient


class BaseCollectionScannerTest(TestCase):
    samples = {
        'test': [('AD%.3d' % i, {'field1': 'value 1-%.3d' % i, 'field2': 'value 2-%.3d' % i}) for i in range(1000)],
        'test2': [('AD%.3d' % i, {'field3': 'value 1-%.3d' % i}) for i in range(1000)]
    }
    scanner_class = CollectionScanner
    def _get_scanner_records(self, client_mock, **kwargs):
        client_mock.return_value = FakeClient(self.samples)
        scanner = self.scanner_class('apikey', 0, **kwargs)
        records = []
        keys = set()
        batch_count = 0
        for batch in scanner.scan_collection_batches():
            batch_count += 1
            for record in batch:
                records.append(record)
                if '_key' in record:
                    keys.add(record['_key'])
        return scanner, records, sorted(keys), batch_count

   
@patch('hubstorage.HubstorageClient', autospec=True)
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
                    self._get_scanner_records(client_mock, collection_name='test', exclude_prefixes=['AD1', 'AD4'], meta=['_key'])
        self.assertEqual(len(keys), 800)
        self.assertEqual(batch_count, 1)

    def test_startafter(self, client_mock):
        scanner, records, keys, batch_count = \
                    self._get_scanner_records(client_mock, collection_name='test', startafter='AD8', meta=['_key'])
        self.assertEqual(len(keys), 200)
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
                    self._get_scanner_records(client_mock, collection_name='test', meta=['_key'], endts='2015-10-01 20:00:00')
        self.assertEqual(len(keys), 500)


@patch('hubstorage.HubstorageClient', autospec=True)
class SecondaryCollectionScannerTest(BaseCollectionScannerTest):
    class MyCollectionScanner(CollectionScanner):
        secondary_collections = ['test2']
    scanner_class = MyCollectionScanner

    def test_get(self, client_mock):
        scanner, records, keys, batch_count = \
                    self._get_scanner_records(client_mock, collection_name='test', meta=['_key'])
        self.assertEqual(len(keys), 1000)
        for record in records:
            self.assertEqual(record['field1'], record['field3'])

    def test_endts(self, client_mock):
        scanner, records, keys, batch_count = \
                    self._get_scanner_records(client_mock, collection_name='test', meta=['_key'], endts='2015-10-01 20:00:00')
        self.assertEqual(len(keys), 500)
        for record in records:
            self.assertEqual(record['field1'], record['field3'])
