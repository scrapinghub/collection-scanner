from unittest import TestCase

from mock import patch

from collection_scanner import CollectionScanner
from collection_scanner.tests import FakeClient


class BaseCollectionScannerTest(TestCase):
    samples = {
        'test': [('AD%.3d' % i, {'field1': 'value 1-%.3d' % i, 'field2': 'value 2-%.3d' % i}) for i in range(1000)]
    }
    def _get_scanner_records(self, client_mock, **kwargs):
        client_mock.return_value = FakeClient(self.samples)
        scanner = CollectionScanner('apikey', 0, **kwargs)
        records = []
        batch_count = 0
        for batch in scanner.scan_collection_batches():
            batch_count += 1
            for record in batch:
                records.append(record)
        return scanner, records, batch_count

   
@patch('hubstorage.HubstorageClient', autospec=True)
class CollectionScannerTest(BaseCollectionScannerTest):

    def test_get(self, client_mock):
        scanner, records, batch_count = self._get_scanner_records(client_mock, collection_name='test')
        self.assertEqual(len(records), 1000)
        self.assertEqual(batch_count, 1)

    def test_prefix(self, client_mock):
        scanner, records, batch_count = self._get_scanner_records(client_mock, collection_name='test', prefix=['AD1', 'AD4'])
        self.assertEqual(len(records), 200)

    def test_exclude_prefixes(self, client_mock):
        scanner, records, batch_count = self._get_scanner_records(client_mock, collection_name='test', exclude_prefixes=['AD1', 'AD4'])
        self.assertEqual(len(records), 800)

    def test_startafter(self, client_mock):
        scanner, records, batch_count = self._get_scanner_records(client_mock, collection_name='test', startafter='AD8')
        self.assertEqual(len(records), 200)


