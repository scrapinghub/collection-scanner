High level hubstorage collection scanner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Provides convenient way to scan a collection in batches
- Allows to merge data from multiple collections
- Accepts endts and startts in many string formats (as accepted by dateparser lib) or standard HS epoch in millisecs
- Accepts excluded prefixes
- Adds stopbefore feature (analogous to startafter but the inverse)
- Provides method for arbitrary prefix aggregation counting
- Supports partitioned collections
- Provides a suite for testing hs collection code.

Up to version 0.1.6: Python2 only
Starting version 0.2: Python3 only

See usage instructions at `scanner.py <https://github.com/scrapinghub/collection-scanner/blob/master/collection_scanner/scanner.py>`_ docstring.

Instalation
~~~~~~~~~~~

pip install collection-scanner
