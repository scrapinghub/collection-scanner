from setuptools import setup, find_packages

setup(
    name='collection_scanner',
    version='0.1.6',
    packages=find_packages(),
    install_requires = [
        'dateparser',
        'retrying',
        'scrapinghub>=2.0.3',
    ]
)
