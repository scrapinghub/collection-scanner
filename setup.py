from setuptools import setup, find_packages

setup(
    name='collection_scanner',
    version='0.1.1',
    packages=find_packages(),
    install_requires = [
        'dateparser',
    ]
)
