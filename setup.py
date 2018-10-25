from setuptools import setup, find_packages

setup(
    name='collection_scanner',
    version='0.3',
    description='Scrapinghub Hubstorage Collection scanner.',
    long_description = open('README.rst').read(),
    license='BSD',
    url= 'https://github.com/scrapinghub/collection-scanner',
    maintainer='Scrapinghub',
    packages=find_packages(),
    install_requires = [
        'dateparser',
        'retrying',
        'scrapinghub>=2.0.3',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ]
)
