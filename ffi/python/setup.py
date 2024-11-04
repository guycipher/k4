# K4 setup.py

from setuptools import setup, find_packages


VERSION = '0.1.0'
DESCRIPTION = 'Python FFI for the K4 storage engine'
LONG_DESCRIPTION = 'Python FFI for the K4 storage engine'


setup(
    name='k4',
    version=VERSION,
    author='Alex Gaetano Padula',
    packages=find_packages(),
    keywords=['keyvalue', 'storage-engine', 'ffi', 'lsm-tree']
)

