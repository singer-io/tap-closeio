#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-closeio',
      version='1.5.0',
      description='Singer.io tap for extracting data from the CloseIO API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_closeio'],
      install_requires=[
          'singer-python==5.2.0',
          'pendulum==1.2.0',
          'requests==2.12.4',
      ],
      entry_points='''
          [console_scripts]
          tap-closeio=tap_closeio:main
      ''',
      packages=['tap_closeio'],
      package_data = {
          'schemas': ['tap_closeio/schemas/*.json']
      },
      include_package_data=True,
)
