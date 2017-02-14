#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-closeio',
      version='0.3.0',
      description='Singer.io tap for extracting data from the CloseIO API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_closeio'],
      install_requires=['stitchclient>=0.4.1',
                        'singer-python>=0.1.0',
                        'requests==2.12.4',
                        'arrow==0.10.0',
                        'six==1.10.0',
                        'backoff==1.3.2',
                        'python-dateutil==2.6.0'],
      entry_points='''
          [console_scripts]
          tap-closeio=tap_closeio:main
      ''',
      packages=['tap_closeio'],
      package_data = {
          'tap_closeio': [
              'leads.json',
              'activities.json'
              ]
          }
)
