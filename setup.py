#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='stream-closeio',
      version='0.1.0',
      description='Streams CloseIO data',
      author='Stitch',
      url='https://github.com/stitchstreams/stream-closeio',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['stream_closeio'],
      install_requires=['stitchclient>=0.4.1',
                        'stitchstream-python>=0.4.1',
                        'requests==2.12.4',
                        'arrow==0.10.0',
                        'six==1.10.0',
                        'backoff==1.3.2',
                        'python-dateutil==2.6.0'],
      entry_points='''
          [console_scripts]
          stream-closeio=stream_closeio:main
      ''',
      packages=['stream_closeio'],
      package_data = {
          'stream_closeio': [
              'leads.json',
              'activities.json'
              ]
          }
)
