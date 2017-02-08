#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-closeio',
      version='0.2.0',
      description='Taps CloseIO data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-closeio',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_closeio'],
      install_requires=['stitchclient>=0.4.1',
                        'stitchstream-python>=0.6.0',
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
