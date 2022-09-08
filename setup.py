#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-closeio',
    version='1.6.3',
    description='Singer.io tap for extracting data from the CloseIO API',
    author='Stitch',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_closeio'],
    install_requires=[
        'singer-python==5.8.1',
        'pendulum==1.2.0',
        'requests==2.20.0',
    ],
    extras_require={
        "test": [
            "pylint==2.13.7",
            "nose"
        ],
        "dev": [
            "ipdb==0.11"
        ]
    },
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
