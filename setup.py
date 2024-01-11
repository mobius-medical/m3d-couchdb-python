#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

from setuptools import setup

setup(
    name='CouchDB',
    version='1.2.1.post1+m3d',
    description='Python library for working with CouchDB',
    long_description="""
    This is a Python library for CouchDB. It provides a convenient high level
    interface for the CouchDB server.""",
    author = 'Christopher Lenz',
    author_email = 'cmlenz@gmx.de',
    maintainer = 'Dirkjan Ochtman',
    maintainer_email = 'dirkjan@ochtman.nl',
    license = 'BSD',
    url = 'https://github.com/djc/couchdb-python/',
    classifiers = [
        'Development Status :: 6 - Mature',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages = ['couchdb', 'couchdb.tools', 'couchdb.tests'],
    entry_points={
        'console_scripts': [
            'couchpy = couchdb.view:main',
            'couchdb-dump = couchdb.tools.dump:main',
            'couchdb-load = couchdb.tools.load:main',
            'couchdb-replicate = couchdb.tools.replicate:main',
            'couchdb-load-design-doc = couchdb.loader:main',
        ],
    },
    install_requires=[
        "furl",
        "requests",
        "requests_toolbelt",
        "six",
    ],
    test_suite='couchdb.tests.__main__.suite',
    zip_safe=True,
)
