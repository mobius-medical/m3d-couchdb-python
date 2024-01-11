# -*- coding: utf-8 -*-

import unittest
import couchdb


class TestPackage(unittest.TestCase):

    def test_exports(self):
        expected = set([
            # couchdb.client
            'Server', 'Database', 'Document',
            'exceptions',
        ])
        exported = set(e for e in dir(couchdb) if not e.startswith('_'))
        self.assertTrue(expected <= exported)
