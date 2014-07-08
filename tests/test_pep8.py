# -*- coding: utf-8 -*-

import pep8
import unittest

class TestCodeFormat(unittest.TestCase):

    def test_pep8(self):
        fchecker = pep8.Checker('mysql.py', show_source=True, ignore='E221')
        file_errors = fchecker.check_all()
        self.assertEqual(file_errors, 0,
                         "Found code style errors (and warnings).")
