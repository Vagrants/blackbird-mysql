# -*- coding: utf-8 -*-

from lxml import etree
import unittest
import glob

class TestTemplateFormat(unittest.TestCase):

    def test_template(self):

        for xml in glob.glob('templates/*xml'):
            with open(xml) as f:
                parser = etree.XMLParser()
                try:
                    etree.XML(f.read(), parser)
                except etree.XMLSyntaxError:
                    pass
                self.assertEqual(len(parser.error_log), 0,
                         "Found xml syntax errors in {0}.".format(xml))

