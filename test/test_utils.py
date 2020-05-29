import unittest

from spaceone.core import utils


class TestUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestUtils, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestUtils, cls).tearDownClass()

    def setUp(self):
        ...

    def tearDown(self):
        pass

    def test_parse_endpoint(self):
        url = 'file:///home/hojinshim/test.yaml'
        url_parsed = utils.parse_endpoint(url)
        print(url_parsed)
        self.assertEqual('file', url_parsed.get('scheme'))

        url = 'http://pyengine.net/hojinshim/test.yaml'
        url_parsed = utils.parse_endpoint(url)
        print(url_parsed)
        self.assertEqual('http', url_parsed.get('scheme'))

        url = 'https://pyengine.net/hojinshim/test.yaml'
        url_parsed = utils.parse_endpoint(url)
        print(url_parsed)
        self.assertEqual('https', url_parsed.get('scheme'))

        url = 'grpc://pyengine.net/hojinshim/test.yaml'
        url_parsed = utils.parse_endpoint(url)
        print(url_parsed)
        self.assertEqual('grpc', url_parsed.get('scheme'))
