import os

from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core.unittest.test_case import SpaceoneTestCase

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(ROOT_DIR, 'src')


class TestGreeterService(SpaceoneTestCase):
    config = {
    }
    src_path = SRC_PATH
    package = 'example_server'

    def setUp(self):
        super(TestGreeterService, self).setUp()
        self.svc: GreeterService = self.locator.get_service('GreeterService')

    def test_hello(self):
        name = 'one'
        result = self.svc.hello({"name": name})
        self.assertEqual(name, result['message'])

    def test_hello_group(self):
        name = 'one'
        results = self.svc.hello_group({"name": name})

        answer = ['Hello a!', 'Hello b!', 'Hello c!', 'Hello d!']
        self.assertEqual(answer, list(answer))

    def test_hello_everyone(self):
        names = ['a', 'b', 'c', 'd']
        result = self.svc.hello_everyone(({"name": name} for name in names))
        self.assertEqual("Hello everyone ['a', 'b', 'c', 'd']!", result['message'])

    def test_hello_one_by_one(self):
        names = ['a', 'b', 'c', 'd']
        results = self.svc.hello_one_by_one(({"name": name} for name in names))
        self.assertEqual([{'message': f'Hello {x}!'} for x in names], list(results))


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
