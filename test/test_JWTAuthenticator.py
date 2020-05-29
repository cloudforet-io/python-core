import unittest

from spaceone.core.auth.jwt import JWTAuthenticator, JWTUtil
from spaceone.core.error import ERROR_AUTHENTICATE_FAILURE


class TestJWTAuthenticator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestJWTAuthenticator, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestJWTAuthenticator, cls).tearDownClass()

    def setUp(self):
        self._generate_key()

    def _generate_key(self):
        self._prv_jwk, self._pub_jwk = JWTUtil.generate_jwk()
        self._jwt_auth = JWTAuthenticator(self._pub_jwk)

    def tearDown(self):
        pass

    def test_validate(self):
        payload = {
            'hello': 'world',
            'did': 'domain-0436002f575f'
        }
        encoded = JWTUtil.encode(payload, self._prv_jwk)

        decoded = self._jwt_auth.validate(encoded)
        self.assertDictEqual(payload, decoded)

    def test_invalid_token_content(self):
        encoded = '12345.12345.12345'
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(encoded)

        encoded = '1234512a34512345'
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(encoded)

        decimal = 1
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(decimal)

        dictionary = {}
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(dictionary)

        boolean = {}
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(boolean)

        float = 1.1
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(float)

        list = []
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(list)

        obj = object
        with self.assertRaises(ERROR_AUTHENTICATE_FAILURE):
            self._jwt_auth.validate(obj)
