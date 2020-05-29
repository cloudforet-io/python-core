import unittest
from spaceone.core.auth.jwt.jwt_util import JWTUtil


class TestJWTUtil(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(TestJWTUtil, cls).setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        super(TestJWTUtil, cls).tearDownClass()

    def setUp(self) -> None:
        self.pub_jwk = None
        self.prv_jwk = None
        self.encoded = None
        self.payload = None

    def tearDown(self) -> None:
        self.pub_jwk = None
        self.prv_jwk = None
        self.payload = None
        self.encoded = None

    def test_generate_jwk(self):
        self.prv_jwk, self.pub_jwk = JWTUtil.generate_jwk()

        print(f'pub_jwk: {self.pub_jwk}')
        print(f'prv_jwk: {self.prv_jwk}')

        self.assertEqual(self.pub_jwk['n'], self.prv_jwk['n'])

    def test_encode_jwt(self):
        self.test_generate_jwk()

        self.payload = {
            'hello': 'world'
        }

        self.encoded = JWTUtil.encode(self.payload, self.prv_jwk)
        print(f'encoded: {self.encoded}')

        extracted = JWTUtil.unverified_decode(self.encoded)
        print(f'payload: {extracted}')
        self.assertDictEqual(self.payload, extracted)

    def test_decode_jwt(self):
        self.test_encode_jwt()
        decoded = JWTUtil.decode(self.encoded, self.pub_jwk)
        print(f'decoded: {decoded}')

        self.assertDictEqual(self.payload, decoded)
