import os
import unittest
import grpc
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core.unittest.test_case import SpaceoneGrpcTestCase
from ..src.example_server.api.v1.helloworld_pb2 import HelloRequest,HelloReply
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(ROOT_DIR, 'src')


# class TestDefaultSecretAPI(SpaceoneGrpcTestCase):
#     config = {
#         "ENCRYPT": False,
#         "CONNECTORS": {
#             'AWSSecretManagerConnector': {
#                 "region_name": "ap-northeast-2",
#
#             },
#             'AWSKMSConnector': {
#                 "region_name": "ap-northeast-2",
#                 "kms_key_id": ""
#             },
#         }
#     }
#     src_path = SRC_PATH
#     package = 'spaceone.secret'
#
#     def get_encrypt_context_by_vo(self, secret_vo: Union[Secret, dict]) -> str:
#         context = collections.OrderedDict()
#         context['domain_id'] = secret_vo['domain_id'] if isinstance(secret_vo, dict) else secret_vo.domain_id
#         context['secret_id'] = secret_vo['secret_id'] if isinstance(secret_vo, dict) else secret_vo.secret_id
#         return base64.b64encode(json.dumps(context).encode()).decode()
#
#
#     def test_get_secret(self):
#         SecretFactory.create_batch(10)
#         secret_1 = SecretFactory()
#
#         request = GetSecretRequest(secret_id=secret_1.secret_id, domain_id=secret_1.domain_id)
#         response, metadata, code, details = self.request_unary_unary('spaceone.api.secret.v1.Secret.get', request)
#         self.assertGrpcStatusCodeOk(code)
#         self.assertEqual(secret_1.secret_id, response.secret_id)
#         self.assertEqual(secret_1.domain_id, response.domain_id)
#
#     def test_get_secret_data(self):
#         SecretFactory.create_batch(10)
#         secret_data = {"secret": f"{os.urandom(10)}"}
#         secret_1 = SecretFactory(secret_data=secret_data)
#
#         request = GetSecretRequest(secret_id=secret_1.secret_id, domain_id=secret_1.domain_id)
#         response, metadata, code, details = self.request_unary_unary('spaceone.api.secret.v1.Secret.get_data', request)
#         self.assertGrpcStatusCodeOk(code)
#         self.assertFalse(response.encrypted)
#         self.assertEqual(secret_data, MessageToDict(response)['data'])
#
#     def _check_secretmanager_exists(self, secret_id):
#         region = config.get_global('CONNECTORS', {}).get('AWSSecretManagerConnector', {}).get("region_name")
#         client = boto3.client('secretsmanager', region_name=region)
#         try:
#             client.describe_secret(
#                 SecretId=secret_id
#             )
#             return True
#         except Exception as e:
#             return False
#
#     def test_create_secret(self):
#         SecretFactory.create_batch(10)
#         secret_data = {
#             "sample": "abcd"
#         }
#         secret_1 = {
#             'name': 'sample',
#             'data': ParseDict(secret_data, Struct()),
#             'secret_type': 'CREDENTIALS',
#             'domain_id': 'domain_1234',
#         }
#
#         request = CreateSecretRequest(**secret_1, )
#         response, metadata, code, details = self.request_unary_unary('spaceone.api.secret.v1.Secret.create', request)
#         self.assertGrpcStatusCodeOk(code)
#         response = MessageToDict(response, preserving_proto_field_name=True)
#
#         self.assertEqual(secret_1.get('domain_id'), response['domain_id'])
#         self.assertEqual(secret_1.get('secret_type'), response['secret_type'])
#         self.assertTrue(self._check_secretmanager_exists(response['secret_id']))


class TestGreeterAPI(SpaceoneGrpcTestCase):
    config = {
    }
    src_path = SRC_PATH
    package = 'example_server'

    def test_hello(self):
        name = 'one'
        request = HelloRequest(name=name)
        response, metadata, code, details = self.request_unary_unary('helloworld.Greeter.SayHello', request)

        self.assertGrpcStatusCodeOk(code)
        self.assertEqual(name, response.message)

    def test_hello_group(self):
        name = 'one'
        request = HelloRequest(name=name)
        responses, metadata, code, details = self.request_unary_stream('helloworld.Greeter.SayHelloGroup', request)

        answers = ['Hello a!', 'Hello b!', 'Hello c!', 'Hello d!']
        for answer,result in zip(answers,responses):
            self.assertEqual(answer, result.message)

    def test_hello_everyone(self):
        names = ['a', 'b', 'c', 'd']
        response, metadata, code, details = self.request_unary_unary('helloworld.Greeter.HelloEveryone', (HelloRequest(name=name) for name in names))

        self.assertGrpcStatusCodeOk(code)
        self.assertEqual("Hello everyone ['a', 'b', 'c', 'd']!", response.message)

    def test_hello_one_by_one(self):
        names = ['a', 'b', 'c', 'd']

        responses, metadata, code, details = self.request_stream_stream('helloworld.Greeter.SayHelloOneByOne', (HelloRequest(name=name) for name in names))

        for name,result in zip(names,responses):
            self.assertEqual(f'Hello {name}!', result.message)


if __name__ == "__main__":
    unittest.main(testRunner=RichTestRunner)
