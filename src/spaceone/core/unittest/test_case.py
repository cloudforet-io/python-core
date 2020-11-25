import sys
import unittest
import grpc
import grpc_testing
import pkg_resources
from google.protobuf import symbol_database
from mongoengine import connect, disconnect_all

from spaceone.core import config
from spaceone.core.locator import Locator


def set_python_path(src_path: str, package: str):
    sys.path.insert(0, src_path)
    pkg_resources.declare_namespace(package)
    try:
        __import__(package)
    except Exception:
        raise Exception(f'The package({package}) can not imported. '
                        'Please check the module path.')



class SpaceoneTestCase(unittest.TestCase):
    config: dict = {}
    package: str = None
    src_path: str = None
    _locator: Locator = None

    @property
    def locator(self):
        if not self._locator:
            self._locator = Locator()
        return self._locator

    @classmethod
    def setUpClass(cls) -> None:
        super(SpaceoneTestCase, cls).setUpClass()
        set_python_path(cls.src_path, cls.package)
        config.init_conf(
            package=cls.package,
            server_type='grpc',
        )
        config.set_service_config()
        config.set_global(**cls.config)
        connect('default', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls) -> None:
        disconnect_all()



def _get_proto_conf(package):
    proto_module = __import__(f'{package}.conf.proto_conf', fromlist=['proto_conf'])
    return getattr(proto_module, 'PROTO', {})


def _get_servicer():
    proto_conf = _get_proto_conf(config.get_package())

    serviceres = {}
    for module_path, servicer_names in proto_conf.items():
        for servicer_name in servicer_names:
            api_module = _import_module(module_path, servicer_name)
            if api_module:
                if hasattr(api_module, servicer_name):
                    servicer = getattr(api_module, servicer_name)()
                    serviceres[servicer.pb2.DESCRIPTOR.services_by_name[servicer_name]] = servicer

    return serviceres


def _import_module(module_path, servicer_name):
    module = None
    try:
        module = __import__(module_path, fromlist=[servicer_name])
    except Exception as e:
        _LOGGER.warning(f'[_import_module] Cannot import grpc servicer module. (reason = {e})')

    return module


class SpaceoneGrpcTestCase(SpaceoneTestCase):
    _test_server = None

    @classmethod
    def setUpClass(cls) -> None:
        super(SpaceoneGrpcTestCase, cls).setUpClass()

    @property
    def test_server(self):
        if not self._test_server:
            servicers = _get_servicer()

            self._test_server = grpc_testing.server_from_dictionary(
                servicers, grpc_testing.strict_real_time()
            )
        return self._test_server

    def get_method_descriptor(self, full_name):
        return symbol_database.Default().pool.FindMethodByName(full_name)

    def request_unary_unary(self, full_name, request, metadata=None, timeout=None, **kwargs):
        method = self.test_server.invoke_unary_unary(
            method_descriptor=self.get_method_descriptor(full_name),
            invocation_metadata=metadata or {},
            request=request, timeout=timeout, **kwargs)
        return method.termination()

    @staticmethod
    def get_all_response(method):
        finish = False
        result = []
        while not finish:
            try:
                result.append(method.take_response())
            except Exception:
                finish = True
        return result

    @staticmethod
    def send_request_all(method, requests):
        [method.send_request(req) for req in requests]
        method.requests_closed()


    def request_unary_stream(self, full_name, request, metadata=None, timeout=None, **kwargs):
        method = self.test_server.invoke_unary_stream(
            method_descriptor=self.get_method_descriptor(full_name),
            invocation_metadata=metadata or {},
            request=request, timeout=timeout, **kwargs
        )
        results = self.get_all_response(method)
        return (results, *method.termination())

    def request_stream_stream(self, full_name, requests, metadata=None, timeout=None, **kwargs):
        method = self.test_server.invoke_stream_stream(
            method_descriptor=self.get_method_descriptor(full_name),
            invocation_metadata=metadata or {},
            timeout=timeout, **kwargs)
        self.send_request_all(method,requests)
        results = self.get_all_response(method)
        return (results, *method.termination())


    def request_stream_unary(self, full_name, requests, metadata=None, timeout=None, **kwargs):
        method = self.test_server.invoke_stream_unary(
            method_descriptor=self.get_method_descriptor(full_name),
            invocation_metadata=metadata or {},
            timeout=timeout, **kwargs)
        self.send_request_all(method,requests)
        return method.termination()

    def assertGrpcStatusCodeOk(self, code):
        self.assertEqual(code, grpc.StatusCode.OK)
