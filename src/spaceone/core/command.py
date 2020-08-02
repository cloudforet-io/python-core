#!/usr/bin/env python3

import argparse
import shutil
import os
import sys
import pkg_resources
import unittest

from spaceone.core import config
from spaceone.core import pygrpc
from spaceone.core import scheduler
from spaceone.core.unittest.runner import RichTestRunner


def _get_env():
    env = {
        'PORT': os.environ.get('SPACEONE_PORT'),
        'CONFIG_FILE': os.environ.get('SPACEONE_CONFIG_FILE'),
        'WORKING_DIR': os.environ.get('SPACEONE_WORKING_DIR') or os.getcwd()
    }

    env['CONFIG_FILE'] = env['CONFIG_FILE'].strip() if env['CONFIG_FILE'] else None
    return env


def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)


def _set_start_project_command(subparsers, env):
    parser = subparsers.add_parser('create-project', description='Create a new project directory',
                                   help='Create a new project directory',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('project_name', metavar='PROJECT_NAME', help='Project name')
    parser.add_argument('-d', '--directory', type=dir_path, help='Project directory')


def _set_grpc_command(subparsers, env):
    parser = subparsers.add_parser('grpc', description='Run a gRPC server',
                                   help='Run a gRPC server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('package', metavar='PACKAGE', help='Package (ex: spaceone.identity)')
    parser.add_argument('-p', '--port', type=int, help='Port of gRPC server', default=env['PORT'] or 50051)
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])
    parser.add_argument('-m', '--module-path', help='Module path')


def _set_rest_command(subparsers, env):
    parser = subparsers.add_parser('rest', description='Run a REST server',
                                   help='Run a REST server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('package', metavar='PACKAGE', help='Package (ex: spaceone.identity)')
    parser.add_argument('-p', '--port', type=int, help='Port of REST server', default=env['PORT'] or 8080)
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])
    parser.add_argument('-m', '--module-path', help='Module path')


def _set_scheduler_command(subparsers, env):
    parser = subparsers.add_parser('scheduler', description='Run a scheduler server',
                                   help='Run a scheduler server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('package', metavar='PACKAGE', help='Package (ex: spaceone.identity)')
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])
    parser.add_argument('-m', '--module-path', help='Module path')


def _set_test_command(subparsers, env):
    parser = subparsers.add_parser('test', description='Unit tests for source code',
                                   help='Unit tests for source code',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dir", action="store", type=str,
                        help="directory containing test files", default=env['WORKING_DIR'])
    parser.add_argument("-f", "--failfast", action="store_true", help="fast failure flag")
    parser.add_argument("-s", "--scenario", help="scenario file path",
                        default=f'{env["WORKING_DIR"]}/scenario.json')
    parser.add_argument('-c', '--config', help='config file path',
                        default=env['CONFIG_FILE'] or f'{env["WORKING_DIR"]}/config.yml')
    parser.add_argument("-p", "--parameters", action='append', type=str,
                        help="custom parameters to override a scenario file. "
                             "ex) -p domain.domain.name=new_name options.update_mode=false")
    parser.add_argument('-v', '--verbose', help='verbosity level', type=int, default=1)


def _set_file_config(conf_file):
    if conf_file and conf_file.name:
        path = conf_file.name
        config.set_file_conf(path)


def _set_remote_config(conf_file):
    if conf_file and conf_file.name:
        path = conf_file.name
        config.set_remote_conf_from_file(path)


def _set_python_path(package, module_path):
    current_path = os.getcwd()\

    if current_path not in sys.path:
        sys.path.insert(0, current_path)

    if module_path and module_path not in sys.path:
        sys.path.insert(0, module_path)

        if '.' in package:
            pkg_resources.declare_namespace(package)

    try:
        __import__(package)
    except Exception:
        raise Exception(f'The package({package}) can not imported. '
                        'Please check the module path.')


def _set_server_config(args):
    params = vars(args)

    # 1. Set a python path
    _set_python_path(params['package'], params.get('module_path'))

    # 2. Initialize config from command argument
    config.init_conf(
        package=params['package'],
        server_type=params['command'],
        port=params.get('port')
    )

    # 3. Get service config from global_conf.py
    config.set_service_config()

    # 4. Merge file conf
    _set_file_config(params['config'])

    # 5. Merge remote conf
    _set_remote_config(params['config'])


def _set_test_config(args):
    if os.path.isfile(args.config):
        os.environ['TEST_CONFIG'] = args.config

    if os.path.isfile(args.scenario):
        os.environ['TEST_SCENARIO'] = args.scenario

    if args.parameters:
        os.environ['TEST_SCENARIO_PARAMS'] = ','.join(args.parameters)


def _initialize_config(args):
    if args.command == 'create-project':
        pass
    elif args.command == 'test':
        _set_test_config(args)
    else:
        _set_server_config(args)


def _run_tests(args):
    # Suites are not hashable, need to use list
    loader = unittest.TestLoader()
    suites = loader.discover(args.dir)

    full_suite = unittest.TestSuite()
    full_suite.addTests(suites)
    RichTestRunner(verbosity=args.verbose, failfast=args.failfast).run(full_suite)


def init_project_file(path, text):
    with open(path, 'w') as f:
        f.write(text)


def _create_project(args):
    # Initialize path
    project_name = args.project_name
    project_directory = args.directory or os.getcwd()
    project_path = os.path.join(project_directory, project_name)

    # Copy skeleton source code
    skeleton_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skeleton')
    shutil.copytree(skeleton_path, project_path, ignore=shutil.ignore_patterns('__pycache__'))

    # Change source code for new project environment
    init_project_file(os.path.join(project_path, 'service', '__init__.py'),
                      f'from {project_name}.service.helloworld_service import *\n')
    init_project_file(os.path.join(project_path, 'manager', '__init__.py'),
                      f'from {project_name}.manager.helloworld_manager import *\n')
    init_project_file(os.path.join(project_path, 'info', '__init__.py'),
                      f'from {project_name}.info.helloworld_info import *\n')
    proto_conf = (
        "PROTO = {\n"
        f"    '{project_name}.api.helloworld': ['HelloWorld']\n"
        "}\n"
    )
    init_project_file(os.path.join(project_path, 'conf', 'proto_conf.py'), proto_conf)


def _run_command(args):
    command = args.command
    if command == 'create-project':
        _create_project(args)
    elif command == 'grpc':
        pygrpc.serve()
    elif command == 'scheduler':
        scheduler.serve()
    elif command == 'test':
        _run_tests(args)
    else:
        raise NotImplementedError(f"{command} not implemented!")


def _parse_argument():
    env = _get_env()
    parser = argparse.ArgumentParser(description='Command line interface for SpaceONE', prog='spaceone')
    subparsers = parser.add_subparsers(dest='command', metavar='COMMAND')

    _set_start_project_command(subparsers, env)
    _set_grpc_command(subparsers, env)
    _set_scheduler_command(subparsers, env)
    #_set_rest_command(subparsers, env)
    _set_test_command(subparsers, env)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        parser.exit()

    return args


def main():
    args = _parse_argument()
    _initialize_config(args)
    _run_command(args)


if __name__ == '__main__':
    main()
