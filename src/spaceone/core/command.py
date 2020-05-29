#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
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


def _set_grpc_command(subparsers, env):
    parser = subparsers.add_parser('grpc', description='Run a gRPC server',
                                   help='Run a gRPC server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('service', metavar='SERVICE', help='service name (identity, inventory, etc.)')
    parser.add_argument('-p', '--port', type=int, help='port of gRPC server', default=env['PORT'] or 50051)
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])


def _set_rest_command(subparsers, env):
    parser = subparsers.add_parser('rest', description='Run a REST server',
                                   help='Run a REST server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('service', metavar='SERVICE', help='service name (identity, inventory, etc.)')
    parser.add_argument('-p', '--port', type=int, help='port of REST server', default=env['PORT'] or 8080)
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])


def _set_scheduler_command(subparsers, env):
    parser = subparsers.add_parser('scheduler', description='Run a scheduler server',
                                   help='Run a scheduler server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('service', metavar='SERVICE', help='service name (identity, inventory, etc.)')
    parser.add_argument('-c', '--config', type=argparse.FileType('r'), help='config file path',
                        default=env['CONFIG_FILE'])


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

def _set_default_config():
    config.set_default_conf()


def _set_file_config(conf_file):
    if conf_file and conf_file.name:
        path = conf_file.name
        config.set_file_conf(path)


def _set_remote_config(conf_file):
    if conf_file and conf_file.name:
        path = conf_file.name
        config.set_remote_conf_from_file(path)


def _set_server_config(args):
    params = vars(args)

    # set server type
    config.init_conf(
        service=params['service'],
        server_type=params['command'],
        port=params.get('port')
    )

    # 1. get default config from global_conf.py
    _set_default_config()

    # 2. merge file conf
    _set_file_config(params['config'])

    # 3. merge remote conf
    _set_remote_config(params['config'])


def _set_test_config(args):
    if os.path.isfile(args.config):
        os.environ['TEST_CONFIG'] = args.config

    if os.path.isfile(args.scenario):
        os.environ['TEST_SCENARIO'] = args.scenario

    if args.parameters:
        os.environ['TEST_SCENARIO_PARAMS'] = ','.join(args.parameters)


def _initialize_config(args):
    if args.command == 'test':
        _set_test_config(args)
    else:
        _set_server_config(args)


def _run_tests(args):
    # suites are not hashable, need to use list
    loader = unittest.TestLoader()
    suites = loader.discover(args.dir)

    full_suite = unittest.TestSuite()
    full_suite.addTests(suites)
    RichTestRunner(verbosity=args.verbose, failfast=args.failfast).run(full_suite)


def _run_command(args):
    command = args.command

    if command == 'grpc':
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
