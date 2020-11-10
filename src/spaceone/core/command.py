import os
import shutil
import sys
import unittest
from typing import List

import click
import pkg_resources

from spaceone.core import config, pygrpc
from spaceone.core import scheduler as scheduler_v1
from spaceone.core.celery import app as celery_app
from spaceone.core.unittest.runner import RichTestRunner


@click.group()
def cli():
    pass


@cli.command()
@click.argument('project_name')
@click.option('-d', '--directory', type=click.Path(), help='Project directory')
def create_project(project_name=None, directory=None):
    _create_project(project_name, directory)


@cli.command()
@click.argument('package')
@click.option('-p', '--port', type=int, default=lambda: os.environ.get('SPACEONE_PORT', 50051),
              help='Port of gRPC server', show_default=True)
@click.option('-c', '--config', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-m', '--module_path', type=click.Path(exists=True), help='Module path')
def grpc(package, port=None, config=None, module_path=None):
    """Run a gRPC server"""
    _set_server_config('grpc', package, module_path, port, config_file=config)
    pygrpc.serve()


@cli.command()
@click.argument('package')
@click.option('-p', '--port', type=int, default=lambda: os.environ.get('SPACEONE_PORT', 8080),
              help='Port of REST server', show_default=True)
@click.option('-c', '--config', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-m', '--module_path', type=click.Path(exists=True), help='Module path')
def rest(package, port=None, config=None, module_path=None):
    """Run a REST server"""
    _set_server_config('rest', package, module_path, port, config_file=config)

    pass


@cli.command()
@click.argument('package')
@click.option('-c', '--config', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-m', '--module_path', type=click.Path(exists=True), help='Module path')
def scheduler(package, config=None, module_path=None):
    """Run a scheduler server"""
    _set_server_config('scheduler', package, module_path, config_file=config)
    scheduler_v1.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-m', '--module-path', 'module_path', type=click.Path(exists=True), help='Module path')
def celery(package, config=None, module_path=None):
    """Run a celery server(worker or beat)"""
    print(config)
    _set_server_config('celery', package, module_path, config_file=config)
    celery_app.serve()


@cli.command()
@click.option('-c', '--config', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-d', '--dir', type=str, help="directory containing test files",
              default=lambda: os.environ.get('SPACEONE_WORKING_DIR', os.getcwd()))
@click.option('-f', '--failfast', help="fast failure flag", is_flag=True)
@click.option('-s', '--scenario', type=str, help="scenario file path")
@click.option('-p', '--parameters', type=str, help="custom parameters to override a scenario file. "
                                                   "ex) -p domain.domain.name=new_name -p options.update_mode=false",
              multiple=True)
@click.option('-v', '--verbose', count=True, help='verbosity level', default=1)
def test(config=None, dir=None, failfast=False, scenario: str = None, parameters: List[str] = None, verbose=1):
    """Unit tests for source code"""
    # set config
    if config:
        os.environ['TEST_CONFIG'] = config

    if scenario:
        os.environ['TEST_SCENARIO'] = scenario

    if parameters:
        os.environ['TEST_SCENARIO_PARAMS'] = ','.join(parameters)

    # run test
    loader = unittest.TestLoader()
    suites = loader.discover(dir)

    full_suite = unittest.TestSuite()
    full_suite.addTests(suites)
    RichTestRunner(verbosity=verbose, failfast=failfast).run(full_suite)


def _set_file_config(conf_file):
    if conf_file:
        config.set_file_conf(conf_file)


def _set_remote_config(conf_file):
    if conf_file:
        config.set_remote_conf_from_file(conf_file)


def _set_python_path(package, module_path):
    current_path = os.getcwd()

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


def _set_server_config(command, package, module_path=None, port=None, config_file=None):
    # 1. Set a python path
    _set_python_path(package, module_path)

    # 2. Initialize config from command argument
    config.init_conf(
        package=package,
        server_type=command,
        port=port
    )

    # 3. Get service config from global_conf.py
    config.set_service_config()

    # 4. Merge file conf
    _set_file_config(config_file)

    # 5. Merge remote conf
    _set_remote_config(config_file)


def init_project_file(path, text):
    with open(path, 'w') as f:
        f.write(text)


def _create_project(project_name, directory=None):
    # Initialize path
    project_name = project_name
    project_directory = directory or os.getcwd()
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


if __name__ == '__main__':
    cli()
