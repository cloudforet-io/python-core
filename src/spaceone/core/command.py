import os
import shutil
import sys
import unittest
from typing import List

import click
import pkg_resources

from spaceone.core import config, pygrpc, fastapi, utils
from spaceone.core import scheduler as scheduler_v1
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
@click.option('-c', '--config-file', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='Config file path')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Module path')
def grpc(package, port=None, config_file=None, module_path=None):
    """Run a gRPC server"""
    _set_server_config(package, module_path, port, config_file=config_file)
    pygrpc.serve()


@cli.command()
@click.argument('package')
@click.option('-h', '--host', type=str, default=lambda: os.environ.get('SPACEONE_HOST', '127.0.0.1'),
              help='Host of REST server', show_default=True)
@click.option('-p', '--port', type=int, default=lambda: os.environ.get('SPACEONE_PORT', 8000),
              help='Port of REST server', show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='Config file path')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Module path')
def rest(package, host=None, port=None, config_file=None, module_path=None):
    """Run a FastAPI REST server"""
    _set_server_config(package, module_path, port, config_file=config_file)
    fastapi.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config-file', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='config file path')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Module path')
def scheduler(package, config_file=None, module_path=None):
    """Run a scheduler server"""
    _set_server_config(package, module_path, config_file=config_file)
    scheduler_v1.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config-file', type=click.Path(exists=True), default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'),
              help='Config file path')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Module path')
@click.option('-o', '--output', default='yaml', help='Output format',
              type=click.Choice(['json', 'yaml']), show_default=True)
def show_config(package, config_file=None, module_path=None, output=None):
    """Show global configurations"""
    _set_server_config(package, module_path, config_file=config_file)
    _print_config(output)


@cli.command()
@click.option('-c', '--config-file', type=str, help='Config file path')
@click.option('-d', '--dir', type=str, help="Directory containing test files",
              default=lambda: os.environ.get('SPACEONE_WORKING_DIR', os.getcwd()))
@click.option('-f', '--failfast', help="Fast failure flag", is_flag=True)
@click.option('-s', '--scenario', type=str, help="Scenario file path")
@click.option('-p', '--parameters', type=str, help="Custom parameters to override a scenario file. "
                                                   "ex) -p domain.domain.name=new_name -p options.update_mode=false",
              multiple=True)
@click.option('-v', '--verbose', count=True, help='Verbosity level', default=1)
def test(config_file=None, dir=None, failfast=False, scenario: str = None, parameters: List[str] = None, verbose=1):
    """Unit tests for source code"""
    # set config
    if config:
        os.environ['TEST_CONFIG'] = config_file

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


def _set_python_path(package, module_path):
    current_path = os.getcwd()

    if current_path not in sys.path:
        sys.path.insert(0, current_path)

    if isinstance(module_path, tuple):
        for path in module_path:
            if path not in sys.path:
                sys.path.insert(0, path)

    if '.' in package:
        pkg_resources.declare_namespace(package)

    try:
        __import__(package)
    except Exception:
        raise Exception(f'The package({package}) can not imported. '
                        'Please check the module path.')


def _set_server_config(package, module_path=None, port=None, config_file=None):
    # 1. Set a python path
    _set_python_path(package, module_path)

    # 2. Initialize config from command argument
    config.init_conf(
        package=package,
        port=port
    )

    # 3. Get service config from global_conf.py
    config.set_service_config()

    # 4. Merge file conf
    if config_file:
        config.set_file_conf(config_file)


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


def _print_config(output):
    data = {
        'GLOBAL': config.get_global()
    }

    if output == 'json':
        print(utils.dump_json(data, indent=4))
    else:
        print(utils.dump_yaml(data))


if __name__ == '__main__':
    cli()
