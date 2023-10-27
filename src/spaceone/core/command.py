import os
import shutil
import sys
import unittest
from typing import List

import click
import pkg_resources

from spaceone.core import config, pygrpc, fastapi, utils, model
from spaceone.core import scheduler as scheduler_v1
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core.logger import set_logger
from spaceone.core.opentelemetry import set_tracer, set_metric

_SOURCE_ALIAS = {
    'auth': 'spaceone.identity.plugin.auth.skeleton',
    'asset': 'spaceone.inventory.plugin.collector.skeleton',
    'metric': 'spaceone.monitoring.plugin.metric.skeleton',
    'log': 'spaceone.monitoring.plugin.log.skeleton',
    'webhook': 'spaceone.monitoring.plugin.webhook.skeleton',
    'cost': 'spaceone.cost_analysis.plugin.data_source.skeleton',
    'notification': 'spaceone.notification.plugin.protocol.skeleton',
}


@click.group()
def cli():
    pass


@cli.command()
@click.argument('project_name')
@click.option('-d', '--directory', type=click.Path(), help='Project directory')
@click.option('-s', '--source', type=str, help=f'skeleton code of the plugin: ['
                                               f'{"|".join(_SOURCE_ALIAS.keys())}] or '
                                               f'module path(e.g. spaceone.core.skeleton)]')
def create_project(project_name, directory=None, source=None):
    """Create a new project"""

    _create_project(project_name, directory, source)


@cli.command()
@click.argument('package')
@click.option('-p', '--port', type=int, default=lambda: os.environ.get('SPACEONE_PORT', 50051),
              help='Port of gRPC server', show_default=True)
@click.option('-a', '--app-path', type=str, default='interface.grpc:app', help='Path of gRPC application',
              show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Path of module')
def grpc(package, port=None, app_path=None, config_file=None, module_path=None):
    """Run a gRPC server"""

    # Initialize config
    _set_server_config(package, module_path, port, config_file=config_file)

    # Enable logging configuration
    set_logger()

    # Set OTel Tracer and Metric
    set_tracer()

    # Connect all databases
    model.init_all()

    # Run gRPC server
    pygrpc.serve(app_path)


@cli.command()
@click.argument('package')
@click.option('-h', '--host', type=str, default=lambda: os.environ.get('SPACEONE_HOST', '127.0.0.1'),
              help='Host of REST server', show_default=True)
@click.option('-p', '--port', type=int, default=lambda: os.environ.get('SPACEONE_PORT', 8000),
              help='Port of REST server', show_default=True)
@click.option('-a', '--app-path', type=str, default='interface.rest:app', help='Path of gRPC application',
              show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Path of module')
def rest(package, host=None, port=None, app_path=None, config_file=None, module_path=None):
    """Run a FastAPI REST server"""
    # Initialize config
    _set_server_config(package, module_path, port, config_file=config_file)

    # Enable logging configuration
    set_logger()

    # Set OTel Tracer and Metric
    set_tracer()

    # Connect all databases
    model.init_all()

    # Run REST server
    fastapi.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Path of module')
def scheduler(package, config_file=None, module_path=None):
    """Run a scheduler server"""
    # Initialize config
    _set_server_config(package, module_path, config_file=config_file)

    # Enable logging configuration
    set_logger()

    # Set OTel Tracer and Metric
    set_tracer()

    # Connect all databases
    model.init_all()

    # Run scheduler server
    scheduler_v1.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True, help='Path of module')
@click.option('-o', '--output', default='yaml', help='Output format',
              type=click.Choice(['json', 'yaml']), show_default=True)
def show_config(package, config_file=None, module_path=None, output=None):
    """Show global configurations"""
    # Initialize config
    _set_server_config(package, module_path, config_file=config_file)

    # Print merged config
    _print_config(output)


@cli.command()
@click.option('-c', '--config-file', type=str, help='Path of config file')
@click.option('-d', '--dir', type=str, help='Directory containing test files',
              default=lambda: os.environ.get('SPACEONE_WORKING_DIR', os.getcwd()))
@click.option('-f', '--failfast', help='Fast failure flag', is_flag=True)
@click.option('-s', '--scenario', type=str, help='Path of scenario file')
@click.option('-p', '--parameters', type=str, help='Custom parameters to override a scenario file. '
                                                   '(e.g. -p domain.domain.name=new_name -p options.update_mode=false)',
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


def _create_project(project_name, directory=None, source=None):
    # Initialize path
    project_name = project_name
    project_directory = directory or os.getcwd()
    project_path = os.path.join(project_directory, project_name)

    # Copy skeleton source code
    if source:
        skeleton = _SOURCE_ALIAS.get(source, source)
    else:
        skeleton = 'spaceone.core.skeleton'

    # Check skeleton module name
    module_name = skeleton.split('.')[-1]
    if module_name != 'skeleton':
        raise Exception('Skeleton module path must be ended with "skeleton".')

    # Copy skeleton source code
    skeleton_module = __import__(skeleton, fromlist=['*'])
    skeleton_path = os.path.dirname(skeleton_module.__file__)
    shutil.copytree(skeleton_path, project_path, ignore=shutil.ignore_patterns('__pycache__'))


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
