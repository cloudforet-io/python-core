import os
import shutil
import sys
import unittest
import click
from typing import List

from spaceone.core import config, pygrpc, fastapi, utils, model
from spaceone.core import plugin as plugin_srv
from spaceone.core import scheduler as scheduler_v1
from spaceone.core.unittest.runner import RichTestRunner
from spaceone.core.logger import set_logger
from spaceone.core.opentelemetry import set_tracer, set_metric
from spaceone.core.plugin.plugin_conf import PLUGIN_SOURCES

_GLOBAL_CONFIG_PATH = '{package}.conf.global_conf:global_conf'


@click.group()
def cli():
    pass


@cli.command()
@click.argument('project_name')
@click.option('-d', '--directory', type=click.Path(), help='Project directory')
@click.option('-s', '--source', type=str, help=f'skeleton code of the plugin: ['
                                               f'{"|".join(PLUGIN_SOURCES.keys())}] or '
                                               f'module path(e.g. spaceone.core.skeleton)]')
def create_project(project_name, directory=None, source=None):
    """Create a new project"""

    _create_project(project_name, directory, source)


@cli.group()
def run():
    """Run a server"""
    pass


@run.command()
@click.argument('package')
@click.option('-a', '--app-path', type=str,
              help='Python path of gRPC application [default: {package}.interface.grpc:app]')
@click.option('-s', '--source-root', type=click.Path(exists=True), default='.',
              help='Path of source root', show_default=True)
@click.option('-p', '--port', type=int, default=os.environ.get('SPACEONE_PORT', 50051),
              help='Port of gRPC server', show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True,
              help='Additional python path')
def grpc_server(package, app_path=None, source_root=None, port=None, config_file=None, module_path=None):
    """Run a gRPC server"""

    # Initialize config
    _set_server_config(package, source_root, port, config_file=config_file, grpc_app_path=app_path,
                       module_path=module_path)

    # Initialize common modules
    _init_common_modules()

    # Run gRPC server
    pygrpc.serve()


@run.command()
@click.argument('package')
@click.option('-a', '--app-path', type=str,
              help='Python path of REST application [default: {package}.interface.rest:app]')
@click.option('-s', '--source-root', type=click.Path(exists=True), default='.',
              help='Path of source root', show_default=True)
@click.option('-p', '--port', type=int, default=os.environ.get('SPACEONE_PORT', 8000),
              help='Port of REST server', show_default=True)
@click.option('-h', '--host', type=str, default=os.environ.get('SPACEONE_HOST', '127.0.0.1'),
              help='Host of REST server', show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True,
              help='Additional python path')
def rest_server(package, app_path=None, source_root=None, port=None, host=None, config_file=None, module_path=None):
    """Run a FastAPI REST server"""

    # Initialize config
    _set_server_config(package, source_root, port, host=host, config_file=config_file, rest_app_path=app_path,
                       module_path=module_path)

    # Initialize common modules
    _init_common_modules()

    # Run REST server
    fastapi.serve()


@run.command()
@click.argument('package')
@click.option('-s', '--source-root', type=click.Path(exists=True), default='.',
              help='Path of source root', show_default=True)
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True,
              help='Additional python path')
def scheduler(package, source_root=None, config_file=None, module_path=None):
    """Run a scheduler server"""

    # Initialize config
    _set_server_config(package, source_root, config_file=config_file, module_path=module_path)

    # Initialize common modules
    _init_common_modules()

    # Run scheduler server
    scheduler_v1.serve()


@run.command()
@click.argument('package')
@click.option('-a', '--app-path', type=str,
              help='Path of Plugin application [default: {package}.main:app]')
@click.option('-s', '--source-root', type=click.Path(exists=True), default='.',
              help='Path of source root', show_default=True)
@click.option('-p', '--port', type=int, default=os.environ.get('SPACEONE_PORT', 50051),
              help='Port of plugin server', show_default=True)
@click.option('-m', '--module-path', type=click.Path(exists=True), multiple=True,
              help='Additional python path')
def plugin_server(package, app_path=None, source_root=None, port=None, module_path=None):
    """Run a plugin server"""

    # Initialize config
    _set_server_config(package, source_root, port, plugin_app_path=app_path, module_path=module_path,
                       set_custom_config=False)

    # Run Plugin Server
    plugin_srv.serve()


@cli.command()
@click.argument('package')
@click.option('-c', '--config-file', type=click.Path(exists=True),
              default=lambda: os.environ.get('SPACEONE_CONFIG_FILE'), help='Path of config file')
@click.option('-s', '--source-root', type=click.Path(exists=True), default='.',
              help='Path of source root', show_default=True)
@click.option('-o', '--output', default='yaml', help='Output format',
              type=click.Choice(['json', 'yaml']), show_default=True)
def show_config(package, source_root=None, config_file=None, output=None):
    """Show global configurations"""
    # Initialize config
    _set_server_config(package, source_root, config_file=config_file)

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


def _set_python_path(package: str, source_root: str = None, module_path: List[str] = None):
    source_root = source_root or os.getcwd()

    if source_root not in sys.path:
        sys.path.insert(0, source_root)

    if module_path:
        for path in module_path:
            if path not in sys.path:
                sys.path.insert(0, path)

    try:
        __import__(package)
    except Exception:
        raise Exception(f'The package({package}) can not imported. '
                        'Please check the module path.')


def _set_server_config(package, source_root=None, port=None, host=None, config_file=None, grpc_app_path=None,
                       rest_app_path=None, plugin_app_path=None, module_path=None, set_custom_config=True):
    # 1. Set a python path
    _set_python_path(package, source_root, module_path)

    # 2. Initialize config from command argument
    config.init_conf(
        package=package,
        port=port,
        host=host,
        grpc_app_path=grpc_app_path,
        rest_app_path=rest_app_path,
        plugin_app_path=plugin_app_path
    )

    if set_custom_config:
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
        skeleton = PLUGIN_SOURCES.get(source, source)
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


def _init_common_modules() -> None:
    # Enable logging configuration
    set_logger()

    # Set OTel Tracer and Metric
    set_tracer()

    # Connect all databases
    model.init_all()


if __name__ == '__main__':
    cli()
