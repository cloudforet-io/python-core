#
#   Copyright 2020 The SpaceONE Authors.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import os
from setuptools import setup, find_packages

setup(
    name='spaceone-core',
    version=os.environ.get('PACKAGE_VERSION'),
    description='Cloudforet Core Library',
    long_description='',
    url='https://cloudforet.io/',
    author='MEGAZONE Cloud Corp.',
    author_email='admin@spaceone.dev',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=[
        # grpc packages
        'protobuf==3.*',
        # 'grpcio',
        'grpcio-reflection',
        'google-api-core',
        'grpcio-health-checking',

        # fastapi packages
        'fastapi',
        'fastapi-utils',
        'uvicorn',

        # asyncio packages
        'asyncio',

        # data parser packages
        'PyYAML',
        'jsonschema',

        # scheduler packages
        'schedule',
        'scheduler-cron',

        # cache packages
        'redis',
        'cachetools',

        # crypto(jwt) packages
        'pycryptodome',
        'jwcrypto',
        'python-jose',

        # utils packages
        'python-dateutil',
        'python-consul',
        'dnspython',

        # HTTP packages
        'requests',

        # CLI packages
        'click',

        # model packages
        'pydantic',
        'mongoengine',

        # AWS packages
        'boto3',

        # test framework packages
        'unittest-xml-reporting',
        'factory-boy',
        'mongomock',

        # tracing packages
        'opentelemetry-api',
        'opentelemetry-sdk',
        'opentelemetry-exporter-otlp-proto-grpc',
        'opentelemetry-instrumentation-logging',
        'opentelemetry-exporter-prometheus'
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'spaceone = spaceone.core.command:cli',
        ]
    },
)
