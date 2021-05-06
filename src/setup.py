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


from setuptools import setup, find_packages

with open('VERSION', 'r') as f:
    VERSION = f.read().strip()
    f.close()

setup(
    name='spaceone-core',
    version=VERSION,
    description='SpaceONE core library',
    long_description='',
    url='https://www.spaceone.dev/',
    author='MEGAZONE SpaceONE Team',
    author_email='admin@spaceone.dev',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=[
        # "grpcio",
        "grpcio-reflection",
        "google-api-core",
        "grpcio-health-checking",
        "PyYAML",
        "unittest-xml-reporting>=3.0.0",
        "pycryptodome==3.8.2",
        "schedule",
        "cachetools",
        "jwcrypto==0.7",
        "python-jose==3.1.0",
        "factory-boy",
        "jsonschema",
        "python-dateutil",
        "python-consul",
        "scheduler-cron",
        "click",
        "celery[redis]==4.4.7",
        'uvicorn',
        'fastapi',
        "python-consul"
    ],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'spaceone = spaceone.core.command:cli',
        ]
    },
)
