#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from setuptools import setup, find_packages

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.read().splitlines()

readme_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'README.md')
if not os.path.exists(readme_path):
    readme_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'README.md')
if os.path.exists(readme_path):
    with open(readme_path, encoding='utf-8') as f:
        long_description = f.read()
else:
    long_description = ""

setup(
    name='pyspark_cassandra',
    maintainer='Julien Anguenot',
    maintainer_email='julien@anguenot.org',
    version='3.5.0',
    description='PySpark RDD support for Apache Cassandra via the '
                'Spark Cassandra Connector.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/anguenot/pyspark-cassandra',
    license='Apache License 2.0',
    python_requires='>=3.9',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'pyspark_cassandra': ['jars/*.jar'],
    },
    install_requires=requirements,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
)
