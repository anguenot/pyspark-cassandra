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

from setuptools import setup, find_packages

requirements = [
]

test_requirements = ['bumpversion==0.5.3',
                     'wheel>=0.29.0',
                     'watchdog==0.8.3',
                     'flake8==2.6.2',
                     'tox==2.3.1',
                     'coverage==4.1',
                     'Sphinx==1.4.4'
                     ],

setup(
    name='pyspark_cassandra',
    maintainer='Julien Anguenot',
    maintainer_email='julien@anguenot.org',
    version='0.4.0',
    description='Utilities to assist in working with Cassandra and PySpark.',
    long_description="",
    url='https://github.com/anguenot/pyspark-cassandra',
    license='Apache License 2.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Other Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Utilities',
    ],
    tests_require=test_requirements
)
