#
#  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from setuptools import setup

import versioneer

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('requirements-dev.txt') as f:
    requirements_dev = f.read().splitlines()

setup(
    name='ts-flint',
    description='Distributed time-series analysis on Spark',
    author='Li Jin, Leif Walsh',
    author_email='ljin@twosigma.com, leif@twosigma.com',
    packages=['ts.flint'],
    setup_requires=[
    ],
    install_requires=requirements,
    tests_require=requirements_dev,
    test_suite='tests',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
