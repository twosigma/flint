#
#  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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


setup(
    name='ts-flint',
    description='Distributed time-series analysis on Spark',
    author='Leif Walsh',
    author_email='leif@twosigma.com',
    packages=['ts.flint'],
    setup_requires=[
    ],
    install_requires=[
    ],
    tests_require=[
        'coverage',
        'numpy',
        'pandas',
    ],
    test_suite='tests',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
