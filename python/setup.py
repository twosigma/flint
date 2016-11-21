#
#       Copyright (c) 2016 Two Sigma Investments, LP
#       All Rights Reserved
#
#       THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF
#       Two Sigma Investments, LP.
#
#       The copyright notice above does not evidence any
#       actual or intended publication of such source code.
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
        'ts-elastic',
    ],
    tests_require=[
        'coverage',
        'numpy',
        'pandas',
        'ts-spark',
    ],
    test_suite='tests',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
