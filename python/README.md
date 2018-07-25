<!--
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
-->
ts-flint - Time Series Library for PySpark
==========================================

ts-flint is a collection of modules related to time series analysis
for PySpark.

[![Documentation Status](https://readthedocs.org/projects/ts-flint/badge/?version=latest)](http://ts-flint.readthedocs.io/en/latest/?badge=latest)

Building
--------

You can build flint by running this in the top level of this repo:

    python setup.py install
    # -or-
    pip install .

You can also install directly from gitlab clone:

    make dist

This will create a jar under target/scala-2.11/flint-assembly-{VERSION}-SNAPSHOT.jar

Running with PySpark
--------------------

You can use ts-flint with PySpark by:

    pyspark --jars /path/to/flint-assembly-{VERSION}-SNAPSHOT.jar --py-files /path/to/flint-assembly-{VERSION}-SNAPSHOT.jar

or

    >>> import os
    >>> import ts.flint
    >>> ts.flint.__file__[len(os.getcwd()):]
    '/ts/flint/__init__.py'

Running in a notebook
---------------------

You can also run ts-flint from within a jupyter notebook.  First, create a virtualenv or conda environment containing pandas and jupyter.

    conda create -n flint  python=3.5 pandas notebook
    source activate flint

    py.test

Then visit http://localhost:8080.

Make sure pyspark is in your PATH.
Then, from the flint project dir, start pyspark with the following options:

    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='$(hostname)' --NotebookApp.port=8888"
    pyspark --master=local --jars /path/to/flint-assembly-{VERSION}-SNAPSHOT.jar --py-files /path/to/flint-assembly-{VERSION}-SNAPSHOT.jar

Documentation
-------------

The Flint python bindings are documented at https://ts-flint.readthedocs.io/en/latest

