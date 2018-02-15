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

You can also install directly from gitlab:

    make dist

This will create a jar under target/scala-2.11/flint-assembly-0.2.0-SNAPSHOT.jar

Running with PySpark
--------------------

You can use ts-flint with PySpark by:

    pyspark --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar

or

    >>> import os
    >>> import ts.flint
    >>> ts.flint.__file__[len(os.getcwd()):]
    '/ts/flint/__init__.py'

Running Tests
-------------

    spark-submit --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar myapp.py


Running in a notebook
---------------------

You can also run ts-flint from within a jupyter notebook.  First, create a virtualenv or conda environment containing pandas and jupyter.

    conda create -n flint  python=3.5 pandas notebook
    source activate flint

    py.test

To run tests against a deployment:

    FLINT_TEST_CODEBASE=/opt/ts/services/spark-2.2-ts-qa.ts_spark_2_2 py.test

To run tests against a specific Flint assembly jar (of version e.g. `0.4.0`):

    FLINT_TEST_JAR=flint-assembly-0.4.0-SNAPSHOT.jar py.test

You can get code coverage by running them under `coverage`:

    coverage run setup.py test
    coverage report

Instead of `coverage report`'s text output, you can get an HTML report
including branch coverage information:

    coverage run setup.py test
    coverage html
    (cd build/coverage/html; python -m http.server 8080)

Then visit http://localhost:8080.

* Note that this issue https://github.com/numpy/numpy/issues/8958 currently prevents Jupyter notebooks running under pyspark from importing the numpy module in python 3.6.  That's why "python=3.5" is specified above.

Make sure pyspark is in your PATH.
Then, from the flint project dir, start pyspark with the following options:

    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='$(hostname)' --NotebookApp.port=8888"
    pyspark --master=local --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar

Now, you can open the sample notebook.  Use the Jupyter interface to browse to python/samples/weather.ipnb.

Documentation
-------------

The Flint python bindings are documented at https://ts-flint.readthedocs.io/en/latest

Run tests
---------

    git tag 0.1.2
    git push origin 0.1.2

Once this tag exists, future package and documentation builds will
automatically get that version, and users can `pip install` using that
git tag from gitlab.

To run tests for the Python code see a separate [README](tests/README.md) file in the tests directory

Examples
--------

An example python notebook is available in the examples directory.  To try it out, start a jupyter notebook as described above, and then open [weather.ipynb](examples/weather.ipynb).

Bugs
----

Please report bugs to <tsos@twosigma.com>.
