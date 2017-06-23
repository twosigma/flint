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

    make dist

This will create a jar under target/scala-2.11/flint-assembly-0.2.0-SNAPSHOT.jar

Running with PySpark
--------------------

You can use ts-flint with PySpark by:

    pyspark --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar

or

    spark-submit --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar myapp.py

Running in a notebook
---------------------

You can also run ts-flint from within a jupyter notebook.  First, create a virtualenv or conda environment containing pandas and jupyter.

    conda create -n flint  python=3.5 pandas notebook
    source activate flint

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

To run tests for the Python code see a separate [README](tests/README.md) file in the tests directory

Examples
--------

An example python notebook is available in the examples directory.  To try it out, start a jupyter notebook as described above, and then open [weather.ipynb](examples/weather.ipynb).

Bugs
----

Please report bugs to <tsos@twosigma.com>.
