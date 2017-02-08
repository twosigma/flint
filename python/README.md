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

You can also run ts-flint from within a jupyter notebook.  First, create a virtualenv or conda environment containing pandas and jupyter:

    conda create -n flint python pandas notebook
    source activate flint
    
Then, start pyspark with the following options:

    export PYSPARK_DRIVER_PYTHON=jupyter
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=False --NotebookApp.ip='$(hostname)' --NotebookApp.port=8888"
    bin/pyspark --master=local --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar

Documentation
-------------

The Flint python bindings are documented at https://ts-flint.readthedocs.io/en/latest

Bugs
----

Please report bugs to <tsos@twosigma.com>.
