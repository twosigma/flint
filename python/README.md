ts-flint - Time Series Library for PySpark
==========================================

ts-flint is a collection of modules related to time series analysis
for PySpark.

Building
----------------------

You can build flint by running:

    make dist

This will create a jar under target/scala-2.11/flint-assembly-0.2.0-SNAPSHOT.jar

Running with PySpark
--------------------

You can use ts-flint with PySpark by:

    pyspark --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar

or

    spark-submit --jars /path/to/flint-assembly-0.2.0-SNAPSHOT.jar --py-files /path/to/flint-assembly-0.2.0-SNAPSHOT.jar myapp.py

Bugs
----

Please report bugs to <tsos@twosigma.com>.
