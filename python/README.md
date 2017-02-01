ts-flint - Time Series Library for PySpark
==========================================

ts-flint is a collection of modules related to time series analysis
for PySpark.

Building
----------------------

To build flint jar:

    sbt assembly

To build flint python egg:

    python setup.py bdist_egg


Building Documentation
----------------------

Docs live in `docs/` and can be built with `setup.py`:

    python setup.py build_sphinx
    (cd build/sphinx/html; python -m http.server 8080)


Running with PySpark
--------------------

You can use ts-flint with PySpark by:

spark-submit --jars target/scala-2.11/flint-assembly-0.3.1-SNAPSHOT.jar --py-files python/dist/ts_flint.egg ...


Bugs
----

Please report bugs to <tsos@twosigma.com>.
