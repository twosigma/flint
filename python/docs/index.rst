.. ts-flint documentation master file, created by
   sphinx-quickstart on Mon Nov 21 16:51:56 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ts-flint - Time Series Library for PySpark
==========================================

ts-flint is a collection of modules related to time series analysis
for PySpark.


Reading Data with FlintContext
------------------------------

:doc:`context` shows how to read data into a
:class:`ts.flint.TimeSeriesDataFrame`, which provides additional
time-series aware functionality.

    >>> prices = flintContext.read.parquet(..., begin, end)


Manipulating and Analyzing Data
-------------------------------

:doc:`flint` describes the structure of
:class:`ts.flint.TimeSeriesDataFrame`, which is a time-series
aware version of a :class:`pyspark.sql.DataFrame`.  Being time-series aware, it
has optimized versions of some operations like joins, and also some
new features like temporal joins.
:mod:`ts.flint.summarizers` contains aggregation functions like
EMAs.

    >>> events.leftJoin(returns, tolerance='5d', key='tid')


Contents:

.. toctree::
   :maxdepth: 2

   context
   flint
   cookbook
   reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
