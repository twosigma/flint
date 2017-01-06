================================================
 Reading and Writing Data
================================================

.. contents::

A :class:`ts.flint.FlintContext` is similar to a :class:`pyspark.sql.SQLContext` in
that it is the main entry point to reading Two Sigma data sources into
a :class:`ts.flint.TimeSeriesDataFrame`.


Converting other data sources to TimeSeriesDataFrame
----------------------------------------------------

You can also use a :class:`ts.flint.FlintContext` to convert an
existing |pandas_DataFrame|_ or :class:`pyspark.sql.DataFrame` to a
:class:`ts.flint.TimeSeriesDataFrame` in order to take
advantage of its time-aware functionality:

    >>> df1 = flintContext.read.pandas(pd.read_csv(path))
    >>> df2 = flintContext.read.dataframe(sqlContext.read.parquet(hdfs_path))


Writing temporary data to HDFS
------------------------------

You can materialize a :class:`pyspark.sql.DataFrame` to HDFS and read it
back later on, to save data between sessions, or to cache the result
of some preprocessing.

    >>> import getpass
    >>> filename = 'hdfs:///user/{}/filename.parquet'.format(getpass.getuser())
    >>> df.write.parquet(filename)

The `Apache Parquet`_ format is a good fit for most tabular data sets
we work with in Flint.

.. _`Apache Parquet`: https://parquet.apache.org/

To read a Parquet file back into Flint, use the normal
:meth:`sqlContext.read.parquet <pyspark.sql.DataFrameReader.parquet>`
function to read it in as a :class:`pyspark.sql.DataFrame`, then use
:meth:`flintContext.read.dataframe
<ts.flint.TSDataFrameReader.dataframe>` to convert it to a
:class:`ts.flint.TimeSeriesDataFrame`.  If you know you originally
wrote a :class:`ts.flint.TimeSeriesDataFrame`, then it will have been
written in time order, and you can pass ``is_sorted=True`` to avoid
doing an unnecessary sort operation:

    >>> vanilla_df = sqlContext.read.parquet(filename)
    >>> ts_df = flintContext.read.dataframe(vanilla_df, is_sorted=True)
