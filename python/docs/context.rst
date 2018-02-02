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
existing :class:`pandas.DataFrame` or :class:`pyspark.sql.DataFrame` to a
:class:`ts.flint.TimeSeriesDataFrame` in order to take
advantage of its time-aware functionality:

    >>> df1 = flintContext.read.pandas(pd.read_csv(path))
    >>> df2 = (flintContext.read
    ...        .option('isSorted', False)
    ...        .dataframe(sqlContext.read.parquet(hdfs_path)))



Writing temporary data to HDFS
------------------------------

You can materialize a :class:`pyspark.sql.DataFrame` to HDFS and read it
back later on, to save data between sessions, or to cache the result
of some preprocessing.

    >>> import getpass
    >>> filename = 'hdfs:///user/{}/filename.parquet'.format(getpass.getuser())
    >>> df.write.parquet(filename)

The `Apache Parquet`_ format is a good fit for most tabular data sets
that we work with in Flint.

.. _`Apache Parquet`: https://parquet.apache.org/

To read a sequence of Parquet files, use the :meth:`flintContext.read.parquet
<ts.flint.readwriter.TSDataFrameReader.parquet>` method.  This method assumes
the Parquet data is sorted by time. You can pass the 
``.option('isSorted', False)`` option to the reader if the underlying data is
not sorted on time:

    >>> ts_df1 = flintContext.read.parquet(hdfs_path)  # assumes sorted by time
    >>> ts_df2 = (flintContext.read
    ...           .option('isSorted', False)
    ...           .parquet(hdfs_path))  # this will sort by time before load

