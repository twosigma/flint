======================================================
 Manipulating and Analyzing Data
======================================================

.. contents::

.. _ts_flint_TimeSeriesDataFrame:

TimeSeriesDataFrame
-------------------

A :class:`ts.flint.TimeSeriesDataFrame` is a time-series
aware version of a :class:`pyspark.sql.DataFrame`.  Being time-series aware, it
has optimized versions of some operations like joins, and also some
new features like temporal joins.

Like a normal :class:`pyspark.sql.DataFrame`, a
:class:`ts.flint.TimeSeriesDataFrame` is a collection of
:class:`pyspark.sql.Row` objects, but which always must have a ``time``
column.  The rows are always sorted by ``time``, and the API affords
special join/aggregation operations that take advantage of that
temporal locality.


.. _dataframes_and_immutability:

Note on Dataframes and Immutability
```````````````````````````````````

.. note::

   All the methods on :class:`ts.flint.TimeSeriesDataFrame`
   that appear to "add" something do not modify the target of the
   method.  Instead, they return a new
   :class:`ts.flint.TimeSeriesDataFrame` which shares most of
   its data with the original one.  In fact, this is true of all Spark
   transformations.

   Therefore, don't discard the results of one of these calls, assign
   it to a different variable.  That way, you can always go back and
   refer to something before you transformed it.

   **Bad Example:**

   This completely discards the results of these operations.  You'll
   simply get the wrong data.

       >>> df.select('time', 'tid', 'openPrice', 'closePrice')
       >>> df.addWindows(windows.past_absolute_time('5d'), key='tid')

   **Okay Example:**

   This is going to compute the right thing, but if you decide you
   want to try something without that window, you have to clear
   everything and start over.  In addition, if you run a notebook cell
   like this multiple times, you'll add multiple layers of the same
   transformations to your dataframes.

       >>> df = df.select('time', 'tid', 'openPrice', 'closePrice')
       >>> df = df.addWindows(windows.past_absolute_time('5d'), key='tid')

   **Good Example:**

   This is the best way to work with
   :class:`ts.flint.TimeSeriesDataFrame` and
   :class:`pyspark.sql.DataFrame`.  You can run this cell any number of
   times and you'll always get the same thing.  Furthermore, you can
   now chain multiple things off price_df later, without re-reading
   raw_df.

       >>> price_df = raw_df.select('time', 'tid', 'openPrice', 'closePrice')
       >>> window_df_7d = price_df.addWindows(windows.past_absolute_time('7d'), key='tid')
       >>> window_df_14d = price_df.addWindows(windows.past_absolute_time('14d'), key='tid')


Summarizers
-----------

In Flint, we specify the summarizations we want
to do in terms of answering two orthogonal questions:

- What aggregation/summarization function do you want to apply to a
  given set of rows?
- Which rows do you want to aggregate/summarize together?

The functions in :mod:`ts.flint.summarizers` are the way to
specify what functions you want to apply.  These are suitable for
passing to functions like
:meth:`ts.flint.TimeSeriesDataFrame.summarize`, and
:meth:`ts.flint.TimeSeriesDataFrame.summarizeCycles`,
which answer the second question, which rows should be aggregated
together.

The Flint summarizer library augments the analysis capabilities of
the normal :class:`pyspark.sql.DataFrame` such as those available in
:mod:`pyspark.sql.functions`.
