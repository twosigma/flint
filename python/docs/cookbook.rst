==========================
Flint Python Cookbook
==========================

This page is a grab-bag of useful code snippets for Flint Python.
We assume you've already read :doc:`ts-spark:getting-started` and can run Flint
code in a Python process or Jupyter Notebook.

:download:`Download the python notebook
<Flint Python Cookbook.ipynb>` to try code out without
copy-pasting!

.. contents::
.. currentmodule: ts.flint

Basic arithmetic on each row
----------------------------

Calculate logarithm of a column
```````````````````````````````

.. code-block:: py

   df = price.withColumn('logVolume', pyspark_fn.log(price.volume))

Raise a column to an exponent
`````````````````````````````

.. code-block:: py

   df = price.withColumn('squaredVolume', pyspark_fn.pow(price.volume, 2))

Calculate difference between two columns
````````````````````````````````````````

.. code-block:: py

   df = price.withColumn('priceChange', price.closePrice - price.openPrice)

Calculate percent change between two columns
````````````````````````````````````````````

.. code-block:: py

   @ts.flint.udf(DoubleType())
   def pricePercentChange(openPrice, closePrice):
       return ((closePrice - openPrice) / openPrice) if openPrice > 0 else None

   df = price.withColumn('pricePercentChange',
                pricePercentChange(price.openPrice, price.closePrice))

Get the first two characters of a column
````````````````````````````````````````

.. code-block:: py

   @ts.flint.udf(StringType())
   def gicsSector(gicsCode):
       return gicsCode[0:2]

   df = active_inst.withColumn('gicsSector',
                gicsSector(active_inst.gicsCode))


Filtering
---------

Select rows where the price went up
```````````````````````````````````

.. code-block:: py

   df = price.filter(price.closePrice > price.openPrice)

.. todo::

   The following paragraph is from the Scala cookbook but these
   functions are not part of the python API.

   The :meth:`TimeSeriesDataFrame.keepRows` and
   :meth:`TimeSeriesDataFrame.deleteRows` methods take a function from
   :class:`pyspark.sql.Row` to boolean as a filtering criteria.

Filter using a regular expression
`````````````````````````````````

.. code-block:: py

   df = active_inst.dropna(subset=['gicsCode'])
   df = df.filter(df.gicsCode.startswith('45'))

Remove all rows that don't have a value in a particular column
``````````````````````````````````````````````````````````````

.. code-block:: py

   df = active_inst.dropna(subset=['gicsCode'])


Joining
-------

Join trades to quotes
`````````````````````

.. code-block:: py

   df = trade.leftJoin(quote, tolerance='1min', key='tid')


Time-based Windowing
--------------------

Using history with :meth:`TimeSeriesDataFrame.addWindows`:

Exponential moving average
``````````````````````````
Exponential moving average over the last 10 days with a decay factor of 0.9 for IBM:

.. code-block:: py

   # Note that we can't pass non-columns to udfs so we wrap it in another method
   def EMA(decay):
       @ts.flint.udf(DoubleType())
       def _EMA(time, window):
               from pandas import Timedelta
               num = 0
               den = 0
               currentnanos = time
               for row in window:
                   rownanos = row.time
                   days_between = Timedelta(nanoseconds=(currentnanos - rownanos)).days
                   weight = pow(decay, days_between)
                   num += weight * row.closePrice
                   den += weight
               return (num/den) if den > 0 else 0
       return _EMA

   decay = 0.9

   df = price.addWindows(windows.past_absolute_time('10days'))
   df = df.withColumn('EMA', EMA(decay)(df.time, df.window_past_10days))

Moving average
``````````````

Moving average over the last two weeks for IBM:

.. code-block:: py

   @ts.flint.udf(DoubleType())
   def movingAverage(window):
       nrows = len(window)
       if nrows == 0:
           return 0
       return sum(row.closePrice for row in window) / nrows

   df = price.addWindows(windows.past_absolute_time('14days'))
   df = df.withColumn('movingAverage', movingAverage(df.window_past_14days))

Moving average over the last two weeks for all tids in ``u.ACTIVE_3000_US``:

.. code-block:: py

   @ts.flint.udf(DoubleType())
   def movingAverage(window):
       nrows = len(window)
       if nrows == 0:
           return 0
       return sum(row.closePrice for row in window) / nrows

   df = price.addWindows(windows.past_absolute_time('14days'), key='tid')
   df = df.withColumn('movingAverage', movingAverage(df.window_past_14days))


Cycles
------

:meth:`TimeSeriesDataFrame.addColumnsForCycle` can be used to
compute a new column based on all rows that share a timestamp.

Adding universe info
````````````````````

Add a column containing the number of instruments in the universe on
each day:

.. code-block:: py

   def universeSize(rows):
       size = len(rows)
       return {row:size for row in rows}

   df = active_price.addColumnsForCycle(
                {'universeSize': (IntegerType(), universeSize)})

Add a column containing the number of instruments that share a GICS
code with the current row on each day:

.. code-block:: py

   def universeSize(rows):
       size = len(rows)
       return {row:size for row in rows}

   df = active_inst.addColumnsForCycle(
                {'universeSize': (IntegerType(), universeSize)},
                key='gicsCode')

Add a column containing the number of instruments that share a GICS
sector with the current row on each day:

.. code-block:: py

   @ts.flint.udf(StringType())
   def gicsSector(gicsCode):
       return gicsCode[0:2] if gicsCode else ''

   def universeSize(rows):
       size = len(rows)
       return {row:size for row in rows}

   df = active_inst.withColumn('gicsSector', gicsSector(active_inst.gicsCode))
   df = df.addColumnsForCycle(
                {'universeSize': (IntegerType(), universeSize)},
                key='gicsSector')

Z-score
```````

Compute the Z-score across an interval:

.. code-block:: py

   import math

   def volumeZScore(rows):
       size = len(rows)
       if size <= 1:
           return {row:0 for row in rows}
       mean = sum(row.volume for row in rows) / size
       stddev = math.sqrt(sum((row.closePrice - mean)**2 for row in rows)) / (size - 1)
       return {row:(row.closePrice - mean)/stddev for row in rows}

   df = active_price.addColumnsForCycle(
                {'volumeZScore': (DoubleType(), volumeZScore)})

Ranking
```````

Add a column with rankings from 0.0 to 1.0 relative to other rows with
the same timestamp:

.. code-block:: py

   import scipy.stats as stats

   def rank_by(column):
       def rank(rows):
           return dict(zip(rows, stats.rankdata(row[column] for row in rows)))
       return rank

   df = active_price.addColumnsForCycle(
                {'r': (DoubleType(), rank_by('volume'))})


Intervalizing
-------------

Volume-weighted average price
`````````````````````````````

Volume-weighted average price for every 30 minute trading interval for
IBM:

.. code-block:: py

   @ts.flint.udf(DoubleType())
   def meanPrice(rows):
       weighted_sum = sum(row.tradePrice * row.tradeSize for row in rows)
       return weighted_sum / sum(row.tradeSize for row in rows)

   df = trade.groupByInterval(intervals)
   df = df.withColumn('meanPrice', meanPrice(df.rows))
   df = df.drop('rows')


Aggregating
-----------

Average daily volume
````````````````````
Average daily volume for all tids in ``u.ACTIVE_3000_US``:

.. code-block:: py

   df = active_price.summarize(summarizers.nth_moment('volume', 1), key='tid')
