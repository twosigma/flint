===========
 Reference
===========

ts.flint
--------

FlintContext
````````````
.. currentmodule:: ts.flint

.. autoclass:: ts.flint.FlintContext
   :members: read

.. autoclass:: ts.flint.readwriter.TSDataFrameReader
   :members:

TimeSeriesDataFrame
```````````````````

.. currentmodule:: ts.flint

.. autoclass:: TimeSeriesDataFrame
   :members:

   **Inherited members:**

   .. method:: cache()

      Same as :meth:`pyspark.sql.DataFrame.cache`

   .. method:: collect()

      Same as :meth:`pyspark.sql.DataFrame.collect`

   .. method:: drop(col)

      Same as :meth:`pyspark.sql.DataFrame.drop`

   .. method:: dropna(col)

      Same as :meth:`pyspark.sql.DataFrame.dropna`

   .. method:: filter(col)

      Same as :meth:`pyspark.sql.DataFrame.filter`

   .. method:: persist(storageLevel)

      Same as :meth:`pyspark.sql.DataFrame.persist`

   .. method:: select(*cols)

      Select columns from an existing :class:`TimeSeriesDataFrame`

      .. note::

         Column names to be selected must contain "time"

      Example:

      .. code-block:: python

         openPrice = price.select("time", "tid", "openPrice")

      :param cols: list of column names
      :type cols: list(str)
      :returns: a new :class:`TimeSeriesDataFrame` with the selected columns
      :rtype: :class:`TimeSeriesDataFrame`

   .. method:: unpersist(blocking)

      Same as :meth:`pyspark.sql.DataFrame.unpersist`

   .. method:: withColumn(colName, col)

      Adds a column or replaces the existing column that has the same
      name. This method invokes
      :meth:`pyspark.sql.DataFrame.withColumn`, but only allows
      :class:`pyspark.sql.Column` expressions that preserve
      order. Currently, only a subset of column expressions under
      :mod:`pyspark.sql.functions` are supported.

      Supported expressions:

      Arithmetic expression:
               +,-,*,/,log,log2,log10,log1p,pow,exp,expm1,sqrt,abs,rand,randn,rint,round,ceil,signum,factorial
      String expression:
               lower,upper,ltrim,rtrim,trim,lpad,rpad,reverse,split,substring,substring_index,concat,concat_ws
               conv,base64,format_number,format_string,hex,translate
      Condition expression:
               when, nanvl
      Boolean expression:
               isnan, isnull, >, <, ==, >=, <=

      Example:

          >>> priceWithNewCol = price.withColumn("newCol",
          ...             (price.closePrice + price.openPrice) / 2)

          >>> priceWithNewCol = price.withColumn("newCol",
          ...             when(price.closePrice > price.openPrice, 0).otherwise(1))

      If these column expressions don't do the thing you want, you can
      use :meth:`pyspark.sql.functions.udf` (user defined function, or
      UDF).

      .. note::

         UDFs are much slower than column expressions and you should
         ONLY use UDFs when the computation cannot expressed using
         column expressions.

      Example:

          >>> @ts.flint.udf(DoubleType())
          ... def movingAverage(window):
          ...     nrows = len(window)
          ...     if nrows == 0:
          ...         return 0.0
          ...     return sum(row.closePrice for row in window) / nrows
          ...
          >>> priceWithMA = (price
          ...                .addWindows(windows.past_absolute_time("14days"), "tid")
          ...                .withColumn("movingAverage", movingAverage(col("window"))))

      :param colName: name of the new column
      :type colName: str
      :param col: column expression to compute values in the new column
      :type col: :class:`pyspark.sql.Column`
      :returns: a new :class:`TimeSeriesDataFrame` with the new column
      :rtype: :class:`TimeSeriesDataFrame`

   .. method:: withColumnRenamed(existing, new)

      Same as :meth:`pyspark.sql.DataFrame.withColumnRenamed`

   **Time-series specific members:**


Summarizers
```````````

.. currentmodule:: ts.flint.summarizers

.. automodule:: ts.flint.summarizers
   :members:


Windows
```````

.. currentmodule:: ts.flint.windows

.. automodule:: ts.flint.windows
   :members:

Clocks
``````

.. currentmodule:: ts.flint.clocks

.. automodule:: ts.flint.clocks
   :members:
