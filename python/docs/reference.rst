===========
 Reference
===========

ts.flint
--------

TimeSeriesDataFrame
```````````````````

.. currentmodule:: ts.flint

.. autoclass:: TimeSeriesDataFrame
   :members:

   **Inherited members:**

   .. method:: cache()

      Same as |pyspark_sql_DataFrame_cache|_

   .. method:: collect()

      Same as |pyspark_sql_DataFrame_collect|_

   .. method:: drop(col)

      Same as |pyspark_sql_DataFrame_drop|_

   .. method:: dropna(col)

      Same as |pyspark_sql_DataFrame_dropna|_

   .. method:: filter(col)

      Same as |pyspark_sql_DataFrame_filter|_

   .. method:: persist(storageLevel)

      Same as |pyspark_sql_DataFrame_persist|_

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

      Same as |pyspark_sql_DataFrame_unpersist|_

   .. method:: withColumn(colName, col)

      Adds a column or replaces the existing column that has the same
      name. This method invokes |pyspark_sql_DataFrame_withColumn|_,
      but only allows |pyspark_sql_Column|_ expressions that preserve
      order. Currently, only a subset of column expressions under
      |pyspark_sql_functions|_ are supported.

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
      use |pyspark_sql_functions_udf|_ (user defined function, or
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
      :type col: |pyspark_sql_Column|_
      :returns: a new :class:`TimeSeriesDataFrame` with the new column
      :rtype: :class:`TimeSeriesDataFrame`

   .. method:: withColumnRenamed(existing, new)

      Same as |pyspark_sql_DataFrame_withColumnRenamed|_

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
