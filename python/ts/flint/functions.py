#
#  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import functools

import py4j
import pyspark
from pyspark import SparkContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType, DataType

from .udf import _tuple_to_struct

__all__ = ['udf']

ATTRIBUTE_REFERENCE_CLS = 'org.apache.spark.sql.catalyst.expressions.AttributeReference'

class FlintUserDefinedFunction(UserDefinedFunction):
    # A subclass of UserDefinedFunction with modification to the
    # __call__ methods to support Flint functions.

    def __init__(self, func, returnType, name=None):
        super(FlintUserDefinedFunction, self).__init__(func, returnType, name)

    def __call__(self, *cols):
        # pyspark UserDefinedFunction takes only `Column`. This
        # function take both `Column` and `DataFrame`. When a
        # `DataFrame`, we take all columns from the `DataFrame`.  We
        # also store the column indices of input so we can pass the
        # correct args to the user function
        sc = SparkContext._active_spark_context

        pyspark_cols = []
        column_indices = []
        for col in cols:
            if isinstance(col, pyspark.sql.DataFrame):
                df = col
                column_indices.append(df.columns)
                pyspark_cols += [df[c] for c in df.columns]
            elif isinstance(col, pyspark.sql.Column):
                jexpr = col._jc.expr()
                # If the column is not an attribute reference, we
                # cannot use it's name for column indices. Instead we
                # set column indices to be None for non attribute
                # reference and deal with it later when column indices
                # are used.
                if py4j.java_gateway.is_instance_of(
                        sc._gateway,
                        jexpr,
                        ATTRIBUTE_REFERENCE_CLS):
                    column_indices.append(jexpr.name())
                else:
                    column_indices.append(None)

                pyspark_cols.append(col)

        udf_col = super(FlintUserDefinedFunction, self).__call__(*pyspark_cols)
        udf_col.column_indices = column_indices

        return udf_col

def udf(f=None, returnType=StringType()):
    # Modified from
    # https://github.com/apache/spark/blob/master/python/pyspark/sql/functions.py
    # to add additional supports for Flint

    '''Creates a column expression representing a user defined
    function (UDF).

    This behaves the same as :meth:`~pyspark.sql.functions.udf` when
    used with a PySpark function, such as
    :meth:`~pyspark.sql.DataFrame.withColumn`.

    This can also be used with Flint functions, such as
    :meth:`ts.flint.TimeSeriesDataFrame.summarizeCycles`.

    This can be used to define a row user define function or
    a columnar user define function:

    1. Row udf

       A row udf takes one or more scalar values for each
       row, and returns a scalar value for that row.

       A :class:`~pyspark.sql.Column` object is needed to specifiy
       the input, for instance, ``df['v']``.

       Example:

           >>> @udf(DoubleType())
           >>> def plus_one(v):
           ...     return v+1
           >>> col = plus_one(df['v'])

    2. Columnar udf

       A columnar udf takes one or more :class:`pandas.Series` or
       :class:`pandas.DataFrame` as input, and returns either a scalar
       value or a :class:`pandas.Series` as output.

       If the user function takes :class:`pandas.Series`, a
       :class:`~pyspark.sql.Column` is needed to specify the input,
       for instance, ``df['v']``.

       If the user function takes a :class:`pandas.DataFrame`, a
       :class:`~pyspark.sql.DataFrame` is needed to specify the input,
       for instance, ``df[['v', 'w']]``.

       Example:

       Takes :class:`pandas.Series`, returns a scalar

           >>> @udf(DoubleType())
           >>> def weighted_mean(v, w):
           ...     return numpy.average(v, weights=w)
           >>> col = weighted_mean(df['v'], df['w'])

       Takes a :class:`pandas.DataFrame`, returns a scalar

           >>> @udf(DoubleType())
           >>> def weighted_mean(df):
           ...     return numpy.average(df.v, weighted=df.w)
           >>> col = weighted_mean(df[['v', 'w']])

       Takes a :class:`pandas.Series`, returns a
          :class:`pandas.Series`

           >>> @udf(DoubleType())
           >>> def percent_rank(v):
           ...     return v.rank(pct=True)
           >>> col = percent_rank(df['v'])

       Different functions take different types of udf. For instance,

       * :meth:`pyspark.sql.DataFrame.withColumn` takes a row udf
       * :meth:`ts.flint.TimeSeriesDataFrame.summarizeCycles` takes a
         columnar udf that returns a scalar value.

       .. seealso::
       :meth:`ts.flint.TimeSeriesDataFrame.summarizeCycles`
    '''
    def _udf(f, returnType=StringType()):
        return FlintUserDefinedFunction(f, returnType)

    # decorator @udf, @udf(), @udf(dataType()) or @udf((dataType(), dataType()))
    if f is None or isinstance(f, (str, tuple, DataType)):
        # If DataType has been passed as a positional argument
        # for decorator use it as a returnType
        return_type = f or returnType
        return_type = _tuple_to_struct(return_type)
        return functools.partial(_udf, returnType=return_type)
    else:
        return_type = _tuple_to_struct(returnType)
        return _udf(f=f, returnType=return_type)
