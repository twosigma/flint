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

from pyspark.sql import DataFrame

from . import java
from . import utils


class TSDataFrameReader(object):
    '''Interface used to load a :class:`.TimeSeriesDataFrame`.
    Use :meth:`TSSparkContext.read` to access this.
    '''
    DEFAULT_TIME_COLUMN = "time"
    DEFAULT_UNIT = "ns"

    def __init__(self, flintContext):
        self._flintContext = flintContext
        self._sc = self._flintContext._sc
        self._sqlContext = self._flintContext._sqlContext
        self._jpkg = java.Packages(self._sc)

    def pandas(self, df, schema=None, *,
               is_sorted=True,
               time_column=DEFAULT_TIME_COLUMN,
               unit=DEFAULT_UNIT):
        '''Creates a :class:`.TimeSeriesDataFrame` from an existing
        |pandas_DataFrame|_. The |pandas_DataFrame|_ must be sorted on
        time column, otherwise user must specify is_sorted=False.

        :param pandas.DataFrame df: the |pandas_DataFrame|_ to convert
        :param bool is_sorted: Default True. Whether the input data is already
            sorted (if already sorted, the conversion will be faster)
        :param str time_column: Column name used to sort rows
            (default: :data:`DEFAULT_TIME_COLUMN`)
        :param str unit: Unit of time_column, can be (s,ms,us,ns)
            (default: :data:`DEFAULT_UNIT`)
        :return: a new :class:`TimeSeriesDataFrame` containing the
            data in ``df``

        '''
        from .dataframe import TimeSeriesDataFrame

        return TimeSeriesDataFrame._from_pandas(
            df, schema, self._flintContext._sqlContext,
            is_sorted=is_sorted)

    def _df_between(self, df, begin, end, time_column, unit):
        """Filter a Python dataframe to contain data between begin (inclusive) and end (exclusive)

        :return: :class:``pyspark.sql.DataFrame``
        """
        jdf = df._jdf
        junit = utils.junit(self._sc, unit)
        new_jdf = self._jpkg.TimeSeriesRDD.DFBetween(jdf, begin, end, junit, time_column)

        return DataFrame(new_jdf, self._sqlContext)

    def dataframe(self, df, begin=None, end=None, *,
                  is_sorted=True,
                  time_column=DEFAULT_TIME_COLUMN,
                  unit=DEFAULT_UNIT):
        """Creates a :class:`TimeSeriesDataFrame` from an existing
        :class:`pyspark.sql.DataFrame`. The :class:`pyspark.sql.DataFrame` must be
        sorted on time column, otherwise user must specify
        is_sorted=False.

        :param pyspark.sql.DataFrame df: the :class:`pyspark.sql.DataFrame`
            to convert
        :param bool is_sorted: Default True. Whether the input data is
            already sorted (if already sorted, the conversion will be
            faster)
        :param str begin: Optional. Inclusive. Supports most common
            date formats. Default timezone is UTC.
        :param str end: Optional. Exclusive. Supports mosat common
            date formats. Default timezone is UTC.
        :param str time_column: Column name used to sort rows (default:
            :data:`DEFAULT_TIME_COLUMN`)
        :param str unit: Unit of time_column, can be (s,ms,us,ns)
            (default: :data:`DEFAULT_UNIT`)
        :return: a new :class:`TimeSeriesDataFrame` containing the
            data in ``df``

        """
        from .dataframe import TimeSeriesDataFrame

        if begin is not None or end is not None:
            df = self._df_between(df, begin, end, time_column, unit)

        return TimeSeriesDataFrame._from_df(
            df,
            time_column=time_column,
            is_sorted=is_sorted,
            unit=unit)
