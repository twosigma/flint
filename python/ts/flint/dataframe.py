#
#  Copyright 2015 TWO SIGMA OPEN SOURCE, LLC
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

import collections
import functools
import inspect
import json
import types

import pyspark
import pyspark.sql
import pyspark.sql.types as pyspark_types
import pandas as pd

from . import java
from . import utils
from . import summarizers

__all__ = ['TimeSeriesDataFrame']

_ORDER_PRESERVING_METHODS = [
    "cache",
    "drop",
    "dropna",
    "fill",
    "fillna",
    "filter",
    "persist",
    "replace",
    "sample",
    "sampleBy",
    "select",
    "toDF",
    "unpersist",
    "withColumn",
    "withColumnRenamed"
]

TimeSeriesRDDPartInfo = collections.namedtuple('TimeSeriesRDDPartInfo', ['jdeps', 'jrange_splits'])

class TimeSeriesDataFrame(pyspark.sql.DataFrame):
    '''A :class:`pyspark.sql.DataFrame` backed by time-ordered rows, with
    additional time-series functionality.

    A :class:`TimeSeriesDataFrame` supports a subset of
    :class:`pyspark.sql.DataFrame` operations: :meth:`cache`, :meth:`count`,
    :meth:`drop`, :meth:`dropna`, :meth:`filter`, :meth:`persist`,
    :meth:`select`, :meth:`unpersist`, :meth:`withColumn`,
    :meth:`withColumnRenamed`

    as well as time series operations:

    :meth:`leftJoin`, :meth:`futureLeftJoin`
        time-aware ("asof") joins

    :meth:`addWindows`
        time-aware windowing operations, in concert with :mod:`.windows`

    :meth:`addColumnsForCycle`, :meth:`groupByCycle`
        processing rows with the same timestamp

    :meth:`groupByInterval`
        processing rows within the same interval

    :meth:`summarize`, :meth:`addSummaryColumns`, :meth:`summarizeCycles`, :meth:`summarizeIntervals`, :meth:`summarizeWindows`
        data summarization, in concert with :mod:`.summarizers`.

    A :class:`TimeSeriesDataFrame` can be created by reading a Two
    Sigma URI with :meth:`.TSDataFrameReader.uri`, or from a pandas or
    Spark DataFrame.

    .. warning::

       Pay special attention to :ref:`dataframes_and_immutability`.

    .. seealso::

       Class :class:`ts.flint.FlintContext`
          Entry point for reading data in to a
          :class:`TimeSeriesDataFrame`.

       Class :class:`pyspark.sql.DataFrame`
          A :class:`TimeSeriesDataFrame` also has most of the
          functionality of a normal PySpark DataFrame.

    '''

    DEFAULT_TIME_COLUMN = "time"
    '''The name of the column assumed to contain timestamps, and used for ordering rows.'''

    DEFAULT_UNIT = "ns"
    '''The units of the timestamps present in :attr:`DEFAULT_TIME_COLUMN`.

    Acceptable values are: ``'s'``, ``'ms'``, ``'us'``, ``'ns'``.

    '''

    def __init__(self, df, sql_ctx, *, time_column=DEFAULT_TIME_COLUMN, is_sorted=True, unit=DEFAULT_UNIT, tsrdd_part_info=None):
        self._time_column = time_column
        self._is_sorted = is_sorted
        self._tsrdd_part_info = tsrdd_part_info

        self._jdf = df._jdf
        self._lazy_tsrdd = None

        super().__init__(self._jdf, sql_ctx)

        if tsrdd_part_info and not is_sorted:
            raise ValueError("Cannot take partition information for unsorted df")

        self._junit = utils.junit(self._sc, unit) if isinstance(unit,str) else unit
        self._jpkg = java.Packages(self._sc)

    @property
    def timeSeriesRDD(self):
        """Returns a Scala TimeSeriesRDD object

        :returns: :class:`py4j.java_gateway.JavaObject` (com.twosigma.flint.timeseries.TimeSeriesRDD)
        """
        if not self._lazy_tsrdd:
            if not self._tsrdd_part_info:
                # This will scan ranges
                self._lazy_tsrdd = self._jpkg.TimeSeriesRDD.fromDF(
                    self._jdf, self._is_sorted, self._junit, self._time_column)
                self._tsrdd_part_info = TimeSeriesDataFrame._get_tsrdd_part_info(self._lazy_tsrdd)
            else:
                self._lazy_tsrdd = self._jpkg.TimeSeriesRDD.fromDFUnSafe(
                    self._jdf, self._junit, self._time_column,
                    self._tsrdd_part_info.jdeps, self._tsrdd_part_info.jrange_splits)

        return self._lazy_tsrdd

    @staticmethod
    def _wrap_df_method(name, method):
        """
        Wraps a DataFrame function:
        (a)
        If the DataFrame function returns a DataFrame, the wrapped
        function returns a TimeSeriesDataFrame instead. Depending
        whether the function is partition preserving, the resulting
        TimeSeriesDataFrame might be unsorted or without partition
        information. If the Dataframe function doesn't return a
        Dataframe, it remains the same.

        (b)
        Add instrumentation

        NOTE: This function should only be called inside
        :meth:`_override_df_methods`
        """
        @functools.wraps(method)
        def _new_method(self, *args, **kwargs):
            return_value = method(self, *args, **kwargs)
            if not isinstance(return_value, pyspark.sql.DataFrame):
                return return_value

            df = return_value

            tsdf_args = {
                "df": df,
                "sql_ctx": df.sql_ctx,
                "time_column": self._time_column,
                "unit": self._junit
            }

            # withColumn sql.functions don't guarantee partitioning order
            if name is 'withColumn':
                # Get col argument from withColumn(colName, col)
                col = args[1]
                tsdf_args['is_sorted'] = self._is_sorted and self._jpkg.ColumnWhitelist.preservesOrdering(col._jc)
            else:
                tsdf_args['is_sorted'] = self._is_sorted and name in _ORDER_PRESERVING_METHODS

            if tsdf_args['is_sorted'] and self._tsrdd_part_info:
                tsdf_args['tsrdd_part_info'] = self._tsrdd_part_info

            return TimeSeriesDataFrame(**tsdf_args)
        return _new_method

    @staticmethod
    def _override_df_methods():
        """Overrides :class:`DataFrame` methods and wraps returned :class:`DataFrame` objects
        as :class:`TimeSeriesDataFrame` objects
        """
        dfmethods = inspect.getmembers(pyspark.sql.DataFrame)
        tsdfmethods = inspect.getmembers(TimeSeriesDataFrame)

        # Only replace non-private methods and methods nor overriden in TimeSeriesDataFrame
        for name, method in dfmethods:
            if not name.startswith('_') and name not in tsdfmethods and callable(method):
                setattr(TimeSeriesDataFrame, name, TimeSeriesDataFrame._wrap_df_method(name, method))

    def _call_dual_function(self, function, *args, **kwargs):
        if self._jdf:
            return pyspark.sql.DataFrame.__getattr__(function)(self._jdf, *args, **kwargs)
        return self._lazy_tsrdd.__getattr__(function)(*args, **kwargs)

    def count(self):
        '''Counts the number of rows in the dataframe

        :returns: the number of rows in the dataframe
        :rtype: int

        '''
        return self._call_dual_function('count')

    @staticmethod
    def _get_deps(tsrdd):
        """
        Helper function.
        Get scala dependencies from a TimeSeriesRDD
        """
        return tsrdd.orderedRdd().getDependencies()

    @staticmethod
    def _get_range_splits(tsrdd):
        """
        Helper function.
        Get scala rangeSplits from a TimeSeriesRDD
        """
        return tsrdd.orderedRdd().rangeSplits()

    @staticmethod
    def _get_tsrdd_part_info(tsrdd):
        """
        Helper function.
        Get partitions info (dependencies and rangeSplits) from a TimeSeriesRDD
        """
        return TimeSeriesRDDPartInfo(TimeSeriesDataFrame._get_deps(tsrdd),
                                     TimeSeriesDataFrame._get_range_splits(tsrdd))

    @staticmethod
    def _from_df(df, *, time_column, is_sorted, unit):
        return TimeSeriesDataFrame(df,
                                   df.sql_ctx,
                                   time_column=time_column,
                                   is_sorted=is_sorted,
                                   unit=unit)

    @staticmethod
    def _from_pandas(df, sql_ctx, *, time_column, is_sorted, unit):
        df = sql_ctx.createDataFrame(df)
        return TimeSeriesDataFrame(df,
                                   sql_ctx,
                                   time_column=time_column,
                                   is_sorted=is_sorted,
                                   unit=unit)

    def _timedelta_ns(self, varname, timedelta, *, default=None):
        """Transforms pandas.Timedelta to a ns string with appropriate checks

        :param varname: str
        :param timedelta: ``pandas.Timedelta``, str formattable by ``pandas.Timedelta``, or None
        :param default: Optional ``pandas.Timedelta`` timedelta will default to
        :returns: A string with the format "Xns" where X is the nanoseconds in timedelta or default
        """
        if timedelta is None:
            timedelta = default
        if isinstance(timedelta, str):
            timedelta = pd.Timedelta(timedelta)
        if not isinstance(timedelta, pd.Timedelta):
            raise Exception("{} should be a pandas.Timedelta object or string formattable pandas.Timedelta".format(varname))
        return '{}ns'.format(int(timedelta.total_seconds()*1e9))

    @staticmethod
    def _from_alf(sql_ctx, tsuri, begin, end, *, num_partitions=None, requests_per_partition=None, timeout=None):
        sc = sql_ctx._sc
        jpkg = java.Packages(sc)
        if not num_partitions:
            num_partitions = sc.defaultParallelism
        if not requests_per_partition:
            requests_per_partition = jpkg.alf.defaultAlfRequestsPerPartition()
        if not timeout:
            timeout = jpkg.WaiterClient.DEFAULT_TIMEOUT()
        # This should not be needed but unfornately alf.timeSeriesRDD requires a timeColumn
        timeColumn = "time"
        # Convert timeout to millis
        if isinstance(timeout, pd.Timedelta):
            timeout = int(timeout.total_seconds() * 1000)
        tsrdd = jpkg.alf.timeSeriesRDD(utils.jsc(sc), tsuri,
                                       begin, end, timeColumn,
                                       num_partitions, requests_per_partition,
                                       jpkg.alf.defaultWaiterConfig(timeout))
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, sql_ctx)

    @staticmethod
    def _from_tsrdd(tsrdd, sql_ctx):
        """Returns a :class:`TimeSeriesDataFrame` from a Scala ``TimeSeriesRDD``

        :param tsrdd: :class:`py4j.java_gateway.JavaObject` (com.twosigma.flint.timeseries.TimeSeriesRDD)
        :param sql_ctx: pyspark.sql.SQLContext
        :param time_column: defaults to ``DEFAULT_TIME_COLUMN``
        :param unit: Unit of time_column, can be (s,ms,us,ns) (default: ``DEFAULT_UNIT``)
        :returns: a new :class:`TimeSeriesDataFrame` from the given Scala ``TimeSeriesRDD``
        """
        sc = sql_ctx._sc
        df = pyspark.sql.DataFrame(tsrdd.toDF(), sql_ctx)

        # TODO: Change ValueError to FlintError or FlintInternalError
        # because this isn't something we expect user to deal with
        if tsrdd.orderedRdd().partitions()[0].index() != 0:
            raise ValueError("Cannot take a tsrdd whose partition index doesn't start with 0")

        # MCA-187
        # In the general case, if we create a tsdf from a regular df,
        # we run the normalization procedure and change
        # partitioning. However, here because we are creating a a tsdf
        # from a tsrdd, we already know the time ranges of partitions
        # and can avoid normalization.
        jrange_splits = TimeSeriesDataFrame._get_tsrdd_part_info(tsrdd).jrange_splits


        # We can only reuse dependency from tsrdd A to create a tsdf B
        # iff:
        # (1) We know tsrdd A is part of a tsdf A and
        # (2) tsdf A -> tsdf B is partition preserving
        # They don't hold here. tsrdd A -> tsdf B -> tsrdd B all have
        # one to one dependency, so we explictly create one.

        # df.rdd is just a place holder. It doesn't matter which rdd
        # we use to create the dependency here, because fromDFUnsafe
        # will always recreate a dependency with the correct rdd.
        jdep = sc._jvm.org.apache.spark.OneToOneDependency(df.rdd._jrdd.rdd())
        jdeps = utils.list_to_seq(sc, [jdep])
        tsrdd_part_info = TimeSeriesRDDPartInfo(jdeps, jrange_splits)

        return TimeSeriesDataFrame(df,
                                   sql_ctx,
                                   tsrdd_part_info=tsrdd_part_info)

    def addColumnsForCycle(self, columns, *, key=None):
        """
        Adds columns by aggregating rows with the same timestamp (and
        optionally, key), and applying a function to each such set of
        rows.  The added column's values are the return values of that
        function.

        The columns specified need a name, a
        :class:`pyspark.sql.types.DataType`, and a function to apply.  The
        function should accept a ``list`` of rows and return a
        ``dict`` from row to computed value.

        Example:

            >>> import math
            >>> from pyspark.sql.types import DoubleType
            >>> def volumeZScore(rows):
            ...     size = len(rows)
            ...     if size <= 1:
            ...         return {row:0 for row in rows}
            ...     mean = sum(row.volume for row in rows) / size
            ...     stddev = math.sqrt(sum((row.closePrice - mean)**2 for row in rows)) / (size - 1)
            ...     return {row:(row.closePrice - mean)/stddev for row in rows}
            ...
            >>> columns = {'volumeZScore': (DoubleType(), volumeZScore)}
            >>> df = active_price.addColumnsForCycle(columns)

        :param columns: a ``dict`` mapping each column name to a pair
            of :class:`pyspark.sql.types.DataType` and the function to
            compute that column, i.e. ``(pyspark.sql.types.DataType,
            callable)``
        :type columns: dict
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with the columns added
        :rtype: :class:`TimeSeriesDataFrame`
        """
        # Need to make a new StructType to prevent from modifying the original schema object
        schema = pyspark_types.StructType.fromJson(self.schema.jsonValue())
        tsdf = self.groupByCycle(key)
        # Don't pickle the whole schema, just the names for the lambda
        schema_names = list(schema.names)

        def flatmap_fn():
            def _(orig_row):
                orig_rows = orig_row.rows
                new_rows = [list(row) for row in orig_rows]
                for column, (datatype, fn) in columns.items():
                    fn_rows = fn(orig_rows)
                    for i,orig_row in enumerate(orig_rows):
                        new_rows[i].append(fn_rows[orig_row])

                NewRow = pyspark_types.Row(*schema_names)
                return [NewRow(*row) for row in new_rows]
            return _

        for column, (datatype, fn) in columns.items():
            schema.add(column, data_type=datatype)

        rdd = tsdf.rdd.flatMap(flatmap_fn())
        df = self.sql_ctx.createDataFrame(rdd, schema)

        return TimeSeriesDataFrame(df,
                                   df.sql_ctx,
                                   time_column=self._time_column,
                                   unit=self._junit,
                                   tsrdd_part_info=tsdf._tsrdd_part_info)

    def leftJoin(self, right, *, tolerance=None, key=None, left_alias=None, right_alias=None):
        """
        Left join this dataframe with a right dataframe using inexact
        timestamp matches. For each row in the left dataframe, append
        the most recent row from the right table at or before the same
        time.

        Example:

            >>> leftdf.leftJoin(rightdf, tolerance='100ns', key='tid')
            >>> leftdf.leftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key='tid')
            >>> leftdf.leftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key=['tid', 'industryGroup'])

        :param right: A dataframe to join
        :type right: :class:`TimeSeriesDataFrame`
        :param tolerance: The most recent row from the right dataframe will only be appended if it was
                          within the specified time of the row from left dataframe. If a str is specified,
                          it must be parsable by ``pandas.Timedelta``. A tolerance of 0 means only rows
                          with exact timestamp match will be joined.
        :type tolerance: ``pandas.Timedelta`` or str
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :param left_alias: Optional. The prefix for columns from left in the output dataframe.
        :type left_alias: str
        :param right_alias: Optional. The prefix for columns from right in the output dataframe.
        :type right_alias: str
        :returns: a new dataframe that results from the join
        :rtype: :class:`TimeSeriesDataFrame`

        """
        tolerance = self._timedelta_ns('tolerance', tolerance, default='0ns')
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.leftJoin(right.timeSeriesRDD, tolerance, scala_key, left_alias, right_alias)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def futureLeftJoin(self, right, *, tolerance=None, key=None, left_alias=None, right_alias=None, strict_lookahead=False):
        """
        Left join this dataframe with a right dataframe using inexact
        timestamp matches. For each row in the left dataframe, append
        the closest row from the right table at or after the same
        time. Similar to :meth:`leftJoin` except it joins with future
        rows when no matching timestamps are found.

        Example:

            >>> leftdf.futureLeftJoin(rightdf, tolerance='100ns', key='tid')
            >>> leftdf.futureLeftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key='tid')
            >>> leftdf.futureLeftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key=['tid', 'industryGroup'])

        :param right: A dataframe to join
        :type right: :class:`TimeSeriesDataFrame`
        :param tolerance: The closest row in the future from the right
                          dataframe will only be appended if it was
                          within the specified time of the row from
                          left dataframe. If a str is specified, it
                          must be parsable by ``pandas.Timedelta``. A
                          tolerance of 0 means only rows with exact
                          timestamp match will be joined.
        :type tolerance: ``pandas.Timedelta`` or str
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :param left_alias: Optional. The prefix for columns from left in the output dataframe.
        :type left_alias: str
        :param right_alias: Optional. The prefix for columns from right in the output dataframe.
        :type right_alias: str
        :param strict_lookahead: Optional. Default False. If True,
                                 rows in the left dataframe will only
                                 be joined with rows in the right
                                 dataframe that have strictly larger
                                 timestamps.
        :type strict_lookahead: bool
        :returns: a new dataframe that results from the join
        :rtype: :class:`TimeSeriesDataFrame`

        """
        tolerance = self._timedelta_ns('tolerance', tolerance, default='0ns')
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.futureLeftJoin(right.timeSeriesRDD, tolerance, scala_key, left_alias, right_alias, strict_lookahead)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def groupByCycle(self, key=None):
        """
        Groups rows that have the same timestamp. The output dataframe
        contains a "rows" column which contains a list of rows of same
        timestamps. The column can later be accessed in computations,
        such as :meth:`withColumn`.

        Example:

            >>> @ts.spark.udf(DoubleType())
            ... def averagePrice(cycle):
            ...     nrows = len(cycle)
            ...     if nrows == 0:
            ...         return 0.0
            ...     return sum(row.closePrice for row in window) / nrows
            ...
            >>> averagePriceDF = (price
            ...                   .groupByCycle()
            ...                   .withColumn("averagePrice", averagePrice(col("rows"))))

        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with list of rows of the same timestamp
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.groupByCycle(scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def groupByInterval(self, clock, key=None, begin_inclusive=True):
        """
        Groups rows within the intervals specified by a clock
        dataframe. For each adjacent pair of rows in the clock
        dataframe, rows from the dataframe that have time stamps
        between the pair are grouped. The output dataframe will have
        the first timestamp of each pair as the time column. The
        output dataframe contains a "rows" column which can be later
        accessed in computations, such as :meth:`withColumn`.

        Example:

            >>> clock = tsContext.read.uri("tsdata:/clock/ts/trading-interval/US/30", begin, end)
            >>> intervalized = price.groupByInterval(clock)

        :param clock: A dataframe used to determine the intervals
        :type clock: :class:`TimeSeriesDataFrame`
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :param begin_inclusive: Optional. Default True. If True, timestamp of output dataframe will
                                be the beginning timestamp of an interval, otherwise, timestamp of
                                the output dataframe will be the ending timestamp of an interval.
        :type begin_inclusive: bool
        :returns: a new dataframe with list of rows of the same interval
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.groupByInterval(clock.timeSeriesRDD, scala_key, begin_inclusive)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeCycles(self, summarizer, key=None):
        """
        Computes aggregate statistics of rows that share a timestamp.

        Example:

            >>> # count the number of rows in each cycle
            >>> counts = df.summarizeCycles(summarizers.count())

        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.summarizeCycles(composed_summarizer._jsummarizer(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeIntervals(self, clock, summarizer, key=None, beginInclusive=True):
        """
        Computes aggregate statistics of rows within the same interval.

        Example:

            >>> # count the number of rows in each interval
            >>> clock = tsContext.read.uri("tsdata:/clock/ts/trading-interval/US/30", begin, end)
            >>> counts = df.summarizeIntervals(clock, summarizers.count())

        :param clock: A dataframe used to determine the intervals
        :type clock: :class:`TimeSeriesDataFrame`
        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :param begin_inclusive: Optional. Default True. If True, timestamp of output dataframe will
                                be the beginning timestamp of an interval, otherwise, timestamp of
                                the output dataframe will be the ending timestamp of an interval.
        :type begin_inclusive: bool
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.summarizeIntervals(clock.timeSeriesRDD, composed_summarizer._jsummarizer(self._sc), scala_key, beginInclusive)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeWindows(self, window, summarizer, key=None):
        """
        Computes aggregate statistics of rows in windows.

        Example:

           >>> # calculates rolling weighted mean of return for each tid
           >>> result = (df.summarizeWindows(windows.past_absolute_time("365days"),
           ...                               summarizers.weighted_mean("return", "volume"),
           ...                               key="tid"))

        :param window: A window that specifies which rows to add to the new column. Lists of windows can be found in :mod:`.windows`.
        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.summarizeWindows(window._jwindow(self._sc), composed_summarizer._jsummarizer(self._sc), scala_key)

        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarize(self, summarizer, key=None):
        """
        Computes aggregate statistics of a table.

        Example:

            >>> # calcuates the weighted mean of return and t-statistic
            >>> result = df.summarize(summarizers.weighted_mean("return", "volume"), key="tid")
            >>> result = df.summarize(summarizers.weighted_mean("return", "volume"), key=["tid", "industryGroup"])

        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """

        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.summarize(composed_summarizer._jsummarizer(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def addSummaryColumns(self, summarizer, key=None):
        """Computes the running aggregate statistics of a table. For a
        given row R, the new columns will be the summarization of all
        rows before R (including R).

        Example:

            >>> # Add row number to each row
            >>> dfWithRowNum = df.addSummaryColumns(summarizers.count())

        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with the summarization columns added
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.addSummaryColumns(composed_summarizer._jsummarizer(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def addWindows(self, window, key=None):
        """Add a window column that contains a list of rows which can later be accessed in computations, such as :meth:`withColumn`.

        Example:

            >>> dfWithWindow = df.addWindows(windows.past_absolute_time("365days"))

        :param window: A window that specifies which rows to add to the new column. Lists of windows can be found in :mod:`.windows`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with the window columns
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.addWindows(window._jwindow(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def shiftTime(self, shift, *, backwards=False):
        """Returns a :class: `TimeSeriesDataFrame` by shifting all timestamps by giving ammount
        Example:

            >>> tsdf.shiftTime('100ns')
            >>> tsdf.shiftTime(pandas.Timedelta(nanoseconds=100))

        :param shift: Amount to shift the dataframe time column, shift is of type ``pandas.Timedelta`` or string that can be
                      formatted by ``pandas.Timedelta``
        :param backwards: Shift time backwards (defaults to False)
        :returns: a new :class:`TimeSeriesDataFrame`
        """
        shift = self._timedelta_ns('shift', shift)
        if backwards:
            tsrdd = self.timeSeriesRDD.lookBackwardClock(shift)
        else:
            tsrdd = self.timeSeriesRDD.lookForwardClock(shift)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def timestamp_df(self):
        """Returns a :class: `pyspark.sql.DataFrame` by casting the time column (Long) to a timestamp

        :returns: a new :class:`TimeSeriesDataFrame`
        """
        df = self.withColumn(self._time_column,
                             (self[self._time_column] * 1e-9).cast(pyspark_types.TimestampType()))
        return pyspark.sql.DataFrame(df._jdf, self.sql_ctx)

    def __str__(self):
        return "TimeSeriesDataFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

TimeSeriesDataFrame._override_df_methods()
