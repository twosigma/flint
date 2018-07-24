#
#  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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
import itertools
import inspect
import json
import types
import uuid
import numpy as np
import pandas as pd
import pyarrow as pa

import pyspark
from pyspark import traceback_utils
import pyspark.sql
import pyspark.sql.types as pyspark_types
from pyspark.sql.types import StructType, StructField
import pyspark.sql.functions as F

from . import java
from . import summarizers
from . import functions
from . import udf
from . import utils
from .group import TimeSeriesGroupedData
from .error import FlintError
from .readwriter import TSDataFrameWriter
from .serializer import arrowfile_to_dataframe, dataframe_to_arrowfile, arrowfile_to_numpy
from .udf import _unwrap_data_types
from .windows import WindowsFactoryBase


__all__ = ['TimeSeriesDataFrame']

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
        '''
        :type df: pyspark.sql.DataFrame
        :type sql_ctx: pyspark.sql.SqlContext
        :param time_column: which column is treated as "time" column
        :type time_column: str
        :param is_sorted: whether the df is sorted
        :type is_sorted: bool
        :param unit: unit of the "time" column
        :type unit: scala.concurrent.duration.TimeUnit
        :param tsrdd_part_info: Partition info
        :type tsrdd_part_info: Option[com.twosigma.flint.timeseries.PartitionInfo]
        '''
        self._time_column = time_column
        self._is_sorted = is_sorted
        self._tsrdd_part_info = tsrdd_part_info

        self._lazy_tsrdd = None
        self._jdf = df._jdf
        super().__init__(self._jdf, sql_ctx)

        self._jpkg = java.Packages(self._sc)
        self._junit = utils.junit(self._sc, unit) if isinstance(unit, str) else unit

        # Only canonizeTime if the time column exists
        # If time column exists but is not in the expected type, canonizeTime will
        # throw exception
        if time_column in df.columns:
            self._jdf = self._jpkg.TimeSeriesRDD.canonizeTime(self._jdf, self._junit)

        if tsrdd_part_info:
            if not is_sorted:
                raise FlintError("Cannot take partition information for unsorted df")
            if not self._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df._jdf):
                raise FlintError("df is not a PartitionPreservingRDDScanDataFrame")

    @property
    def timeSeriesRDD(self):
        """Returns a Scala TimeSeriesRDD object

        :returns: :class:`py4j.java_gateway.JavaObject` (com.twosigma.flint.timeseries.TimeSeriesRDD)
        """
        if not self._lazy_tsrdd:
            if not self._tsrdd_part_info:
                # This will scan ranges
                with traceback_utils.SCCallSiteSync(self._sc) as css:
                    self._lazy_tsrdd = self._jpkg.TimeSeriesRDD.fromDF(
                        self._jdf, self._is_sorted, self._junit, self._time_column)
            else:
                # TODO: Ideally we should use fromDFWithPartInfo, but
                # fromDFWithPartInfo doesn't take unit and time column
                # args.
                self._lazy_tsrdd = self._jpkg.TimeSeriesRDD.fromDFUnSafe(
                    self._jdf, self._junit, self._time_column,
                    self._tsrdd_part_info.get().deps(), self._tsrdd_part_info.get().splits().array())

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
            if self._jpkg.OrderPreservingOperation.isDerivedFrom(self._jdf, df._jdf):
                tsdf_args = {
                    "df": df,
                    "sql_ctx": df.sql_ctx,
                    "time_column": self._time_column,
                    "unit": self._junit
                }

                tsdf_args['is_sorted'] = self._is_sorted and self._jpkg.OrderPreservingOperation.isOrderPreserving(self._jdf, df._jdf)
                if self._tsrdd_part_info and self._jpkg.PartitionPreservingOperation.isPartitionPreserving(self._jdf, df._jdf):
                    tsdf_args['tsrdd_part_info'] = self._tsrdd_part_info
                else:
                    tsdf_args['tsrdd_part_info'] = None

                # Return a DataFrame if time column changes
                # TODO: Handle all the case where time column changes
                if name == 'withColumn':
                    # Get col argument from withColumn(colName, col)
                    col_name = args[0]
                    if col_name == self._time_column:
                        return df
                return TimeSeriesDataFrame(**tsdf_args)
            else:
                return df

        return _new_method

    @classmethod
    def _override_df_methods(cls):
        """Overrides :class:`DataFrame` methods and wraps returned :class:`DataFrame` objects
        as :class:`TimeSeriesDataFrame` objects
        """
        methods = inspect.getmembers(cls, predicate=inspect.isfunction)
        # Only replace non-private methods and methods not overridden in TimeSeriesDataFrame
        for name, method in methods:
            if (not name.startswith('_')
                    and inspect.getmodule(method) == pyspark.sql.dataframe):
                setattr(cls, name, cls._wrap_df_method(name, method))

    def _call_dual_function(self, function, *args, **kwargs):
        if self._jdf:
            return getattr(self._jdf, function)(*args, **kwargs)
        return getattr(self._lazy_tsrdd, function)(*args, **kwargs)

    def count(self):
        '''Counts the number of rows in the dataframe

        :returns: the number of rows in the dataframe
        :rtype: int

        '''
        return self._call_dual_function('count')


    @staticmethod
    def _from_df(df, *, time_column, is_sorted, unit):
        return TimeSeriesDataFrame(df,
                                   df.sql_ctx,
                                   time_column=time_column,
                                   is_sorted=is_sorted,
                                   unit=unit)

    @staticmethod
    def _from_pandas(df, schema, sql_ctx, *, time_column, is_sorted, unit):
        df = sql_ctx.createDataFrame(df, schema)
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
    def _from_tsrdd(tsrdd, sql_ctx):
        """Returns a :class:`TimeSeriesDataFrame` from a Scala ``TimeSeriesRDD``.
           This is a zero-copy conversion.

        :param tsrdd: :class:`py4j.java_gateway.JavaObject` (com.twosigma.flint.timeseries.TimeSeriesRDD)
        :param sql_ctx: pyspark.sql.SQLContext
        :param time_column: defaults to ``DEFAULT_TIME_COLUMN``
        :param unit: Unit of time_column, can be (s,ms,us,ns) (default: ``DEFAULT_UNIT``)
        :returns: a new :class:`TimeSeriesDataFrame` from the given Scala ``TimeSeriesRDD``
        """
        sc = sql_ctx._sc
        df = pyspark.sql.DataFrame(tsrdd.toDF(), sql_ctx)
        tsrdd_part_info = tsrdd.partInfo()
        return TimeSeriesDataFrame(df,
                                   sql_ctx,
                                   tsrdd_part_info=tsrdd_part_info)

    def addColumnsForCycle(self, columns, *, key=None):
        """
        Adds columns to each cycle by computing over data in the cycle.

        The columns are specified as a column spec, which is a ``dict``.
        Each entry can be either:

        1. column name to UDF column. A UDF column is defined by
           :meth:`ts.flint.functions.udf` with a python function,
           a return type and a list of input columns. The map entry can
           be one of the following:

           1. str -> udf

              This will add a single column. The input args to the python
              function are ``pandas.Series`` or ``pandas.DataFrame``. The
              return value of the function must be ``pandas.Series``. The
              returned ``pandas.Series`` must have the same length as inputs.
              The ``returnType`` argument of the udf object must be a single
              :class:`~pyspark.sql.types.DataType` describing the type of
              the added column.

           2. tuple(str) -> udf

              This will add multiple columns. The input args to the python
              function are ``pandas.Series`` or ``pandas.DataFrame``. The
              return value of the function must be ``pandas.Series``. The
              returned `pandas.Series` must have the same length as inputs.
              The ``returnType`` argument of the udf object must be a single
              :class:`~pyspark.sql.types.DataType` describing the types of
              the added columns.

              The cardinality of the column names, return data types and
              returned ``pandas.Series`` must match, i.e, if you are adding
              two columns, then the column names must be a tuple of two
              strings, the return type must be two data types, and the python
              must return a tuple of two ``pandas.Series``.

        User-defined function examples:

            Use user-defined functions

            >>> from ts.flint.functions import udf
            >>>
            >>> # v is a pandas.Series of double
            >>> @udf(DoubleType())
            ... def pct_rank(v):
            ...    return v.rank(pct=True)
            >>>
            >>> df.addColumnsForCycle({
            ...     'rank': pct_rank(df.v)
            ... })

            Add multiple-columns

            >>> from ts.flint.functions import udf
            >>>
            >>> # v is a pandas.Series of double
            >>> @udf((DoubleType(), DoubleType()))
            ... def ranks(v):
            ...    return v.rank(), v.rank(pct=True)
            >>>
            >>> df.addColumnsForCycle({
            ...     ('rank', 'rank_pct'): ranks(df.v)
            ... })

        :param columns: a column spec
        :type columns: collections.Mapping
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with the columns added
        :rtype: :class:`TimeSeriesDataFrame`
        """
        assert isinstance(columns, collections.Mapping), "columns must be a mapping (e.g., dict)"

        legacy_udfs = collections.OrderedDict()
        udfs = collections.OrderedDict()
        builtin_functions = collections.OrderedDict()

        for col_name, col_obj in columns.items():
            if isinstance(col_obj, collections.Sequence):
                if (isinstance(col_obj[0], pyspark_types.DataType) and
                    callable(col_obj[1])):
                    legacy_udfs[col_name] = col_obj
                else:
                    raise ValueError("Invalid udf spec. Legacy udf column spec should a tuple of "
                                     "(pyspark.sqltypes.DataType, callable). Key: {} Value: {}"
                                     .format(col_name, col_obj))
            # ts.flint.udf object
            elif hasattr(col_obj, 'func'):
                udfs[col_name] = col_obj
            else:
                raise ValueError("Invalid column spec. Key: {} Value: {}"
                                 .format(col_name, col_obj))

        tsdf = self
        if builtin_functions:
            tsdf = tsdf._addColumnsForCycle_builtin(builtin_functions, key)

        if udfs:
            tsdf = tsdf._addColumnsForCycle_udf(udfs, key)

        if legacy_udfs:
            tsdf = tsdf._addColumnsForCycle_legacy_udf(legacy_udfs, key)

        new_columns = udf._flat_column_indices(columns.keys())
        final_columns = self.columns + new_columns
        # Reorder to maintain order specified in `columns`
        tsdf = tsdf.select(*final_columns)

        return tsdf

    def _addColumnsForCycle_builtin(self, builtin_bindings, key):
        """
        Add columns using built-in ``CycleColumn`` bindings.
        :param builtin_bindings: A `dict` containing target columns as keys and
            :class:`rankers.RankFactory` as values.
        """

        scala_key = utils.list_to_seq(self._sc, key)
        scala_bindings = utils.list_to_seq(
            self._sc,
            [rf(self._sc, target_column) for target_column, rf in builtin_bindings.items()]
        )
        tsrdd = self.timeSeriesRDD.addColumnsForCycle(scala_bindings, scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def _addColumnsForCycle_udf(self, columns, key):
        """
        Add columns using user-defined functions.

        """
        import pandas as pd

        base_rows_col_name = self._jpkg.ArrowSummarizer.baseRowsColumnName()
        arrow_batch_col_name = self._jpkg.ArrowSummarizer.arrowBatchColumnName()

        udf._check_invalid_udfs(columns.values())
        required_col_names = udf._required_column_names(columns.values())
        grouped = self.summarizeCycles(
            summarizers.arrow(required_col_names, include_base_rows=True),
            key=key)

        schema_col_names = []
        data_col_names = []

        for i, (col_name, udf_column) in enumerate(columns.items()):
            fn, col_t = udf._fn_and_type(udf_column)
            column_indices = udf_column.column_indices
            arg_type = udf_column.arg_type

            if arg_type != 'pandas':
                raise ValueError('Only arg_type == pandas is supported')

            if isinstance(col_name, str):
                col_name = (col_name,)
                col_t = (col_t,)
            elif isinstance(col_name, collections.Sequence):
                col_t = _unwrap_data_types(col_t)
            else:
                raise ValueError('Column names must be either a string'
                 'or a sequence of strings. {}'.format(col_name))

            schema_col_name = '__schema_{}'.format(i)
            data_col_name = '__data_{}'.format(i)
            schema_col_names.append(schema_col_name)
            data_col_names.append(data_col_name)

            def _fn(arrow_bytes):
                pdf = arrowfile_to_dataframe(arrow_bytes)
                inputs = [pdf[index] for index in column_indices]
                result = fn(*inputs)

                if len(col_name) == 1:
                    if isinstance(result, pd.core.series.Series):
                        if len(result) == len(inputs[0]):
                            result_df = pd.concat((result,), axis=1, keys=col_name)
                        else:
                            raise ValueError('Invalid return value for column {}. '
                                             'Expected length of the returned series is {} but is'
                                             ' {}'.format(col_name, len(inputs[0]), len(result)))
                    else:
                        raise ValueError('Invalid return value for column {}. '
                                         'Return value of the user-defined function should be '
                                         'Series but is {}'.format(col_name, type(result)))
                else:
                    if isinstance(result, collections.Sequence):
                        if len(result) == len(col_name):
                            result_df = pd.concat(result, axis=1, keys=col_name)
                            if len(result_df) != len(inputs[0]):
                                raise ValueError('Invalid return value for column {}. '
                                                 'Expected length of the returned series is {} but is'
                                                 ' {}'.format(col_name, len(inputs[0]), len(result_df)))
                        else:
                            raise ValueError('Invalud return value for column {}. '
                                             'Expected a sequence of {} Series but received a sequence'
                                             'of {}'.format(col_name, len(col_name), len(result)))
                    else:
                        raise ValueError('Invalid return value for column {}. '
                                         'Return value of the user-defined function should be '
                                         'sequence of pd.Series but is '
                                         '{}'.format(col_name, type(result)))

                return dataframe_to_arrowfile(result_df)

            # Create a column that has the schema of the returned Arrow batch
            # but no data. This is a hack around not having the Arrow batch schema
            # in query planning phase.
            col_schema = StructType([StructField(name, t) for (name, t) in zip(col_name, col_t)])

            grouped = grouped.withColumn(schema_col_name, F.udf(lambda : None, col_schema)())
            grouped = grouped.withColumn(data_col_name, F.udf(_fn, pyspark_types.BinaryType())(
                grouped[arrow_batch_col_name]
            ))

        result = grouped._concatArrowAndExplode(base_rows_col_name, schema_col_names, data_col_names)

        return result

    def _addColumnsForCycle_legacy_udf(self, udfs, key):
        """
        Add columns using legacy user-defined functions.
        :param udfs: A `dict` containing target columns as keys and a tuple as the value,
            where the tuple is (1) a Pyspark DataType and, (2) a function that takes
            a list of rows and returns a dict of row to value.
        """

        # Need to make a new StructType to prevent from modifying the original schema object
        schema = pyspark_types.StructType.fromJson(self.schema.jsonValue())

        grouped = self.groupByCycle(key)

        # Don't pickle the whole schema, just the names for the lambda
        schema_names = list(schema.names)

        def flatmap_fn():
            def _(orig_row):
                orig_rows = orig_row.rows
                new_rows = [list(row) for row in orig_rows]
                for column, (datatype, fn) in udfs.items():
                    fn_rows = fn(orig_rows)
                    for i, orig_row in enumerate(orig_rows):
                        new_rows[i].append(fn_rows[orig_row])

                NewRow = pyspark_types.Row(*schema_names)
                return [NewRow(*row) for row in new_rows]
            return _

        for column, (datatype, fn) in udfs.items():
            schema.add(column, data_type=datatype)

        rdd = grouped.rdd.flatMap(flatmap_fn())
        df = self.sql_ctx.createDataFrame(rdd, schema)

        return TimeSeriesDataFrame(df,
                                   df.sql_ctx,
                                   time_column=self._time_column,
                                   unit=self._junit,
                                   tsrdd_part_info=self._tsrdd_part_info)

    def merge(self, other):
        """
        Merge this dataframe and the other dataframe with the same schema.
        The merged dataframe includes all rows from each in temporal order.
        If there is a timestamp ties, the rows in this dataframe will be
        returned earlier than those from the other dataframe.

        Example:

            >>> thisdf.merge(otherdf)

        :param other: The other dataframe to merge. It must have the same schema as this
                      dataframe.
        :type other: :class:`TimeSeriesDataFrame`
        :returns: a new dataframe that results from the merge
        :rtype: :class:`TimeSeriesDataFrame`

        """
        tsrdd = self.timeSeriesRDD.merge(other.timeSeriesRDD)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def leftJoin(self, right, *, tolerance=None, key=None, left_alias=None, right_alias=None):
        """
        Left join this dataframe with a right dataframe using inexact
        timestamp matches. For each row in the left dataframe, append
        the most recent row from the right table at or before the same
        time.

        Example:

            >>> leftdf.leftJoin(rightdf, tolerance='100ns', key='id')
            >>> leftdf.leftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key='id')
            >>> leftdf.leftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key=['id', 'industryGroup'])

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

            >>> leftdf.futureLeftJoin(rightdf, tolerance='100ns', key='id')
            >>> leftdf.futureLeftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key='id')
            >>> leftdf.futureLeftJoin(rightdf, tolerance=pandas.Timedelta(nanoseconds=100), key=['id', 'industryGroup'])

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

    def groupByInterval(self, clock, key=None, inclusion='begin', rounding='end'):
        """
        Groups rows within the intervals specified by a clock
        dataframe. For each adjacent pair of rows in the clock
        dataframe, rows from the dataframe that have time stamps
        between the pair are grouped. The output dataframe will have
        the first timestamp of each pair as the time column. The
        output dataframe contains a "rows" column which can be later
        accessed in computations, such as :meth:`withColumn`.

        Example:

            >>> clock = clocks.uniform(sqlContext, frequency="1day", offset="0ns", begin_date_time="2016-01-01", end_date_time="2017-01-01")
            >>> intervalized = price.groupByInterval(clock)

        :param clock: A dataframe used to determine the intervals
        :type clock: :class:`TimeSeriesDataFrame`
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :param inclusion: Defines the shape of the intervals, i.e, whether intervals are
                          [begin, end) or (begin, end].
                          "begin" causes rows that are at the exact beginning of an interval to be
                          included and rows that fall on the exact end to be excluded, as
                          represented by the interval [begin, end). "end" causes rows that are at
                          the exact beginning of an interval to be excluded and rows that fall on
                          the exact end to be included, as represented by the interval (begin, end].
                          Defaults to "begin".
        :type inclusion: str
        :param rounding: Determines how timestamps of input rows are rounded to timestamps of
                         intervals.
                         "begin" causes the input rows to be rounded to the beginning timestamp of
                         an interval. "end" causes the input rows to be rounded to the ending
                         timestamp of an interval.
                         Defaults to "end".
        :type rounding: str
        :returns: a new dataframe with list of rows of the same interval
        :rtype: :class:`TimeSeriesDataFrame`
        """
        scala_key = utils.list_to_seq(self._sc, key)
        with traceback_utils.SCCallSiteSync(self._sc) as css:
            tsrdd = self.timeSeriesRDD.groupByInterval(clock.timeSeriesRDD, scala_key,
                                                       inclusion, rounding)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeCycles(self, summarizer, key=None):
        """
        Computes aggregate statistics of rows that share a timestamp
        using a summarizer spec.

        A summarizer spec can be either:

        1. A summarizer or a list of summarizers. Available
           summarizers can be found in :mod:`.summarizers`.

        2. A map from column names to columnar udf objects. A columnar
           udf object is defined by :meth:`ts.flint.functions.udf`
           with a python function, a return type and a list of input
           columns. Each map entry can be one of the following:

           1. str -> udf

              This will add a single column. The python function must
              return a single scalar value, which will be the value
              for the new column. The ``returnType`` argument of the
              udf object must be a single
              :class:`~pyspark.sql.types.DataType`.

           2. tuple(str) -> udf

              This will add multiple columns. The python function must
              return a tuple of scalar values. The ``returnType``
              argument of the udf object must be a tuple of
              :class:`~pyspark.sql.types.DataType`. The cardinality of
              the column names, return data types and return values
              must match, i.e, if you are adding
              two columns, then the column names must be a tuple of two
              strings, the return type must be two data types, and the python
              must return a tuple of two scalar values.

        Examples:

        Use built-in summarizers

            >>> df.summarizeCycles(summarizers.mean('v'))

            >>> df.summarizeCycles([summarizers.mean('v'), summarizers.stddev('v')])

        Use user-defined functions (UDFs):

            >>> from ts.flint.functions import udf
            >>> @udf(DoubleType())
            ... def mean(v):
            ...     return v.mean()
            >>>
            >>> @udf(DoubleType())
            ... def std(v):
            ...     return v.std()
            >>>
            >>> df.summarizeCycles({
            ...     'mean': mean(df['v']),
            ...     'std': std(df['v'])
            ... })

        Use a OrderedDict to specify output column order

            >>> from collections import OrderedDict
            >>> df.summarizeCycles(OrderedDict([
            ...     ('mean', mean(df['v'])),
            ...     ('std', std(df['v'])),
            ... ]))

        Return multiple columns from a single udf as a tuple

            >>> @udf((DoubleType(), DoubleType()))
            >>> def mean_and_std(v):
            ...     return (v.mean(), v.std())
            >>> df.summarizeCycles({
            ...     ('mean', 'std'): mean_and_std(df['v']),
            ... })

        Use other python libraries in udf

            >>> from statsmodels.stats.weightstats import DescrStatsW
            >>> @udf(DoubleType())
            ... def weighted_mean(v, w):
            ...     return DescrStatsW(v, w).mean
            >>>
            >>> df.summarizeCycles({
            ...     'wm': weighted_mean(df['v'], df['w'])
            ... })

        Use :class:`pandas.DataFrame` as input to udf

            >>> @udf(DoubleType())
            ... def weighted_mean(cycle_df):
            ...     return DescrStatsW(cycle_df.v, cycle_df.w).mean
            >>>
            >>> df.summarizeCycles({
            ...     'wm': weighted_mean(df[['v', 'w']])
            ... })

        :param summarizer: A summarizer spec. See above for the
            allowed types of objects.
        :param key: Optional. One or multiple column names to use as
            the grouping key
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`

        .. seealso:: :meth:`ts.flint.functions.udf`

        """

        if isinstance(summarizer, collections.Mapping):
            return self._summarizeCycles_udf(summarizer, key)
        else:
            return self._summarizeCycles_builtin(summarizer, key)

    def _summarizeGroup_udf(self, columns, group_fn):
        """Summarize groups using a udf. A group can be either a cycle or an
        interval, defined by ``group_fn``.

        This function consist of two steps:

        1. summarize groups using Arrow summarizer
           This step summarize data for each group into an Arrow batch

        2. withColumn with PySpark UDF to compute each batch
           This is done on PySpark side. Once we have each group in Arrow
           format, we can now send the bytes to python worker using regular
           PySpark UDF, compute the result, and return the result in regular
           PySpark format.
        """
        arrow_batch_col_name = self._jpkg.ArrowSummarizer.arrowBatchColumnName()

        # Check if illegal columns exists
        udf._check_invalid_udfs(columns.values())
        required_col_names = udf._required_column_names(columns.values())
        arrow_summarizer = summarizers.arrow(required_col_names, include_base_rows=False)
        grouped = group_fn(self, arrow_summarizer)

        # (1) Turns row in each group into an Arrow file format
        # (2) For each udf, we apply the function and put the
        #     result in a new column. If the udf returns multiple
        #     values, we put the values in a struct first and later
        #     explode it into multiple columns.
        for i, (col_name, udf_column) in enumerate(columns.items()):
            fn, t = udf._fn_and_type(udf_column)
            column_indices = udf_column.column_indices
            arg_type = udf_column.arg_type

            if arg_type != 'pandas':
                raise ValueError('Only arg_type == pandas is supported')

            def _fn(arrow_bytes):
                pdf = arrowfile_to_dataframe(arrow_bytes)
                inputs = [pdf[index] for index in column_indices]
                ret = fn(*inputs)
                return udf._numpy_to_python(ret)

            if isinstance(col_name, tuple):
                struct_col_name = "__struct_{}".format(i)
                grouped = grouped.withColumn(
                    struct_col_name,
                    F.udf(_fn, t)(grouped[arrow_batch_col_name]))

                for i in range(len(col_name)):
                    grouped = grouped.withColumn(
                        col_name[i],
                        grouped[struct_col_name]['_{}'.format(i)])

                grouped = grouped.drop(struct_col_name)
            else:
                grouped = grouped.withColumn(
                    col_name,
                    F.udf(_fn, t)(grouped[arrow_batch_col_name]))

        return grouped.drop(arrow_batch_col_name)

    def _summarizeCycles_udf(self, columns, key):
        def group_fn(df, summarizer):
            return df._summarizeCycles_builtin(
                summarizer,
                key=key)

        return self._summarizeGroup_udf(columns, group_fn)

    def _summarizeCycles_builtin(self, summarizer, key):
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        tsrdd = self.timeSeriesRDD.summarizeCycles(composed_summarizer._jsummarizer(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def _summarizeWindowBatches(self, window, columns=None, key=None):
        jwindow = window._jwindow(self._sc)
        scala_columns = utils.list_to_seq(self._sc, columns, preserve_none=True)
        scala_key = utils.list_to_seq(self._sc, key)
        tsrdd = self.timeSeriesRDD.summarizeWindowBatches(jwindow, scala_columns, scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def _concatArrowAndExplode(self, base_rows_col, schema_cols, data_cols):
        assert self._tsrdd_part_info is not None
        tsrdd = self.timeSeriesRDD.concatArrowAndExplode(
            base_rows_col,
            utils.list_to_seq(self._sc, schema_cols),
            utils.list_to_seq(self._sc, data_cols))
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeIntervals(self, clock, summarizer,
                           key=None, inclusion='begin', rounding='end'):
        """
        Computes aggregate statistics of rows within the same interval
        using a summarizer spec.

        A summarizer spec can be either:

        1. A summarizer or a list of summarizers. Available
           summarizers can be found in :mod:`.summarizers`.

        2. A map from column names to columnar udf objects. A columnar
           udf object is defined by :meth:`ts.flint.functions.udf`
           with a python function, a return type and a list of input
           columns. Each map entry can be one of the following:

           1. str -> udf

              This will add a single column. The python function must
              return a single scalar value, which will be the value
              for the new column. The ``returnType`` argument of the
              udf object must be a single
              :class:`~pyspark.sql.types.DataType`.

           2. tuple(str) -> udf

              This will add multiple columns. The python function must
              return a tuple of scalar values. The ``returnType``
              argument of the udf object must be a tuple of
              :class:`~pyspark.sql.types.DataType`. The cardinality of
              the column names, return data types and return values
              must match, i.e, if you are adding
              two columns, then the column names must be a tuple of two
              strings, the return type must be two data types, and the python
              must return a tuple of two scalar values.

        Examples:

        Create a uniform clock

            >>> from ts.flint import clocks
            >>> clock = clocks.uniform(sqlContext, '1day')

        Use built-in summarizers

            >>> df.summarizeIntervals(clock, summarizers.mean('v'))

            >>> df.summarizeIntervals(clock, [summarizers.mean('v'), summarizers.stddev('v')])

        Use user-defined functions (UDFs):

            >>> from ts.flint.functions import udf
            >>> @udf(DoubleType())
            ... def mean(v):
            ...     return v.mean()
            >>>
            >>> @udf(DoubleType())
            ... def std(v):
            ...     return v.std()
            >>>
            >>> df.summarizeIntervals(
            ...     clock,
            ...     {
            ...         'mean': mean(df['v']),
            ...         'std': std(df['v'])
            ...     }
            ... )

        Use a OrderedDict to specify output column order

            >>> from collections import OrderedDict
            >>> df.summarizeIntervals(
            ...     clock,
            ...     OrderedDict([
            ...         ('mean', mean(df['v'])),
            ...         ('std', std(df['v'])),
            ...     ])
            ... )

        Return multiple columns from a single udf as a tuple

            >>> @udf((DoubleType(), DoubleType()))
            >>> def mean_and_std(v):
            ...     return (v.mean(), v.std())
            >>>
            >>> df.summarizeIntervals(
            ...     clock,
            ...     {
            ...         ('mean', 'std'): mean_and_std(df['v']),
            ...     }
            ... )

        Use :class:`pandas.DataFrame` as input to udf

            >>> @udf(DoubleType())
            ... def weighted_mean(cycle_df):
            ...     return numpy.average(cycle_df.v, weights=cycle_df.w)
            >>>
            >>> df.summarizeIntervals(
            ...    clock,
            ...    {
            ...       'wm': weighted_mean(df[['v', 'w']])
            ...    }
            ... )

        :param clock: A :class:`TimeSeriesDataFrame` used to determine the intervals
        :type clock: :class:`TimeSeriesDataFrame`
        :param summarizer: A summarizer spec. See above for the
            allowed types of objects.
        :param key: Optional. One or multiple column names to use as
            the grouping key
        :type key: str, list of str
        :param inclusion: Defines the shape of the intervals, i.e, whether intervals are
                          [begin, end) or (begin, end].
                          "begin" causes rows that are at the exact beginning of an interval to be
                          included and rows that fall on the exact end to be excluded, as
                          represented by the interval [begin, end). "end" causes rows that are at
                          the exact beginning of an interval to be excluded and rows that fall on
                          the exact end to be included, as represented by the interval (begin, end].
                          Defaults to "begin".
        :type inclusion: str
        :param rounding: Determines how timestamps of input rows are rounded to timestamps of
                         intervals.
                         "begin" causes the input rows to be rounded to the beginning timestamp of
                         an interval. "end" causes the input rows to be rounded to the ending
                         timestamp of an interval.
                         Defaults to "end".
        :type rounding: str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`

        .. seealso:: :meth:`ts.flint.functions.udf`

        """

        if isinstance(summarizer, collections.Mapping):
            with traceback_utils.SCCallSiteSync(self._sc) as css:
                return self._summarizeIntervals_udf(clock, summarizer, key, inclusion, rounding)
        else:
            with traceback_utils.SCCallSiteSync(self._sc) as css:
                return self._summarizeIntervals_builtin(clock, summarizer, key, inclusion, rounding)

    def _summarizeIntervals_udf(self, clock, columns,
                                key=None, inclusion='begin', rounding='end'):
        def group_fn(df, summarizer):
            return df._summarizeIntervals_builtin(
                clock,
                summarizer,
                key=key, inclusion=inclusion, rounding=rounding)

        return self._summarizeGroup_udf(columns, group_fn)

    def _summarizeIntervals_builtin(self, clock, summarizer,
                                    key=None, inclusion='begin', rounding='end'):
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)

        tsrdd = self.timeSeriesRDD.summarizeIntervals(
            clock.timeSeriesRDD,
            composed_summarizer._jsummarizer(self._sc),
            scala_key,
            inclusion,
            rounding)

        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeWindows(self, window, summarizer, key=None):
        """
        Computes aggregate statistics of rows in windows using a
        window spec and a summarizer spec.

        A window spec can be created using one the functions in
        :mod:`.windows`.

        A summarizer spec can be either:

        1. A summarizer or a list of summarizers. Available
           summarizers can be found in :mod:`.summarizers`.

        2. A map from column names to UDF columns. A UDF column
           is defined by :meth:`ts.flint.functions.udf`
           with a python function, a return type and a list of input
           columns. Each map entry can be one of the following:

           1. str -> udf

              This will add a single column. The python function must
              return a single scalar value, which will be the value
              for the new column. The ``returnType`` argument of the
              udf object must be a single
              :class:`~pyspark.sql.types.DataType`.

           2. tuple(str) -> udf

              This will add multiple columns. The python function must
              return a tuple of scalar values. The ``returnType``
              argument of the udf object must be a tuple of
              :class:`~pyspark.sql.types.DataType`. The cardinality of
              the column names, return data types and return values
              must match, i.e, if you are adding two columns, then the
              column names must be a tuple of two strings, the return
              type must be
              two data types, and the python must return a tuple of two
              `pandas.Series`.

        Built-in summarizer examples:

           Use built-in summarizers

           >>> # calculates rolling weighted mean of return for each id
           >>> result = df.summarizeWindows(
           ...     windows.past_absolute_time("7days"),
           ...     summarizers.weighted_mean("return", "volume"),
           ...     key="id"
           ... )

        User-defined function examples:

           Use user-defined functions

           >>> from ts.flint.functions import udf
           >>>
           >>> # v is a pandas.Series of double
           >>> @udf(DoubleType())
           ... def mean(v):
           ...     return v.mean()
           >>>
           >>> # v is a pandas.Series of double
           >>> @udf(DoubleType())
           ... def std(v):
           ...     return v.std()
           >>>
           >>> df.summarizeWindows(
           ...     windows.past_absolute_time('7days'),
           ...     {
           ...       'mean': mean(df['v']),
           ...       'std': std(df['v'])
           ...     },
           ...     key='id'
           ... )

           Use an OrderedDict to specify output column order

           >>> # v is a pandas.Series of double
           >>> from ts.flint.functions import udf
           >>> @udf(DoubleType())
           ... def mean(v):
           ...     return v.mean()
           >>>
           >>> # v is a pandas.Series of double
           >>> @udf(DoubleType())
           ... def std(v):
           ...     return v.std()
           >>>
           >>> udfs = OrderedDict([
           ...     ('mean', mean(df['v'])),
           ...     ('std', std(df['v']))
           ... ])
           >>>
           >>> df.summarizeWindows(
           ...     windows.past_absolute_time('7days'),
           ...     udfs,
           ...     key='id'
           ... )

           Return multiple columns from a single UDF

           >>> # v is a pandas.Series of double
           >>> @udf((DoubleType(), DoubleType()))
           >>> def mean_and_std(v):
           ...     return v.mean(), v.std()
           >>>
           >>> df.summarizeWindows(
           ...     windows.past_absolute_time('7days'),
           ...     {
           ...       ('mean', 'std'): mean_and_std(df['v'])
           ...     },
           ...     key='id'
           ... )

           Use multiple input columns

           >>> from ts.flint.functions import udf
           >>> # window is a pandas.DataFrame that has two columns - v and w
           >>> @udf(DoubleType())
           ... def weighted_mean(window):
           ...     return np.average(window.v, weights=window.w)
           >>>
           >>> df.summarizeWindows(
           ...    windows.past_absolute_time('7days'),
           ...    {
           ...      'weighted_mean': weighted_mean(df[['v', 'w']])
           ...    },
           ...    key='id'
           ... )

           Use numpy user-defined function to compute rank:

           >>> from scipy import stats
           >>> @udf(DoubleType(), arg_type='numpy')
           >>> def rank_np(v):
           >>>     # v is a numpy.ndarray
           >>>     return stats.percentileofscore(v, v[-1], kind='rank') / 100.0
           >>>
           >>> df.summarizeWindows(
           ...     windows.past_absolute_time('7days'),
           ...     {
           ...       'rank': rank_np(df['v']),
           ...     },
           ...     key='id'
           ... )

           Use numpy user-defined function to compute weighted mean:

           >>> @udf(DoubleType(), arg_type='numpy')
           >>> def weighted_mean_np(window):
           >>>     # window is a list of numpy.ndarray
           >>>     return np.average(window[0], weights=window[1])
           >>> df.summarizeWindows(
           ...    windows.past_absolute_time('7days'),
           ...    {
           ...       'weighted_mean': weighted_mean_np(df[['v', 'w']])
           ...    },
           ...    key='id'
           ... )

        :param window: A window that specifies which rows to add to
            the new column. Lists of windows can be found in
            :mod:`.windows`.
        :param summarizer: A summarizer spec
        :param key: Optional. One or multiple column names to use as
            the grouping key.
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """

        if isinstance(summarizer, collections.Mapping):
            return self._summarizeWindows_udf(window, summarizer, key)
        else:
            return self._summarizeWindows_builtin(window, summarizer, key)

    def _summarizeWindows_builtin(self, window, summarizer, key=None):
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)

        tsrdd = self.timeSeriesRDD.summarizeWindows(
            window._jwindow(self._sc),
            composed_summarizer._jsummarizer(self._sc),
            scala_key)

        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def _summarizeWindows_udf(self, window, columns, key):
        """Summarize windows using UDFs.

           This function consists of three steps:

           1. summarizeWindowBatches
              This step breaks the left table and right table into multiple Arrow batches
              and compute indices for each left row.

           2. withColumn with PySpark UDF to compute each batch
              This is done on PySpark side. Once we have each batch in Arrow format, we can now
              send the bytes to python worker using regular PySpark UDF, compute rolling windows
              in python using precomputed indices, and return the result in Arrow format.

           3. concatArrowAndExplode
              The final step concat new columns to the original rows, and explode each batch back
              to multiple rows.

           See TimeSeriesRDD.scala for details.
        """

        base_rows_col_name = self._jpkg.ArrowWindowBatchSummarizer.baseRowsColumnName()
        left_batch_col_name = self._jpkg.ArrowWindowBatchSummarizer.leftBatchColumnName()
        right_batch_col_name = self._jpkg.ArrowWindowBatchSummarizer.rightBatchColumnName()
        indices_col_name = self._jpkg.ArrowWindowBatchSummarizer.indicesColumnName()
        is_placeholder_col_name = '__window_is_placeholder'

        # required columns for column pruning
        required_columns = set()

        for col in columns.values():
            if len(col.column_indices) == 1:
                required_columns.update(udf._flat_column_indices(col.column_indices))
            else:
                raise ValueError("Received more than 1 args to the UDF. Use udf(df[[col]]).")

            for index in col.column_indices:
                if index is None:
                    raise ValueError(
                        'Column passed to the udf function must be a column in the DataFrame, '
                        'i.e, df[col] or df[[col]]. Other types of Column are not supported.')

        windowed = self._summarizeWindowBatches(
            window,
            columns=list(required_columns),
            key=key)

        # This is a hack to get around the issue where pyspark batches rows in groups of 100 for python udf.
        # As a result it significantly increases memory usage.
        # https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution
        # /python/BatchEvalPythonExec.scala#L68

        # The workaround is to insert 99 empty rows for every batch.
        is_placeholder = F.array(F.lit(False), *(F.lit(True) for i in range(0, 99)))

        windowed = (windowed
                     .select('time', base_rows_col_name, left_batch_col_name, right_batch_col_name, indices_col_name)
                     .withColumn(is_placeholder_col_name , is_placeholder)
                     .withColumn(is_placeholder_col_name , F.explode(F.col(is_placeholder_col_name))))

        for column in [base_rows_col_name, left_batch_col_name, right_batch_col_name, indices_col_name]:
            windowed = windowed.withColumn(column, F.when(windowed[is_placeholder_col_name], None).otherwise(windowed[column]))

        schema_col_names = []
        data_col_names = []

        for i, (col_name, udf_column) in enumerate(columns.items()):
            fn, col_t = udf._fn_and_type(udf_column)
            column_indices = udf_column.column_indices
            arg_type = udf_column.arg_type

            if isinstance(col_name, str):
                col_name = (col_name,)
                col_t = (col_t,)
            elif isinstance(col_name, collections.Sequence):
                col_t = _unwrap_data_types(col_t)
            else:
                raise ValueError('Column names must be either a string'
                                 'or a sequence of strings. {}'.format(col_name))

            schema_col_name = '__schema_{}'.format(i)
            data_col_name = '__data_{}'.format(i)

            schema_col_names.append(schema_col_name)
            data_col_names.append(data_col_name)

            def _fn_numpy(left_batch, right_batch, indices):

                if indices:
                    # left_table and right_table can be either a np.ndarray or dict of a list
                    # of np.ndarray, depending on the column indices
                    if len(column_indices) == 1:
                        left_column_index = None
                        left_table = None
                        right_column_index = column_indices[0]
                        right_table = arrowfile_to_numpy(right_batch, right_column_index)
                    elif len(column_indices) == 2:
                        left_column_index = column_indices[0]
                        left_table = arrowfile_to_numpy(left_batch, left_column_index)
                        right_column_index = column_indices[1]
                        right_table = arrowfile_to_numpy(right_batch, right_column_index)
                    else:
                        raise ValueError('Too many column indices: {}'.format(column_indices))

                    indices_df = arrowfile_to_dataframe(indices)
                    begin_index = indices_df.ix[:,0].values
                    end_index = indices_df.ix[:,1].values
                    batch_num = len(begin_index)

                    if left_table is None:
                        if isinstance(right_column_index, str):
                            data = [fn(right_table[begin_index[i]: end_index[i]].copy())
                                    for i in range(batch_num)]
                        else:
                            data = \
                                [fn([v[begin_index[i]: end_index[i]].copy() for v in right_table])
                                for i in range(batch_num)]
                    else:
                        if isinstance(left_column_index, str):
                            data = [fn(left_table[i], [v[begin_index[i]: end_index[i]].copy() for v in right_table])
                                    for i in range(batch_num)]
                        else:
                            data = \
                                [fn([v[i] for v in left_table],
                                    [v[begin_index[i]: end_index[i]].copy() for v in right_table])
                                 for i in range(batch_num)]

                    df = pd.DataFrame(data, columns=col_name)
                    return dataframe_to_arrowfile(df)
                else:
                    return None

            def _fn_pandas(left_batch, right_batch, indices):

                if indices:
                    # left_table and right_table can be either a pd.Series or pd.DataFrame
                    # depending on the column indices
                    if len(column_indices) == 1:
                        left_column_index = None
                        left_table = None
                        right_column_index = column_indices[0]
                        right_table = arrowfile_to_dataframe(right_batch)[right_column_index]
                    elif len(column_indices) == 2:
                        left_column_index = column_indices[0]
                        left_table = arrowfile_to_dataframe(left_batch)[left_column_index]
                        right_column_index = column_indices[1]
                        right_table = arrowfile_to_dataframe(right_batch)[right_column_index]
                    else:
                        raise ValueError('Too many column indices: {}'.format(column_indices))

                    indices_df = arrowfile_to_dataframe(indices)

                    data = []

                    if left_table is None:
                        for (begin, end) in indices_df.itertuples(index=False):
                            right = right_table.take(np.arange(begin, end))
                            data.append(fn(right))
                    else:
                        assert len(left_table) == len(indices_df)
                        if isinstance(left_column_index, str):
                            for (i, begin, end) in indices_df.itertuples():
                                left = left_table[i]
                                right = right_table.take(np.arange(begin, end))
                                data.append(fn(left, right))
                        else:
                            for (i, left) in enumerate(left_table.itertuples(index=False)):
                                begin = indices_df.iloc[i,0]
                                end = indices_df.iloc[i,1]
                                right = right_table.take(np.arange(begin, end))
                                data.append(fn(left, right))

                    df = pd.DataFrame(data, columns=col_name)

                    return dataframe_to_arrowfile(df)
                else:
                    return None

            # Create a column that has the schema of the returned arrow batch
            # but no data. This is a hack around not having the arrow batch schema
            # in query planning phase.
            col_schema = StructType([StructField(name, t) for (name, t) in zip(col_name, col_t)])

            windowed = windowed.withColumn(
                schema_col_name,
                F.udf(lambda : None,col_schema)())

            if arg_type == "pandas":
                _fn = _fn_pandas
            elif arg_type == "numpy":
                _fn = _fn_numpy
            else:
                raise ValueError("Unsupported arg type {}".format(arg_type))

            windowed = windowed.withColumn(
                data_col_name,
                F.udf(_fn, pyspark_types.BinaryType())(
                    windowed[left_batch_col_name],
                    windowed[right_batch_col_name],
                    windowed[indices_col_name]))

        windowed = windowed.filter(windowed[base_rows_col_name].isNotNull())

        result = windowed._concatArrowAndExplode(base_rows_col_name, schema_col_names, data_col_names)

        return result

    def summarize(self, summarizer, key=None):
        """
        Computes aggregate statistics of a table.

        Example:

            >>> # calcuates the weighted mean of return and t-statistic
            >>> result = df.summarize(summarizers.weighted_mean("return", "volume"), key="id")
            >>> result = df.summarize(summarizers.weighted_mean("return", "volume"), key=["id", "industryGroup"])

        :param summarizer: A summarizer or a list of summarizers that will calculate results for the new columns. Available summarizers can be found in :mod:`.summarizers`.
        :param key: Optional. One or multiple column names to use as the grouping key
        :type key: str, list of str
        :returns: a new dataframe with summarization columns
        :rtype: :class:`TimeSeriesDataFrame`
        """

        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        with traceback_utils.SCCallSiteSync(self._sc) as css:
            tsrdd = self.timeSeriesRDD.summarize(composed_summarizer._jsummarizer(self._sc), scala_key)
        return TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sql_ctx)

    def summarizeState(self, summarizer, key=None):
        """
        Undocumented function for the bravest.

        Returns a Java map from key to summarize state (also Java object).
        This function can be changed/removed/broken without notice.

        Use at your own risk.
        """
        scala_key = utils.list_to_seq(self._sc, key)
        composed_summarizer = summarizers.compose(self._sc, summarizer)
        with traceback_utils.SCCallSiteSync(self._sc) as css:
            result = self.timeSeriesRDD.summarizeState(composed_summarizer._jsummarizer(self._sc), scala_key)
        return result

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
        with traceback_utils.SCCallSiteSync(self._sc) as css:
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
        """Returns a :class: `TimeSeriesDataFrame` by shifting all timestamps by given amount.

           When time type is timestamp:
             If shift forward amount is less than 1 microsecond, then this is a no op.
             If shift backward amount if less than 1 microsecond, then this will shift back 1 microsecond.

        Example:

            >>> tsdf.shiftTime('100s')
            >>> tsdf.shiftTime(pandas.Timedelta(seconds=100))
            >>> tsdf.shiftTime(windows.futureTradingTime('1day', 'US'))

        :param shift: Amount to shift the dataframe time column, shift can be a ``pandas.Timedelta`` or a string that can be
                      formatted by ``pandas.Timedelta`` or a ``window``.
        :param backwards: Shift time backwards (defaults to False). Ignored when shift is a ``window``.
        :returns: a new :class:`TimeSeriesDataFrame`
        """

        if isinstance(shift, WindowsFactoryBase):
            window = shift
            tsrdd = self.timeSeriesRDD.shift(window._jwindow(self._sc))
        else:
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

    def toPandas(self):
        pdf = super().toPandas()
        if 'time' in pdf.columns:
            try:
                series = pd.to_datetime(pdf['time'])
            except:
                series = pdf['time']
            pdf = pdf.assign(time=series)

            if not self._is_sorted:
                pdf = pdf.sort_values(by='time').reset_index(drop=True)

        return pdf

    def groupBy(self, *cols):
        gd = super().groupBy(*cols)
        return TimeSeriesGroupedData(gd)

    # Will be instrumented by groupBy
    def groupby(self, *cols):
        return self.groupBy(*cols)

    def preview(self, n=10):
        """
        Return the first n rows of the :class:`TimeSeriesDataFrame` as ``pandas.DataFrame``

        This is only available if Pandas is installed and available.

        The time column will be converted to timestamp type.

        :param n: number of rows to return. Default is 10.
        """
        df = pd.DataFrame(self.head(n), columns=self.columns)
        return df.assign(**{self._time_column: pd.to_datetime(df[self._time_column])})

TimeSeriesDataFrame._override_df_methods()
