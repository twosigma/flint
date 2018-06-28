#
#  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

from tests.utils import *

BASE = get_test_base()

class TestPartitionPreserve(BASE):

    def shared_test_partition_preserving(self, func, preserve, create = None):
        from pyspark.sql.functions import month
        from tests.test_data import FORECAST_DATA

        flintContext = self.flintContext

        def create_dataframe():
            return flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))

        if create is None:
            create = create_dataframe

        df_lazy = create()

        df_eager = create()
        df_eager.timeSeriesRDD

        df = create()
        df_joined = df.leftJoin(df, right_alias="right")

        df = create()
        df_cached = df.cache()
        df_cached.count()

        df_cached_joined = df_cached.leftJoin(df_cached, right_alias="right")

        partition_preserving_input_tranforms = [
            lambda df: df,
            lambda df: df.withColumn("f2", df.forecast * 2),
            lambda df: df.select("time", "id", "forecast"),
            lambda df: df.filter(month(df.time) == 1)
        ]

        order_preserving_input_tranforms = [
            lambda df: df.orderBy("time")
        ]

        input_dfs = [df_lazy, df_eager, df_joined, df_cached, df_cached_joined]

        for transform in partition_preserving_input_tranforms:
            for input_df in input_dfs:
                self.assert_partition_preserving(transform(input_df), func, preserve)

        for transform in order_preserving_input_tranforms:
            for input_df in input_dfs:
                self.assert_order_preserving(transform(input_df), func, preserve)

        df_cached.unpersist()

    def get_nonempty_partitions(self, df):
        rows_to_pandas = lambda rows: [pd.DataFrame(list(rows))]
        pdfs = df.rdd.mapPartitions(rows_to_pandas).collect()
        return [pdf for pdf in pdfs if not pdf.empty]

    def assert_partition_equals(self, df1, df2):
        partitions1 = self.get_nonempty_partitions(df1)
        partitions2 = self.get_nonempty_partitions(df2)

        assert(len(partitions1) == len(partitions2))
        for pdf1, pdf2 in zip(partitions1, partitions2):
            pdt.assert_frame_equal(pdf1, pdf2)

    def assert_sorted(self, df):
        pdf = df.toPandas()
        if len(pdf.index) < 2:
            return
        assert np.diff(pdf.time).min() >= np.timedelta64(0)

    def assert_partition_preserving(self, input_df, func, preserve):
        output_df = func(input_df)

        if preserve:
            assert(input_df.rdd.getNumPartitions() == output_df.rdd.getNumPartitions())
            assert(input_df._is_sorted == output_df._is_sorted)
            assert(input_df._tsrdd_part_info == output_df._tsrdd_part_info)
            if output_df._is_sorted:
                self.assert_sorted(output_df)
            if output_df._tsrdd_part_info:
                output_df.timeSeriesRDD.validate()

        else:
            assert(output_df._tsrdd_part_info is None)

    def assert_order_preserving(self, input_df, func, preserve):
        output_df = func(input_df)

        if preserve:
            assert(input_df._is_sorted == output_df._is_sorted)
            if output_df._is_sorted:
                self.assert_sorted(output_df)

        else:
            assert(not output_df._is_sorted)
            assert(output_df._tsrdd_part_info is None)

    def test_from_tsrdd(self):
        from tests.test_data import FORECAST_DATA
        from ts.flint import TimeSeriesDataFrame

        df = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        tsrdd = df.timeSeriesRDD
        df2 = TimeSeriesDataFrame._from_tsrdd(tsrdd, self.sqlContext)
        tsrdd2 = df2.timeSeriesRDD

        assert(tsrdd.count() == tsrdd2.count())
        assert(tsrdd.orderedRdd().getNumPartitions() == tsrdd2.orderedRdd().getNumPartitions())

    def test_sorted_df_partitioning(self):
        from pyspark.sql import DataFrame
        forecast = self.forecast()

        sorted_df = forecast.sort("time")
        # if the underlying data frame is sorted then conversion to TSDF or tsrdd shouldn't affect partitioning
        normalized_df = DataFrame(self.flintContext.read.dataframe(sorted_df).timeSeriesRDD.toDF(),
                                  self.sqlContext)
        self.assert_partition_equals(sorted_df, normalized_df)

    def test_with_column_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.withColumn("neg_forecast", -df.forecast), True)

    def test_drop_column_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.drop("forecast"), True)

    def test_filter_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.filter(df.id == 3), True)

    def test_select_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.select("time", "id"), True)


    def test_with_column_renamed_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.withColumnRenamed("forecast", "signal"), True)


    def test_replace_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.replace([3, 7], [4, 8], 'id'), True)


    def test_na_preserve_order(self):
        from tests.test_data import FORECAST_DATA
        from pyspark.sql.functions import lit
        from pyspark.sql.types import StringType

        def create_dataframe():
            return (self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
                    .withColumn("null_column", lit(None).cast(StringType())))

        self.shared_test_partition_preserving(lambda df: df.fillna("v1"), True, create_dataframe)
        self.shared_test_partition_preserving(lambda df: df.dropna(), True, create_dataframe)
        self.shared_test_partition_preserving(lambda df: df.fillna("v1").replace("v1", "v2", 'null_column'), True, create_dataframe)


    def test_with_column_udf_preserve_order(self):
        def with_udf_column(df):
            from pyspark.sql.types import DoubleType
            from pyspark.sql.functions import udf
            times_two = udf(lambda x: x * 2, DoubleType())
            return df.withColumn("forecast2", times_two(df.forecast))
        self.shared_test_partition_preserving(with_udf_column, True)


    def test_sort_dont_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.orderBy("id"), False)


    def test_repatition_dont_preserve_order(self):
        self.shared_test_partition_preserving(lambda df: df.repartition(df.rdd.getNumPartitions() * 2), False)


    def test_select_aggregate_dont_preserve_order(self):
        from pyspark.sql.functions import sum
        self.shared_test_partition_preserving(lambda df: df.select(sum('forecast')), False)


    def test_with_window_column_dont_preserve_order(self):
        def with_window_column(df):
            from pyspark.sql.window import Window
            from pyspark.sql.functions import percent_rank
            windowSpec = Window.partitionBy(df['id']).orderBy(df['forecast'])
            return df.withColumn("r", percent_rank().over(windowSpec))
        self.shared_test_partition_preserving(with_window_column, False)


    def test_explode_preserve_order(self):
        def with_explode_column(df):
            import pyspark.sql.functions as F
            df2 = df.withColumn('values', F.array(F.lit(1), F.lit(2)))
            df2 = df2.withColumn('value', F.explode(df2.values))
            return df2
        self.shared_test_partition_preserving(with_explode_column, True)


    def test_df_lazy(self):
        from tests.test_data import FORECAST_DATA

        df_lazy = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        assert(df_lazy._is_sorted is True)
        assert(df_lazy._tsrdd_part_info is None)


    def test_df_eager(self):
        from tests.test_data import FORECAST_DATA

        df_eager = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df_eager.timeSeriesRDD
        assert(df_eager._is_sorted)
        assert(df_eager._lazy_tsrdd is not None)
        assert(df_eager._tsrdd_part_info is None)


    def test_df_joined(self):
        from tests.test_data import FORECAST_DATA

        df = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df_joined = df.leftJoin(df, right_alias="right")
        assert(df_joined._is_sorted)
        assert(df_joined._tsrdd_part_info is not None)
        assert(df_joined._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_joined._jdf))


    def test_df_cached(self):
        from tests.test_data import FORECAST_DATA

        df_cached = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df_cached.cache()
        df_cached.count()
        assert(df_cached._is_sorted)
        assert(df_cached._tsrdd_part_info is None)
        assert(df_cached._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_cached._jdf))


    def test_df_cached_joined(self):
        from tests.test_data import FORECAST_DATA

        df_cached = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df_cached.cache()
        df_cached.count()
        df_cached_joined = df_cached.leftJoin(df_cached, right_alias="right")
        assert(df_cached_joined._is_sorted)
        assert(df_cached_joined._tsrdd_part_info is not None)
        assert(df_cached_joined._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_cached_joined._jdf))


    def test_df_orderBy(self):
        from tests.test_data import FORECAST_DATA

        df = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df = df.orderBy("time")
        assert(not df._is_sorted)
        assert(df._tsrdd_part_info is None)
