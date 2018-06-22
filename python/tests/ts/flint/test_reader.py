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

import os

import pandas as pd

from tests.utils import *

BASE = get_test_base()

class TestReader(BASE):
    def test_read_parquet(self):
        from pyspark.sql.functions import col

        columns = ['time', 'id', 'value']
        data_unsorted = [(3000, 1, 'a'),
                         (4000, 2, 'b'),
                         (2000, 3, 'c'),
                         (1000, 4, 'd')]
        df_unsorted = self.sqlContext.createDataFrame(data_unsorted, columns) \
            .withColumn('time', col('time').cast('timestamp'))

        path = "flint/test/parquet/{}".format(pd.Timestamp.now().value)
        # Write unsorted dataframe Parquet file for testing
        df_unsorted.write.parquet(path)

        actual_pdf = (self.flintContext.read
                      .option('isSorted', False)
                      .parquet(path).toPandas())
        expected_pdf = (make_pdf(data_unsorted, columns)
                        .sort_values(by='time')
                        .reset_index(drop=True))

        assert_same(actual_pdf, expected_pdf)

    def test_reader_parameters(self):
        from ts.flint import utils as flint_utils
        reader = (self.flintContext.read
                  .option("numPartitions", 5)
                  .option("partitionGranularity", '30d')
                  .option("columns", ['x', 'y', 'z'])
                  .option('timeUnit', 'ns')
                  .option('timeColumn', 'test_time')
                  .option("listOption", [1,2,3])
                  .option("boolOption", False)
                  .range('20170101', '2017-02-01', timezone='America/New_York'))

        options = reader._extra_options
        params = reader._parameters

        assert options['numPartitions'] == "5"
        assert options['partitionGranularity'] == "30d"

        assert params.range().beginNanos() == pd.Timestamp('20170101', tz='America/New_York').value
        assert params.range().endNanos()   == pd.Timestamp('20170201', tz='America/New_York').value
        assert params.columnsOrNull() == flint_utils.list_to_seq(self.sc, ['x', 'y', 'z'])

        assert params.timeUnit().toString() == 'NANOSECONDS'
        assert params.timeColumn() == 'test_time'

        assert options['listOption'] == '1,2,3'
        assert options['boolOption'] == 'False'

    def test_reader_reconcile_args(self):
        from ts.flint import utils as flint_utils
        reader = self.flintContext.read._reconcile_reader_args(
            begin='20170101',
            end='20170201',
            numPartitions=10,
            columns=['x', 'y', 'z']
        )
        params = reader._parameters
        options = reader._extra_options

        assert params.range().beginNanos() == pd.Timestamp('20170101').value
        assert params.range().endNanos() == pd.Timestamp('20170201').value
        assert params.columnsOrNull() == flint_utils.list_to_seq(self.sc, ['x', 'y', 'z'])
        assert options['numPartitions'] == '10'

    def test_reader_multiple_options(self):
        from ts.flint import utils as flint_utils
        reader = self.flintContext.read.options(
            columns=['x', 'y', 'z'],
            numPartitions=10,
            timeUnit='ms'
        )
        options = reader._extra_options
        params = reader._parameters
        assert options['columns'] == 'x,y,z'
        assert params.columnsOrNull() == flint_utils.list_to_seq(self.sc, ['x', 'y', 'z'])
        assert params.timeUnitString() == 'ms'
        assert options['numPartitions'] == '10'

    def test_reader_range(self):
        """
        It should parse begin / end ranges as datetime, integers in YYYYMMDD format,
        or strings parsable by ``pd.Timestamp``.
        """
        import pytz
        import datetime
        expected_begin = pd.Timestamp('20170101', tz='UTC')
        expected_end = pd.Timestamp('20170201', tz='UTC')

        # using timezone-aware datetime
        reader1 = self.flintContext.read.range(
            pytz.timezone("America/New_York").localize(
                datetime.datetime(2016, 12, 31, 19, 0, 0)),
            pytz.timezone("America/New_York").localize(
                datetime.datetime(2017, 1, 31, 19, 0, 0)))
        assert reader1._parameters.range().beginNanos() == expected_begin.value
        assert reader1._parameters.range().endNanos()   == expected_end.value

        # Using integers
        reader2 = self.flintContext.read.range(20170101, 20170201)
        assert reader2._parameters.range().beginNanos() == expected_begin.value
        assert reader2._parameters.range().endNanos()   == expected_end.value

        # Using Timestamps
        reader3 = self.flintContext.read.range(
            pd.Timestamp('2017-01-01', tz='UTC'),
            pd.Timestamp('2017-02-01', tz='UTC')
        )
        assert reader3._parameters.range().beginNanos() == expected_begin.value
        assert reader3._parameters.range().endNanos()   == expected_end.value

    def test_reader_missing_range(self):
        """It should raise an IllegalArgumentException when the range is not set"""
        from pyspark.sql.utils import IllegalArgumentException

        with self.assertRaises(IllegalArgumentException):
            self.flintContext.read._parameters.range().beginNanos()

        with self.assertRaises(IllegalArgumentException):
            reader1 = self.flintContext.read.range(None, '20170101')
            reader1._parameters.range().beginNanos()

        with self.assertRaises(IllegalArgumentException):
            reader2 = self.flintContext.read.range('20170101', None)
            reader2._parameters.range().endNanos()

    def test_reader_expand(self):
        expected_begin = pd.Timestamp('20161231', tz='UTC')
        expected_end = pd.Timestamp('20170203', tz='UTC')

        reader1 = self.flintContext.read.range('20170101', '20170201').expand(end="2day")
        assert reader1._parameters.range().endNanos() == expected_end.value

        reader2 = self.flintContext.read.range('20170101', '20170201').expand(begin="1day")
        assert reader2._parameters.range().beginNanos() == expected_begin.value

        reader3 = self.flintContext.read.range('20170101', '20170201').expand("1day", "2days")
        assert reader3._parameters.range().beginNanos() == expected_begin.value
        assert reader3._parameters.range().endNanos() == expected_end.value

    def test_read_dataframe_begin_end(self):
        # Data goes from time 1000 to 1250
        from tests.test_data import VOL_DATA
        pdf = make_pdf(VOL_DATA, ['time', 'id', 'volume'])
        pdf['time'] = pdf.time.astype('long')
        df = self.sqlContext.createDataFrame(pdf)
        begin_nanos, end_nanos = 1100, 1200

        df = self.flintContext.read.range(begin_nanos, end_nanos).dataframe(df)
        expected_df = df.filter(df.time >= make_timestamp(begin_nanos)) \
            .filter(df.time < make_timestamp(end_nanos))
        expected = expected_df.count()
        assert(df.count() == expected)

        df2 = self.flintContext.read.range(begin_nanos, end_nanos).dataframe(df)
        assert(df2.count() == expected)

    def test_read_dataframe_unsorted(self):
        columns = ['time', 'value']
        data = [(4000, 4),
                (2000, 2),
                (3000, 3),
                (1000, 1)]
        pdf_shuffled = make_pdf(data, columns)
        pdf_sorted = pdf_shuffled.sort_values(by='time').reset_index(drop=True)

        df_sorted = self.sqlContext.createDataFrame(pdf_sorted)
        df_shuffled = self.sqlContext.createDataFrame(pdf_shuffled)

        # default isSorted is True
        tsdf_sorted = (self.flintContext.read.dataframe(df_sorted)).toPandas()
        tsdf_shuffled = (self.flintContext.read
                         .option('isSorted', False)
                         .dataframe(df_shuffled)).toPandas()

        assert_same(tsdf_sorted, tsdf_shuffled)

    def test_read_pandas_unsorted(self):
        columns = ['time', 'value']
        data = [(4000, 4),
                (2000, 2),
                (3000, 3),
                (1000, 1)]
        pdf_shuffled = make_pdf(data, columns)
        pdf_sorted = pdf_shuffled.sort_values(by='time').reset_index(drop=True)

        # default isSorted is True
        tsdf_sorted = (self.flintContext.read.pandas(pdf_sorted)).toPandas()
        tsdf_shuffled = (self.flintContext.read
                         .option('isSorted', False)
                         .pandas(pdf_shuffled)).toPandas()

        assert_same(tsdf_sorted, tsdf_shuffled)