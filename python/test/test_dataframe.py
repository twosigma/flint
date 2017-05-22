#
#  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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
import unittest

from py_spark_test_case import PySparkTestCase
from pyspark.sql import types
from pyspark.sql.functions import col
from ts.flint import summarizers
import ts.flint.dataframe

class TestDataframe(PySparkTestCase):
    def date_parser(self, fmt):
        @ts.flint.udf(types.LongType())
        def parse(x):
            dt = types.datetime.datetime.strptime(str(x), fmt)
            return int(dt.strftime("%s%f"))
        return parse

    def test_summary(self):
        weather = (self.sqlContext.read.csv('examples/weather.csv', header=True, inferSchema=True)
                   .withColumn('time', self.date_parser('%Y%m%d')(col('DATE'))))
        spy = (self.sqlContext.read.csv('examples/spy.csv', header=True, inferSchema=True)
               .withColumn('time', self.date_parser('%Y-%m-%d %H:%M:%S')(col('DATE'))))
        weather_df = self.flintContext.read.dataframe(weather, is_sorted=False)
        spy_df = self.flintContext.read.dataframe(spy, is_sorted=False)
        joined = spy_df.leftJoin(weather_df, tolerance="3d")
        joined = joined.withColumn('change', joined.Close - joined.Open)
        self.assertEqual(joined.__class__.__name__, "TimeSeriesDataFrame")
        summary = joined.summarize(summarizers.linear_regression('change', ['PRCP', 'SNOW'])).toPandas()
        self.assertEqual(summary.__class__.__name__, "DataFrame")

if __name__ == '__main__':
    unittest.main()
