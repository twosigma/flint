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

from . import utils
from .dataframe import TimeSeriesDataFrame

def uniform(sql_ctx, frequency,
            offset = "0s",
            begin_date_time = "1900-01-01",
            end_date_time = "2100-01-01",
            time_zone = "UTC"):
    """ Returns an evenly sampled clock :class:`TimeSeriesDataFrame` which has only a "time" column.

    :param sql_ctx: pyspark.sql.SQLContext
    :param frequency: the time interval between rows, e.g "1s", "2m", "3d" etc.
    :param offset: the time to offset this clock from the begin time. Defaults to "0s".
        Note that specifying an offset greater than the frequency is the same as specifying (offset % frequency).
    :param begin_date_time: the begin date time of this clock. Defaults to "1900-01-01".
    :param end_date_time: the end date time of this clock. Defaults to "2100-01-01".
    :param time_zone: the time zone which will be used to parse `begin_date_time` and `end_date_time` when time
        zone information is not included in the date time string. Defaults to "UTC".
    :returns: a new :class:`TimeSeriesDataFrame`
    """
    sc = sql_ctx._sc
    scala_sc = utils.jvm(sc).org.apache.spark.api.java.JavaSparkContext.toSparkContext(sc._jsc)
    tsrdd = utils.jvm(sc).com.twosigma.flint.timeseries.Clocks.uniform(scala_sc, frequency, offset, begin_date_time, end_date_time, time_zone)
    return TimeSeriesDataFrame._from_tsrdd(tsrdd, sql_ctx)
