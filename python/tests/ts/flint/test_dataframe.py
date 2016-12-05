##
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

import numpy  as np
import os
import pandas as pd
import pytest
import shutil
import sys
import pandas.util.testing as pdt

from ts.spark import pypusa
from ts.elastic import test_support

# The fixtures in this test are weird.  Because we don't have pyspark
# on our path until after we have pypusa add it to our path, we
# actually can't import a bunch of things, including pyspark.sql.*,
# and even flint itself.  Therefore, we implement imports as
# fixtures, and rely on them to depend on each other to force the
# ordering.  This also means that each test needs to "declare" what
# imports it wants in its parameters.
#
# This could stand to be cleaned up and centralized, because we do
# similar things in other tests in ts-spark.

# Pypusa and ts.spark.setup modify the environment and sys.path.  In
# order to run both without messing each other up, we have to reset
# these variables back to their original values.
original_environ = dict(os.environ)
original_sys_path = list(sys.path)


@pytest.yield_fixture(scope='module', autouse=True)
def cleanup():
    yield
    reset_env()


def reset_env():
    os.environ = dict(original_environ)
    sys.path = list(original_sys_path)
    metastore_db = os.path.join(os.getcwd(), 'metastore_db')
    if os.path.isdir(metastore_db):
        shutil.rmtree(metastore_db)


@pytest.fixture(scope='module')
def launcher_params():
    # TODO: run on real datacenters for more thorough integration
    #       tests, but not often
    return {'datacenter': 'local',
            'codebase': '/opt/ts/services/spark-ts.ts_spark_1_6/',
            'num_executors': 1,
            'executor_cpus': 1,
            'executor_memory': (1*1024**3),
            'driver_memory': (1*1024**3)}


@pytest.fixture(scope='module')
def launcher(launcher_params):
    with test_support.suppress_instrumentation:
        return pypusa.Launcher(**launcher_params)


@pytest.fixture(scope='module')
def pyspark(launcher):
    return launcher.import_pyspark()


@pytest.fixture(scope='module')
def pyspark_types(pyspark):
    import pyspark.sql.types as pyspark_types
    return pyspark_types


@pytest.fixture(scope='module')
def py4j(pyspark):
    import py4j
    return py4j


@pytest.fixture(scope='module')
def flint(pyspark):
    import ts.flint
    return ts.flint


@pytest.fixture(scope='module')
def summarizers(flint):
    from ts.flint import summarizers
    return summarizers


@pytest.fixture(scope='module')
def windows(flint):
    from ts.flint import windows
    return windows


@pytest.yield_fixture(scope='module')
def sc(launcher, pyspark):
    with test_support.suppress_instrumentation:
        with launcher.create_spark_context() as sc:
            yield sc
    from pyspark import SparkContext
    SparkContext._gateway.shutdown()
    SparkContext._gateway = None
    reset_env()


@pytest.fixture(scope='module')
def sqlContext(pyspark, sc):
    from pyspark.sql import HiveContext
    return HiveContext(sc)


@pytest.fixture(scope='module')
def flintContext(pyspark, sqlContext):
    from ts.flint import FlintContext
    return FlintContext(sqlContext)


@pytest.fixture(scope='module')
def tests_utils(flint):
    from . import utils
    return utils


def make_pdf(data, schema):
    return pd.DataFrame({
        schema[i]:[row[i] for row in data] for i in range(len(schema))
    })[schema]


intervals_data = [
    (1000,),
    (1100,),
    (1200,),
    (1300,)
]


forecast_data = [
    (1000, 7, 3.0,),
    (1000, 3, 5.0,),
    (1050, 3, -1.5,),
    (1050, 7, 2.0,),
    (1100, 3, -2.4,),
    (1100, 7, 6.4,),
    (1150, 3, 1.5,),
    (1150, 7, -7.9,),
    (1200, 3, 4.6,),
    (1200, 7, 1.4,),
    (1250, 3, -9.6,),
    (1250, 7, 6.0,)
]


price_data = [
    (1000, 7, 0.5,),
    (1000, 3, 1.0,),
    (1050, 3, 1.5,),
    (1050, 7, 2.0,),
    (1100, 3, 2.5,),
    (1100, 7, 3.0,),
    (1150, 3, 3.5,),
    (1150, 7, 4.0,),
    (1200, 3, 4.5,),
    (1200, 7, 5.0,),
    (1250, 3, 5.5,),
    (1250, 7, 6.0,)
]


vol_data = [
    (1000, 7, 100,),
    (1000, 3, 200,),
    (1050, 3, 300,),
    (1050, 7, 400,),
    (1100, 3, 500,),
    (1100, 7, 600,),
    (1150, 3, 700,),
    (1150, 7, 800,),
    (1200, 3, 900,),
    (1200, 7, 1000,),
    (1250, 3, 1100,),
    (1250, 7, 1200,)
]


vol2_data = [
    (1000, 7, 100,),
    (1000, 7, 100,),
    (1000, 3, 200,),
    (1000, 3, 200,),
    (1050, 3, 300,),
    (1050, 3, 300,),
    (1050, 7, 400,),
    (1050, 7, 400,),
    (1100, 3, 500,),
    (1100, 7, 600,),
    (1100, 3, 500,),
    (1100, 7, 600,),
    (1150, 3, 700,),
    (1150, 7, 800,),
    (1150, 3, 700,),
    (1150, 7, 800,),
    (1200, 3, 900,),
    (1200, 7, 1000,),
    (1200, 3, 900,),
    (1200, 7, 1000,),
    (1250, 3, 1100,),
    (1250, 7, 1200,),
    (1250, 3, 1100,),
    (1250, 7, 1200,)
]


vol3_data = [
    (1000, 7, 100,),
    (1000, 7, 101,),
    (1000, 3, 200,),
    (1000, 3, 201,),
    (1050, 3, 300,),
    (1050, 3, 301,),
    (1050, 7, 400,),
    (1050, 7, 401,),
    (1100, 3, 500,),
    (1100, 7, 600,),
    (1100, 3, 501,),
    (1100, 7, 601,),
    (1150, 3, 700,),
    (1150, 7, 800,),
    (1150, 3, 701,),
    (1150, 7, 801,),
    (1200, 3, 900,),
    (1200, 7, 1000,),
    (1200, 3, 901,),
    (1200, 7, 1001,),
    (1250, 3, 1100,),
    (1250, 7, 1200,),
    (1250, 3, 1101,),
    (1250, 7, 1201,),
]


@pytest.fixture(scope='module')
def intervals(flintContext):
    return flintContext.read.pandas(make_pdf(intervals_data, ['time']))


@pytest.fixture(scope='module')
def forecast(flintContext):
    return flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))


@pytest.fixture(scope='module')
def price(flintContext):
    return flintContext.read.pandas(make_pdf(price_data, ["time", "tid", "price"]))


@pytest.fixture(scope='module')
def vol(flintContext):
    return flintContext.read.pandas(make_pdf(vol_data, ["time", "tid", "volume"]))


@pytest.fixture(scope='module')
def vol2(flintContext):
    return flintContext.read.pandas(make_pdf(vol2_data, ["time", "tid", "volume"]))


@pytest.fixture(scope='module')
def vol3(flintContext):
    return flintContext.read.pandas(make_pdf(vol3_data, ["time", "tid", "volume"]))


#--[ Tests ]--------------------------------------
def test_py4j(flint, tests_utils, sc):
    jpkg = flint.dataframe.java.Packages(sc)
    tests_utils.assert_java_object_exists(jpkg.TimeSeriesRDD, "TimeSeriesRDD")
    tests_utils.assert_java_object_exists(jpkg.WaiterClient, "WaiterClient")
    tests_utils.assert_java_object_exists(jpkg.Summarizers, "Summarizers")
    tests_utils.assert_java_object_exists(jpkg.Windows, "Window")
    tests_utils.assert_java_object_exists(jpkg.alf, "alf")
    tests_utils.assert_java_object_exists(jpkg.alf.defaultAlfRequestsPerPartition, "alf.defaultAlfRequestsPerPartition")
    tests_utils.assert_java_object_exists(jpkg.alf.defaultWaiterConfig, "alf.defaultWaiterConfig")
    tests_utils.assert_java_object_exists(jpkg.ColumnWhitelist, "ColumnWhitelist")


def test_addColumnsForCycle(pyspark_types, tests_utils, price, vol3):
    expected_pdf = make_pdf([
        [1000, 7, 0.5, 1.0],
        [1000, 3, 1.0, 2.0],
        [1050, 3, 1.5, 3.0],
        [1050, 7, 2.0, 4.0],
        [1100, 3, 2.5, 5.0],
        [1100, 7, 3.0, 6.0],
        [1150, 3, 3.5, 7.0],
        [1150, 7, 4.0, 8.0],
        [1200, 3, 4.5, 9.0],
        [1200, 7, 5.0, 10.0],
        [1250, 3, 5.5, 11.0],
        [1250, 7, 6.0, 12.0],
    ], ["time", "tid", "price", "adjustedPrice"])

    def fn(rows):
        size = len(rows)
        return {row:row.price*size for row in rows}

    new_pdf = price.addColumnsForCycle(
        {"adjustedPrice": (pyspark_types.DoubleType(), fn)}
    ).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)

    expected_pdf = make_pdf([
        [1000, 7, 100, 301],
        [1000, 7, 101, 302],
        [1000, 3, 200, 601],
        [1000, 3, 201, 602],
        [1050, 7, 400, 1201],
        [1050, 7, 401, 1202],
        [1050, 3, 300, 901],
        [1050, 3, 301, 902],
        [1100, 7, 600, 1801],
        [1100, 7, 601, 1802],
        [1100, 3, 500, 1501],
        [1100, 3, 501, 1502],
        [1150, 7, 800, 2401],
        [1150, 7, 801, 2402],
        [1150, 3, 700, 2101],
        [1150, 3, 701, 2102],
        [1200, 7, 1000, 3001],
        [1200, 7, 1001, 3002],
        [1200, 3, 900, 2701],
        [1200, 3, 901, 2702],
        [1250, 7, 1200, 3601],
        [1250, 7, 1201, 3602],
        [1250, 3, 1100, 3301],
        [1250, 3, 1101, 3302],
    ], ["time", "tid", "volume", "totalVolume"])

    def fn(rows):
        volsum = sum([row.volume for row in rows])
        return {row:row.volume + volsum for row in rows}

    new_pdf = vol3.addColumnsForCycle(
        {"totalVolume": (pyspark_types.LongType(), fn)},
        key=["tid"]
    ).toPandas()

    # Test API to support key as list.
    tests_utils.assert_same(
        new_pdf,
        vol3.addColumnsForCycle(
            {"totalVolume": (pyspark_types.LongType(), fn)},
            key="tid"
        ).toPandas()
    )

    # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf, "with key")
    # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 3].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 3].reset_index(drop=True),
        "with key 3"
    )
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 7].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 7].reset_index(drop=True),
        "with key 7"
    )


def test_leftJoin(pyspark_types, tests_utils, price, vol):
    expected_pdf = make_pdf([
        (1000, 7, 0.5, 100,),
        (1000, 3, 1.0, 200,),
        (1050, 3, 1.5, 300,),
        (1050, 7, 2.0, 400,),
        (1100, 3, 2.5, 500,),
        (1100, 7, 3.0, 600,),
        (1150, 3, 3.5, 700,),
        (1150, 7, 4.0, 800,),
        (1200, 3, 4.5, 900,),
        (1200, 7, 5.0, 1000,),
        (1250, 3, 5.5, 1100,),
        (1250, 7, 6.0, 1200,)
    ], ["time", "tid", "price", "volume"])

    new_pdf = price.leftJoin(vol, key=["tid"]).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)
    tests_utils.assert_same(new_pdf, price.leftJoin(vol, key="tid").toPandas())

    expected_pdf = make_pdf([
        (1000, 7, 0.5, 100),
        (1000, 3, 1.0, 200),
        (1050, 3, 1.5, None),
        (1050, 7, 2.0, None),
        (1100, 3, 2.5, 500),
        (1100, 7, 3.0, 600),
        (1150, 3, 3.5, 700),
        (1150, 7, 4.0, 800),
        (1200, 3, 4.5, 900),
        (1200, 7, 5.0, 1000),
        (1250, 3, 5.5, 1100),
        (1250, 7, 6.0, 1200),
    ], ["time", "tid", "price", "volume"])

    new_pdf = price.leftJoin(vol.filter(vol.time != 1050), key="tid").toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_futureLeftJoin(pyspark_types, tests_utils, price, vol):
    expected_pdf = make_pdf([
        (1000, 7, 0.5, 400, 1050),
        (1000, 3, 1.0, 300, 1050),
        (1050, 3, 1.5, 500, 1100),
        (1050, 7, 2.0, 600, 1100),
        (1100, 3, 2.5, 700, 1150),
        (1100, 7, 3.0, 800, 1150),
        (1150, 3, 3.5, 900, 1200),
        (1150, 7, 4.0, 1000, 1200),
        (1200, 3, 4.5, 1100, 1250),
        (1200, 7, 5.0, 1200, 1250),
        (1250, 3, 5.5, None, None),
        (1250, 7, 6.0, None, None),
    ], ["time", "tid", "price", "volume", "time2"])

    new_pdf = price.futureLeftJoin(vol.withColumn("time2", vol.time.cast(pyspark_types.LongType())),
                                     tolerance=pd.Timedelta("100ns"),
                                     key=["tid"], strict_lookahead=True).toPandas()
    new_pdf1 = price.futureLeftJoin(vol.withColumn("time2", vol.time.cast(pyspark_types.LongType())),
                                     tolerance=pd.Timedelta("100ns"),
                                     key="tid", strict_lookahead=True).toPandas()
    tests_utils.assert_same(new_pdf, new_pdf1)
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_groupByCycle(tests_utils, vol):
    expected_pdf1 = make_pdf([
        (1000, [(1000, 7, 100,), (1000, 3, 200,)]),
        (1050, [(1050, 3, 300,), (1050, 7, 400,)]),
        (1100, [(1100, 3, 500,), (1100, 7, 600,)]),
        (1150, [(1150, 3, 700,), (1150, 7, 800,)]),
        (1200, [(1200, 3, 900,), (1200, 7, 1000,)]),
        (1250, [(1250, 3, 1100,), (1250, 7, 1200,)]),
    ], ["time", "rows"])

    new_pdf1 = vol.groupByCycle().toPandas()
    tests_utils.assert_same(new_pdf1, expected_pdf1)


def test_groupByInterval(tests_utils, vol, intervals):
    tid = vol.collect()

    expected_pdf = make_pdf([
        (1000, 7, [tid[0], tid[3]]),
        (1000, 3, [tid[1], tid[2]]),
        (1100, 7, [tid[5], tid[7]]),
        (1100, 3, [tid[4], tid[6]]),
        (1200, 7, [tid[9], tid[11]]),
        (1200, 3, [tid[8], tid[10]]),
    ], ["time", "tid", "rows"])

    new_pdf = vol.groupByInterval(intervals, key=["tid"]).toPandas()
    new_pdf1 = vol.groupByInterval(intervals, key="tid").toPandas()
    tests_utils.assert_same(new_pdf, new_pdf1)

    # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf)
    # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 3].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 3].reset_index(drop=True),
    )
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 7].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 7].reset_index(drop=True),
    )


def test_summarizeCycles(summarizers, tests_utils, vol, vol2):
    expected_pdf1 = make_pdf([
        (1000, 300.0,),
        (1050, 700.0,),
        (1100, 1100.0,),
        (1150, 1500.0,),
        (1200, 1900.0,),
        (1250, 2300.0,),
    ], ["time", "volume_sum"])
    new_pdf1 = vol.summarizeCycles(summarizers.sum("volume")).toPandas()
    tests_utils.assert_same(new_pdf1, expected_pdf1)

    expected_pdf2 = make_pdf([
        (1000, 3, 400.0),
        (1000, 7, 200.0),
        (1050, 3, 600.0),
        (1050, 7, 800.0),
        (1100, 3, 1000.0),
        (1100, 7, 1200.0),
        (1150, 3, 1400.0),
        (1150, 7, 1600.0),
        (1200, 3, 1800.0),
        (1200, 7, 2000.0),
        (1250, 3, 2200.0),
        (1250, 7, 2400.0),
    ], ["time", "tid", "volume_sum"])
    new_pdf2 = vol2.summarizeCycles(summarizers.sum("volume"), key="tid").toPandas()
    tests_utils.assert_same(new_pdf2, expected_pdf2)


def test_summarizeIntervals(flintContext, tests_utils, summarizers, vol):
    clock = flintContext.read.pandas(make_pdf([
        (1000,),
        (1100,),
        (1200,),
        (1300,),
    ], ["time"]))

    new_pdf1 = vol.summarizeIntervals(clock, summarizers.sum("volume")).toPandas()
    expected_pdf1 = make_pdf([
        (1000, 1000.0),
        (1100, 2600.0),
        (1200, 4200.0),
    ], ["time", "volume_sum"])
    tests_utils.assert_same(new_pdf1, expected_pdf1)

    new_pdf2 = vol.summarizeIntervals(clock, summarizers.sum("volume"), key="tid").toPandas()
    expected_pdf2 = make_pdf([
        (1000, 3, 500.0),
        (1000, 7, 500.0),
        (1100, 3, 1200.0),
        (1100, 7, 1400.0),
        (1200, 3, 2000.0),
        (1200, 7, 2200.0),
    ], ["time", "tid", "volume_sum"])

    tests_utils.assert_same(new_pdf2, expected_pdf2)


def test_summarizeWindows(flintContext, tests_utils, windows, summarizers, vol):
    new_pdf1 = vol.summarizeWindows(windows.past_absolute_time('99ns'), summarizers.sum("volume")).toPandas()
    expected_pdf1 = make_pdf([
        (1000, 7, 100, 300.0),
        (1000, 3, 200, 300.0),
        (1050, 3, 300, 1000.0),
        (1050, 7, 400, 1000.0),
        (1100, 3, 500, 1800.0),
        (1100, 7, 600, 1800.0),
        (1150, 3, 700, 2600.0),
        (1150, 7, 800, 2600.0),
        (1200, 3, 900, 3400.0),
        (1200, 7, 1000, 3400.0),
        (1250, 3, 1100, 4200.0),
        (1250, 7, 1200, 4200.0),
    ], ["time", "tid", "volume", "volume_sum"])
    tests_utils.assert_same(new_pdf1, expected_pdf1)

    new_pdf2 = (vol.summarizeWindows(windows.past_absolute_time('99ns'),
                                     summarizers.sum("volume"),
                                     key="tid").toPandas())
    expected_pdf2 = make_pdf([
        (1000, 7, 100, 100.0),
        (1000, 3, 200, 200.0),
        (1050, 3, 300, 500.0),
        (1050, 7, 400, 500.0),
        (1100, 3, 500, 800.0),
        (1100, 7, 600, 1000.0),
        (1150, 3, 700, 1200.0),
        (1150, 7, 800, 1400.0),
        (1200, 3, 900, 1600.0),
        (1200, 7, 1000, 1800.0),
        (1250, 3, 1100, 2000.0),
        (1250, 7, 1200, 2200.0),
    ], ["time", "tid", "volume", "volume_sum"])
    tests_utils.assert_same(new_pdf2, expected_pdf2)


def test_summary_sum(summarizers, tests_utils, vol):
    expected_pdf = make_pdf([
        (0, 7800.0,)
    ], ["time", "volume_sum"])

    new_pdf = vol.summarize(summarizers.sum("volume")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)

    expected_pdf = make_pdf([
        (0, 7, 4100.0,),
        (0, 3, 3700.0,),
    ], ["time", "tid", "volume_sum"])

    new_pdf = vol.summarize(summarizers.sum("volume"), key=["tid"]).toPandas()
    new_pdf1 = vol.summarize(summarizers.sum("volume"), key="tid").toPandas()
    tests_utils.assert_same(new_pdf, new_pdf1)

    # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf, "by tid")
    # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 3].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 3].reset_index(drop=True),
        "by tid 3"
    )
    tests_utils.assert_same(
        new_pdf[new_pdf['tid'] == 7].reset_index(drop=True),
        expected_pdf[expected_pdf['tid'] == 7].reset_index(drop=True),
        "by tid 7"
    )


def test_summary_zscore(summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 1.5254255396193801,)
    ], ["time", "price_zScore"])

    new_pdf = price.summarize(summarizers.zscore("price", in_sample=True)).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "in-sample")

    expected_pdf = make_pdf([
        (0, 1.8090680674665818,)
    ], ["time", "price_zScore"])

    new_pdf = price.summarize(summarizers.zscore("price", in_sample=False)).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "out-of-sample)")


def test_summary_nth_moment(summarizers, tests_utils, price):
    moments = [price.summarize(summarizers.nth_moment("price", i), key="tid").collect() for i in range(5)]
    for m in moments:
        m.sort(key=lambda r: r['tid'])
    moments = [[r["price_{}thMoment".format(i)] for r in moments[i]] for i in range(len(moments))]

    tests_utils.assert_same(moments[0][0], 1.0, "moment 0: 0")
    tests_utils.assert_same(moments[0][1], 1.0, "moment 0: 1")

    tests_utils.assert_same(moments[1][0], 3.0833333333333335, "moment 1: 1")
    tests_utils.assert_same(moments[1][1], 3.416666666666667, "moment 1: 0")

    tests_utils.assert_same(moments[2][0], 12.041666666666668, "moment 2: 1")
    tests_utils.assert_same(moments[2][1], 15.041666666666666, "moment 2: 0")

    tests_utils.assert_same(moments[3][0], 53.39583333333333, "moment 3: 1")
    tests_utils.assert_same(moments[3][1], 73.35416666666667, "moment 3: 0")

    tests_utils.assert_same(moments[4][0], 253.38541666666669, "moment 4: 1")
    tests_utils.assert_same(moments[4][1], 379.0104166666667, "moment 4: 0")


def test_summary_nth_central_moment(summarizers, tests_utils, price):
    moments = [price.summarize(summarizers.nth_central_moment("price", i), key="tid").collect() for i in range(1,5)]
    for m in moments:
        m.sort(key=lambda r: r['tid'])
    moments = [[r["price_{}thCentralMoment".format(i+1)] for r in moments[i]] for i in range(len(moments))]

    tests_utils.assert_same(moments[0][0], 0.0, "moment 1: 0")
    tests_utils.assert_same(moments[0][1], 0.0, "moment 1: 1")

    tests_utils.assert_same(moments[1][0], 2.534722222222222, "moment 2: 1")
    tests_utils.assert_same(moments[1][1], 3.3680555555555554, "moment 2: 0")

    tests_utils.assert_same(moments[2][0], 0.6365740740740735, "moment 3: 1")
    tests_utils.assert_same(moments[2][1], -1.0532407407407405, "moment 3: 0")

    tests_utils.assert_same(moments[3][0], 10.567563657407407, "moment 4: 1")
    tests_utils.assert_same(moments[3][1], 21.227285879629633, "moment 4: 0")


def test_summary_correlation(pyspark, summarizers, tests_utils, price, forecast):
    joined = price.leftJoin(forecast, key="tid")
    joined = (joined
              .withColumn("price2", joined.price)
              .withColumn("price3", -joined.price)
              .withColumn("price4", 2 * joined.price)
              .withColumn("price5", pyspark.sql.functions.lit(0)))

    def price_correlation(column):
        corr = joined.summarize(summarizers.correlation("price", column), key=["tid"])
        tests_utils.assert_same(
            corr.toPandas(),
            joined.summarize(summarizers.correlation(["price"], [column]), key="tid").toPandas()
        )
        tests_utils.assert_same(
            corr.toPandas(),
            joined.summarize(summarizers.correlation(["price", column]), key="tid").toPandas()
        )
        return corr.collect()

    results = [price_correlation("price{}".format(i)) for i in range(2,6)]
    for r in results:
        r.sort(key=lambda r: r['tid'])
    results.append(price_correlation("forecast"))

    tests_utils.assert_same(results[0][0]["price_price2_correlation"], 1.0, "price2: 1")
    tests_utils.assert_same(results[0][1]["price_price2_correlation"], 1.0, "price2: 0")

    tests_utils.assert_same(results[1][0]["price_price3_correlation"], -1.0, "price3: 1")
    tests_utils.assert_same(results[1][1]["price_price3_correlation"], -1.0, "price3: 0")

    tests_utils.assert_same(results[2][0]["price_price4_correlation"], 1.0, "price4: 1")
    tests_utils.assert_same(results[2][1]["price_price4_correlation"], 1.0, "price4: 0")

    tests_utils.assert_true(np.isnan(results[3][0]["price_price5_correlation"]), "price5: 1")
    tests_utils.assert_true(np.isnan(results[3][1]["price_price5_correlation"]), "price5: 0")

    tests_utils.assert_same(results[4][0]["price_forecast_correlation"], -0.47908485866330514, "forecast: 1")
    tests_utils.assert_same(results[4][0]["price_forecast_correlationTStat"], -1.0915971793294055, "forecastTStat: 1")
    tests_utils.assert_same(results[4][1]["price_forecast_correlation"], -0.021896121374023046, "forecast: 0")
    tests_utils.assert_same(results[4][1]["price_forecast_correlationTStat"], -0.04380274440368827, "forecastTStat: 0")


def test_summary_linearRegression(pyspark, summarizers, tests_utils, price, forecast):
    """
    Test the python binding for linearRegression. This does NOT test the correctness of the regression.
    """
    joined = price.leftJoin(forecast, key="tid")
    result = joined.summarize(summarizers.linear_regression("price", ["forecast"])).collect()


def test_summary_mean(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 3.25,)
    ], ["time", "price_mean"])
    joined = price.leftJoin(forecast, key="tid")
    result = joined.summarize(summarizers.mean("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_summary_stddev(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 1.802775638,)
    ], ["time", "price_stddev"])
    joined = price.leftJoin(forecast, key="tid")
    result = joined.summarize(summarizers.stddev("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_summary_variance(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 3.25,)
    ], ["time", "price_variance"])
    joined = price.leftJoin(forecast, key="tid")
    result = joined.summarize(summarizers.variance("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_summary_covariance(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, -1.802083333,)
    ], ["time", "price_forecast_covariance"])
    joined = price.leftJoin(forecast, key="tid")
    result = joined.summarize(summarizers.covariance("price", "forecast")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_addSummaryColumns(summarizers, tests_utils, vol):
    expected_pdf = make_pdf([
        (1000, 7, 100, 100.0),
        (1000, 3, 200, 300.0),
        (1050, 3, 300, 600.0),
        (1050, 7, 400, 1000.0),
        (1100, 3, 500, 1500.0),
        (1100, 7, 600, 2100.0),
        (1150, 3, 700, 2800.0),
        (1150, 7, 800, 3600.0),
        (1200, 3, 900, 4500.0),
        (1200, 7, 1000, 5500.0),
        (1250, 3, 1100, 6600.0),
        (1250, 7, 1200, 7800.0),
    ], ["time", "tid", "volume", "volume_sum"])

    new_pdf = vol.addSummaryColumns(summarizers.sum("volume")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)

    expected_pdf = make_pdf([
        (1000, 7, 100, 100.0),
        (1000, 3, 200, 200.0),
        (1050, 3, 300, 500.0),
        (1050, 7, 400, 500.0),
        (1100, 3, 500, 1000.0),
        (1100, 7, 600, 1100.0),
        (1150, 3, 700, 1700.0),
        (1150, 7, 800, 1900.0),
        (1200, 3, 900, 2600.0),
        (1200, 7, 1000, 2900.0),
        (1250, 3, 1100, 3700.0),
        (1250, 7, 1200, 4100.0),
    ], ["time", "tid", "volume", "volume_sum"])

    new_pdf = vol.addSummaryColumns(summarizers.sum("volume"), "tid").toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "with key")


def test_addWindows(tests_utils, windows, vol):
    tid = vol.collect()

    expected_pdf = make_pdf([
        (1000, 7, 100, [tid[0], tid[1]]),
        (1000, 3, 200, [tid[0], tid[1]]),
        (1050, 3, 300, [tid[0], tid[1], tid[2], tid[3]]),
        (1050, 7, 400, [tid[0], tid[1], tid[2], tid[3]]),
        (1100, 3, 500, [tid[2], tid[3], tid[4], tid[5]]),
        (1100, 7, 600, [tid[2], tid[3], tid[4], tid[5]]),
        (1150, 3, 700, [tid[4], tid[5], tid[6], tid[7]]),
        (1150, 7, 800, [tid[4], tid[5], tid[6], tid[7]]),
        (1200, 3, 900, [tid[6], tid[7], tid[8], tid[9]]),
        (1200, 7, 1000, [tid[6], tid[7], tid[8], tid[9]]),
        (1250, 3, 1100, [tid[8], tid[9], tid[10], tid[11]]),
        (1250, 7, 1200, [tid[8], tid[9], tid[10], tid[11]]),
    ], ["time", "tid", "volume", "window_past_50ns"])

    new_pdf = vol.addWindows(windows.past_absolute_time("50ns")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_shiftTime(tests_utils, price):
    expected_pdf = price.toPandas()
    expected_pdf.time += 1000
    new_pdf = price.shiftTime(pd.Timedelta("1000ns")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "forwards")

    expected_pdf = price.toPandas()
    expected_pdf.time -= 1000
    new_pdf = price.shiftTime(pd.Timedelta("1000ns"), backwards=True).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "backwards")


@pytest.mark.net
def test_read_dataframe_begin_end(sqlContext, flintContext, tests_utils):
    test_url="hdfs:///user/tsram/spark_example_data/tsdata/price/ts/datasys/6558"
    df = sqlContext.read.parquet(test_url)

    begin = "20100101"
    end = "20110101"

    begin_nanos = tests_utils.to_nanos(begin)
    end_nanos = tests_utils.to_nanos(end)

    df = flintContext.read.dataframe(df, begin, end)
    df2 = df.filter(df.time >= begin_nanos).filter(df.time < end_nanos)

    assert(df.count() == df2.count())


def test_na_preserve_order(sqlContext, flintContext):
    from pyspark.sql.functions import lit
    from pyspark.sql.types import StringType

    df_lazy = (flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
               .withColumn("null_column", lit(None).cast(StringType())))

    df_eager = (flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
                .withColumn("null_column", lit(None).cast(StringType())))
    df_eager.timeSeriesRDD

    ops = [lambda df: df,
           lambda df: df.sort("tid"),
           lambda df: df.repartition(int(df.rdd.getNumPartitions() / 2)),
           lambda df: df.repartition(df.rdd.getNumPartitions() * 2),
           lambda df: df.repartition("tid")]

    for df in [df_eager]:
        for op in ops:
            test_df = op(df)
            assert_invariants_unchanged(test_df, lambda df: df.fillna("v1"))
            assert_invariants_unchanged(test_df, lambda df: df.na.fill("v1"))
            assert_invariants_unchanged(test_df, lambda df: df.dropna())
            assert_invariants_unchanged(test_df, lambda df: df.na.drop())
            assert_invariants_unchanged(test_df, lambda df: df.fillna("v1").replace("v1", "v2", 'null_column'))
            assert_invariants_unchanged(test_df, lambda df: df.fillna("v1").na.replace("v1", "v2", 'null_column'))


def test_with_column_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.withColumn("neg_forecast", -df.forecast)

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_drop_column_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.drop("forecast")

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_filter_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.filter(df.tid == 3)

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_select_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.select("time", "tid")

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_with_column_renamed_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.withColumnRenamed("forecast", "signal")

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_replace_preserve_order(sqlContext, flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    df_eager.timeSeriesRDD

    def func(df):
        return df.replace([3, 7], [4, 8], 'tid')

    assert_invariants_unchanged(df_lazy, func)
    assert_invariants_unchanged(df_eager, func)


def test_from_tsrdd(sqlContext, flintContext, flint):
    df = flintContext.read.pandas(make_pdf(forecast_data, ["time", "tid", "forecast"]))
    tsrdd = df.timeSeriesRDD
    df2 = flint.TimeSeriesDataFrame._from_tsrdd(tsrdd, sqlContext)
    tsrdd2 = df2.timeSeriesRDD

    assert(tsrdd.count() == tsrdd2.count())
    assert(tsrdd.orderedRdd().getNumPartitions() == tsrdd2.orderedRdd().getNumPartitions())


def assert_invariants_unchanged(input_df, func):
    """Assert certain properties of a :class:`TimeSeriesDataFrame` is unchanged after a tranformation.

    This is used to test order preserving dataframe operations.
    """
    output_df = func(input_df)

    assert(input_df.rdd.getNumPartitions() == output_df.rdd.getNumPartitions())
    assert(input_df._is_sorted == output_df._is_sorted)

    assert(bool(input_df._tsrdd_part_info) == bool(output_df._tsrdd_part_info))

    if input_df._tsrdd_part_info and output_df._tsrdd_part_info:
        assert(input_df._tsrdd_part_info.jdeps == output_df._tsrdd_part_info.jdeps)
        assert(input_df._tsrdd_part_info.jrange_splits == output_df._tsrdd_part_info.jrange_splits)
