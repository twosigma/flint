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

import datetime
import numpy  as np
import os
import pandas as pd
import pytest
import shutil
import sys
import pandas.util.testing as pdt

from ts.spark import pypusa
from ts.elastic import test_support

from . import utils

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
    params = {'datacenter': 'local',
              'spark_conf': {'spark.ui.showConsoleProgress': 'false'},
              'executor_memory': (1*1024**3),
              'driver_memory': (4*1024**3)}

    params.update(utils.get_codebase_params())
    return params


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
def rankers(flint):
    from ts.flint import rankers
    return rankers

@pytest.fixture(scope='module')
def summarizers(flint):
    from ts.flint import summarizers
    return summarizers


@pytest.fixture(scope='module')
def windows(flint):
    from ts.flint import windows
    return windows


@pytest.fixture(scope='module')
def clocks(flint):
    from ts.flint import clocks
    return clocks


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
    from pyspark.sql import SQLContext
    return SQLContext(sc)


@pytest.fixture(scope='module')
def flintContext(pyspark, sqlContext):
    from ts.flint import FlintContext
    return FlintContext(sqlContext)


@pytest.fixture(scope='module')
def tests_utils(flint):
    from . import utils
    return utils


@pytest.fixture(scope='module')
def F(pyspark):
    import pyspark.sql.functions as F
    return F

def make_pdf(data, schema, dtypes=None):
    d = {schema[i]:[row[i] for row in data] for i in range(len(schema))}
    df = pd.DataFrame(data=d)[schema]

    if dtypes:
        df = df.astype(dict(zip(schema, dtypes)))

    if 'time' in df.columns:
        df = df.assign(time=pd.to_datetime(df['time'], unit='ns'))

    return df

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


price2_data = [
    (0, 1, 1.0,),
    (0, 2, 2.0,),
    (1, 1, 3.0,),
    (1, 2, 4.0,),
    (1, 3, 5.0,),
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
    return flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))


@pytest.fixture(scope='module')
def price(flintContext):
    return flintContext.read.pandas(make_pdf(price_data, ["time", "id", "price"]))


@pytest.fixture(scope='module')
def price2(flintContext):
    return flintContext.read.pandas(make_pdf(price2_data, ["time", "id", "price"]))


@pytest.fixture(scope='module')
def vol(flintContext):
    return flintContext.read.pandas(make_pdf(vol_data, ["time", "id", "volume"]))


@pytest.fixture(scope='module')
def vol2(flintContext):
    return flintContext.read.pandas(make_pdf(vol2_data, ["time", "id", "volume"]))


@pytest.fixture(scope='module')
def vol3(flintContext):
    return flintContext.read.pandas(make_pdf(vol3_data, ["time", "id", "volume"]))


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
    tests_utils.assert_java_object_exists(jpkg.PartitionPreservingOperation, "PartitionPreservingOperation")
    tests_utils.assert_java_object_exists(jpkg.OrderPreservingOperation, "OrderPreservingOperation")


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
    ], ["time", "id", "price", "adjustedPrice"])

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
        [1050, 3, 300, 901],
        [1050, 3, 301, 902],
        [1050, 7, 400, 1201],
        [1050, 7, 401, 1202],
        [1100, 3, 500, 1501],
        [1100, 3, 501, 1502],
        [1100, 7, 600, 1801],
        [1100, 7, 601, 1802],
        [1150, 3, 700, 2101],
        [1150, 3, 701, 2102],
        [1150, 7, 800, 2401],
        [1150, 7, 801, 2402],
        [1200, 3, 900, 2701],
        [1200, 3, 901, 2702],
        [1200, 7, 1000, 3001],
        [1200, 7, 1001, 3002],
        [1250, 3, 1100, 3301],
        [1250, 3, 1101, 3302],
        [1250, 7, 1200, 3601],
        [1250, 7, 1201, 3602],
    ], ["time", "id", "volume", "totalVolume"])

    def fn(rows):
        volsum = sum([row.volume for row in rows])
        return {row:row.volume + volsum for row in rows}

    new_pdf = vol3.addColumnsForCycle(
        {"totalVolume": (pyspark_types.LongType(), fn)},
        key=["id"]
    ).toPandas()

    # Test API to support key as list.
    tests_utils.assert_same(
        new_pdf,
        vol3.addColumnsForCycle(
            {"totalVolume": (pyspark_types.LongType(), fn)},
            key="id"
        ).toPandas()
    )

    tests_utils.assert_same(new_pdf, expected_pdf)

def test_merge(pyspark_types, tests_utils, price):
    price1 = price.filter(price.time > 1100)
    price2 = price.filter(price.time <= 1100)
    assert price1.count() > 0
    assert price2.count() > 0
    assert price1.count() < price.count()
    assert price2.count() < price.count()
    merged_price = price1.merge(price2)
    tests_utils.assert_same(merged_price.toPandas(), price.toPandas())

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
    ], ["time", "id", "price", "volume"])

    new_pdf = price.leftJoin(vol, key=["id"]).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)
    tests_utils.assert_same(new_pdf, price.leftJoin(vol, key="id").toPandas())

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
    ], ["time", "id", "price", "volume"])

    new_pdf = price.leftJoin(vol.filter(vol.time != 1050), key="id").toPandas()
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
    ], ["time", "id", "price", "volume", "time2"])

    new_pdf = price.futureLeftJoin(vol.withColumn("time2", vol.time.cast(pyspark_types.LongType())),
                                   tolerance=pd.Timedelta("100ns"),
                                   key=["id"], strict_lookahead=True).toPandas()
    new_pdf1 = price.futureLeftJoin(vol.withColumn("time2", vol.time.cast(pyspark_types.LongType())),
                                    tolerance=pd.Timedelta("100ns"),
                                    key="id", strict_lookahead=True).toPandas()
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
    id = vol.collect()

    expected_pdf = make_pdf([
        (1000, 7, [id[0], id[3]]),
        (1000, 3, [id[1], id[2]]),
        (1100, 7, [id[5], id[7]]),
        (1100, 3, [id[4], id[6]]),
        (1200, 7, [id[9], id[11]]),
        (1200, 3, [id[8], id[10]]),
    ], ["time", "id", "rows"])

    new_pdf = vol.groupByInterval(intervals, key=["id"]).toPandas()
    new_pdf1 = vol.groupByInterval(intervals, key="id").toPandas()
    tests_utils.assert_same(new_pdf, new_pdf1)

    # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf)
    # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
    tests_utils.assert_same(
        new_pdf[new_pdf['id'] == 3].reset_index(drop=True),
        expected_pdf[expected_pdf['id'] == 3].reset_index(drop=True),
    )
    tests_utils.assert_same(
        new_pdf[new_pdf['id'] == 7].reset_index(drop=True),
        expected_pdf[expected_pdf['id'] == 7].reset_index(drop=True),
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
        (1000, 7, 200.0),
        (1000, 3, 400.0),
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
    ], ["time", "id", "volume_sum"])
    new_pdf2 = vol2.summarizeCycles(summarizers.sum("volume"), key="id").toPandas()
    tests_utils.assert_same(new_pdf2, expected_pdf2)


def test_summarizeCycles_udf(tests_utils, vol, pyspark, F):
    from ts.flint import udf
    from pyspark.sql.types import DoubleType, LongType
    from collections import OrderedDict

    weighted_vol = vol.withColumn('weight', F.lit(1))

    result1 = vol.summarizeCycles(
        OrderedDict([
            ('mean', udf(lambda v: v.mean(), DoubleType())(weighted_vol.volume)),
            ('sum', udf(lambda v: v.sum(), LongType())(weighted_vol.volume)),
        ])
    ).toPandas()

    result2 = vol.summarizeCycles(
        OrderedDict([
            ('mean', udf(lambda df: df.volume.mean(), DoubleType())(weighted_vol[['volume']])),
            ('sum', udf(lambda df: df.volume.sum(), LongType())(weighted_vol[['volume']])),
        ])
    ).toPandas()

    result3 = weighted_vol.summarizeCycles(
        OrderedDict([
            ('mean', udf(lambda v, w: np.average(v, weights=w), DoubleType())(weighted_vol['volume'], weighted_vol['weight'])),
            ('sum', udf(lambda v, w: (v * w).sum(), LongType())(weighted_vol['volume'], weighted_vol['weight'])),
        ])
    ).toPandas()

    result4 = weighted_vol.summarizeCycles(
        OrderedDict([
            ('mean', udf(lambda df: np.average(df.volume, weights=df.weight), DoubleType())(weighted_vol[['volume', 'weight']])),
            ('sum', udf(lambda df: (df.volume * df.weight).sum(), LongType())(weighted_vol[['volume', 'weight']])),
        ])
    ).toPandas()

    @udf(DoubleType())
    def foo(v, w):
        return np.average(v, weights=w)

    @udf(LongType())
    def bar(v, w):
        return (v * w).sum()

    result5 = weighted_vol.summarizeCycles(
        OrderedDict([
            ('mean', foo(weighted_vol['volume'], weighted_vol['weight'])),
            ('sum', bar(weighted_vol['volume'], weighted_vol['weight']))
        ])
    ).toPandas()

    @udf(DoubleType())
    def foo1(df):
        return np.average(df.volume, weights=df.weight)

    @udf(LongType())
    def bar1(df):
        return (df.volume * df.weight).sum()

    result6 = weighted_vol.summarizeCycles(
        OrderedDict([
            ('mean', foo1(weighted_vol[['volume', 'weight']])),
            ('sum', bar1(weighted_vol[['volume', 'weight']]))
        ])
    ).toPandas()

    expected_pdf1 = make_pdf([
        (1000, 150.0, 300,),
        (1050, 350.0, 700,),
        (1100, 550.0, 1100,),
        (1150, 750.0, 1500,),
        (1200, 950.0, 1900,),
        (1250, 1150.0, 2300,),
    ], ["time", "mean", "sum"])

    @udf((DoubleType(), LongType()))
    def foobar(v, w):
        foo = np.average(v, weights=w)
        bar = (v * w).sum()
        return (foo, bar)

    result7 = weighted_vol.summarizeCycles(
        {('mean', 'sum'): foobar(weighted_vol['volume'], weighted_vol['weight'])}
    ).toPandas()

    result8 = weighted_vol.summarizeCycles({
        "mean": udf(lambda v: v.mean(), DoubleType())(weighted_vol['volume']),
        "sum": udf(lambda v: v.sum(), LongType())(weighted_vol['volume'])
    }).toPandas()

    tests_utils.assert_same(result1, expected_pdf1)
    tests_utils.assert_same(result2, expected_pdf1)
    tests_utils.assert_same(result3, expected_pdf1)
    tests_utils.assert_same(result4, expected_pdf1)
    tests_utils.assert_same(result5, expected_pdf1)
    tests_utils.assert_same(result6, expected_pdf1)
    tests_utils.assert_same(result7, expected_pdf1)
    tests_utils.assert_same(result8, expected_pdf1)


def test_udf(flintContext, tests_utils, vol):
    from ts.flint import udf
    from pyspark.sql.types import LongType

    @udf(LongType())
    def foo(v):
        return v*2

    result1 = vol.withColumn("volume", foo(vol['volume'])).toPandas()
    result2 = vol.withColumn("volume", udf(lambda v: v * 2, LongType())(vol['volume'])).toPandas()

    expected_pdf1 = make_pdf([
        (1000, 7, 200,),
        (1000, 3, 400,),
        (1050, 3, 600,),
        (1050, 7, 800,),
        (1100, 3, 1000,),
        (1100, 7, 1200,),
        (1150, 3, 1400,),
        (1150, 7, 1600,),
        (1200, 3, 1800,),
        (1200, 7, 2000,),
        (1250, 3, 2200,),
        (1250, 7, 2400,)
    ], ['time', 'id', 'volume'])

    tests_utils.assert_same(result1, expected_pdf1)
    tests_utils.assert_same(result2, expected_pdf1)


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

    new_pdf2 = vol.summarizeIntervals(clock, summarizers.sum("volume"), key="id").toPandas()
    expected_pdf2 = make_pdf([
        (1000, 7, 500.0),
        (1000, 3, 500.0),
        (1100, 3, 1200.0),
        (1100, 7, 1400.0),
        (1200, 3, 2000.0),
        (1200, 7, 2200.0),
    ], ["time", "id", "volume_sum"])

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
    ], ["time", "id", "volume", "volume_sum"])
    tests_utils.assert_same(new_pdf1, expected_pdf1)

    new_pdf2 = (vol.summarizeWindows(windows.past_absolute_time('99ns'),
                                     summarizers.sum("volume"),
                                     key="id").toPandas())
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
    ], ["time", "id", "volume", "volume_sum"])
    tests_utils.assert_same(new_pdf2, expected_pdf2)

    interval_with_id = flintContext.read.pandas(make_pdf([
        (1000, 3),
        (1000, 7),
        (1050, 3),
        (1050, 7),
        (1100, 3),
        (1150, 3),
        (1150, 7),
        (1200, 3),
        (1200, 7),
        (1250, 7),
    ], ["time", "id"]))

    new_pdf3 = (interval_with_id.summarizeWindows(windows.past_absolute_time('99ns'),
                                                   summarizers.sum("volume"),
                                                   key="id",
                                                   other=vol).toPandas())
    expected_pdf3 = make_pdf([
        (1000, 3, 200.0),
        (1000, 7, 100.0),
        (1050, 3, 500.0),
        (1050, 7, 500.0),
        (1100, 3, 800.0),
        (1150, 3, 1200.0),
        (1150, 7, 1400.0),
        (1200, 3, 1600.0),
        (1200, 7, 1800.0),
        (1250, 7, 2200.0),
    ], ["time", "id", "volume_sum"])
    tests_utils.assert_same(new_pdf3, expected_pdf3)


@pytest.mark.net
def test_summarizeWindows_trading_time(flintContext, tests_utils, windows, summarizers):

    def to_nanos(dt):
        return int(dt.timestamp() * 1e9)

    friday = to_nanos(datetime.datetime(2001, 11, 9, 15, 0))
    monday = to_nanos(datetime.datetime(2001, 11, 12, 15, 0))
    tuesday = to_nanos(datetime.datetime(2001, 11, 13, 15, 0))
    wednesday = to_nanos(datetime.datetime(2001, 11, 14, 15, 0))

    data_with_weekend = flintContext.read.pandas(make_pdf([
        (friday, 1, 100.0),
        (monday, 1, 100.0),
        (tuesday, 1, 100.0),
        (wednesday, 1, 100.0),
    ], ['time', 'id', 'volume']))

    actual_past = data_with_weekend.summarizeWindows(
        windows.past_trading_time('1d', 'US'), summarizers.sum('volume')
    ).toPandas()

    expected_past = make_pdf([
        (friday, 1, 100.0, 100.0),
        (monday, 1, 100.0, 200.0),
        (tuesday, 1, 100.0, 200.0),
        (wednesday, 1, 100.0, 200.0),
    ], ['time', 'id', 'volume', 'volume_sum'])
    tests_utils.assert_same(actual_past, expected_past)

    actual_future = data_with_weekend.summarizeWindows(
        windows.future_trading_time('1d', "US"), summarizers.sum('volume')
    ).toPandas()

    expected_future = make_pdf([
        (friday, 1, 100.0, 200.0),
        (monday, 1, 100.0, 200.0),
        (tuesday, 1, 100.0, 200.0),
        (wednesday, 1, 100.0, 100.0),
    ], ['time', 'id', 'volume', 'volume_sum'])
    tests_utils.assert_same(actual_future, expected_future)


def test_summary_sum(summarizers, tests_utils, vol):
    expected_pdf = make_pdf([
        (0, 7800.0,)
    ], ["time", "volume_sum"])

    new_pdf = vol.summarize(summarizers.sum("volume")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)

    expected_pdf = make_pdf([
        (0, 7, 4100.0,),
        (0, 3, 3700.0,),
    ], ["time", "id", "volume_sum"])

    new_pdf = vol.summarize(summarizers.sum("volume"), key=["id"]).toPandas()
    new_pdf1 = vol.summarize(summarizers.sum("volume"), key="id").toPandas()
    tests_utils.assert_same(new_pdf, new_pdf1)

    # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf, "by id")
    # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
    tests_utils.assert_same(
        new_pdf[new_pdf['id'] == 3].reset_index(drop=True),
        expected_pdf[expected_pdf['id'] == 3].reset_index(drop=True),
        "by id 3"
    )
    tests_utils.assert_same(
        new_pdf[new_pdf['id'] == 7].reset_index(drop=True),
        expected_pdf[expected_pdf['id'] == 7].reset_index(drop=True),
        "by id 7"
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
    moments = [price.summarize(summarizers.nth_moment("price", i), key="id").collect() for i in range(5)]
    for m in moments:
        m.sort(key=lambda r: r['id'])
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
    moments = [price.summarize(summarizers.nth_central_moment("price", i), key="id").collect() for i in range(1,5)]
    for m in moments:
        m.sort(key=lambda r: r['id'])
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
    joined = price.leftJoin(forecast, key="id")
    joined = (joined
              .withColumn("price2", joined.price)
              .withColumn("price3", -joined.price)
              .withColumn("price4", 2 * joined.price)
              .withColumn("price5", pyspark.sql.functions.lit(0)))

    def price_correlation(column):
        corr = joined.summarize(summarizers.correlation("price", column), key=["id"])
        tests_utils.assert_same(
            corr.toPandas(),
            joined.summarize(summarizers.correlation(["price"], [column]), key="id").toPandas()
        )
        tests_utils.assert_same(
            corr.toPandas(),
            joined.summarize(summarizers.correlation(["price", column]), key="id").toPandas()
        )
        return corr.collect()

    results = [price_correlation("price{}".format(i)) for i in range(2,6)]
    for r in results:
        r.sort(key=lambda r: r['id'])
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
    joined = price.leftJoin(forecast, key="id")
    result = joined.summarize(summarizers.linear_regression("price", ["forecast"])).collect()

def test_summary_max(pyspark, summarizers, tests_utils, forecast):
    expected_pdf = make_pdf([
        (0, 6.4,)
    ], ["time", "forecast_max"])
    result = forecast.summarize(summarizers.max("forecast")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_mean(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 3.25,)
    ], ["time", "price_mean"])
    joined = price.leftJoin(forecast, key="id")
    result = joined.summarize(summarizers.mean("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_weighted_mean(pyspark, summarizers, tests_utils, price, vol):
    expected_pdf = make_pdf([
        (0, 4.166667, 1.547494, 8.237545, 12,)
        ], ["time", "price_volume_weightedMean", "price_volume_weightedStandardDeviation", "price_volume_weightedTStat", "price_volume_observationCount"])
    joined = price.leftJoin(vol, key="id")
    result = joined.summarize(summarizers.weighted_mean("price", "volume")).toPandas()

    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_min(pyspark, summarizers, tests_utils, forecast):
    expected_pdf = make_pdf([
        (0, -9.6,)
    ], ["time", "forecast_min"])
    result = forecast.summarize(summarizers.min("forecast")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_quantile(sc, summarizers, forecast):
    expected_pdf = make_pdf([
        (0, -2.22, 1.75)
    ], ["time", "forecast_0.2quantile", "forecast_0.5quantile"])
    result = forecast.summarize(summarizers.quantile("forecast", (0.2, 0.5))).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_stddev(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 1.802775638,)
    ], ["time", "price_stddev"])
    joined = price.leftJoin(forecast, key="id")
    result = joined.summarize(summarizers.stddev("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_summary_variance(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, 3.25,)
    ], ["time", "price_variance"])
    joined = price.leftJoin(forecast, key="id")
    result = joined.summarize(summarizers.variance("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)


def test_summary_covariance(pyspark, summarizers, tests_utils, price, forecast):
    expected_pdf = make_pdf([
        (0, -1.802083333,)
    ], ["time", "price_forecast_covariance"])
    joined = price.leftJoin(forecast, key="id")
    result = joined.summarize(summarizers.covariance("price", "forecast")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_product(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 116943.75)
    ], ["time", "price_product"])

    result = price.summarize(summarizers.product("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_dot_product(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 162.5)
    ], ["time", "price_price_dotProduct"])

    result = price.summarize(summarizers.dot_product("price", "price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_geometric_mean(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 2.644425997)
    ], ["time", "price_geometricMean"])

    result = price.summarize(summarizers.geometric_mean("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_skewness(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 0.0)
    ], ["time", "price_skewness"])

    result = price.summarize(summarizers.skewness("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_kurtosis(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, -1.216783217)
    ], ["time", "price_kurtosis"])

    result = price.summarize(summarizers.kurtosis("price")).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_compose(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 6.0, 0.5, 3.25, 1.802775638,)
    ], ["time", "price_max", "price_min", "price_mean", "price_stddev"])

    result = price.summarize([summarizers.max("price"),
                              summarizers.min("price"),
                              summarizers.mean("price"),
                              summarizers.stddev("price")]).toPandas()
    pdt.assert_frame_equal(result, expected_pdf)

def test_summary_prefix(pyspark, summarizers, tests_utils, price):
    expected_pdf = make_pdf([
        (0, 6.0, 6.0)
    ], ["time", "price_max", "prefix_price_max"])

    result = price.summarize([summarizers.max("price"),
                              summarizers.max("price").prefix("prefix")]).toPandas()
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
    ], ["time", "id", "volume", "volume_sum"])

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
    ], ["time", "id", "volume", "volume_sum"])

    new_pdf = vol.addSummaryColumns(summarizers.sum("volume"), "id").toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "with key")


def test_percentile_rank(rankers, tests_utils, price2):
    new_pdf = price2.addColumnsForCycle({
        'result': rankers.percentile('price')
    }).toPandas()

    expected_pdf = make_pdf([
        (0, 1, 1.0, 1.0 / 3),
        (0, 2, 2.0, 2.0 / 3),
        (1, 1, 3.0, .25),
        (1, 2, 4.0, .5),
        (1, 3, 5.0, .75),
    ], ['time', 'id', 'price', 'result'])
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_exclusive_rank(rankers, tests_utils, price2):
    new_pdf = price2.addColumnsForCycle({
        'result': rankers.exclusive('price', 0, 1)
    }).toPandas()

    expected_pdf = make_pdf([
        (0, 1, 1.0, 1.0 / 3),
        (0, 2, 2.0, 2.0 / 3),
        (1, 1, 3.0, .25),
        (1, 2, 4.0, .5),
        (1, 3, 5.0, .75),
    ], ['time', 'id', 'price', 'result'])
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_inclusive_rank(rankers, tests_utils, price2):
    new_pdf = price2.addColumnsForCycle({
        'result': rankers.inclusive('price', 0, 1)
    }).toPandas()

    expected_pdf = make_pdf([
        (0, 1, 1.0, 0.0),
        (0, 2, 2.0, 1.0),
        (1, 1, 3.0, 0.0),
        (1, 2, 4.0, 0.5),
        (1, 3, 5.0, 1.0),
    ], ['time', 'id', 'price', 'result'])
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_quantile_rank(rankers, tests_utils, price2):
    new_pdf = price2.addColumnsForCycle({
        'result': rankers.quantile('price', 2)
    }).toPandas()

    # SPARK-21163 converts new IntegerType columns to int32 when calling toPandas().
    # We specify the return type of the 'result' column explicitly as int32 below.
    expected_pdf = make_pdf([
        (0, 1, 1.0, 0),
        (0, 2, 2.0, 1),
        (1, 1, 3.0, 0),
        (1, 2, 4.0, 1),
        (1, 3, 5.0, 1),
    ], ['time', 'id', 'price', 'result'], dtypes=[np.int64, np.int64, np.double, np.int32])
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_normal_distribution_percentile_rank(rankers, tests_utils, price2):
    new_pdf = price2.addColumnsForCycle({
        'result': rankers.normal_distribution_percentile('price')
    }).toPandas()

    expected_pdf = make_pdf([
        (0, 1, 1.0, -0.43029175890572224,),
        (0, 2, 2.0, 0.430291758905722,),
        (1, 1, 3.0, -0.6741891400433162,),
        (1, 2, 4.0, -1.0100667546808495e-7,),
        (1, 3, 5.0, 0.6741891400433162)
    ], ['time', 'id', 'price', 'result'])

    tests_utils.assert_same(new_pdf, expected_pdf)


def test_mixed_udfs(rankers, pyspark, price2):
    """
    addColumnsForCycle should support both built-in bindings and
    Python UDFs in the same call.
    """
    from pyspark.sql.types import DoubleType
    from scipy import stats

    def percentile(column):
        def f(rows):
            values = [row[column] for row in rows] + [float("inf")]  # Add a max value so it's 1 exclusive
            return {row: float(stats.percentileofscore(values, row[column], kind='rank') / 100) for row in rows}
        return f

    actual = price2.addColumnsForCycle({
        'flint': rankers.percentile('price'),
        'python': (DoubleType(), percentile('price'))
    }).toPandas()

    expected = make_pdf([
        (0, 1, 1.0, 1/3, 1/3),
        (0, 2, 2.0, 2/3, 2/3),
        (1, 1, 3.0, .25, .25),
        (1, 2, 4.0,  .5,  .5),
        (1, 3, 5.0, .75, .75),
    ], ['time', 'id', 'price', 'flint', 'python'])

    # np.isclose doesn't handle datetimes, so we have to cast the time
    # column to int before using it.
    actual = actual.assign(time=actual['time'].astype(np.int64))
    expected = expected.assign(time=expected['time'].astype(np.int64))
    # For some reason, we need to raise the tolerence threshold for comparing the python udf column
    assert np.all(np.isclose(actual, expected, atol=1e-5)), "Left: {}\nRight: {}".format(actual, expected)


def test_retain_column_order(rankers, pyspark, price2):
    """
    addColumnsForCycle should retain the order of the columns if specified
    using an OrderedDict.
    """
    from pyspark.sql.types import DoubleType
    from collections import OrderedDict

    actual = price2.addColumnsForCycle(OrderedDict([
        ('col1', rankers.exclusive('price', 0.0, 1.0)),
        ('col2', (DoubleType(), lambda rows: {row: 1.0 for row in rows})),
        ('col3', rankers.inclusive('price', 0.0, 1.0)),
        ('col4', (DoubleType(), lambda rows: {row: 0.0 for row in rows})),
    ]))

    assert actual.columns == price2.columns + ['col1', 'col2', 'col3', 'col4']


def test_addColumnsForCycle_unsupported_columns(rankers, pyspark, price2):
    with pytest.raises(ValueError):
        price2.addColumnsForCycle({
            'invalid': 5.0,
        })

    with pytest.raises(ValueError):
        price2.addColumnsForCycle({
            'invalid': ('foo', lambda x: 1.0)
        })

    from pyspark.sql.types import DoubleType
    with pytest.raises(ValueError):
        price2.addColumnsForCycle({
            'invalid': (DoubleType(), 5.0)
        })


def test_addWindows(tests_utils, windows, vol):
    id = vol.collect()

    expected_pdf = make_pdf([
        (1000, 7, 100, [id[0], id[1]]),
        (1000, 3, 200, [id[0], id[1]]),
        (1050, 3, 300, [id[0], id[1], id[2], id[3]]),
        (1050, 7, 400, [id[0], id[1], id[2], id[3]]),
        (1100, 3, 500, [id[2], id[3], id[4], id[5]]),
        (1100, 7, 600, [id[2], id[3], id[4], id[5]]),
        (1150, 3, 700, [id[4], id[5], id[6], id[7]]),
        (1150, 7, 800, [id[4], id[5], id[6], id[7]]),
        (1200, 3, 900, [id[6], id[7], id[8], id[9]]),
        (1200, 7, 1000, [id[6], id[7], id[8], id[9]]),
        (1250, 3, 1100, [id[8], id[9], id[10], id[11]]),
        (1250, 7, 1200, [id[8], id[9], id[10], id[11]]),
    ], ["time", "id", "volume", "window_past_50ns"])

    new_pdf = vol.addWindows(windows.past_absolute_time("50ns")).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf)


def test_shiftTime(tests_utils, price):
    delta = pd.Timedelta('1000ns')
    expected_pdf = price.toPandas()
    expected_pdf.time += delta
    new_pdf = price.shiftTime(delta).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "forwards")

    expected_pdf = price.toPandas()
    expected_pdf.time -= delta
    new_pdf = price.shiftTime(delta, backwards=True).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "backwards")

def test_shiftTime_windows(flintContext, tests_utils, windows):

    def to_nanos(dt):
        return int(dt.timestamp() * 1e9)

    friday = to_nanos(datetime.datetime(2001, 11, 9, 15, 0))
    saturday = to_nanos(datetime.datetime(2001, 11, 10, 15, 0))
    monday = to_nanos(datetime.datetime(2001, 11, 12, 15, 0))
    tuesday = to_nanos(datetime.datetime(2001, 11, 13, 15, 0))
    wednesday = to_nanos(datetime.datetime(2001, 11, 14, 15, 0))
    thrusday = to_nanos(datetime.datetime(2001, 11, 15, 15, 0))

    dates = flintContext.read.pandas(make_pdf([
        (friday,),
        (monday,),
        (tuesday,),
        (wednesday,),
    ], ['time']))

    expected1 = make_pdf([
        (saturday,),
        (tuesday,),
        (wednesday,),
        (thrusday,),
    ], ['time'])
    result1 = dates.shiftTime(windows.future_absolute_time('1day')).toPandas()
    tests_utils.assert_same(result1, expected1)

    expected2= make_pdf([
        (monday,),
        (tuesday,),
        (wednesday,),
        (thrusday,),
    ], ['time'])
    result2 = dates.shiftTime(windows.future_trading_time('1day', 'US')).toPandas()
    tests_utils.assert_same(result2, expected2)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
@pytest.mark.human
@pytest.mark.net
def test_addXrefColumn(flintContext, tests_utils):
    from pyspark.sql.types import StructType, StructField, LongType, IntegerType
    pdf = make_pdf([(1451606400000000000, 64106)], ["time", "tid"])
    input_df = flintContext.read.pandas(pdf, StructType([StructField("time", LongType(), True), StructField("tid", IntegerType(), True)]))
    output = input_df.addXrefColumn(('TICKER')).toPandas()
    expected_pdf = make_pdf([(1451606400000000000, 64106, "CAB")], ["time", "tid", "TICKER"],
                            dtypes=[np.int64, np.int32, np.str])
    tests_utils.assert_same(output, expected_pdf)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
@pytest.mark.human
@pytest.mark.net
def test_addTidColumn(flintContext, tests_utils):
    input_df = flintContext.read.pandas(make_pdf([(1451606400000000000, "CAB")], ["time", "ticker"]))
    output = input_df.addTidColumn('ticker', 'ACTIVE_3000_US').toPandas()

    expected_pdf = make_pdf([(1451606400000000000, "CAB", 64106)], ["time", "ticker", "tid"],
                            dtypes=[np.int64, np.str, np.int32])
    tests_utils.assert_same(output, expected_pdf)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
@pytest.mark.human
@pytest.mark.net
def test_shiftToNextClosestTradingTime(flintContext, tests_utils):
    # 12/25/2016, 2:00:00 AM
    input_df = flintContext.read.pandas(make_pdf([(1482649200000000000, 64106)], ["time", "id"]))
    output = input_df.shiftToNextClosestTradingTime('US').toPandas()

    # 12/27/2016 9:30:00AM ET - US stock exchanges opened on Tuesday after Christmas
    expected_pdf = make_pdf([(1482849000000000000, 64106)], ["time", "id"])
    tests_utils.assert_same(output, expected_pdf)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
# TODO: This doesn't work with Spark 2.0 branch. Will fix this separately.
# @pytest.mark.human
# @pytest.mark.net
# def test_addFutureSeriesSequenceTidColumn(flintContext, tests_utils):
#     from pyspark.sql.types import StructType, StructField, LongType, IntegerType
#     pdf = make_pdf([(1451606400000000000, 37261721)], ["time", "tid"])
#     input = flintContext.read.pandas(pdf, StructType([StructField("time", LongType(), True), StructField("tid", IntegerType(), True)]))
#     output = input.addFutureSeriesSequenceTidColumn().toPandas()

#     expected_pdf = make_pdf([(1451606400000000000, 37261721, 11186134)], ["time", "tid", "futureSeriesTid"])
#     tests_utils.assert_same(output, expected_pdf)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
@pytest.mark.human
@pytest.mark.net
def test_read_uri(flintContext):
    from ts.data import tsdata_http
    uri = 'tsdata:/price/ts/golden/u.ACTIVE_3000_US'
    begin = '20010101'
    end = '20010201'
    spark_df = flintContext.read.uri(uri, begin, end)
    pandas_df = tsdata_http.load_dataframe(uri, (begin, end))

    assert spark_df.count() == len(pandas_df.index)


# Not really human, but requires jobsystem creds and tsmoto doesn't have them.
@pytest.mark.human
@pytest.mark.net
def test_read_alf(flintContext):
    from ts.data import tsdata_http
    uri = 'tsdata:/price/ts/golden/u.ACTIVE_3000_US'
    begin = '20010101'
    end = '20010201'
    spark_df = flintContext.read.alf(uri, begin, end)
    pandas_df = tsdata_http.load_dataframe(uri, (begin, end))

    assert spark_df.count() == len(pandas_df.index)


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


def test_uniform_clocks(sqlContext, clocks):
    df = clocks.uniform(sqlContext, '1d', '0s', '2016-11-07', '2016-11-17')
    assert(df.count() == 11)
    # the last timestamp should be 17 Nov 2016 00:00:00 GMT
    assert(df.collect()[-1]['time'] == 1479340800000000000)


def test_from_tsrdd(sqlContext, flintContext, flint):
    df = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    tsrdd = df.timeSeriesRDD
    df2 = flint.TimeSeriesDataFrame._from_tsrdd(tsrdd, sqlContext)
    tsrdd2 = df2.timeSeriesRDD

    assert(tsrdd.count() == tsrdd2.count())
    assert(tsrdd.orderedRdd().getNumPartitions() == tsrdd2.orderedRdd().getNumPartitions())


def test_sorted_df_partitioning(sqlContext, flintContext, pyspark, forecast):
    sorted_df = forecast.sort("time")
    # if the underlying data frame is sorted then conversion to TSDF or tsrdd shouldn't affect partitioning
    normalized_df = pyspark.sql.DataFrame(flintContext.read.dataframe(sorted_df).timeSeriesRDD.toDF(), sqlContext)

    assert_partition_equals(sorted_df, normalized_df)


def test_with_column_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.withColumn("neg_forecast", -df.forecast), True)


def test_drop_column_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.drop("forecast"), True)


def test_filter_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.filter(df.id == 3), True)


def test_select_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.select("time", "id"), True)


def test_with_column_renamed_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.withColumnRenamed("forecast", "signal"), True)


def test_replace_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.replace([3, 7], [4, 8], 'id'), True)


def test_na_preserve_order(sqlContext, flintContext):
    from pyspark.sql.functions import lit
    from pyspark.sql.types import StringType

    def create_dataframe():
        return (flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
                .withColumn("null_column", lit(None).cast(StringType())))

    shared_test_partition_preserving(flintContext, lambda df: df.fillna("v1"), True, create_dataframe)
    shared_test_partition_preserving(flintContext, lambda df: df.dropna(), True, create_dataframe)
    shared_test_partition_preserving(flintContext, lambda df: df.fillna("v1").replace("v1", "v2", 'null_column'), True, create_dataframe)


def test_with_column_udf_preserve_order(sqlContext, flintContext):
    def with_udf_column(df):
        from pyspark.sql.types import DoubleType
        from pyspark.sql.functions import udf
        times_two = udf(lambda x: x * 2, DoubleType())
        return df.withColumn("forecast2", times_two(df.forecast))
    shared_test_partition_preserving(flintContext, with_udf_column, True)


def test_sort_dont_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.orderBy("id"), False)


def test_repatition_dont_preserve_order(sqlContext, flintContext):
    shared_test_partition_preserving(flintContext, lambda df: df.repartition(df.rdd.getNumPartitions() * 2), False)


def test_select_aggregate_dont_preserve_order(sqlContext, flintContext):
    from pyspark.sql.functions import sum
    shared_test_partition_preserving(flintContext, lambda df: df.select(sum('forecast')), False)


def test_with_window_column_dont_preserve_order(sqlContext, flintContext):
    def with_window_column(df):
        from pyspark.sql.window import Window
        from pyspark.sql.functions import percent_rank
        windowSpec = Window.partitionBy(df['id']).orderBy(df['forecast'])
        return df.withColumn("r", percent_rank().over(windowSpec))
    shared_test_partition_preserving(flintContext, with_window_column, False)


def test_explode_preserve_order(flintContext):
    def with_explode_column(df):
        import pyspark.sql.functions as F
        df2 = df.withColumn('values', F.array(F.lit(1), F.lit(2)))
        df2 = df2.withColumn('value', F.explode(df2.values))
        return df2
    shared_test_partition_preserving(flintContext, with_explode_column, True)


def test_df_lazy(flintContext):
    df_lazy = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    assert(df_lazy._is_sorted is True)
    assert(df_lazy._tsrdd_part_info is None)


def test_df_eager(flintContext):
    df_eager = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df_eager.timeSeriesRDD
    assert(df_eager._is_sorted)
    assert(df_eager._lazy_tsrdd is not None)
    assert(df_eager._tsrdd_part_info is None)


def test_df_joined(flintContext):
    df = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df_joined = df.leftJoin(df, right_alias="right")
    assert(df_joined._is_sorted)
    assert(df_joined._tsrdd_part_info is not None)
    assert(df_joined._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_joined._jdf))


def test_df_cached(flintContext):
    df_cached = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df_cached.cache()
    df_cached.count()
    assert(df_cached._is_sorted)
    assert(df_cached._tsrdd_part_info is None)
    assert(df_cached._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_cached._jdf))


def test_df_cached_joined(flintContext):
    df_cached = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df_cached.cache()
    df_cached.count()
    df_cached_joined = df_cached.leftJoin(df_cached, right_alias="right")
    assert(df_cached_joined._is_sorted)
    assert(df_cached_joined._tsrdd_part_info is not None)
    assert(df_cached_joined._jpkg.PartitionPreservingOperation.isPartitionPreservingDataFrame(df_cached_joined._jdf))


def test_df_orderBy(flintContext):
    df = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df = df.orderBy("time")
    assert(not df._is_sorted)
    assert(df._tsrdd_part_info is None)


def test_withColumn_time(flintContext, tests_utils):
    from ts.flint import TimeSeriesDataFrame
    from pyspark.sql import DataFrame

    pdf = make_pdf(forecast_data, ["time", "id", "forecast"])
    df = flintContext.read.pandas(pdf)
    df = df.withColumn("time", df.time * 2)
    assert(not isinstance(df, TimeSeriesDataFrame))
    assert(isinstance(df, DataFrame))
    expected = pdf.assign(time=pdf['time'].astype(np.int64) * 2)
    tests_utils.assert_same(df.toPandas(), expected)


def test_describe(flintContext):
    from ts.flint import TimeSeriesDataFrame
    from pyspark.sql import DataFrame

    df = flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))
    df.describe()


def test_empty_df(sc, sqlContext, flintContext, summarizers):
    from pyspark.sql.types import LongType, StructType, StructField
    df = sqlContext.createDataFrame(
        sc.emptyRDD(),
        schema=StructType([StructField('time', LongType())]))
    df2 = flintContext.read.dataframe(df)
    df3 = df2.summarize(summarizers.count())
    assert(df2.count() == 0)
    assert(df3.count() == 0)
    assert(df2.schema == StructType([StructField('time', LongType())]))
    assert(df3.schema == StructType([StructField('time', LongType()), StructField('count', LongType())]))


def shared_test_partition_preserving(flintContext, func, preserve, create = None):
    def create_dataframe():
        return flintContext.read.pandas(make_pdf(forecast_data, ["time", "id", "forecast"]))

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
        lambda df: df.filter(df.time % 1000 == 0)
    ]

    order_preserving_input_tranforms = [
        lambda df: df.orderBy("time")
    ]

    input_dfs = [df_lazy, df_eager, df_joined, df_cached, df_cached_joined]

    for transform in partition_preserving_input_tranforms:
        for input_df in input_dfs:
            assert_partition_preserving(transform(input_df), func, preserve)

    for tranform in order_preserving_input_tranforms:
        for input_df in input_dfs:
            assert_order_preserving(transform(input_df), func, preserve)

    df_cached.unpersist()


def rows_to_pandas(rows):
    return [pd.DataFrame(list(rows))]


def get_nonempty_partitions(df):
    pdfs = df.rdd.mapPartitions(rows_to_pandas).collect()
    return [pdf for pdf in pdfs if not pdf.empty]


def assert_partition_equals(df1, df2):
    partitions1 = get_nonempty_partitions(df1)
    partitions2 = get_nonempty_partitions(df2)

    assert(len(partitions1) == len(partitions2))
    for pdf1, pdf2 in zip(partitions1, partitions2):
        pdt.assert_frame_equal(pdf1, pdf2)


def assert_sorted(df):
    pdf = df.toPandas()
    if len(pdf.index) < 2:
        return
    assert np.diff(pdf.time).min() >= np.timedelta64(0)


def assert_partition_preserving(input_df, func, preserve):
    output_df = func(input_df)

    if preserve:
        assert(input_df.rdd.getNumPartitions() == output_df.rdd.getNumPartitions())
        assert(input_df._is_sorted == output_df._is_sorted)
        assert(input_df._tsrdd_part_info == output_df._tsrdd_part_info)
        if output_df._is_sorted:
            assert_sorted(output_df)
        if output_df._tsrdd_part_info:
            output_df.timeSeriesRDD.validate()

    else:
        assert(output_df._tsrdd_part_info == None)

def assert_order_preserving(input_df, func, preserve):
    output_df = func(input_df)

    if preserve:
        assert(input_df._is_sorted == output_df._is_sorted)
        if output_df._is_sorted:
            assert_sorted(output_df)

    else:
        assert(not output_df._is_sorted)
        assert(output_df._tsrdd_part_info == None)
