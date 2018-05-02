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

original_environ = dict(os.environ)
original_sys_path = list(sys.path)


def reset_env():
    cleaned_env = dict(original_environ)
    # pytest sets some env vars it expects to still be there, but if we delete
    # them here, it explodes
    for k, v in os.environ.items():
        if k.startswith('PYTEST_'):
            cleaned_env[k] = v
    os.environ = cleaned_env
    sys.path = list(original_sys_path)
    metastore_db = os.path.join(os.getcwd(), 'metastore_db')
    if os.path.isdir(metastore_db):
        shutil.rmtree(metastore_db)


@pytest.fixture(scope='module')
def launcher_params():
    # TODO: run on real datacenters for more thorough integration
    #       tests, but not often
    params = {'datacenter': 'local',
              'spark_conf': {'spark.ui.showConsoleProgress': 'false',
                             'flint.timetype': 'timestamp'},
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

@pytest.yield_fixture(scope='module')
def sc(launcher, pyspark):
    with test_support.suppress_instrumentation:
        with launcher.create_spark_context() as sc:
            yield sc
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
def tests_utils(flintContext):
    from . import utils
    return utils

def make_pdf(data, schema, dtypes=None):
    d = {schema[i]:[row[i] for row in data] for i in range(len(schema))}
    df = pd.DataFrame(data=d)[schema]

    if dtypes:
        df = df.astype(dict(zip(schema, dtypes)))

    return df

data_raw = [
    (1000000000000, 7, 0.5,),
    (1000000000000, 3, 1.0,),
    (1050000000000, 3, 1.5,),
    (1050000000000, 7, 2.0,),
    (1100000000000, 3, 2.5,),
    (1100000000000, 7, 3.0,),
    (1150000000000, 3, 3.5,),
    (1150000000000, 7, 4.0,),
    (1200000000000, 3, 4.5,),
    (1200000000000, 7, 5.0,),
    (1250000000000, 3, 5.5,),
    (1250000000000, 7, 6.0,)
]

@pytest.fixture(scope='module')
def data(flintContext):
    return flintContext.read.pandas(make_pdf(data_raw, ["time", "id", "v"]))

@pytest.fixture(scope='module')
def data_timestamp(flintContext):
    pdf = make_pdf(data_raw, ["time", "id", "v"])
    pdf['time'] = pd.to_datetime(pdf['time'])
    return flintContext.read.pandas(pdf)

def test_basic(data, data_timestamp):
    assert data.collect() == data_timestamp.collect()
    assert data.collect()[0].time == pd.Timestamp('1970-01-01 00:16:40')

def test_shiftTime(tests_utils, data):
    delta = pd.Timedelta('7days')
    expected_pdf = data.toPandas()
    expected_pdf.time += delta
    new_pdf = data.shiftTime(delta).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "forwards")

    expected_pdf = data.toPandas()
    expected_pdf.time -= delta
    new_pdf = data.shiftTime(delta, backwards=True).toPandas()
    tests_utils.assert_same(new_pdf, expected_pdf, "backwards")

def test_toPandas(data, data_timestamp):
    assert data.toPandas().time[0].tz_localize(None) == pd.Timestamp('1970-01-01 00:16:40')
    assert data_timestamp.toPandas().time[0].tz_localize(None) == pd.Timestamp('1970-01-01 00:16:40')

def test_read_range(sqlContext, flintContext, tests_utils):
    from datetime import datetime
    from pyspark.sql.types import TimestampType
    df = sqlContext.createDataFrame(
        [datetime(2016, 1, 1), datetime(2017, 1, 1), datetime(2018, 1, 1)], schema=TimestampType()).toDF('time')

    flint_df = flintContext.read.range(20170101, 20180101).dataframe(df)
    tests_utils.assert_same(flint_df.toPandas(), df.toPandas().iloc[1:2,].reset_index(drop=True))
    flint_df = flintContext.read.range(20170101).dataframe(df)
    tests_utils.assert_same(flint_df.toPandas(), df.toPandas().iloc[1:,].reset_index(drop=True))
    flint_df = flintContext.read.range(end=20170101).dataframe(df)
    tests_utils.assert_same(flint_df.toPandas(), df.toPandas().iloc[:1,].reset_index(drop=True))
    flint_df = flintContext.read.dataframe(df)
    tests_utils.assert_same(flint_df.toPandas(), df.toPandas())