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

import unittest

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

from tests.utils import *

BASE = get_test_base()

class TestDataframe(BASE):
    ''' Tests for TimeSeriesDataFrame '''
    def test_leftJoin(self):
        price = self.price()
        vol = self.vol()

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
        assert_same(new_pdf, expected_pdf)
        assert_same(new_pdf, price.leftJoin(vol, key="id").toPandas())

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

        new_pdf = price.leftJoin(vol.filter(vol.time != make_timestamp(1050)), key="id").toPandas()
        assert_same(new_pdf, expected_pdf)

    def test_futureLeftJoin(self):
        price = self.price()
        vol = self.vol()

        expected_pdf = make_pdf([
            (1000, 7, 0.5, 400),
            (1000, 3, 1.0, 300),
            (1050, 3, 1.5, 500),
            (1050, 7, 2.0, 600),
            (1100, 3, 2.5, 700),
            (1100, 7, 3.0, 800),
            (1150, 3, 3.5, 900),
            (1150, 7, 4.0, 1000),
            (1200, 3, 4.5, 1100),
            (1200, 7, 5.0, 1200),
            (1250, 3, 5.5, None),
            (1250, 7, 6.0, None),
        ], ["time", "id", "price", "volume"])

        new_pdf = price.futureLeftJoin(
            vol, tolerance=pd.Timedelta("100s"),
            key=["id"], strict_lookahead=True).toPandas()
        new_pdf1 = price.futureLeftJoin(
            vol, tolerance=pd.Timedelta("100s"),
            key="id", strict_lookahead=True).toPandas()
        assert_same(new_pdf, new_pdf1)
        assert_same(new_pdf, expected_pdf)

    def test_groupByCycle(self):
        vol = self.vol()

        expected_pdf1 = make_pdf([
            (1000, [(1000, 7, 100,), (1000, 3, 200,)]),
            (1050, [(1050, 3, 300,), (1050, 7, 400,)]),
            (1100, [(1100, 3, 500,), (1100, 7, 600,)]),
            (1150, [(1150, 3, 700,), (1150, 7, 800,)]),
            (1200, [(1200, 3, 900,), (1200, 7, 1000,)]),
            (1250, [(1250, 3, 1100,), (1250, 7, 1200,)]),
        ], ["time", "rows"])

        new_pdf1 = vol.groupByCycle().toPandas()
        assert_same(new_pdf1, expected_pdf1)

    def test_summarizeCycles(self):
        from ts.flint import summarizers

        vol = self.vol()
        vol2 = self.vol2()

        expected_pdf1 = make_pdf([
            (1000, 300.0,),
            (1050, 700.0,),
            (1100, 1100.0,),
            (1150, 1500.0,),
            (1200, 1900.0,),
            (1250, 2300.0,),
        ], ["time", "volume_sum"])
        new_pdf1 = vol.summarizeCycles(summarizers.sum("volume")).toPandas()
        assert_same(new_pdf1, expected_pdf1)

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
        assert_same(new_pdf2, expected_pdf2)

    def test_summarizeCycles_udf(self):
        from ts.flint import udf
        from pyspark.sql.types import DoubleType, LongType
        from collections import OrderedDict
        import pyspark.sql.functions as F

        vol = self.vol()
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
                ('mean', udf(lambda v, w: np.average(v, weights=w), DoubleType())(weighted_vol['volume'],
                                                                                  weighted_vol['weight'])),
                ('sum', udf(lambda v, w: (v * w).sum(), LongType())(weighted_vol['volume'], weighted_vol['weight'])),
            ])
        ).toPandas()

        result4 = weighted_vol.summarizeCycles(
            OrderedDict([
                ('mean', udf(lambda df: np.average(df.volume, weights=df.weight), DoubleType())(
                    weighted_vol[['volume', 'weight']])),
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

        assert_same(result1, expected_pdf1)
        assert_same(result2, expected_pdf1)
        assert_same(result3, expected_pdf1)
        assert_same(result4, expected_pdf1)
        assert_same(result5, expected_pdf1)
        assert_same(result6, expected_pdf1)
        assert_same(result7, expected_pdf1)
        assert_same(result8, expected_pdf1)

    def test_udf(self):
        from ts.flint import udf
        import pyspark.sql.functions as F
        from pyspark.sql.types import LongType

        vol = self.vol()

        @udf(LongType())
        def foo(v, w):
            return v*2

        result1 = vol.withColumn("volume", foo(vol['volume'], F.lit(42))).toPandas()
        result2 = vol.withColumn("volume", udf(lambda v, w: v*2, LongType())(vol['volume'], F.lit(42))).toPandas()

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

        assert_same(result1, expected_pdf1)
        assert_same(result2, expected_pdf1)

    def test_groupByInterval(self):
        vol = self.vol()

        clock = self.flintContext.read.pandas(make_pdf([
            (1000,),
            (1100,),
            (1200,),
            (1300,),
        ], ["time"]))

        expected_pdf1 = make_pdf([
            (1100, [(1000, 7, 100,), (1000, 3, 200,), (1050, 3, 300,), (1050, 7, 400,)]),
            (1200, [(1100, 3, 500,), (1100, 7, 600,), (1150, 3, 700,), (1150, 7, 800,)]),
            (1300, [(1200, 3, 900,), (1200, 7, 1000,), (1250, 3, 1100,), (1250, 7, 1200,)])
        ], ["time", "rows"])

        new_pdf1 = vol.groupByInterval(clock).toPandas()
        assert_same(new_pdf1, expected_pdf1)

    def test_summarizeIntervals(self):
        from ts.flint import summarizers

        vol = self.vol()

        clock = self.flintContext.read.pandas(make_pdf([
            (1000,),
            (1100,),
            (1200,),
            (1300,),
        ], ["time"]))

        new_pdf1 = vol.summarizeIntervals(clock, summarizers.sum("volume")).toPandas()
        expected_pdf1 = make_pdf([
            (1100, 1000.0),
            (1200, 2600.0),
            (1300, 4200.0),
        ], ["time", "volume_sum"])

        assert_same(new_pdf1, expected_pdf1)

        new_pdf2 = vol.summarizeIntervals(clock, summarizers.sum("volume"), key="id").toPandas()
        expected_pdf2 = make_pdf([
            (1100, 7, 500.0),
            (1100, 3, 500.0),
            (1200, 3, 1200.0),
            (1200, 7, 1400.0),
            (1300, 3, 2000.0),
            (1300, 7, 2200.0),
        ], ["time", "id", "volume_sum"])

        assert_same(new_pdf2, expected_pdf2)

    def test_summarizeIntervals_udf(self):
        from ts.flint.functions import udf
        from pyspark.sql.types import LongType, DoubleType

        vol = self.vol()

        clock = self.flintContext.read.pandas(make_pdf([
            (1000,),
            (1100,),
            (1200,),
            (1300,),
        ], ["time"]))

        result1 = vol.summarizeIntervals(
            clock,
            {'sum': udf(lambda v: v.sum(), LongType())(vol.volume)}
        ).toPandas()
        expected1 = make_pdf([
            (1100, 1000),
            (1200, 2600),
            (1300, 4200),
        ], ["time", "sum"])
        assert_same(result1, expected1)

        result2 = vol.summarizeIntervals(
            clock,
            {'sum': udf(lambda v: v.sum(), LongType())(vol.volume)},
            key="id"
        ).toPandas()
        expected2 = make_pdf([
            (1100, 7, 500),
            (1100, 3, 500),
            (1200, 3, 1200),
            (1200, 7, 1400),
            (1300, 3, 2000),
            (1300, 7, 2200),
        ], ["time", "id", "sum"])
        assert_same(result2, expected2)

        result3 = vol.summarizeIntervals(
            clock,
            {'sum': udf(lambda v: v.sum(), LongType())(vol.volume)},
            rounding="begin"
        ).toPandas()
        expected3 = make_pdf([
            (1000, 1000),
            (1100, 2600),
            (1200, 4200),
        ], ["time", "sum"])
        assert_same(result3, expected3)

        result4 = vol.summarizeIntervals(
            clock,
            {'sum': udf(lambda v: v.sum(), LongType())(vol.volume)},
            inclusion="end"
        ).toPandas()
        expected4 = make_pdf([
            (1100, 1800),
            (1200, 3400),
            (1300, 2300),
        ], ["time", "sum"])
        assert_same(result4, expected4)

        result5 = vol.summarizeIntervals(
            clock,
            {'mean': udf(lambda v: v.mean(), DoubleType())(vol.volume),
             'sum': udf(lambda v: v.sum(), LongType())(vol.volume)}
        ).toPandas()
        expected5 = make_pdf([
            (1100, 250.0, 1000),
            (1200, 650.0, 2600),
            (1300, 1050.0, 4200),
        ], ["time", "mean", "sum"])
        assert_same(result5, expected5)

        @udf((DoubleType(), LongType()))
        def mean_and_sum(v):
            return v.mean(), v.sum()

        result6 = vol.summarizeIntervals(
            clock,
            {('mean', 'sum'): mean_and_sum(vol.volume) }
        ).toPandas()
        expected6 = make_pdf([
            (1100, 250.0, 1000),
            (1200, 650.0, 2600),
            (1300, 1050.0, 4200),
        ], ["time", "mean", "sum"])
        assert_same(result6, expected6)

        from pyspark.sql.functions import lit

        vol_with_weights = vol.withColumn('w', lit(1.0))
        result7 = vol_with_weights.summarizeIntervals(
            clock,
            {'weighted_mean': udf(lambda df: np.average(df.volume, weights=df.w), DoubleType()) \
                (vol_with_weights[['volume', 'w']]) }
        ).toPandas()
        expected7 = make_pdf([
            (1100, 250.0),
            (1200, 650.0),
            (1300, 1050.0),
        ], ["time", "weighted_mean"])
        assert_same(result7, expected7)

    def test_summarizeWindows(self):
        from ts.flint import windows
        from ts.flint import summarizers

        vol = self.vol()

        w = windows.past_absolute_time('99s')

        new_pdf1 = vol.summarizeWindows(w, summarizers.sum("volume")).toPandas()
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
        assert_same(new_pdf1, expected_pdf1)

        new_pdf2 = (vol.summarizeWindows(w,
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
        assert_same(new_pdf2, expected_pdf2)

    def test_summarizeWindows_udf(self):
        from ts.flint import udf
        from ts.flint import windows
        from collections import OrderedDict
        from pyspark.sql.types import DoubleType, LongType

        vol = self.vol()
        w = windows.past_absolute_time('99s')

        @udf(DoubleType())
        def mean(v):
            return v.mean()
        result7 = vol.summarizeWindows(
            w,
            {'mean': mean(vol['volume'])},
            key='id'
        ).toPandas()
        expected7 = make_pdf([
            (1000, 7, 100, 100.0),
            (1000, 3, 200, 200.0),
            (1050, 3, 300, 250.0),
            (1050, 7, 400, 250.0),
            (1100, 3, 500, 400.0),
            (1100, 7, 600, 500.0),
            (1150, 3, 700, 600.0),
            (1150, 7, 800, 700.0),
            (1200, 3, 900, 800.0),
            (1200, 7, 1000, 900.0),
            (1250, 3, 1100, 1000.0),
            (1250, 7, 1200, 1100.0),
        ], ['time', 'id', 'volume', 'mean'])
        assert_same(result7, expected7)

        result8 = vol.summarizeWindows(
            w,
            {'mean': mean(vol['volume'])}
        ).toPandas()
        expected8 = make_pdf([
            (1000, 7, 100, 150.0),
            (1000, 3, 200, 150.0),
            (1050, 3, 300, 250.0),
            (1050, 7, 400, 250.0),
            (1100, 3, 500, 450.0),
            (1100, 7, 600, 450.0),
            (1150, 3, 700, 650.0),
            (1150, 7, 800, 650.0),
            (1200, 3, 900, 850.0),
            (1200, 7, 1000, 850.0),
            (1250, 3, 1100, 1050.0),
            (1250, 7, 1200, 1050.0),
        ], ['time', 'id', 'volume', 'mean'])
        assert_same(result8, expected8)

    def test_summarizeWindows_numpy_udf(self):
        from ts.flint import windows
        from ts.flint.functions import udf
        from pyspark.sql.types import DoubleType, LongType

        vol = self.vol()
        df = self.flintContext.read.pandas(make_pdf([
            (1000, 3, 10.0),
            (1000, 7, 20.0),
            (1050, 3, 30.0),
            (1050, 7, 40.0),
            (1100, 3, 50.0),
            (1150, 3, 60.0),
            (1150, 7, 70.0),
            (1200, 3, 80.0),
            (1200, 7, 90.0),
            (1250, 7, 100.0),
        ], ['time', 'id', 'v']))

        @udf(DoubleType(), arg_type='numpy')
        def mean_np(v):
            assert isinstance(v, np.ndarray)
            return v.mean()

        @udf((DoubleType(), LongType()), arg_type='numpy')
        def mean_and_sum_np(v):
            assert isinstance(v, np.ndarray)
            return v.mean(), v.sum()

        @udf(DoubleType(), arg_type='numpy')
        def mean_np_df(window):
            assert isinstance(window, list)
            assert isinstance(window[-1], np.ndarray)
            return window[-1].mean()

        @udf(DoubleType(), arg_type='numpy')
        def mean_np_2(v, window):
            assert isinstance(v, np.float64)
            assert isinstance(window, list)
            assert isinstance(window[-1], np.ndarray)
            return v + window[-1].mean()

        @udf(DoubleType(), arg_type='numpy')
        def mean_np_df_2(left, window):
            assert isinstance(left, list)
            assert isinstance(left[0], np.float64)
            assert isinstance(window, list)
            assert isinstance(window[-1], np.ndarray)
            return window[-1].mean()

        w = windows.past_absolute_time('99s')

        result1 = vol.summarizeWindows(
            w,
            {'mean': mean_np(vol['volume'])}
        ).toPandas()
        expected1 = make_pdf([
            (1000, 7, 100, 150.0),
            (1000, 3, 200, 150.0),
            (1050, 3, 300, 250.0),
            (1050, 7, 400, 250.0),
            (1100, 3, 500, 450.0),
            (1100, 7, 600, 450.0),
            (1150, 3, 700, 650.0),
            (1150, 7, 800, 650.0),
            (1200, 3, 900, 850.0),
            (1200, 7, 1000, 850.0),
            (1250, 3, 1100, 1050.0),
            (1250, 7, 1200, 1050.0),
        ], ['time', 'id', 'volume', 'mean'])
        assert_same(result1, expected1)

        result2 = vol.summarizeWindows(
            w,
            {'mean': mean_np(vol['volume'])},
            key = 'id'
        ).toPandas()
        expected2 = make_pdf([
            (1000, 7, 100, 100.0),
            (1000, 3, 200, 200.0),
            (1050, 3, 300, 250.0),
            (1050, 7, 400, 250.0),
            (1100, 3, 500, 400.0),
            (1100, 7, 600, 500.0),
            (1150, 3, 700, 600.0),
            (1150, 7, 800, 700.0),
            (1200, 3, 900, 800.0),
            (1200, 7, 1000, 900.0),
            (1250, 3, 1100, 1000.0),
            (1250, 7, 1200, 1100.0),
        ], ['time', 'id', 'volume', 'mean'])
        assert_same(result2, expected2)

        result3 = vol.summarizeWindows(
            w,
            {'mean': mean_np_df(vol[['volume']])},
        ).toPandas()
        expected3 = expected1
        assert_same(result3, expected3)

        result4 = vol.summarizeWindows(
            w,
            {'mean': mean_np_df(vol[['time', 'volume']])},
        ).toPandas()
        expected4 = expected1
        assert_same(result4, expected4)

        result8 = vol.summarizeWindows(
            w,
            {('mean', 'sum'): mean_and_sum_np(vol['volume'])},
            key = 'id'
        ).toPandas()
        expected8 = make_pdf([
            (1000, 7, 100, 100.0, 100),
            (1000, 3, 200, 200.0, 200),
            (1050, 3, 300, 250.0, 500),
            (1050, 7, 400, 250.0, 500),
            (1100, 3, 500, 400.0, 800),
            (1100, 7, 600, 500.0, 1000),
            (1150, 3, 700, 600.0, 1200),
            (1150, 7, 800, 700.0, 1400),
            (1200, 3, 900, 800.0, 1600),
            (1200, 7, 1000, 900.0, 1800),
            (1250, 3, 1100, 1000.0, 2000),
            (1250, 7, 1200, 1100.0, 2200),
        ], ['time', 'id', 'volume', 'mean', 'sum'])
        assert_same(result8, expected8)

    def test_addSummaryColumns(self):
        from ts.flint import summarizers

        vol = self.vol()

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
        assert_same(new_pdf, expected_pdf)

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
        assert_same(new_pdf, expected_pdf, "with key")

    def test_addColumnsForCycle_udf(self):
        from ts.flint import udf
        from pyspark.sql.types import DoubleType
        from collections import OrderedDict

        price2 = self.price2()

        result1 = price2.addColumnsForCycle({
            'rank': udf(lambda v: v.rank(), DoubleType())(price2['price'])
        }).toPandas()

        expected1 = make_pdf([
            (0, 1, 1.0, 1.0),
            (0, 2, 2.0, 2.0),
            (1, 1, 3.0, 1.0),
            (1, 2, 4.0, 2.0),
            (1, 3, 5.0, 3.0),
        ], ['time', 'id', 'price', 'rank'])
        assert_same(result1, expected1)

        result2 = price2.addColumnsForCycle(OrderedDict([
            ('rank', udf(lambda v: v.rank(), DoubleType())(price2['price'])),
            ('pct_rank', udf(lambda v: v.rank(pct=True), DoubleType())(price2['price']))
        ])).toPandas()

        expected2 = make_pdf([
            (0, 1, 1.0, 1.0, 0.5),
            (0, 2, 2.0, 2.0, 1.0),
            (1, 1, 3.0, 1.0, 0.333333),
            (1, 2, 4.0, 2.0, 0.666667),
            (1, 3, 5.0, 3.0, 1.0),
        ], ['time', 'id', 'price', 'rank', 'pct_rank'])

        pdt.assert_frame_equal(result2, expected2)

        @udf((DoubleType(), DoubleType()))
        def rank(v):
            return v.rank(), v.rank(pct=True)

        result3 = price2.addColumnsForCycle({
            ('rank', 'pct_rank'): rank(price2['price']),
        }).toPandas()
        expected3 = expected2
        pdt.assert_frame_equal(result3, expected3)

    def test_addWindows(self):
        from ts.flint import windows
        from pyspark.sql import Row

        vol = self.vol()
        VolRow = Row('time', 'id', 'volume')

        print(vol.collect())

        id = [VolRow(int(r['time'].strftime('%s')), r['id'], r['volume'])
              for r in vol.collect()]

        print(id)

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
        ], ["time", "id", "volume", "window_past_50s"])

        new_pdf = vol.addWindows(windows.past_absolute_time("50s")).toPandas()
        assert_same(new_pdf, expected_pdf)


    def test_shiftTime(self):
        price = self.price()

        delta = pd.Timedelta('1000s')
        expected_pdf = price.toPandas()
        expected_pdf.time += delta
        new_pdf = price.shiftTime(delta).toPandas()
        assert_same(new_pdf, expected_pdf, "forwards")

        expected_pdf = price.toPandas()
        expected_pdf.time -= delta
        new_pdf = price.shiftTime(delta, backwards=True).toPandas()
        assert_same(new_pdf, expected_pdf, "backwards")


    def test_shiftTime_windows(self):
        import datetime
        from ts.flint import windows

        friday = datetime.datetime(2001, 11, 9, 15, 0).timestamp()
        saturday = datetime.datetime(2001, 11, 10, 15, 0).timestamp()
        monday = datetime.datetime(2001, 11, 12, 15, 0).timestamp()
        tuesday = datetime.datetime(2001, 11, 13, 15, 0).timestamp()
        wednesday = datetime.datetime(2001, 11, 14, 15, 0).timestamp()
        thrusday = datetime.datetime(2001, 11, 15, 15, 0).timestamp()

        dates = self.flintContext.read.pandas(make_pdf([
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
        assert_same(result1, expected1)

    def test_uniform_clocks(self):
        from ts.flint import clocks
        df = clocks.uniform(self.sqlContext, '1d', '0s', '2016-11-07', '2016-11-17')
        assert(df.count() == 11)
        # the last timestamp should be 17 Nov 2016 00:00:00 GMT
        assert(df.collect()[-1]['time'] == make_timestamp(1479340800))

    def test_read_uniform_clock(self):
        expected_exclusive = pd.date_range('20171116 12:00:05am',
                                           tz='Asia/Tokyo', periods=2880,
                                           freq='30s').tz_convert("UTC").tz_localize(None)
        actual_exclusive = (self.flintContext.read
                            .range('2017-11-16', '2017-11-17 12:00:05am',
                                   'Asia/Tokyo')
                            .clock('uniform', '30s', '5s', end_inclusive=False)
                            .toPandas()['time'])

        assert np.all(expected_exclusive == actual_exclusive)

        expected_inclusive = pd.date_range('20171116', periods=2881,
                                           freq='30s').tz_localize(None)
        actual_inclusive = (self.flintContext.read
                            .range('2017-11-16', '2017-11-17')
                            .clock('uniform', '30s')
                            .toPandas()['time'])

        assert np.all(expected_inclusive == actual_inclusive)

    def test_groupedData(self):
        from pyspark.sql import DataFrame
        from pyspark.sql.functions import sum, pandas_udf, PandasUDFType
        from ts.flint import TimeSeriesGroupedData

        price = self.price()

        assert(type(price.groupBy('time')) is TimeSeriesGroupedData)
        assert(type(price.groupby('time')) is TimeSeriesGroupedData)

        result1 = price.groupBy('time').agg(sum(price['price'])).sort('time').toPandas()
        expected1 = DataFrame.groupBy(price, 'time').agg(sum(price['price'])).sort('time').toPandas()
        assert_same(result1, expected1)

        result2 = price.groupBy('time').pivot('id').sum('price').toPandas()
        expected2 = DataFrame.groupBy(price, 'time').pivot('id').sum('price').toPandas()
        assert_same(result2, expected2)

        @pandas_udf(price.schema, PandasUDFType.GROUPED_MAP)
        def foo(df):
            return df
        result3 = price.groupby('time').apply(foo).toPandas()
        expected3 = DataFrame.groupBy(price, 'time').apply(foo).toPandas()
        assert_same(result3, expected3)

        result4 = price.groupby('time').count().toPandas()
        expected4 = DataFrame.groupBy(price, 'time').count().toPandas()
        assert_same(result4, expected4)

        result5 = price.groupby('time').mean('price').toPandas()
        expected5 = DataFrame.groupBy(price, 'time').mean('price').toPandas()
        assert_same(result5, expected5)

    def test_preview(self):
        price = self.price()
        assert_same(price.limit(10).toPandas(), price.preview())

    def test_column_selections(self):
        price = self.price()
        assert_same(price.select('price').toPandas(), price.toPandas()[['price']])
        assert_same(price.select('time').toPandas(), price.toPandas()[['time']])

    def test_withColumn_time(self):
        from ts.flint import TimeSeriesDataFrame
        from pyspark.sql import DataFrame
        from tests.test_data import FORECAST_DATA

        pdf = make_pdf(FORECAST_DATA, ["time", "id", "forecast"])
        df = self.flintContext.read.pandas(pdf)
        df = df.withColumn("time", df.time)
        assert(not isinstance(df, TimeSeriesDataFrame))
        assert(isinstance(df, DataFrame))
        expected = pdf.assign(time=pdf['time'])
        assert_same(df.toPandas(), expected)

    def test_describe(self):
        from tests.test_data import FORECAST_DATA

        df = self.flintContext.read.pandas(make_pdf(FORECAST_DATA, ["time", "id", "forecast"]))
        df.describe()

    def test_empty_df(self):
        from pyspark.sql.types import LongType, TimestampType, StructType, StructField
        from ts.flint import summarizers
        df = self.sqlContext.createDataFrame(
            self.sc.emptyRDD(),
            schema=StructType([StructField('time', TimestampType())]))
        df2 = self.flintContext.read.dataframe(df)
        df3 = df2.summarize(summarizers.count())
        assert(df2.count() == 0)
        assert(df3.count() == 0)
        assert(df2.schema == StructType([StructField('time', TimestampType())]))
        assert(df3.schema == StructType([StructField('time', TimestampType()), StructField('count', LongType())]))


if __name__ == '__main__':
    unittest.main()
