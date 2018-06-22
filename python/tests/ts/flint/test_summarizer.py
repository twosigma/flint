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

import unittest

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

from tests.utils import *

BASE = get_test_base()

class TestSummarizer(BASE):
    def test_summary_sum(self):
        from ts.flint import summarizers

        vol = self.vol()

        expected_pdf = make_pdf([
            (0, 7800.0,)
        ], ["time", "volume_sum"])

        new_pdf = vol.summarize(summarizers.sum("volume")).toPandas()
        assert_same(new_pdf, expected_pdf)

        expected_pdf = make_pdf([
            (0, 7, 4100.0,),
            (0, 3, 3700.0,),
        ], ["time", "id", "volume_sum"])

        new_pdf = vol.summarize(summarizers.sum("volume"), key=["id"]).toPandas()
        new_pdf1 = vol.summarize(summarizers.sum("volume"), key="id").toPandas()
        assert_same(new_pdf, new_pdf1)

        # XXX: should just do tests_utils.assert_same(new_pdf, expected_pdf, "by id")
        # once https://gitlab.twosigma.com/analytics/huohua/issues/26 gets resolved.
        assert_same(
            new_pdf[new_pdf['id'] == 3].reset_index(drop=True),
            expected_pdf[expected_pdf['id'] == 3].reset_index(drop=True),
            "by id 3"
        )
        assert_same(
            new_pdf[new_pdf['id'] == 7].reset_index(drop=True),
            expected_pdf[expected_pdf['id'] == 7].reset_index(drop=True),
            "by id 7"
        )


    def test_summary_zscore(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 1.5254255396193801,)
        ], ["time", "price_zScore"])

        new_pdf = price.summarize(summarizers.zscore("price", in_sample=True)).toPandas()
        assert_same(new_pdf, expected_pdf, "in-sample")

        expected_pdf = make_pdf([
            (0, 1.8090680674665818,)
        ], ["time", "price_zScore"])

        new_pdf = price.summarize(summarizers.zscore("price", in_sample=False)).toPandas()
        assert_same(new_pdf, expected_pdf, "out-of-sample)")


    def test_summary_nth_moment(self):
        from ts.flint import summarizers

        price = self.price()

        moments = [price.summarize(summarizers.nth_moment("price", i), key="id").collect() for i in range(5)]
        for m in moments:
            m.sort(key=lambda r: r['id'])
        moments = [[r["price_{}thMoment".format(i)] for r in moments[i]] for i in range(len(moments))]

        assert_same(moments[0][0], 1.0, "moment 0: 0")
        assert_same(moments[0][1], 1.0, "moment 0: 1")

        assert_same(moments[1][0], 3.0833333333333335, "moment 1: 1")
        assert_same(moments[1][1], 3.416666666666667, "moment 1: 0")

        assert_same(moments[2][0], 12.041666666666668, "moment 2: 1")
        assert_same(moments[2][1], 15.041666666666666, "moment 2: 0")

        assert_same(moments[3][0], 53.39583333333333, "moment 3: 1")
        assert_same(moments[3][1], 73.35416666666667, "moment 3: 0")

        assert_same(moments[4][0], 253.38541666666669, "moment 4: 1")
        assert_same(moments[4][1], 379.0104166666667, "moment 4: 0")


    def test_summary_nth_central_moment(self):
        from ts.flint import summarizers

        price = self.price()

        moments = [price.summarize(summarizers.nth_central_moment("price", i), key="id").collect() for i in range(1,5)]
        for m in moments:
            m.sort(key=lambda r: r['id'])
        moments = [[r["price_{}thCentralMoment".format(i+1)] for r in moments[i]] for i in range(len(moments))]

        assert_same(moments[0][0], 0.0, "moment 1: 0")
        assert_same(moments[0][1], 0.0, "moment 1: 1")

        assert_same(moments[1][0], 2.534722222222222, "moment 2: 1")
        assert_same(moments[1][1], 3.3680555555555554, "moment 2: 0")

        assert_same(moments[2][0], 0.6365740740740735, "moment 3: 1")
        assert_same(moments[2][1], -1.0532407407407405, "moment 3: 0")

        assert_same(moments[3][0], 10.567563657407407, "moment 4: 1")
        assert_same(moments[3][1], 21.227285879629633, "moment 4: 0")


    def test_summary_correlation(self):
        import pyspark
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        joined = price.leftJoin(forecast, key="id")
        joined = (joined
                  .withColumn("price2", joined.price)
                  .withColumn("price3", -joined.price)
                  .withColumn("price4", 2 * joined.price)
                  .withColumn("price5", pyspark.sql.functions.lit(0)))

        def price_correlation(column):
            corr = joined.summarize(summarizers.correlation("price", column), key=["id"])
            assert_same(
                corr.toPandas(),
                joined.summarize(summarizers.correlation(["price"], [column]), key="id").toPandas()
            )
            assert_same(
                corr.toPandas(),
                joined.summarize(summarizers.correlation(["price", column]), key="id").toPandas()
            )
            return corr.collect()

        results = [price_correlation("price{}".format(i)) for i in range(2,6)]
        for r in results:
            r.sort(key=lambda r: r['id'])
        results.append(price_correlation("forecast"))

        assert_same(results[0][0]["price_price2_correlation"], 1.0, "price2: 1")
        assert_same(results[0][1]["price_price2_correlation"], 1.0, "price2: 0")

        assert_same(results[1][0]["price_price3_correlation"], -1.0, "price3: 1")
        assert_same(results[1][1]["price_price3_correlation"], -1.0, "price3: 0")

        assert_same(results[2][0]["price_price4_correlation"], 1.0, "price4: 1")
        assert_same(results[2][1]["price_price4_correlation"], 1.0, "price4: 0")

        assert_true(np.isnan(results[3][0]["price_price5_correlation"]), "price5: 1")
        assert_true(np.isnan(results[3][1]["price_price5_correlation"]), "price5: 0")

        assert_same(results[4][0]["price_forecast_correlation"], -0.47908485866330514, "forecast: 1")
        assert_same(results[4][0]["price_forecast_correlationTStat"], -1.0915971793294055, "forecastTStat: 1")
        assert_same(results[4][1]["price_forecast_correlation"], -0.021896121374023046, "forecast: 0")
        assert_same(results[4][1]["price_forecast_correlationTStat"], -0.04380274440368827, "forecastTStat: 0")

    def test_summary_weighted_correlation(self):
        import pyspark.sql.functions as F
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        joined = price.leftJoin(forecast, key="id").withColumn('weight', F.lit(1.0)).withColumn('weight2', F.lit(42.0))
        result = joined.summarize(summarizers.weighted_correlation("price", "forecast", "weight")).toPandas()
        result2 = joined.summarize(summarizers.weighted_correlation("price", "forecast", "weight2")).toPandas()
        expected = joined.summarize(summarizers.correlation("price", "forecast")).toPandas()

        assert(np.isclose(
            result['price_forecast_weight_weightedCorrelation'][0],
            expected['price_forecast_correlation'][0]))

        assert(np.isclose(
            result2['price_forecast_weight2_weightedCorrelation'][0],
            expected['price_forecast_correlation'][0]))

    def test_summary_linearRegression(self):
        """
        Test the python binding for linearRegression. This does NOT test the correctness of the regression.
        """
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        joined = price.leftJoin(forecast, key="id")
        result = joined.summarize(summarizers.linear_regression("price", ["forecast"])).collect()

    def test_summary_ema_halflife(self):
        """
        Test the python binding for ema_halflife. This does NOT test the correctness of the ema.
        """
        from ts.flint import summarizers
        price = self.price()
        result = price.summarize(summarizers.ema_halflife("price", "1d")).collect()

    def test_summary_ewma(self):
        """
        Test the python binding for ewma. This does NOT test the correctness of the ewma.
        """
        from ts.flint import summarizers
        price = self.price()
        result = price.summarize(summarizers.ewma("price")).collect()

    def test_summary_max(self):
        from ts.flint import summarizers

        forecast = self.forecast()
        expected_pdf = make_pdf([
            (0, 6.4,)
        ], ["time", "forecast_max"])

        result = forecast.summarize(summarizers.max("forecast")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_mean(self):
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()
        expected_pdf = make_pdf([
            (0, 3.25,)
        ], ["time", "price_mean"])

        joined = price.leftJoin(forecast, key="id")
        result = joined.summarize(summarizers.mean("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_weighted_mean(self):
        from ts.flint import summarizers

        price = self.price()
        vol = self.vol()

        expected_pdf = make_pdf([
            (0, 4.166667, 1.547494, 8.237545, 12,)
        ], ["time", "price_volume_weightedMean", "price_volume_weightedStandardDeviation", "price_volume_weightedTStat", "price_volume_observationCount"])

        joined = price.leftJoin(vol, key="id")
        result = joined.summarize(summarizers.weighted_mean("price", "volume")).toPandas()

        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_min(self):
        from ts.flint import summarizers

        forecast = self.forecast()

        expected_pdf = make_pdf([
            (0, -9.6,)
        ], ["time", "forecast_min"])

        result = forecast.summarize(summarizers.min("forecast")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_quantile(self):
        from ts.flint import summarizers

        forecast = self.forecast()
        expected_pdf = make_pdf([
            (0, -2.22, 1.75)
        ], ["time", "forecast_0.2quantile", "forecast_0.5quantile"])

        result = forecast.summarize(summarizers.quantile("forecast", (0.2, 0.5))).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_stddev(self):
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        expected_pdf = make_pdf([
            (0, 1.802775638,)
        ], ["time", "price_stddev"])
        joined = price.leftJoin(forecast, key="id")

        result = joined.summarize(summarizers.stddev("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_variance(self):
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        expected_pdf = make_pdf([
            (0, 3.25,)
        ], ["time", "price_variance"])

        joined = price.leftJoin(forecast, key="id")
        result = joined.summarize(summarizers.variance("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_covariance(self):
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        expected_pdf = make_pdf([
            (0, -1.802083333,)
        ], ["time", "price_forecast_covariance"])

        joined = price.leftJoin(forecast, key="id")
        result = joined.summarize(summarizers.covariance("price", "forecast")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_weighted_covariance(self):
        import pyspark.sql.functions as F
        from ts.flint import summarizers

        price = self.price()
        forecast = self.forecast()

        expected_pdf = make_pdf([
            (0, -1.96590909091,)
        ], ["time", "price_forecast_weight_weightedCovariance"])

        joined = price.leftJoin(forecast, key="id").withColumn('weight', F.lit(2.0))
        result = joined.summarize(summarizers.weighted_covariance("price", "forecast", "weight")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_product(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 116943.75)
        ], ["time", "price_product"])

        result = price.summarize(summarizers.product("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_dot_product(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 162.5)
        ], ["time", "price_price_dotProduct"])

        result = price.summarize(summarizers.dot_product("price", "price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_geometric_mean(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 2.644425997)
        ], ["time", "price_geometricMean"])

        result = price.summarize(summarizers.geometric_mean("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_skewness(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 0.0)
        ], ["time", "price_skewness"])

        result = price.summarize(summarizers.skewness("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_kurtosis(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, -1.216783217)
        ], ["time", "price_kurtosis"])

        result = price.summarize(summarizers.kurtosis("price")).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_compose(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 6.0, 0.5, 3.25, 1.802775638,)
        ], ["time", "price_max", "price_min", "price_mean", "price_stddev"])

        result = price.summarize([summarizers.max("price"),
                                  summarizers.min("price"),
                                  summarizers.mean("price"),
                                  summarizers.stddev("price")]).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)

    def test_summary_prefix(self):
        from ts.flint import summarizers

        price = self.price()

        expected_pdf = make_pdf([
            (0, 6.0, 6.0)
        ], ["time", "price_max", "prefix_price_max"])

        result = price.summarize([summarizers.max("price"),
                                  summarizers.max("price").prefix("prefix")]).toPandas()
        pdt.assert_frame_equal(result, expected_pdf)