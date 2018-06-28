/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesGenerator }
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.sql.functions._

class WeightedCovarianceSummarizerSpec extends SummarizerSuite {

  private[this] val cycles = 10000L

  private[this] val frequency = 1000L

  private[this] lazy val data = {
    val _data = new TimeSeriesGenerator(
      sc,
      begin = 0L,
      end = cycles * frequency,
      frequency = frequency
    )(
      uniform = false,
      ids = Seq(1),
      ratioOfCycleSize = 1.0,
      columns = Seq(
        "x" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        },
        "y" -> { (_: Long, _: Int, rand: util.Random) =>
          rand.nextDouble()
        },
        "w1" -> { (_: Long, _: Int, rand: util.Random) =>
          1.0
        },
        "w2" -> { (_: Long, _: Int, rand: util.Random) =>
          Math.abs(rand.nextDouble())
        }
      ),
      numSlices = defaultPartitionParallelism,
      seed = 31415926L
    ).generate()

    _data.cache()
    _data.count()
    _data
  }

  override def afterAll(): Unit = {
    data.unpersist()
    super.afterAll()
  }

  "WeightedCovarianceSummarize" should "compute unweighted covariance correctly" in {
    /*
    Verified using the following code

    def m(x, w):
        """Weighted Mean"""
        return np.sum(x * w) / np.sum(w)

    def cov(x, y, w):
        """Weighted Covariance"""
        return np.sum(w * (x - m(x, w)) * (y - m(y, w))) / np.sum(w)

    def corr(x, y, w):
        """Weighted Correlation"""
        return cov(x, y, w) / np.sqrt(cov(x, x, w) * cov(y, y, w))
    */

    val test = data
      .summarize(Summarizers.weightedCovariance("x", "y", "w1"))
      .collect()
      .head
      .getAs[Double]("x_y_w1_weightedCovariance")

    assert(test === 0.0020384576995828955)
  }

  it should "compute weighted covariance correctly" in {
    val df = data.toDF
    val ret = df
      .withColumn("wx", col("x") * col("w2"))
      .withColumn("wy", col("y") * col("w2"))
      .withColumn("ww", col("w2") * col("w2"))
      .agg(
        sum(col("wy")),
        sum(col("wx")),
        sum(col("wy")),
        sum(col("w2")),
        sum(col("ww"))
      )
      .head()
    val sumWX = ret.getAs[Double]("sum(wx)")
    val sumWY = ret.getAs[Double]("sum(wy)")
    val sumW = ret.getAs[Double]("sum(w2)")
    val sumWW = ret.getAs[Double]("sum(ww)")
    val meanX = sumWX / sumW
    val meanY = sumWY / sumW

    val coMoment = df
      .withColumn(
        "c",
        col("w2") * (col("x") - lit(meanX)) * (col("y") - lit(meanY))
      )
      .agg(sum(col("c")))
      .head()
      .getAs[Double]("sum(c)")

    val expected = coMoment / (sumW - sumWW / sumW)

    val test = data
      .summarize(Summarizers.weightedCovariance("x", "y", "w2"))
      .first()
      .getAs[Double]("x_y_w2_weightedCovariance")

    assert(expected === test)
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(
      Summarizers.weightedCovariance("x1", "x2", "w2")
    )
    summarizerPropertyTest(AllProperties)(
      Summarizers.weightedCovariance("x1", "x2", "w1")
    )
  }

  it should "ignore null values" in {
    val dataWithNull = insertNullRows(data, "x")
    assertAlmostEquals(
      data.summarize(Summarizers.weightedCovariance("x", "y", "w2")),
      dataWithNull.summarize(Summarizers.weightedCovariance("x", "y", "w2"))
    )
  }
}
