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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.{ Summarizers, TimeSeriesGenerator }
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.types.DoubleType

class WeightedCorrelationSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String =
    "/timeseries/summarize/summarizer/weightedcorrelationsummarizer"

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
          2.0
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

  "WeightedCorrelationSummarizer" should "compute uniform weighted correlation correctly" in {
    val test = data
      .summarize(Summarizers.weightedCorrelation("x", "y", "w1"))
      .collect()
      .head
      .getAs[Double]("x_y_w1_weightedCorrelation")

    val xRdd = data.rdd.map(_.getAs[Double]("x"))
    val yRdd = data.rdd.map(_.getAs[Double]("y"))

    assert(test === Statistics.corr(xRdd, yRdd))
  }

  it should "compute non-uniform weighted correlation correctly" in {
    val tsRdd =
      fromCSV(
        "Data.csv",
        Schema("w" -> DoubleType, "x" -> DoubleType, "y" -> DoubleType)
      )

    assert(
      tsRdd
        .summarize(Summarizers.weightedCorrelation("x", "y", "w"))
        .first()
        .getAs[Double]("x_y_w_weightedCorrelation") === -1.0
    )

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

    assert(
      data
        .summarize(Summarizers.weightedCorrelation("x", "y", "w2"))
        .first()
        .getAs[Double]("x_y_w2_weightedCorrelation") === 0.02017465386798503
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(
      Summarizers.weightedCorrelation("x1", "x2", "w2")
    )
    summarizerPropertyTest(AllProperties)(
      Summarizers.weightedCorrelation("x1", "x2", "w1")
    )
  }

  it should "ignore null values" in {
    val dataWithNull = insertNullRows(data, "x")
    assertAlmostEquals(
      data.summarize(Summarizers.weightedCorrelation("x", "y", "w2")),
      dataWithNull.summarize(Summarizers.weightedCorrelation("x", "y", "w2"))
    )
  }
}
