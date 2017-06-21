/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.timeseries.summarize.summarizer.subtractable

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.SequentialArrayQueue
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import com.twosigma.flint.timeseries.{ Clocks, Summarizers, TimeSeriesRDD }
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType

class QuantileSummarizerSpec extends SummarizerSuite {
  var clockTSRdd: TimeSeriesRDD = _
  private lazy val init = {
    clockTSRdd = Clocks.uniform(
      sc,
      frequency = "1d", offset = "0d", beginDateTime = "1970-01-01", endDateTime = "1980-01-01"
    )
  }

  "SequentialArrayQueue" should "resize up correctly" in {
    val queue = new SequentialArrayQueue[Double]()
    (1 to 32).map{
      i => queue.add(i.toDouble)
    }
    assert(queue.view()._3.length == 32)
    queue.add(0.0)
    assert(queue.view()._3.length == 64)
  }

  it should "shift down correctly" in {
    val queue = new SequentialArrayQueue[Double]()
    (1 to 64).map{
      i => queue.add(i.toDouble)
    }
    assert(queue.view()._3.length == 64)
    (1 to 32).map{
      _ => queue.remove()
    }
    assert(queue.view()._1 == 0)
  }

  it should "addAll and preserve order" in {
    val queue1 = new SequentialArrayQueue[Double]()
    val queue2 = new SequentialArrayQueue[Double]()

    // Move the begin index
    (1 to 5).map{
      i =>
        queue1.add(i.toDouble)
        queue1.remove()
    }
    (1 to 3).map{
      i => queue1.add(i.toDouble)
    }

    (4 to 10).map{
      i => queue2.add(i.toDouble)
    }
    queue1.addAll(queue2)
    var index = queue1.view()._1
    for (i <- 1 to 10) {
      assert(queue1.view()._3(index) == i)
      index += 1
    }
  }

  "QuantileSummarizer" should "compute `quantile` correctly" in {
    init
    val p = (1 to 100).map(_ / 100.0)
    val results = clockTSRdd.summarize(Summarizers.quantile("time", p)).first()

    val percentileEstimator = new Percentile().withEstimationType(Percentile.EstimationType.R_7)
    percentileEstimator.setData(clockTSRdd.collect().map(_.getAs[Long]("time").toDouble))
    val expectedResults = p.map { i => percentileEstimator.evaluate(i * 100.0) }
    (1 to 100).foreach { i => assert(results.getAs[Double](s"time_${i / 100.0}quantile") === expectedResults(i - 1)) }
  }

  it should "ignore null values" in {
    init
    val input = clockTSRdd.addColumns("v" -> LongType -> { row: Row => row.getAs[Long]("time") })
    assertEquals(
      input.summarize(Summarizers.quantile("v", Seq(0.25, 0.5, 0.75, 0.9, 0.95))),
      insertNullRows(input, "v").summarize(Summarizers.quantile("v", Seq(0.25, 0.5, 0.75, 0.9, 0.95)))
    )
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllPropertiesAndSubtractable)(Summarizers.quantile("x1", Seq(0.25, 0.5, 0.75, 0.9, 0.95)))
  }
}
