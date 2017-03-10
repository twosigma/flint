/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.timeseries.Summarizers
import com.twosigma.flint.timeseries.Clocks
import com.twosigma.flint.timeseries.summarize.SummarizerSuite
import org.apache.commons.math3.stat.descriptive.rank.Percentile

class QuantileSummarizerSpec extends SummarizerSuite {

  "QuantileSummarizer" should "compute `quantile` correctly" in {
    val clockTSRdd = Clocks.uniform(
      sc,
      frequency = "1d", offset = "0d", beginDateTime = "1970-01-01", endDateTime = "1980-01-01"
    )
    val p = (1 to 100).map(_ / 100.0)
    val results = clockTSRdd.summarize(Summarizers.quantile("time", p)).first()

    val percentileEstimator = new Percentile().withEstimationType(Percentile.EstimationType.R_7)
    percentileEstimator.setData(clockTSRdd.collect().map(_.getAs[Long]("time").toDouble))
    val expectedResults = p.map { i => percentileEstimator.evaluate(i * 100.0) }
    (1 to 100).foreach { i => assert(results.getAs[Double](s"time_${i / 100.0}quantile") === expectedResults(i - 1)) }
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.quantile("x1", Seq(0.25, 0.5, 0.75, 0.9, 0.95)))
  }
}
