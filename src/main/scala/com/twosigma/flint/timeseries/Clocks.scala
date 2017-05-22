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

package com.twosigma.flint.timeseries

import org.apache.spark.SparkContext
import com.twosigma.flint.timeseries.clock.{ RandomClock, UniformClock }

object Clocks {

  private val defaultBegin = "1970-01-01"
  private val defaultEnd = "2030-01-01"

  /**
   * Returns a evenly sampled clock as a [[TimeSeriesRDD]]. The [[TimeSeriesRDD]] has only a "time" column.
   *
   * @param sc            The spark context.
   * @param frequency     The time between rows, e.g "1s", "2m", "3d" etc.
   * @param offset        The time to offset this clock from the begin time. Defaults to "0s". Note that specifying an
   *                      offset greater than the frequency is the same as specifying (offset % frequency).
   * @param beginDateTime A date time specifies the begin of this clock. Default "1970-01-01".
   * @param endDateTime   A date time specifies the end of this clock. Default "2030-01-01". It is inclusive when the
   *                      last tick is at the end of this clock.
   * @param timeZone      The time zone which will be used to parse the `beginDateTime` and `endDateTime` when time
   *                      zone information is not included in the date time string. Default "UTC".
   * @return a [[TimeSeriesRDD]] with just a "time" column and rows at a specified frequency
   */
  def uniform(
    sc: SparkContext,
    frequency: String,
    offset: String = "0s",
    beginDateTime: String = defaultBegin,
    endDateTime: String = defaultEnd,
    timeZone: String = "UTC"
  ): TimeSeriesRDD = {
    val clock = new UniformClock(sc, beginDateTime, endDateTime, frequency, offset, timeZone)
    clock.asTimeSeriesRDD(sc.defaultParallelism)
  }

  /**
   * Returns a unevenly sampled clock as a [[TimeSeriesRDD]]. The [[TimeSeriesRDD]] has only a "time" column and
   * intervals between two sequential timestamps are uniformly distributed within the range from 1 NANOSECOND
   * to `frequency`.
   *
   * @param sc            The spark context.
   * @param frequency     The time between rows, e.g "1s", "2m", "3d" etc.
   * @param offset        The time to offset this clock from the begin time. Defaults to "0s". Note that specifying an
   *                      offset greater than the frequency is the same as specifying (offset % frequency).
   * @param beginDateTime A date time specifies the begin of this clock. Default "1990-01-01".
   * @param endDateTime   A date time specifies the end of this clock. Default "2030-01-01". It is inclusive when the
   *                      last tick is at the end of this clock.
   * @param timeZone      The time zone which will be used to parse the `beginDateTime` and `endDateTime` when time
   *                      zone information is not included in the date time string. Default "UTC".
   * @param seed          The random seed expected to use when randomly generating a new tick. Default current time
   *                      in MILLISECOND.
   * @return a [[TimeSeriesRDD]] with just a "time" column.
   */
  def random(
    sc: SparkContext,
    frequency: String,
    offset: String = "0s",
    beginDateTime: String = defaultBegin,
    endDateTime: String = defaultEnd,
    timeZone: String = "UTC",
    seed: Long = System.currentTimeMillis()
  ): TimeSeriesRDD = {
    val clock = new RandomClock(sc, beginDateTime, endDateTime, frequency, offset, timeZone, seed)
    clock.asTimeSeriesRDD(sc.defaultParallelism)
  }
}
