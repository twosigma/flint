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

package com.twosigma.flint.timeseries

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.timeseries.clock.UniformClock
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.time.TimeFormat
import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration

class ClockSpec extends FlatSpec with SharedSparkContext {

  "Clock" should "generate clock ticks correctly" in {
    var clock = UniformClock(1000L, 2000L, 300L, 0L).toArray
    var benchmark = Array(1000L, 1300L, 1600L, 1900L)
    assert(clock.deep == benchmark.deep)

    clock = UniformClock(1000L, 2000L, 300L, 500L).toArray
    benchmark = Array(1200L, 1500L, 1800L)
    assert(clock.deep == benchmark.deep)
  }

  it should "generate clock ticks in RDD correctly" in {
    var clockRdd = UniformClock(sc, 1000L, 5000L, 30, 0L, 10).map(_._1)
    var clockStream = UniformClock(1000L, 5000L, 30, 0L)
    assert(clockRdd.collect().deep == clockStream.toArray.deep)

    clockRdd = UniformClock(sc, 1000L, 5000L, 30, 500L, 10).map(_._1)
    clockStream = UniformClock(1000L, 5000L, 30, 500L)
    assert(clockRdd.collect().deep == clockStream.toArray.deep)
  }

  it should "generate clock ticks in TimeSeriesRDD correctly" in {
    val clockTSRdd: TimeSeriesRDD = Clocks.uniform(sc, "5h", "0d", "20010101", "20010201")
    assert(clockTSRdd.schema == Schema())
    val clockStream = UniformClock(
      TimeFormat.parseNano("20010101"),
      TimeFormat.parseNano("20010201"),
      Duration("5h").toNanos,
      0L
    )
    val rdd = clockTSRdd.rdd.map(r => r.getAs[Long](TimeSeriesRDD.timeColumnName))
    assert(rdd.collect().deep == clockStream.toArray.deep)
  }

  it should "generate clock ticks with offset in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD = Clocks.uniform(sc, "1d", "8h", "20010101", "20010201")
    val clockTSRdd2: TimeSeriesRDD = Clocks.uniform(sc, "1d", "0h", "20010101 08:00", "20010201")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }

  it should "generate clock ticks with offset & time zone in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD = Clocks.uniform(sc, "1d", "8h", "20010101", "20010201", "UTC")
    val clockTSRdd2: TimeSeriesRDD = Clocks.uniform(sc, "1d", "0h", "20010101 08:00 +0000", "20010201 00:00 +0000", "MST")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }

  it should "generate clock ticks with default in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD = Clocks.uniform(sc, "1d", "0h", "19000101", "21000101", "UTC")
    val clockTSRdd2: TimeSeriesRDD = Clocks.uniform(sc, "1d")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }
}
