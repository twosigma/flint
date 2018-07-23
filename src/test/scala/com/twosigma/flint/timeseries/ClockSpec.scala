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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.clock.{ RandomClock, UniformClock }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.time.TimeFormat

import scala.concurrent.duration.Duration

class ClockSpec extends TimeSeriesSuite with TimeTypeSuite {

  "UniformClock" should "generate clock ticks correctly" in {
    var clock = new UniformClock(sc, 1000L, 2000L, 300L, 0L, true).asStream().toArray
    var benchmark = Array(1000L, 1300L, 1600L, 1900L)
    assert(clock.deep == benchmark.deep)

    clock = new UniformClock(sc, 1000L, 2000L, 300L, 500L, true).asStream().toArray
    benchmark = Array(1200L, 1500L, 1800L)
    assert(clock.deep == benchmark.deep)
  }

  it should "generate clock ticks in RDD correctly" in {
    val clock = new UniformClock(sc, 1000L, 5000L, 30L, 0L, true)
    var clockRdd = clock.asOrderedRDD(10).map(_._1)
    val clockStream = clock.asStream().toArray
    assert(clockRdd.collect().deep == clockStream.deep)

    clockRdd = clock.asOrderedRDD(10).map(_._1)
    assert(clockRdd.collect().deep == clockStream.deep)
  }

  it should "generate clock ticks in TimeSeriesRDD correctly" in {
    val clockTSRdd: TimeSeriesRDD = Clocks.uniform(sc, "5h", "0d", "20010101", "20010201")
    assert(clockTSRdd.schema == Schema())
    val clockStream = new UniformClock(
      sc,
      TimeFormat.parseNano("20010101"),
      TimeFormat.parseNano("20010201"),
      Duration("5h").toNanos,
      0L,
      true
    ).asStream()
    val rdd = clockTSRdd.rdd.map(r => r.getAs[Long](TimeSeriesRDD.timeColumnName))
    assert(rdd.collect().deep == clockStream.toArray.deep)
  }

  it should "generate clock ticks with offset in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD = Clocks.uniform(sc, "1d", "8h", "20010101", "20010201")
    val clockTSRdd2: TimeSeriesRDD = Clocks.uniform(sc, "1d", "0h", "20010101 08:00", "20010201")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }

  it should "generate clock ticks with offset & time zone in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD =
      Clocks.uniform(sc, "1d", "8h", "20010101", "20010201", "UTC")
    val clockTSRdd2: TimeSeriesRDD =
      Clocks.uniform(sc, "1d", "0h", "20010101 08:00 +0000", "20010201 00:00 +0000", "MST")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }

  it should "generate clock ticks with default in TimeSeriesRDD correctly" in {
    val clockTSRdd1: TimeSeriesRDD = Clocks.uniform(sc, "1d", "0h", "19700101", "20300101", "UTC")
    val clockTSRdd2: TimeSeriesRDD = Clocks.uniform(sc, "1d")
    assert(clockTSRdd1.collect().deep == clockTSRdd2.collect().deep)
  }

  it should "generate timestamp correctly" in {
    import org.apache.spark.sql.functions.{ year, month, dayofmonth, hour, minute, second, col }

    withTimeType("timestamp") {
      val clock1 = Clocks.uniform(sc, "1day", beginDateTime = "19900101")

      val df = clock1.toDF
        .withColumn("year", year(col("time")))
        .withColumn("month", month(col("time")))
        .withColumn("day", dayofmonth(col("time")))
        .withColumn("hour", hour(col("time")))
        .withColumn("minute", minute(col("time")))
        .withColumn("second", second(col("time")))

      val firstRow = df.take(1)(0)

      assert(firstRow.getInt(1) == 1990)
      assert(firstRow.getInt(2) == 1)
      assert(firstRow.getInt(3) == 1)
      assert(firstRow.getInt(4) == 0)
      assert(firstRow.getInt(5) == 0)
      assert(firstRow.getInt(6) == 0)
    }
  }

  "RandomClock" should "generate clock ticks randomly" in {
    def verifyDistribution(clock: Array[Long], frequency: Long): Unit = {
      val ratio = 0.33
      val half = frequency / 2
      var i = 1
      var count = 0
      while (i < clock.length) {
        val interval = clock(i) - clock(i - 1)
        assert(interval > 0 && interval <= frequency)
        if (interval < half) {
          count = count + 1
        }
        i = i + 1
      }
      assert(count > clock.length * ratio && count < (1 - ratio) * clock.length)
    }

    (1 to 10).foreach { _ =>
      val clock = new RandomClock(
        sc,
        beginDateTime = "20100101",
        endDateTime = "20150101",
        frequency = "1h",
        offset = "0s",
        timeZone = "UTC",
        seed = System.currentTimeMillis(),
        endInclusive = true
      )
      verifyDistribution(
        clock.asTimeSeriesRDD().collect().map(_.getAs[Long]("time")),
        clock.frequency
      )
      verifyDistribution(
        clock.asStream().toArray,
        clock.frequency
      )
    }

  }
}
