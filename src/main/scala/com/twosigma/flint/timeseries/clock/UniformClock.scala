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

package com.twosigma.flint.timeseries.clock

import com.twosigma.flint.rdd.{ CloseOpen, OrderedRDD }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.time.TimeFormat
import com.twosigma.flint.timeseries.{ TimeSeriesRDD, TimeSeriesRDDImpl }
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.joda.time.DateTimeZone

import scala.concurrent.duration._

/**
 * Clock with even intervals
 */
object UniformClock {
  def apply(
    sc: SparkContext,
    frequency: String,
    offset: String = "0s",
    beginDateTime: String = "1990-01-01",
    endDateTime: String = "2030-01-01",
    timeZone: String = "UTC"
  ): TimeSeriesRDD = {
    val tz = DateTimeZone.forID(timeZone)
    val beginNanos = TimeFormat.parseNano(beginDateTime, tz)
    val endNanos = TimeFormat.parseNano(endDateTime, tz)
    val frequencyNanos = Duration(frequency).toNanos
    val offsetNanos = Duration(offset).toNanos
    val schema = Schema()
    val rdd = apply(sc, beginNanos, endNanos, frequencyNanos, offsetNanos, sc.defaultParallelism).mapValues {
      case (t, _) => InternalRow(t)
    }
    TimeSeriesRDD.fromInternalOrderedRDD(rdd, schema)
  }

  protected[flint] def apply(
    begin: Long,
    end: Long,
    frequency: Long,
    offset: Long
  ): Stream[Long] = {
    require(end >= begin, s"end $end should be larger or equal to begin $begin.")
    require(frequency > 0, s"frequency $frequency should be larger that 0.")
    val next = begin + offset % frequency
    require(next <= end, s"The first tick of clock is larger than the end $end.")

    def loop(t: Long): Stream[Long] = t #:: loop(t + frequency)
    loop(next).takeWhile(_ <= end)
  }

  protected[flint] def apply(
    sc: SparkContext,
    begin: Long,
    end: Long,
    frequency: Long,
    offset: Long,
    numSlices: Int
  ): OrderedRDD[Long, Long] = {
    require(end >= begin, s"end $end should be larger or equal to begin $begin.")
    require(frequency > 0, s"frequency $frequency should be larger that 0.")
    val adjustedBegin = begin + offset % frequency
    require(adjustedBegin < end, s"The begin + offset % frequency $adjustedBegin is larger than the end $end.")

    // Make sure the number of ticks per slice is always larger than one.
    val ticksPerSlice: Long = (end - adjustedBegin) / frequency / numSlices + 1L
    val begins = for (b <- adjustedBegin to end by (ticksPerSlice * frequency)) yield b
    val rdd = sc.parallelize(begins, begins.length).mapPartitionsWithIndex {
      case (index, _) =>
        val b = begins(index)
        val e = if (index < begins.length - 1) begins(index + 1) else end
        def loop(t: Long): Stream[Long] = t #:: loop(t + frequency)
        loop(b).takeWhile { t => t <= end && t < e }.toIterator
    }
    val splits = begins.zipWithIndex.map {
      case (b, index) => if (index < begins.length - 1) {
        CloseOpen(b, Some(begins(index + 1)))
      } else {
        CloseOpen(b, None)
      }
    }
    OrderedRDD.fromRDD(rdd.map { t => (t, t) }, splits)
  }
}
