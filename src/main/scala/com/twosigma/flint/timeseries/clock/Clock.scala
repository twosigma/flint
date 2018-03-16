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

package com.twosigma.flint.timeseries.clock

import java.util.concurrent.TimeUnit

import com.twosigma.flint.rdd.{ CloseOpen, OrderedRDD }
import com.twosigma.flint.timeseries.TimeSeriesRDD
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.time.TimeFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DFConverter, SQLContext }
import org.apache.spark.sql.catalyst.InternalRow
import org.joda.time.DateTimeZone

import scala.concurrent.duration.Duration
import scala.util.Random

object Clock {

  /**
   * Generate a sequence of ticks.
   *
   * @param begin     The first tick.
   * @param end       The end of ticks. The generated ticks are less or equal to `end`.
   * @param frequency The frequency of ticks.
   * @param nextTick  A function to return the next tick for a given tick.
   * @return a sequence of ticks.
   */
  protected[flint] def apply(
    begin: Long,
    end: Long,
    frequency: Long,
    nextTick: (Long) => Long,
    endInclusive: Boolean
  ): Stream[Long] = {
    def loop(t: Long): Stream[Long] = t #:: loop(nextTick(t))
    if (endInclusive) {
      loop(begin).takeWhile(_ <= end)
    } else {
      loop(begin).takeWhile(_ < end)
    }
  }
}

abstract class Clock(
  @transient val sc: SparkContext,
  val begin: Long,
  val end: Long,
  val frequency: Long,
  val offset: Long,
  endInclusive: Boolean
) extends Serializable {

  /**
   * A function expected to return the next tick t2 for a given tick t1 such that
   *  - t2 > t1
   *  - t2 - t1 <= frequency
   *
   * @param t The given tick.
   * @return the next tick.
   */
  def nextTick(t: Long): Long

  /**
   * Return the first tick which is
   * {{{
   *     begin + offset % frequency.
   * }}}
   * This implies that if offset = 0 then the firstTick = begin.
   */
  val firstTick: Long = {
    require(end >= begin, s"end $end should be larger or equal to begin $begin.")
    require(frequency > 0, s"frequency $frequency should be larger that 0.")
    val first = begin + offset % frequency
    require(first < end, s"The first tick $first is larger than the end $end.")
    first
  }

  /**
   * Generate a sequence of ticks as a [[Stream]].
   *
   * @return a sequence of ticks.
   */
  protected[flint] def asStream(): Stream[Long] = Clock(
    begin = firstTick,
    end = end,
    frequency = frequency,
    nextTick,
    endInclusive
  )

  /**
   * Generate a sequence of ticks as an [[OrderedRDD]] whose key and value for each row are the same.
   *
   * @param numSlices Number of desired partitions for the [[OrderedRDD]]. The actual number
   *                  of partitions could be smaller as it will try to guarantee each partition
   *                  has at least one tick.
   * @return a sequence of ticks as an [[OrderedRDD]].
   */
  protected[flint] def asOrderedRDD(numSlices: Int = sc.defaultParallelism): OrderedRDD[Long, Long] = {
    // Make sure the number of ticks per slice is always larger than one.
    val ticksPerSlice = (end - firstTick) / frequency / numSlices + 1L
    val begins = for (b <- firstTick to end by (ticksPerSlice * frequency)) yield b
    val rdd = sc.parallelize(begins, begins.length).mapPartitionsWithIndex {
      case (index, _) =>
        val b = begins(index)
        val e = if (index < begins.length - 1) begins(index + 1) - 1L else end
        Clock(b, e, frequency, nextTick, endInclusive).toIterator
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

  /**
   * Generate a sequence of ticks as a [[TimeSeriesRDD]] that has just only one column named "time".
   *
   * @param numSlices Number of desired partitions for the [[OrderedRDD]]. The actual number
   *                  of partitions could be smaller as it will try to guarantee each partition
   *                  has at least one tick.
   * @return a sequence of ticks as a [[TimeSeriesRDD]].
   */
  protected[flint] def asTimeSeriesRDD(numSlices: Int = sc.defaultParallelism): TimeSeriesRDD = {
    val rdd = asOrderedRDD(numSlices)
    val rowRdd = rdd.map(kv => InternalRow(kv._2))
    val ranges = rdd.rangeSplits.map(_.range)
    val schema = Schema()
    val df = TimeSeriesRDD.canonizeTime(DFConverter.toDataFrame(rowRdd, schema), TimeUnit.NANOSECONDS)
    TimeSeriesRDD.fromDFWithRanges(df, ranges)
  }
}

/**
 * Clock with uniform distributed intervals/ticks.
 */
class UniformClock(
  @transient override val sc: SparkContext,
  begin: Long,
  end: Long,
  frequency: Long,
  offset: Long,
  endInclusive: Boolean
) extends Clock(sc, begin, end, frequency, offset, endInclusive) {
  override def nextTick(t: Long): Long = t + frequency

  def this(
    sc: SparkContext,
    beginDateTime: String,
    endDateTime: String,
    frequency: String,
    offset: String,
    timeZone: String,
    endInclusive: Boolean
  ) = this(
    sc,
    TimeFormat.parseNano(beginDateTime, DateTimeZone.forID(timeZone)),
    TimeFormat.parseNano(endDateTime, DateTimeZone.forID(timeZone)),
    Duration(frequency).toNanos,
    Duration(offset).toNanos,
    endInclusive
  )
}

/**
 * Clock with unevenly distributed intervals/ticks. Intervals between two sequential ticks
 * are uniformly distributed in the range of [1, `frequency`] and rounded up to the closest
 * microseconds.
 */
class RandomClock(
  @transient override val sc: SparkContext,
  begin: Long,
  end: Long,
  frequency: Long,
  offset: Long,
  endInclusive: Boolean,
  val seed: Long = System.currentTimeMillis()
) extends Clock(sc, begin, end, frequency, offset, endInclusive) {
  private val rand = new Random(seed)

  override def nextTick(t: Long): Long = {
    val rawTick = (t + Math.abs(rand.nextLong()) % frequency + 1L)
    val tick = rawTick - rawTick % 1000 + 1000
    tick
  }

  def this(
    sc: SparkContext,
    beginDateTime: String,
    endDateTime: String,
    frequency: String,
    offset: String,
    timeZone: String,
    seed: Long,
    endInclusive: Boolean
  ) = this(
    sc,
    TimeFormat.parseNano(beginDateTime, DateTimeZone.forID(timeZone)),
    TimeFormat.parseNano(endDateTime, DateTimeZone.forID(timeZone)),
    Duration(frequency).toNanos,
    Duration(offset).toNanos,
    endInclusive,
    seed
  )
}
