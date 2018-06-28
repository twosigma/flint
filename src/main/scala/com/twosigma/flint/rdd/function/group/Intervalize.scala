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

package com.twosigma.flint.rdd.function.group

import com.twosigma.flint.rdd._

import scala.collection.Searching._
import org.apache.spark.rdd.RDD
import org.apache.spark._

import scala.reflect.ClassTag
import grizzled.slf4j.Logger
import org.apache.spark.broadcast.Broadcast

object Intervalize {

  private val logger = Logger()

  private def broadcastClock[K: ClassTag](sparkContext: SparkContext, clock: Array[K]): Broadcast[Array[K]] = {
    val INTERVALS_PER_YEAR = 365 * 24 * 12
    val NUMBER_OF_YEAR = 20
    val BYTES_PER_INTERVAL = 8
    // 20 years of 5 min interval is about 16M
    val maxClocksize = NUMBER_OF_YEAR * INTERVALS_PER_YEAR * BYTES_PER_INTERVAL
    if (clock.length > maxClocksize) {
      logger.warn(s"Broadcast clock is bigger than ${maxClocksize / 1024 / 1024} M. " +
        s"Please provide a smaller time range.")
    }
    sparkContext.broadcast(clock)
  }

  def roundFn[K: Ordering](inclusion: String, rounding: String): (K, Array[K]) => Option[K] =
    (inclusion, rounding) match {
      case ("begin", "begin") =>
        (k: K, clock: Array[K]) =>
          clock.search(k) match {
            case Found(idx) => if (idx < clock.length - 1) Some(clock(idx)) else None
            case InsertionPoint(idx) => if (idx > 0 && idx < clock.length) Some(clock(idx - 1)) else None
          }
      case ("begin", "end") =>
        (k: K, clock: Array[K]) =>
          clock.search(k) match {
            case Found(idx) => if (idx < clock.length - 1) Some(clock(idx + 1)) else None
            case InsertionPoint(idx) => if (idx > 0 && idx < clock.length) Some(clock(idx)) else None
          }
      case ("end", "begin") =>
        (k: K, clock: Array[K]) =>
          clock.search(k) match {
            case Found(idx) => if (idx > 0) Some(clock(idx - 1)) else None
            case InsertionPoint(idx) => if (idx > 0 && idx < clock.length) Some(clock(idx - 1)) else None
          }
      case ("end", "end") =>
        (k: K, clock: Array[K]) =>
          clock.search(k) match {
            case Found(idx) => if (idx > 0) Some(clock(idx)) else None
            case InsertionPoint(idx) => if (idx > 0 && idx < clock.length) Some(clock(idx)) else None
          }
      case _ => sys.error(s"Unrecognized args. Inclusion: $inclusion rounding: $rounding")
    }

  /**
   * Intervalize an [[OrderedRDD]] by mapping its keys to the begin or the end of an interval where
   * they fall into. Intervals are defined by the provided `clock`.
   *
   * @param rdd            The [[OrderedRDD]] expected to intervalize.
   * @param clock          A sequence of sorted keys where two sequential keys are treated as an interval.
   * @param inclusion      "begin" or "end". If begin, intervals are [begin, end). Otherwise, (begin, end]
   * @param rounding       "begin" or "end". If begin, rows are rounded to the begin of each interval,
   *                        otherwise, end.
   * @return an [[OrderedRDD]] whose keys are intervalized and the original keys are kept in the values as (K, V)s.
   */
  def intervalize[K: ClassTag, V](
    rdd: OrderedRDD[K, V],
    clock: Array[K],
    inclusion: String,
    rounding: String
  )(implicit ord: Ordering[K]): OrderedRDD[K, (K, V)] = {
    require(Seq("begin", "end").contains(inclusion), "inclusion must be \"begin\" or \"end\"")
    require(Seq("begin", "end").contains(rounding), "rounding must be \"begin\" or \"end\"")

    // ensure ordering
    var i = 0
    while (i < clock.size - 1) {
      require(
        ord.lt(clock(i), clock(i + 1)),
        s"Invalid interval. clock[n] must < clock[n + 1] for all n. " +
          s"n: ${i} clock[n]: ${clock(i)} clock[n + 1]: ${clock(i + 1)}"
      )
      i += 1
    }

    if (rdd.rangeSplits.isEmpty) {
      // TODO: This should be FlintContext.emptyOrderedRDD similar to sc.emptyRDD
      Conversion.fromSortedRDD(rdd.sc.emptyRDD[(K, (K, V))])
    } else {
      val rddBegin = rdd.rangeSplits.head.range.begin
      val rddEnd = rdd.rangeSplits.last.range.end

      // Optimization: Reduce the size of the broadcast clock if possible
      val from = clock.search(rddBegin) match {
        case Found(idx) => Math.max(0, idx - 1) // -1 because beginInclusive can be false
        case InsertionPoint(idx) => Math.max(0, idx - 1) // between idx - 1 and idx
      }

      val until = rddEnd.fold(clock.length) { end =>
        clock.search(end) match {
          // +2 because: (1) until is exclusive (2) need to include one more interval towards the end
          case Found(idx) => Math.min(clock.length, idx + 2)
          case InsertionPoint(idx) => Math.min(clock.length, idx + 2)
        }
      }

      val trimmedClock = clock.slice(from, until)
      val broadcast = broadcastClock(rdd.sparkContext, trimmedClock)

      // Lift switching on (inclusion, rounding) out of the cell level to improve performance.
      val round = roundFn[K](inclusion, rounding)

      // TODO: This should be rewritten to be an O(n) algorithms instead of O(nlog(n))
      val intervalized = rdd.map {
        case (k, v) => (round(k, broadcast.value), (k, v))
      }.filter(_._1.isDefined).map {
        case (k, v) => (k.get, v)
      }

      // Normalize the above rdd such that rows with the same keys won't spread across multiple partitions.
      Conversion.fromSortedRDD(intervalized)
    }
  }
}
