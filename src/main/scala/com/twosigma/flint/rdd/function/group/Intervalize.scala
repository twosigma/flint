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

package com.twosigma.flint.rdd.function.group

import com.twosigma.flint.rdd._

import scala.collection.Searching._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ NarrowDependency, OneToOneDependency, Partition, TaskContext }

import scala.reflect.ClassTag
import grizzled.slf4j.Logger

object Intervalize {

  private val logger = Logger()

  /**
   * Round a given key to one of boundaries defined by the clock.
   *
   * @param k            The key expected to round.
   * @param clock        A sequence of sorted keys where two sequential keys are treated as an interval.
   * @param roundToBegin A flag to determine how to treat keys that fall exactly on the clock intervals.
   *                     If it is true, keys that are at the exact beginning of an interval will be included and
   *                     keys that fall on the exact end will be excluded, as represented by the interval [begin, end).
   *                     Otherwise, it is (begin, end].
   */
  private[function] def round[K: Ordering](
    k: K,
    clock: Array[K],
    roundToBegin: Boolean
  ): Option[K] = {
    clock.search(k) match {
      case Found(idx) => Some(clock(idx))
      case InsertionPoint(idx) => if (roundToBegin) {
        if (idx > 0) Some(clock(idx - 1)) else None
      } else {
        if (idx < clock.size) Some(clock(idx)) else None
      }
    }
  }

  /**
   * Intervalize an [[OrderedRDD]] by mapping its keys to the begin or the end of an interval where
   * they fall into. Intervals are defined by the provided `clock`.
   *
   * @param rdd            The [[OrderedRDD]] expected to intervalize.
   * @param clock          A sequence of sorted keys where two sequential keys are treated as an interval.
   * @param beginInclusive A flag to determine how to treat keys that fall exactly on the clock intervals.
   *                       If it is true, keys that are at the exact beginning of an interval will be included and keys
   *                       that fall on the exact end will be excluded, as represented by the interval [begin, end).
   *                       Otherwise, it is (begin, end].
   * @return an [[OrderedRDD]] whose keys are intervalized and the original keys are kept in the values as (K, V)s.
   */
  def intervalize[K: ClassTag, V](
    rdd: OrderedRDD[K, V],
    clock: Array[K],
    beginInclusive: Boolean
  )(implicit ord: Ordering[K]): OrderedRDD[K, (K, V)] = {
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

    val rddBegin = rdd.rangeSplits.head.range.begin
    val rddEnd = rdd.rangeSplits.last.range.end

    // Optimization: Reduce the size of the boardcast clock if possible
    val from = clock.search(rddBegin) match {
      case Found(idx) => Math.max(0, idx - 1) // -1 because beginInclusive can be false
      case InsertionPoint(idx) => Math.max(0, idx - 1) // between idx - 1 and idx
    }

    val until = rddEnd.fold(clock.size) { end =>
      clock.search(end) match {
        // +2 because: (1) until is exclusive (2) need to include one more interval towards the end
        case Found(idx) => Math.min(clock.size, idx + 2)
        case InsertionPoint(idx) => Math.min(clock.size, idx + 2)
      }
    }

    val trimedClock = clock.slice(from, until)
    // 20 years of 5 min interval is about 16M
    val maxClocksize = 20 * 365 * 24 * 12 * 8
    if (trimedClock.size > maxClocksize) {
      logger.warn(s"Boardcast clock is bigger than ${maxClocksize / 1024 / 1024} M. " +
        s"Please provide a smaller time range.")
    }

    val broadcastClock = rdd.sparkContext.broadcast(trimedClock)
    val intervalized = rdd.map {
      case (k, v) => (round(k, broadcastClock.value, beginInclusive), (k, v))
    }.filter(_._1.isDefined).map {
      case (k, v) => (k.get, v)
    }

    // Normalize the above rdd such that rows with the same keys won't spread across multiple partitions.
    Conversion.fromSortedRDD(intervalized)
  }

  /**
   * Intervalize an [[OrderedRDD]] by mapping its keys to the begin or the end of an interval where
   * they fall into. The intervals are defined by `clock`.
   *
   * @param rdd            The [[OrderedRDD]] expected to intervalize.
   * @param clock          A [[OrderedRDD]] of sorted keys where two sequential keys are treated as an interval.
   * @param beginInclusive A flag to determine how to treat keys that fall exactly on the clock intervals.
   *                       If it is true, keys that are at the exact beginning of an interval will be included and keys
   *                       that fall on the exact end will be excluded, as represented by the interval [begin, end).
   *                       Otherwise, it is (begin, end].
   * @return an [[OrderedRDD]] whose keys are intervalized and the original keys are kept in the
   *         values as (K, V)s.
   */
  def intervalize[K: Ordering: ClassTag, SK, V, V1](
    rdd: OrderedRDD[K, V],
    clock: OrderedRDD[K, V1],
    beginInclusive: Boolean
  ): OrderedRDD[K, (K, V)] = {
    // TODO: This algorithm doesn't deal with empty partitions correctly.
    sys.error("This algorithm is broken, don't use this. " +
      "If you see this message, please contact spark-ts-support@twosigma.com")

    val rddSplits = rdd.rangeSplits
    val clockSplits = clock.rangeSplits
    require(RangeSplit.isSortedByRange(clockSplits))

    val rddPartToClockParts = rddSplits.map { split =>
      val clockParts = RangeSplit.getIntersectingSplits(split.range, clockSplits).map(_.partition)
      val extraPart = if (beginInclusive) {
        val pos = clockParts.map(_.index).min
        if (pos > 0) Some(clockSplits(pos - 1).partition) else None
      } else {
        val pos = clockParts.map(_.index).max
        if (pos < clockSplits.length - 1) Some(clockSplits(pos + 1).partition) else None
      }
      (split.partition.index, extraPart.fold(clockParts)(clockParts.+:(_)).sortBy(_.index))
    }.toMap

    val rddDep = new OneToOneDependency(rdd)
    val clockDep = new NarrowDependency(clock) {
      override def getParents(partitionId: Int) = rddPartToClockParts(partitionId).map(_.index)
    }

    val sc = rdd.sparkContext

    val intervalizedRDD = new RDD[(K, (K, V))](sc, Seq(rddDep, clockDep)) {
      override def compute(part: Partition, context: TaskContext): Iterator[(K, (K, V))] = {
        val parts = rddPartToClockParts(part.index)
        // TODO: we should use rdd.mapPartitions to make it much more efficient.
        val littleClock = PartitionsIterator(clock, parts, context).toArray.map(_._1)
        rdd.iterator(part, context).map {
          case (k, v) => (round(k, littleClock, beginInclusive), (k, v))
        }.filter(_._1.isDefined).map {
          case (k, v) => (k.get, v)
        }
      }

      override protected def getPartitions: Array[Partition] = rdd.partitions
    }
    // Normalize the above rdd such that rows with the same keys won't spread across multiple partitions.
    Conversion.fromSortedRDD(intervalizedRDD)
  }
}
