/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.rdd.{ Conversion, PartitionsIterator, RangeSplit, Range }

import scala.collection.Searching._
import com.twosigma.flint.rdd.OrderedRDD
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.{ NarrowDependency, OneToOneDependency, Partition, TaskContext }

import scala.reflect.ClassTag

object Intervalize {

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
    clock: IndexedSeq[K],
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
  @Experimental
  def intervalize[K: Ordering: ClassTag, V](
    rdd: OrderedRDD[K, V],
    clock: IndexedSeq[K],
    beginInclusive: Boolean
  ): OrderedRDD[K, (K, V)] = {
    val broadcastClock = rdd.sparkContext.broadcast(clock)
    // TODO: we should use rdd.mapPartitions to make it much more efficient.
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
  @Experimental
  def intervalize[K: Ordering: ClassTag, SK, V, V1](
    rdd: OrderedRDD[K, V],
    clock: OrderedRDD[K, V1],
    beginInclusive: Boolean
  ): OrderedRDD[K, (K, V)] = {
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
