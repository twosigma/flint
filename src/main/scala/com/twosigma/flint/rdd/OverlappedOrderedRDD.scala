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

package com.twosigma.flint.rdd

import com.twosigma.flint.rdd.function.join.RangeMergeJoin
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._

import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag

protected[flint] object OverlappedOrderedRDD {

  /**
   * Convert a [[OrderedRDD]] to an [[OverlappedOrderedRDD]] which allows partitions overlapped with each other while
   * preserving ordering.
   *
   * @param rdd    An [[OrderedRDD]] expected to convert to an [[OverlappedOrderedRDD]]
   * @param window A function specifies the overlap, i.e. for a partition of `rdd` with range [b, e), it will expand
   *               to include all rows within range [b1, e1) where b1 is the left window boundary of b, and e1 is the
   *               right window boundary of e.
   * @return an [[OverlappedOrderedRDD]].
   */
  def apply[K: ClassTag, V: ClassTag](
    rdd: OrderedRDD[K, V], window: K => (K, K)
  )(implicit ord: Ordering[K]): OverlappedOrderedRDD[K, V] = {
    // TODO: should use an O(n) algorithm to find the dependencies instead.
    val windowDependencies = TreeMap(rdd.rangeSplits.map {
      split =>
        // Note that the split.range is an open-close range.
        val expandedRange = split.range.expand(window)
        (split.partition.index, rdd.rangeSplits.filter(_.range.intersects(expandedRange)))
    }: _*)

    val dependency = new NarrowDependency(rdd) {
      override def getParents(partitionId: Int) = windowDependencies(partitionId).map(_.partition.index)
    }

    val expandedRanges = rdd.rangeSplits.map(_.range.expand(window))

    new OverlappedOrderedRDD[K, V](rdd.sc, rdd.rangeSplits, Seq(dependency))(
      (part, context) => {
        val parents = windowDependencies(part.index).map(_.partition)
        // Only return rows within the expanded partition by window.
        val expandedRange = expandedRanges(part.index)
        OrderedIterator(PartitionsIterator(rdd, parents, context)).filterByRange(expandedRange)
      }
    )
  }

  /**
   * Create an [[OverlappedOrderedRDD]] from a left [[OrderedRDD]] and a right [[OrderedRDD]]
   *
   * Partitions of the resulting [[OverlappedOrderedRDD]] has the expanded key range of the left [[OrderedRDD]],
   * with data from the right [[OrderedRDD]]
   *
   * For instance, if
   *   left range is [0, 100) [100, 200) [200, 300)
   *
   *   right range is [100, 200), [200, 300), [300, 400)
   *
   *   window is K => (K, K + 50)
   *
   *   result range is [0, 150) [100, 250) [200, 350)
   */
  def apply[K: ClassTag, V: ClassTag](
    left: OrderedRDD[K, V], right: OrderedRDD[K, V], window: K => (K, K)
  )(implicit ord: Ordering[K]): OverlappedOrderedRDD[K, V] = {
    val leftIndexToRightParts = TreeMap(RangeMergeJoin.windowJoinSplits(
      window, left.rangeSplits, right.rangeSplits
    ).map {
      case (leftRangeSplit, rightPartitions) => (leftRangeSplit.partition.index, rightPartitions)
    }: _*)
    val deps = new NarrowDependency(right) {
      override def getParents(partitionId: Int) = leftIndexToRightParts(partitionId).map(_.index)
    }

    val expandedRanges = left.rangeSplits.map(_.range.expand(window))

    new OverlappedOrderedRDD[K, V](left.sc, left.rangeSplits, Seq(deps))(
      (part, context) => {
        val expandedRange = expandedRanges(part.index)
        OrderedIterator(
          PartitionsIterator(right, leftIndexToRightParts(part.index), context)
        ).filterByRange(expandedRange)
      }
    )
  }
}

/**
 * An [[OverlappedOrderedRDD]] is normally created from an [[OrderedRDD]] where a partition is extended to include rows
 * of previous or later partitions in order, i.e. it overlaps with adjacent partitions.
 *
 * @note should make it private to make sure the above method is the only entry point.
 *
 */
protected[flint] class OverlappedOrderedRDD[K: ClassTag, V: ClassTag](
  @transient val sc: SparkContext,
  private val splits: Seq[RangeSplit[K]],
  private val deps: Seq[Dependency[_]] = Nil
)(create: (Partition, TaskContext) => Iterator[(K, V)])(implicit ord: Ordering[K])
  extends RDD[(K, V)](sc, deps) {

  val self = this

  /**
   * A sequence of [[RangeSplit]]s sorted by their partitions' indices where a [[RangeSplit]]
   * represents an partition's non-overlapped [[Range]] information.
   */
  val rangeSplits: Array[RangeSplit[K]] = splits.toArray

  /**
   * Remove the overlapped rows and convert it back to an [[OrderedRDD]].
   */
  def nonOverlapped(): OrderedRDD[K, V] = new OrderedRDD[K, V](sc, splits, Seq(new OneToOneDependency(self)))(
    (p, tc) => OrderedIterator(iterator(p, tc)).filterByRange(splits(p.index).range)
  )

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = create(split, context)

  override protected def getPartitions: Array[Partition] = rangeSplits.map(_.partition)

  @DeveloperApi
  def mapPartitionsWithIndexOverlapped[V2: ClassTag](
    f: (Int, Iterator[(K, V)]) => Iterator[(K, V2)]
  ): OverlappedOrderedRDD[K, V2] = new OverlappedOrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(self)))(
    (partition, taskContext) => f(partition.index, self.iterator(partition, taskContext))
  )

  /**
   * Associate each row a flag to indicator whether it is overlapped. This transforms elements from (K, V) to
   * (K, (V, Boolean)) where the Boolean is true when the element is part of the other partition or in the extended
   * range instead of origin partition.
   */
  @DeveloperApi
  def zipOverlapped(): OverlappedOrderedRDD[K, (V, Boolean)] =
    new OverlappedOrderedRDD(sc, rangeSplits, Seq(new OneToOneDependency(self)))(
      (partition, taskContext) => {
        val range = rangeSplits(partition.index).range
        self.iterator(partition, taskContext).map {
          case (k, v) => (k, (v, !range.contains(k)))
        }
      }
    )
}
