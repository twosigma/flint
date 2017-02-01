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

package com.twosigma.flint.rdd.function.join

import com.twosigma.flint.rdd._
import org.apache.spark.NarrowDependency

import scala.collection.mutable
import scala.reflect.ClassTag

protected[flint] object SymmetricJoin {

  def apply[K: ClassTag, SK, V, V2](
    leftRdd: OrderedRDD[K, V],
    rightRdd: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK
  )(implicit ord: Ordering[K]): OrderedRDD[K, (Option[(K, V)], Option[(K, V2)])] = {
    // map from new partition to a RangeMergeJoin
    val partToMergeJoin = RangeMergeJoin.mergeSplits(
      leftRdd.rangeSplits,
      rightRdd.rangeSplits,
      toleranceFn
    ).zipWithIndex.map {
      case (mergeJoin, idx) => (OrderedRDDPartition(idx), mergeJoin)
    }.toMap

    // map from partition index to a RangeMergeJoin
    val partitionIndexToMergeJoin = partToMergeJoin.map { case (p, m) => (p.index, m) }

    // left table's dependencies
    val leftDep = new NarrowDependency(leftRdd) {
      override def getParents(partitionId: Int) =
        partitionIndexToMergeJoin(partitionId).left.map(_.partition.index)
    }

    // right table's dependencies
    val rightDep = new NarrowDependency(rightRdd) {
      override def getParents(partitionId: Int) =
        partitionIndexToMergeJoin(partitionId).right.map(_.partition.index)
    }

    // array of RangeSplits
    val mergedSplits = partToMergeJoin.map {
      case (p, mergeJoin) => RangeSplit(p, mergeJoin.range)
    }.toArray

    val indexToMergeJoin = leftRdd.sc.broadcast(partitionIndexToMergeJoin)

    // return new OrderedRDD
    new OrderedRDD[K, (Option[(K, V)], Option[(K, V2)])](leftRdd.sc, mergedSplits, Seq(leftDep, rightDep))(
      (part, context) => {
        // the RangeMergeJoin for the given partition
        val mergedJoin = indexToMergeJoin.value(part.index)

        // Get iterators for both RDDs. We must have duplicate iterators because we
        // want to "look ahead" when matching a left or right RDD against the merged
        // RDD. The "look ahead" requires the full universe, while the merged requires
        // that the rows be limited to only those whose keys belong to the range.

        val leftParts = mergedJoin.left.map(_.partition)
        val leftIter = PeekableIterator(PartitionsIterator(leftRdd, leftParts, context).filter {
          case (k, _) => mergedJoin.range.contains(k)
        })
        val leftIterFull = PeekableIterator(PartitionsIterator(leftRdd, leftParts, context))

        val rightParts = mergedJoin.right.map(_.partition)
        val rightIter = PeekableIterator(PartitionsIterator(rightRdd, rightParts, context).filter {
          case (k, _) => mergedJoin.range.contains(k)
        })
        val rightIterFull = PeekableIterator(PartitionsIterator(rightRdd, rightParts, context))

        // ordered merge of the limited iterators
        val mergedIter = MergeIterator(leftIter, rightIter)

        // iterate over the merged iterator and find the matching elements
        val leftLastSeen = mutable.Map.empty[SK, (K, V)]
        val rightLastSeen = mutable.Map.empty[SK, (K, V2)]
        mergedIter.map {
          // Catch-up the iterator from the "other" table to match the current key. In the
          // process, we'll have the last-seen row for each SK from the other table.
          case (k, Left(v)) =>
            val sk = leftSk(v)
            LeftJoin.catchUp(k, rightSk, rightIterFull, rightLastSeen)
            (k, (Option(k, v), rightLastSeen.get(sk).filter { t => ord.gteq(t._1, toleranceFn(k)) }))
          case (k, Right(v)) =>
            val sk = rightSk(v)
            LeftJoin.catchUp(k, leftSk, leftIterFull, leftLastSeen)
            (k, (leftLastSeen.get(sk).filter { t => ord.gteq(t._1, toleranceFn(k)) }, Option(k, v)))
        }
      }
    )
  }
}
