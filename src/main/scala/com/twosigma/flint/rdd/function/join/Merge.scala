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

import com.twosigma.flint.rdd.{ PeekableIterator, PartitionsIterator, MergeIterator, RangeSplit }
import org.apache.spark.NarrowDependency
import com.twosigma.flint.rdd._

import scala.reflect.ClassTag

protected[flint] object Merge {

  def apply[K: Ordering: ClassTag, V: ClassTag](
    left: OrderedRDD[K, V],
    right: OrderedRDD[K, V]
  ): OrderedRDD[K, V] = ++(left, right).mapValues {
    case (_, Left(v)) => v
    case (_, Right(v)) => v
  }

  def ++[K: ClassTag, V: ClassTag, V2: ClassTag](
    left: OrderedRDD[K, V],
    right: OrderedRDD[K, V2]
  )(
    implicit
    ord: Ordering[K]
  ): OrderedRDD[K, Either[V, V2]] = {
    // A map from new partition to a RangeMergeJoin.
    val partToMergeJoin = RangeMergeJoin.mergeSplits(left.rangeSplits, right.rangeSplits).zipWithIndex.map {
      case (mergeJoin, idx) => (OrderedRDDPartition(idx), mergeJoin)
    }.toMap

    // A map from partition index to a RangeMergeJoin.
    val partitionIndexToMergeJoin = partToMergeJoin.map { case (p, m) => (p.index, m) }
    val leftDep = new NarrowDependency(left) {
      override def getParents(partitionId: Int) =
        partitionIndexToMergeJoin(partitionId).left.map(_.partition.index)
    }
    val rightDep = new NarrowDependency(right) {
      override def getParents(partitionId: Int) =
        partitionIndexToMergeJoin(partitionId).right.map(_.partition.index)
    }
    val mergedSplits = partToMergeJoin.map {
      case (p, mergeJoin) => RangeSplit(p, mergeJoin.range)
    }.toArray

    new OrderedRDD[K, Either[V, V2]](left.sc, mergedSplits, Seq(leftDep, rightDep))(
      (part, context) => {
        val mergedJoin = partitionIndexToMergeJoin(part.index)
        // Select rows from both RDDs whose key belongs to this RangeMergeJoin's range
        val leftParts = mergedJoin.left.map(_.partition)
        val leftIter = PeekableIterator(PartitionsIterator(left, leftParts, context).filter {
          case (k, _) => mergedJoin.range.contains(k)
        })
        val rightParts = mergedJoin.right.map(_.partition)
        val rightIter = PeekableIterator(PartitionsIterator(right, rightParts, context).filter {
          case (k, _) => mergedJoin.range.contains(k)
        })
        // Perform an ordered merge of the selected rows.
        MergeIterator(leftIter, rightIter)
      }
    )
  }

}
