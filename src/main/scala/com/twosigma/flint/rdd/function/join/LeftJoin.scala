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

import com.twosigma.flint.rdd.{ PeekableIterator, PartitionsIterator }
import org.apache.spark.{ NarrowDependency, OneToOneDependency }

import com.twosigma.flint.rdd.OrderedRDD

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.reflect.ClassTag

protected[flint] object LeftJoin {

  def apply[K: ClassTag, SK, V, V2](
    leftRdd: OrderedRDD[K, V],
    rightRdd: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK
  )(implicit ord: Ordering[K]): OrderedRDD[K, (V, Option[(K, V2)])] = {
    // A map from left partition index to left range split and right partitions.
    val leftIndexToJoinSplits = TreeMap(RangeMergeJoin.leftJoinSplits(
      toleranceFn, leftRdd.rangeSplits, rightRdd.rangeSplits
    ).map { case (split, parts) => (split.partition.index, (split, parts)) }: _*)

    val leftDep = new OneToOneDependency(leftRdd)
    val rightDep = new NarrowDependency(rightRdd) {
      override def getParents(partitionId: Int) =
        leftIndexToJoinSplits(partitionId)._2.map(_.index)
    }

    // A map from left partition index to right partitions
    val rightPartitions = leftRdd.sc.broadcast(leftIndexToJoinSplits.map {
      case (idx, joinSplit) => (idx, joinSplit._2)
    })

    val joinedSplits = leftIndexToJoinSplits.map { case (_, (split, _)) => split }.toArray

    // We don't need the left dependency as we will just load it on demand here
    new OrderedRDD[K, (V, Option[(K, V2)])](leftRdd.sc, joinedSplits, Seq(leftDep, rightDep))(
      (part, context) => {
        val parts = rightPartitions.value(part.index)
        val rightIter = PeekableIterator(PartitionsIterator(rightRdd, parts, context))
        val lastSeen = mutable.Map.empty[SK, (K, V2)]
        leftRdd.iterator(part, context).map {
          case (k, v) =>
            // Catch-up the iterator for the right table to match the left key. In the
            // process, we'll have the last-seen row for each SK in the right table.
            val sk = leftSk(v)
            catchUp(k, rightSk, rightIter, lastSeen)
            (k, (v, lastSeen.get(sk).filter { t => ord.gteq(t._1, toleranceFn(k)) }))
        }
      }
    )
  }

  /**
   * Iterates until we are at the last row without going over current key, and maps each SK
   * to its last-seen row.
   */
  @annotation.tailrec
  private[rdd] def catchUp[K, SK, V](
    cur: K,
    skFn: V => SK,
    iter: PeekableIterator[(K, V)],
    lastSeen: mutable.Map[SK, (K, V)]
  )(implicit ord: Ordering[K]) {
    val peek = iter.peek
    if (peek.nonEmpty && ord.lteq(peek.get._1, cur)) {
      val (k, v) = iter.next
      val sk = skFn(v)
      lastSeen += (sk -> (k, v))
      catchUp(cur, skFn, iter, lastSeen)
    }
  }

}
