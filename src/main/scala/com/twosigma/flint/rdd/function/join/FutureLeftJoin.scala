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

import java.util

protected[flint] object FutureLeftJoin {

  /**
   * rightK passes leftK.
   */
  @inline
  private def passes[K](leftK: K, rightK: K, strictForward: Boolean)(implicit ord: Ordering[K]) =
    if (strictForward) ord.lt(leftK, rightK) else ord.lteq(leftK, rightK)

  def apply[K: ClassTag, SK, V, V2](
    leftRdd: OrderedRDD[K, V],
    rightRdd: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK,
    strictForward: Boolean
  )(implicit ord: Ordering[K]): OrderedRDD[K, (V, Option[(K, V2)])] = {
    // A map from left partition index to left range split and right partitions.
    val indexToJoinSplits = TreeMap(RangeMergeJoin.futureLeftJoinSplits(
      toleranceFn, leftRdd.rangeSplits, rightRdd.rangeSplits
    ).map { case (split, parts) => (split.partition.index, (split, parts)) }: _*)

    val leftDep = new OneToOneDependency(leftRdd)
    val rightDep = new NarrowDependency(rightRdd) {
      override def getParents(partitionId: Int) = indexToJoinSplits(partitionId)._2.map(_.index)
    }
    // A map from left partition index to right partitions.
    val leftPartitions = indexToJoinSplits.map { case (idx, joinSplit) => (idx, joinSplit._2) }
    val joinedSplits = indexToJoinSplits.map { case (_, (split, _)) => split }.toArray

    new OrderedRDD[K, (V, Option[(K, V2)])](leftRdd.sc, joinedSplits, Seq(leftDep, rightDep))(
      (part, context) => {
        val parts = leftPartitions(part.index)
        val rightIter = PeekableIterator(PartitionsIterator(rightRdd, parts, context))
        val foreSeen = new java.util.HashMap[SK, util.Deque[(K, V2)]]()
        leftRdd.iterator(part, context).map {
          case (k, v) =>
            val sk = leftSk(v)
            val queueForSk = forward(k, sk, toleranceFn(k), rightSk, rightIter, foreSeen, strictForward)
            if (queueForSk.isEmpty) {
              (k, (v, None))
            } else {
              (k, (v, Some(queueForSk.peekFirst())))
            }
        }
      }
    )
  }

  /**
   * The basic algorithm works as follows. For each SK, it creates a queue for it. The following
   * process iterates through the given iterator until finding a row under the tolerance that could
   * be used for join.
   *
   * Before finding that row, all the previously iterated / scanned rows will be put into the
   * corresponding queues for possibly later use and those earlier ones at the top of the queue of leftSk
   * (relatively to the reference) will be cleaned out when performing enqueue operations.
   */
  private[rdd] def forward[K, SK, V](
    leftK: K,
    leftSk: SK,
    forwardK: K,
    rightSk: V => SK,
    rightIter: PeekableIterator[(K, V)],
    foreSeen: util.HashMap[SK, util.Deque[(K, V)]],
    strictForward: Boolean
  )(implicit ord: Ordering[K]) = {
    // Scan through the rightIter until next rightK matches leftK
    // This is safe because if next rightK < current leftK, it will never match in the future and skipping it also
    // doesn't change the matching of previous rows (because the results of matching for previous rows have been
    // finalized at this point).
    while (rightIter.hasNext && !passes(leftK, rightIter.peek.get._1, strictForward)) {
      rightIter.next
    }

    var found = false
    var rightKeyInRange = true

    var queueForLeftSk = foreSeen.get(leftSk)
    if (queueForLeftSk == null) {
      queueForLeftSk = new util.ArrayDeque[(K, V)]()
      foreSeen.put(leftSk, queueForLeftSk)
    }
    found = !queueForLeftSk.isEmpty && passes(leftK, queueForLeftSk.peekFirst()._1, strictForward)

    // Add elements from rightIter until we find a match
    while (!found && rightIter.hasNext && rightKeyInRange) {
      val Some((rightK, rightV)) = rightIter.peek
      rightKeyInRange = ord.lteq(rightK, forwardK)
      if (rightKeyInRange) {
        rightIter.next
        val sk = rightSk(rightV)
        var queueForRightSk = foreSeen.get(sk)
        if (queueForRightSk == null) {
          queueForRightSk = new util.ArrayDeque[(K, V)]()
          foreSeen.put(sk, queueForRightSk)
        }
        queueForRightSk.addLast((rightK, rightV))
        found = !queueForLeftSk.isEmpty && passes(leftK, queueForLeftSk.peekFirst()._1, strictForward)
      }
    }

    // This is outside the while loop because we still need to remove old data if rightIter is empty
    while (!queueForLeftSk.isEmpty && !passes(leftK, queueForLeftSk.peekFirst()._1, strictForward)) {
      queueForLeftSk.pollFirst()
    }
    queueForLeftSk
  }
}
