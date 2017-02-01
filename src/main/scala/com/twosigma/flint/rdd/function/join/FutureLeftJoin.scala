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

protected[flint] object FutureLeftJoin {

  def apply[K: ClassTag, SK, V, V2](
    leftRdd: OrderedRDD[K, V],
    rightRdd: OrderedRDD[K, V2],
    toleranceFn: K => K,
    leftSk: V => SK,
    rightSk: V2 => SK,
    strictForward: Boolean = false
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
        val foreSeen = mutable.Map.empty[SK, mutable.Queue[(K, V2)]]
        leftRdd.iterator(part, context).map {
          case (k, v) =>
            val sk = leftSk(v)
            forward(k, sk, toleranceFn(k), rightSk, rightIter, foreSeen)
            (k, (v, foreSeen.get(sk).flatMap(_.find {
              case (k1, _) => if (strictForward) ord.lt(k, k1) else ord.lteq(k, k1)
            })))
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
   * corresponding queues for possibly later use and those earlier ones at the top of the queue
   * (relatively to the reference) will be cleaned out when performing enqueue operations.
   */
  private[rdd] def forward[K, SK, V](
    referenceK: K,
    referenceSk: SK,
    forwardK: K,
    forwardSkFn: V => SK,
    forwardIter: PeekableIterator[(K, V)],
    foreSeen: mutable.Map[SK, mutable.Queue[(K, V)]],
    strictForward: Boolean = true
  )(implicit ord: Ordering[K]) = {
    while (forwardIter.peek.fold(false) {
      // The criterion that allows to move forward.
      case (fK, _) =>
        // Haven't found one in the corresponding queue for join.
        foreSeen.get(referenceSk).fold(true) {
          case q =>
            if (strictForward) ord.lteq(q.last._1, referenceK) else ord.lt(q.last._1, referenceK)
        } && ord.lt(fK, forwardK) // Still within the range.
    }) {
      val (fK, fV) = forwardIter.next()
      val sk = forwardSkFn(fV)
      val queueForSk = foreSeen.getOrElse(sk, mutable.Queue.empty[(K, V)])
      // Clean the old data first.
      queueForSk.dropWhile { case (k, _) => ord.lt(k, referenceK) }
      queueForSk.enqueue((fK, fV))
      foreSeen += sk -> queueForSk
    }
  }

}
