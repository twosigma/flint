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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.rdd.OverlappedOrderedRDD
import com.twosigma.flint.rdd._
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.OverlappableSummarizer
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.reflect.ClassTag
import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer

protected[flint] object Summarize {

  /**
   * Apply an summarizer to each partition of an [[org.apache.spark.rdd.RDD]] and return the partially summarized
   * results.
   *
   * @param rdd        An [[org.apache.spark.rdd.RDD]] whose partitions are expected to be summarized
   * @param summarizer The summarizer that is expected to apply.
   * @param skFn       A function that extracts keys from rows such that the summarizer will be applied per key level.
   * @return an array of tuples where left(s) are partition indices and right(s) are the intermediate
   *         summarized results each of which is obtained by applying the summarizer only to the
   *         corresponding partition.
   */
  def summarizePartition[K, SK, V, U, V2](
    rdd: RDD[(K, V)],
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): Array[(Int, Map[SK, U])] = {
    rdd.mapPartitionsWithIndex {
      case (idx, iter) =>
        // Initialize the initial state.
        val uPerSK = mutable.HashMap.empty[SK, U]
        while (iter.hasNext) {
          val (_, v) = iter.next()
          val sk = skFn(v)
          val previousU = uPerSK.getOrElse(sk, summarizer.zero())
          uPerSK += sk -> summarizer.add(previousU, v)
        }
        // Make it immutable to return.
        Array((idx, uPerSK)).iterator
    }.collect.map{
      case (idx, m) => (idx, collection.immutable.Map(m.toSeq: _*))
    }
  }

  /**
   * Apply an [[summarizer]] to an [[OrderedRDD]].
   *
   * For a summarizer s, let us define
   *   - 0 = s.zero()
   *   - u + a = s.add(u, a)
   *   - u1 ++ u1 = s.merge(u1, u2)
   *
   * Note that for any sequence of values  (a[1], a[2], ..., a[n]), (a'[1], a'[2], ..., a'[m]),
   * by the definition of [[Summarizer]] if
   *   - u1 = 0 + a[1] + ... + a[n]
   *   - u2 = 0 + a'[1] + ... + a'[m]
   *
   * then
   *   u1 + u2 = 0 + a[1] + ... + a[n] + a'[1] + ... + a'[m]
   *
   * This implies that we could have a two-pass algorithm where the first pass is to apply the
   * summarizer per partition; and the second pass is to calculate the summary for every rows
   * [[https://www.cs.cmu.edu/~guyb/papers/Ble93.pdf]].
   *
   * @param rdd        An [[OrderedRDD]] of tuples (K, V)
   * @param summarizer A [[Summarizer]] expected to apply
   * @param skFn       A function that extracts the secondary keys from V such that the summarizer will be
   *                   applied per secondary key level in the order of K.
   * @param depth      The depth of tree for merging partial summarized results across different partitions
   *                   in a a multi-level tree aggregation fashion.
   * @return the summarized results.
   */
  def apply[K: Ordering, SK, V, U, V2](
    rdd: RDD[(K, V)],
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK,
    depth: Int
  ): Map[SK, V2] = {
    val partiallySummarized: RDD[Map[SK, U]] = rdd.mapPartitions {
      iter =>
        // Initialize the initial state.
        val uPerSK = mutable.HashMap.empty[SK, U]
        while (iter.hasNext) {
          val (_, v) = iter.next()
          val sk = skFn(v)
          val previousU = uPerSK.getOrElse(sk, summarizer.zero())
          uPerSK += sk -> summarizer.add(previousU, v)
        }
        Iterator(uPerSK)
    }.map { m => Map[SK, U](m.toSeq: _*) } // Make it immutable to return.

    val mergeOp = (u1: Map[SK, U], u2: Map[SK, U]) => (u1 ++ u2).map {
      case (sk, _) =>
        (sk, summarizer.merge(u1.getOrElse(sk, summarizer.zero()), u2.getOrElse(sk, summarizer.zero())))
    }

    TreeReduce(partiallySummarized)(mergeOp, depth).map {
      case (sk, v) => (sk, summarizer.render(v))
    }
  }

  /**
   * Apply an [[OverlappableSummarizer]] to an [[OrderedRDD]].
   *
   * @param rdd        An [[OrderedRDD]] of tuples (K, V)
   * @param summarizer An [[OverlappableSummarizer]] expected to apply
   * @param windowFn   A function expected to expand the range of a partition.
   *                   Consider a partition of `rdd` with a range [b, e). The function expands
   *                   the range to [b1, e1) where b1 is the left windowFn(b) and e1 is the right
   *                   of windowFn(e). The `summarizer` will be applied to an expanded partition
   *                   that includes all rows failing into [b1, e1).
   * @param skFn       A function that extracts the secondary keys from V such that the summarizer will be
   *                   applied per secondary key level in the order of K.
   * @param depth      The depth of tree for merging partial summarized results across different partitions
   *                   in a a multi-level tree aggregation fashion.
   * @return the summarized results.
   */
  def apply[K: ClassTag: Ordering, SK, V: ClassTag, U, V2](
    rdd: OrderedRDD[K, V],
    summarizer: OverlappableSummarizer[V, U, V2],
    windowFn: K => (K, K),
    skFn: V => SK,
    depth: Int
  ): Map[SK, V2] = {
    // Basically, an RDD of (K, (V, Boolean)) where the boolean flag indicates whether a row is overlapped.
    val overlappedRdd = OverlappedOrderedRDD(rdd, windowFn).zipOverlapped()
    val partiallySummarized: RDD[Map[SK, U]] = overlappedRdd.map(_._2).mapPartitions {
      iter =>
        // Initialize the initial state.
        val uPerSK = mutable.HashMap.empty[SK, U]
        while (iter.hasNext) {
          val v = iter.next()
          val sk = skFn(v._1)
          val previousU = uPerSK.getOrElse(sk, summarizer.zero())
          uPerSK += sk -> summarizer.addOverlapped(previousU, v)
        }
        Iterator(uPerSK)
    }.map { m => Map[SK, U](m.toSeq: _*) } // Make it immutable to return.

    val mergeOp = (u1: Map[SK, U], u2: Map[SK, U]) => (u1 ++ u2).map {
      case (sk, _) =>
        (sk, summarizer.merge(u1.getOrElse(sk, summarizer.zero()), u2.getOrElse(sk, summarizer.zero())))
    }

    TreeReduce(partiallySummarized)(mergeOp, depth).map {
      case (sk, v) => (sk, summarizer.render(v))
    }
  }

}
