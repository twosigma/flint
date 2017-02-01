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
   * Apply an summarizer to each partition of an [[org.apache.spark.rdd.RDD]] and return the intermediate summarized
   * results for each partition.
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
   * Merge the intermediate summarized results from different partitions by applying a summarizer's merge operator.
   * The merging is applied in the order of partitions' indices.
   *
   * @param summarizer    The summarizer whose merge operator will be used to merge the intermediate summarized results
   *                      from different partitions.
   * @param intermediates An array of tuples where left(s) are partition indices and right(s) are the intermediate
   *                      summarized results each of which is obtained by applying the summarizer only to the
   *                      corresponding partition.
   * @param skFn          A function that extracts keys from rows such that the summarizer will be applied per
   *                      key level.
   * @return the merged the intermediate summarized results.
   */
  def merge[K: Ordering, SK, V, U, V2](
    summarizer: Summarizer[V, U, V2],
    intermediates: Array[(Int, Map[SK, U])],
    skFn: V => SK
  ): Map[SK, V2] = {
    val res = intermediates.map(_._2).foldLeft(mutable.Map.empty[SK, U]) {
      case (sum, perPartition) =>
        (sum ++ perPartition).map {
          case (sk, _) => (sk, summarizer.merge(
            sum.getOrElse(sk, summarizer.zero()), perPartition.getOrElse(sk, summarizer.zero())
          ))
        }
    }.map { case (sk, v) => (sk, summarizer.render(v)) }
    collection.immutable.Map(res.toSeq: _*)
  }

  /**
   * Add intermediate values of the reduction (as per reduce) of summarizer, i.e. it applies the
   * summarizer to all its previous rows and the the current row in order.
   *
   * For a summarizer s, let us define
   * - 0 = s.zero()
   * - u + a = s.add(u, a)
   * - u1 ++ u1 = s.merge(u1, u2)
   *
   * Note that for any sequence of values  (a[1], a[2], ..., a[n]), (a'[1], a'[2], ..., a'[m]),
   * by the definition of [[Summarizer]] if
   * - u1 = 0 + a[1] + ... + a[n]
   * - u2 = 0 + a'[1] + ... + a'[m]
   * then
   * u1 + u2 = 0 + a[1] + ... + a[n] + a'[1] + ... + a'[m]
   *
   * This implies that we could have a two-pass algorithm where the first pass is to apply the
   * summarizer per partition; and the second pass is to calculate the summary for every rows
   * [[https://www.cs.cmu.edu/~guyb/papers/Ble93.pdf]].
   *
   * @param rdd        An [[OrderedRDD]] of tuples (K, V).
   * @param summarizer A [[Summarizer]] that is expected to apply.
   * @param skFn       A function specifies that if the summarization is per secondary key then the
   *                   secondary key is defined by this function.
   * @return the summarized results.
   */
  def apply[K: Ordering, SK, V, U, V2](
    rdd: RDD[(K, V)],
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): Map[SK, V2] = {
    val intermediates = summarizePartition(rdd, summarizer, skFn)
    merge(summarizer, intermediates, skFn)
  }

  /**
   * Apply an [[OverlappableSummarizer]] to each partition of an [[OrderedRDD]].
   *
   * @param rdd        An [[OrderedRDD]] of tuples (K, V).
   * @param summarizer An [[OverlappableSummarizer]] that is expected to apply.
   * @param windowFn   A function specifies the overlap, i.e. for a partition of `rdd` with range [b, e), it will
   *                   expand to include all rows within range [b1, e1) where b1 is the left window boundary of b,
   *                   and e1 is the right window boundary of e. Then the summarizer will be applied to each
   *                   expanded partition where each row includes a flag to indicates whether a row is overlapped or
   *                   not.
   * @param skFn       A function that extracts keys from rows such that the summarizer will be applied per
   *                   key level.
   * @return the summarized results.
   */
  def apply[K: ClassTag: Ordering, SK, V: ClassTag, U, V2](
    rdd: OrderedRDD[K, V],
    summarizer: OverlappableSummarizer[V, U, V2],
    windowFn: K => (K, K),
    skFn: V => SK
  ): Map[SK, V2] = {
    val overlappedRdd = OverlappedOrderedRDD(rdd, windowFn).zipOverlapped() // (K, (V, Boolean))
    val partiallySummarized: RDD[Map[SK, U]] = overlappedRdd.map(_._2).mapPartitions {
      iter =>
        // Initialize the initial state.
        val uPerSK = mutable.HashMap.empty[SK, U]
        while (iter.hasNext) {
          val v = iter.next()
          val sk = skFn(v._1)
          val previousU = uPerSK.getOrElse(sk, summarizer.zero())
          uPerSK += sk -> summarizer.add(previousU, v)
        }
        Iterator(uPerSK)
    }.map { m => Map[SK, U](m.toSeq: _*) } // Make it immutable to return.

    val mergeOp = (u1: Map[SK, U], u2: Map[SK, U]) => (u1 ++ u2).map {
      case (sk, _) =>
        (sk, summarizer.merge(u1.getOrElse(sk, summarizer.zero()), u2.getOrElse(sk, summarizer.zero())))
    }

    TreeReduce(partiallySummarized)(mergeOp).map {
      case (sk, v) => (sk, summarizer.render(v))
    }
  }
}
