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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.rdd._
import scala.collection.mutable.{ HashMap => MHashMap, Map => MMap }
import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer

/**
 * Add intermediate values of the reduction (as per reduce) of summarizer, i.e. it applies the
 * summarizer to all its previous rows and the the current row in order.
 *
 * For a summarizer s, let us define
 *
 * 0 = s.zero()
 *
 * u + a = s.add(u, a)
 *
 * u1 + u1 = s.merge(u1, u2)
 *
 * Note that for any sequence of values  (a[1], a[2], ..., a[n]), (a'[1], a'[2], ..., a'[m]),
 * by the definition of [[Summarizer]] if
 *
 * u1 = 0 + a[1] + ... + a[n]
 *
 * u2 = 0 + a'[1] + ... + a'[m]
 *
 * then
 *
 * u1 + u2 = 0 + a[1] + ... + a[n] + a'[1] + ... + a'[m]
 *
 * This implies that we could have a two-pass algorithm where the first pass is to apply the
 * summarizer per partition; and the second pass is to calculate the summary for every rows
 * per partition based on the intermediate summary results from the first pass. See
 * more information at [[https://www.cs.cmu.edu/~guyb/papers/Ble93.pdf Prefix Sums and Their Applications]].
 */
object Summarizations {
  /**
   * This is in the favor of OrderedRDD.mapPartitionsWithIndexOrdered to convert an iterator of
   * a partition to an iterator with desired type. It is quite similar to Iterator.scanLeft but
   * easier to use.
   */
  private[this] def scanLeft[K, SK, V, Z, V2](
    iter: Iterator[(K, V)], z: Z
  )(
    op: (Z, (K, V)) => (Z, V2)
  ): Iterator[(K, (V, V2))] =
    new Iterator[(K, (V, V2))] {
      var s = z

      override def hasNext = iter.hasNext

      override def next() = if (hasNext) {
        val (k, v) = iter.next()
        val res = op(s, (k, v))
        s = res._1
        (k, (v, res._2))
      } else {
        Iterator.empty.next()
      }
    }

  /**
   * Add intermediate values of the reduction (as per reduce) of summarizer, i.e. it applies the
   * summarizer to all its previous rows and the the current row in order.
   *
   * @param rdd        An [[OrderedRDD]] of tuples (K, V).
   * @param summarizer A [[Summarizer]] expect to apply.
   * @param skFn       A function specifies that if the summarization is per secondary key then the
   *                   secondary key is defined by this function.
   * @return an [[OrderedRDD]] of tuples (K, (V, V2)) where V2 is the summary of all rows prior
   *         to and also including the current row.
   * @note In the current implementation, the first pass is not lazy. We should implement
   *       OrderedRDD.treeAggregateOrdered() similar to RDD.treeAggregated() method and make it lazy.
   */
  def apply[K, SK, V, U, V2](
    rdd: OrderedRDD[K, V],
    summarizer: Summarizer[V, U, V2],
    skFn: V => SK
  ): OrderedRDD[K, (V, V2)] = {
    val partitionIdToIntermediate = Summarize.summarizePartition(rdd, summarizer, skFn)
    // prefixes(k) is the prefix sum (per secondary key) of partitions 0 until (k - 1)
    val prefixes = partitionIdToIntermediate.foldLeft(
      // The elements of a tuple is a partition index k, the prefix sum (per secondary key)
      // of partitions 1, 2, ..., (k - 1), the sum of (per secondary key) partition k.
      List((0, Map.empty[SK, U], Map.empty[SK, U]))
    ) {
        case (p, (i, uPerSK)) =>
          val uPerSK1 = p.head._2
          val uPerSK2 = p.head._3
          (i, (uPerSK1 ++ uPerSK2).map {
            case (sk, _) => (sk, summarizer.merge(
              uPerSK1.getOrElse(sk, summarizer.zero()), uPerSK2.getOrElse(sk, summarizer.zero())
            ))
          }, uPerSK) :: p
        // Remove the start value per calling foldLeft()
      }.reverse.tail.map { x => (x._1, x._2) }.toMap

    rdd.mapPartitionsWithIndexOrdered {
      case (idx, iter) => scanLeft(iter, MHashMap.empty ++ prefixes(idx)){
        case (uPerSK: MMap[SK, U], (k, v)) =>
          val sk = skFn(v)
          val added = summarizer.add(uPerSK.getOrElse(sk, summarizer.zero), v)
          (uPerSK += sk -> added, summarizer.render(added))
      }
    }
  }
}