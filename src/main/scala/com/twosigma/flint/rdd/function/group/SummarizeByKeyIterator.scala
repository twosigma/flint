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

package com.twosigma.flint.rdd.function.group

import java.util

import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer
import org.apache.spark.TaskContext

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
 * Summarizes rows for each key and secondary key using a constant
 * amount of memory per SK. This means memory is bounded to the number
 * of distinct secondary keys times the size of the intermediate representation
 *
 * Assuming that you are summarizing with RowSummarizer, we can use the following
 * example to illustrate what this iterator looks like.
 *
 * {{{
 * val l = List(
 *   (1000L, (1, 0.01)),
 *   (1000L, (2, 0.02)),
 *   (1000L, (1, 0.03)),
 *   (1000L, (2, 0.04)))
 * val iter = SummarizeByKeyIterator(l.iterator, (x: (Int, Double)) => x._1, new RowSummarizer[(Int, Double)])
 * iter.next
 * // (1000L, Array((1, 0.01), (1, 0.03)))
 * iter.next
 * // (1000L, Array((2, 0.02), (2, 0.04)))
 * }}}
 */
private[rdd] case class SummarizeByKeyIterator[K, V, SK, U, V2](
  iter: Iterator[(K, V)],
  skFn: V => SK,
  summarizer: Summarizer[V, U, V2]
)(implicit tag: ClassTag[V], ord: Ordering[K]) extends Iterator[(K, (SK, V2))] {
  private val bufferedIter = iter.buffered

  private var currentKey: K = _
  // We use a mutable linked hash map in order to preserve the secondary key ordering.
  private val intermediates: util.LinkedHashMap[SK, U] = new util.LinkedHashMap()

  // This class is tested independently of Spark. In test, TaskContext can be null.
  if (TaskContext.get != null) {
    TaskContext.get.addTaskCompletionListener { _ => cleanup() }
  }

  private def cleanup(): Unit =
    intermediates.asScala.toMap.values.foreach{ u => summarizer.close(u) }

  override def hasNext: Boolean = !intermediates.isEmpty || bufferedIter.hasNext

  // Update intermediates with next key if bufferedIter.hasNext.
  private def nextKey(): Unit = if (bufferedIter.hasNext) {
    currentKey = bufferedIter.head._1
    // Iterates through all rows from the given iterator until seeing a different key.
    do {
      val v = bufferedIter.next._2
      val sk = skFn(v)
      val intermediate = intermediates.getOrDefault(sk, summarizer.zero())
      intermediates.put(sk, summarizer.add(intermediate, v))
    } while (bufferedIter.hasNext && ord.equiv(bufferedIter.head._1, currentKey))
  }

  override def next(): (K, (SK, V2)) = {
    if (intermediates.isEmpty) {
      nextKey()
    }
    if (hasNext) {
      val entry = intermediates.entrySet().iterator().next()
      val sk = entry.getKey
      val intermediate = entry.getValue
      intermediates.remove(sk)
      (currentKey, (sk, summarizer.render(intermediate)))
    } else {
      Iterator.empty.next()
    }
  }
}
