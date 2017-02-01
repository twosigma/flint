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

package com.twosigma.flint.rdd.function.group

import scala.reflect.ClassTag
import scala.collection.mutable

/**
 * We use the following example to illustrate what this iterator looks like.
 *
 * {{{
 * val l = List(
 *   (1000L, (1, 0.01)),
 *   (1000L, (2, 0.02)),
 *   (1000L, (1, 0.03)),
 *   (1000L, (2, 0.04)))
 * val iter = GroupByKeyIterator(l.iterator, (x: (Int, Double)) => x._1)
 * iter.next
 * // (1000L, Array((1, 0.01), (1, 0.03)))
 * iter.next
 * // (1000L, Array((2, 0.02), (2, 0.04)))
 * }}}
 */
private[rdd] case class GroupByKeyIterator[K, SK, V](
  iter: Iterator[(K, V)],
  skFn: V => SK
)(implicit tag: ClassTag[V], ord: Ordering[K]) extends Iterator[(K, Array[V])] {
  private val bufferedIter = iter.buffered
  private var groupedBySkIter: Iterator[(K, Array[V])] = Iterator.empty

  override def hasNext: Boolean = groupedBySkIter.hasNext || bufferedIter.hasNext

  // Update groupedBySkIter with next key if bufferedIter.hasNext.
  private def nextKey(): Unit = if (bufferedIter.hasNext) {
    val groupKey = bufferedIter.head._1
    val group = mutable.ArrayBuffer[V]()
    // Iterates through all rows from the given iterator until seeing a different key.
    do {
      group.append(bufferedIter.next._2)
    } while (bufferedIter.hasNext && ord.equiv(bufferedIter.head._1, groupKey))
    groupedBySkIter = group.groupBy(skFn(_)).iterator.map { t => (groupKey, t._2.toArray) }
  }

  override def next(): (K, Array[V]) = {
    if (!groupedBySkIter.hasNext) {
      nextKey()
    }
    if (hasNext) groupedBySkIter.next() else Iterator.empty.next()
  }
}
