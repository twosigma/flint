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

package com.twosigma.flint.rdd

import scala.reflect.ClassTag

/**
 * An iterator expected to iterate through an ordered collection, i.e. elements in the collection
 * are ordered by their keys.
 */
protected[flint] class OrderedIterator[T, K: Ordering: ClassTag](
  iterator: Iterator[T],
  key: (T) => K
) extends Iterator[T] {

  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = iterator.next()

  /**
   * Filter the iterator by a given range. It will first drop the elements out of range and then take elements
   * continuously until out of range.
   *
   * @param range A range specifies a range of keys to keep.
   * @return an [[OrderedIterator]] preserving the ordering.
   */
  def filterByRange(range: Range[K]): OrderedIterator[T, K] = {
    val filtered = iterator.dropWhile {
      t => !range.contains(key(t))
    }.takeWhile {
      t => range.contains(key(t))
    }
    new OrderedIterator(filtered, key)
  }
}

protected[flint] class OrderedKeyValueIterator[K: Ordering: ClassTag, V](
  iterator: Iterator[(K, V)]
) extends OrderedIterator[(K, V), K](iterator, { case (k, v) => k })

protected[flint] object OrderedIterator {

  def apply[K: Ordering: ClassTag, V](iterator: Iterator[(K, V)]): OrderedKeyValueIterator[K, V] =
    new OrderedKeyValueIterator(iterator)

  def apply[T, K: Ordering: ClassTag](iterator: Iterator[T], key: (T) => K): OrderedIterator[T, K] =
    new OrderedIterator(iterator, key)
}
