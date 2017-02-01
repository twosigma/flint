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

protected[flint] object PeekableIterator {
  def apply[T](iter: Iterator[T]): PeekableIterator[T] = new PeekableIterator(iter)
}

protected[flint] class PeekableIterator[T](iter: Iterator[T]) extends Iterator[T] {
  var peeked: Option[T] = None

  def peek: Option[T] = {
    if (peeked.isEmpty && iter.hasNext) {
      peeked = Some(iter.next)
    }
    peeked
  }

  override def hasNext: Boolean = peeked.nonEmpty || iter.hasNext

  override def next: T =
    if (peeked.isEmpty) {
      iter.next
    } else {
      val ret = peeked.get
      peeked = None
      ret
    }
}
