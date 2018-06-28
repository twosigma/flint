/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.util.collection

import java.util.{ LinkedList => JLinkedList }

private[flint] class LinkedListHolder[V](l: JLinkedList[V]) {

  /**
   * Drop elements from the beginning of held list while they satisfy the predicate `p`.
   *
   * @note The held list will be modified.
   * @param p A predicate
   * @return a tuple where the left is the list of removed elements and the right is the list of left elements.
   */
  def dropWhile(
    p: (V) => Boolean
  ): (JLinkedList[V], JLinkedList[V]) = {
    val dropped = new JLinkedList[V]()
    while (!l.isEmpty && p(l.getFirst)) {
      dropped.add(l.removeFirst())
    }
    (dropped, l)
  }

  def foldLeft[U](u: U)(op: (U, V) => U): U = {
    var result = u
    val iter = l.iterator()
    while (iter.hasNext) {
      result = op(result, iter.next())
    }
    result
  }

  def foldRight[U](u: U)(op: (U, V) => U): U = {
    var result = u
    val iter = l.descendingIterator()
    while (iter.hasNext) {
      result = op(result, iter.next())
    }
    result
  }
}
