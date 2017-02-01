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

/**
 * Merge two iterators of different types into one single iterator. The types of iterators to be merged are (K, A)
 * and (K, B). The merged iterator is of type (K, Either[A, B]) and of order K.
 */
case class MergeIterator[K, A, B](
  leftIter: PeekableIterator[(K, A)],
  rightIter: PeekableIterator[(K, B)]
)(
  implicit
  ord: Ordering[K]
) extends Iterator[(K, Either[A, B])] {

  def e(t: (K, A)): (K, Either[A, B]) = (t._1, Left(t._2))

  def e(t: (K, B))(implicit dumb: Null = null): (K, Either[A, B]) =
    (t._1, Right(t._2))

  override def hasNext: Boolean = leftIter.hasNext || rightIter.hasNext

  override def next: (K, Either[A, B]) = leftIter.peek.fold(e(rightIter.next)) {
    left =>
      rightIter.peek.fold(e(leftIter.next)) { right =>
        if (ord.lteq(left._1, right._1)) {
          e(leftIter.next)
        } else {
          e(rightIter.next)
        }
      }
  }
}
