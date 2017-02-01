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

import scala.collection.Searching._
import scala.collection.mutable
import scala.reflect.ClassTag

// TODO: possibly use google.common.collection.range,
//       or twitter.algebird.Interval.scala,
//       or spire.math.Interval.scala instead.
object Range {
  /**
   * @return a close-open range if begin is not equal to end or a close range if the begin equals
   *         to the end.
   */
  def apply[K](begin: K, end: Option[K])(implicit ord: Ordering[K]): Range[K] =
    closeOpen(begin, end)

  /**
   * @return a close-open range if the begin is not equal to the end or a close range if the
   *         begin equals to the end.
   */
  def closeOpen[K](begin: K, end: Option[K])(implicit ord: Ordering[K]): Range[K] = end match {
    case None => CloseOpen(begin, None)
    case Some(e) => if (ord.equiv(begin, e)) CloseSingleton(begin) else CloseOpen(begin, end)
  }

  /**
   * @return a close-close range.
   */
  def apply[K: Ordering](begin: K, end: K): Range[K] = closeClose(begin, end)

  /**
   * @return a close-close range.
   */
  def closeClose[K](begin: K, end: K)(implicit ord: Ordering[K]): Range[K] = if (ord.equiv(begin, end)) {
    CloseSingleton(begin)
  } else {
    CloseClose(begin, Some(end))
  }

  /**
   * Test if a sequence of ranges are sorted.
   *
   * A sequence of ranges are sorted if and only if both the begin and the end of i-th range are less than
   * or equal to those of (i+1)-th range, respectively, for all i. This implies that if a range
   * intersects with i-th range but not the (i+1)-th range, then it won't intersect with any j-th range
   * when j > i, etc.
   *
   * @param ranges A sequence of ranges to test
   * @return true if the ranges is empty or it is sorted.
   */
  def isSorted[K](ranges: Seq[Range[K]])(implicit ord: Ordering[K]): Boolean =
    isSorted(ranges, { r: Range[K] => r })

  /**
   * Test if a sequence of items' ranges are sorted.
   *
   * A sequence of ranges are sorted if and only if both the begin and the end of i-th range are less than
   * or equal to those of (i+1)-th range, respectively, for all i. This implies that if a range
   * intersects with i-th range but not the (i+1)-th range, then it won't intersect with any j-th range
   * when j > i, etc.
   *
   * The `range` function will be used to exact the range information from an item and thus avoid extra
   * copying of `items` to get a sequence of ranges for binary search purpose.
   *
   * @param items A sequence of items whose ranges are expected to test
   * @param range A function to exact a range from an `item`
   * @return true if the ranges of items are sorted, i.e. the sequence of ranges is empty or if both the begin and
   *         the end of i-th range are less or equal than that of (i+1)-th range for all i.
   */
  def isSorted[K, U](items: Seq[U], range: U => Range[K])(implicit ord: Ordering[K]): Boolean =
    items.headOption.fold(true) { head =>
      var prev = range(head)
      val iter = items.toIterator.map(range(_))
      var isSorted = true
      while (isSorted && iter.hasNext) {
        val next = iter.next()
        isSorted = ord.gteq(next.begin, prev.begin) && next.endGteq(prev)
        prev = next
      }
      isSorted
    }

  /**
   * Return a sequence of indices of items whose ranges intersect with the range of given item.
   *
   * The `range` function will be used to exact the range information from an item and thus avoid extra
   * copying of `items` to get a sequence of ranges for binary search purpose.
   *
   * @param item     An item with a range
   * @param items    An indexed sequence of items whose ranges will be searched for intersections with that of `range`.
   * @param range    A function to exact a range from an `item`
   * @param isSorted A flag specifies whether ranges of items are sorted. If true, it will use binary search.
   *                 Otherwise, it will perform a linear scan. See [[isSorted()]] for testing if they are sorted.
   * @return a sequence of indices of items whose ranges intersect with the range of `item`. The order is preserved.
   */
  protected[rdd] def intersect[K: Ordering, U: ClassTag](
    item: U,
    items: IndexedSeq[U],
    range: U => Range[K],
    isSorted: Boolean
  ): Seq[Int] = if (isSorted) {
    val result: SearchResult = items.search(item)(Ordering.by[U, K](range(_).begin))
    // Need to search both sides from the insertion point.
    val left = mutable.ListBuffer[Int]()
    val right = mutable.ListBuffer[Int]()
    val r = range(item)

    var i = result.insertionPoint - 1
    while (i >= 0 && range(items(i)).intersects(r)) {
      left += i
      i = i - 1
    }

    i = result.insertionPoint
    while (i < items.length && range(items(i)).intersects(range(item))) {
      right += i
      i = i + 1
    }
    left.reverse ++ right
  } else {
    val r = range(item)
    val indices = mutable.ListBuffer[Int]()
    var i = 0
    while (i < items.length) {
      if (range(items(i)).intersects(r)) {
        indices += i
      }
      i = i + 1
    }
    indices
  }
}

sealed trait Range[K] {
  implicit val ord: Ordering[K]

  // TODO: the begin should be an option as well to support left open range.
  def begin: K

  def end: Option[K]

  /**
   * @return true if and only if this range intersects with others.
   */
  def intersects(other: Range[K]): Boolean

  /**
   * Return a sequence of indices of ranges that intersect with this range.
   *
   * @param ranges   An indexed sequence of ranges that will be searched for intersections.
   * @param isSorted A flag specifies whether ranges are sorted. If true, it will use binary search.
   *                 Otherwise, it will perform a linear scan. See [[Range.isSorted()]] for
   *                 for more information.
   * @return a sequence of indices of ranges that intersect with this range. The order is preserved.
   */
  def intersectsWith(ranges: IndexedSeq[Range[K]], isSorted: Boolean): Seq[Int] =
    Range.intersect(this, ranges, { r: Range[K] => r }, isSorted)

  /**
   * @return true if and only if its begin is strictly greater than k in the ordering.
   */
  def beginGt(k: K): Boolean = ord.gt(begin, k)

  /**
   * @return true if and only if its begin equals to k in the ordering.
   */
  def beginEquals(k: K): Boolean = ord.equiv(begin, k)

  /**
   * @return true if and only if its begin equals to the begin of the other range.
   */
  def beginEquals(other: Range[K]): Boolean = beginEquals(other.begin)

  /**
   * @return true if and only if the range contains the given element k.
   */
  def contains(k: K): Boolean

  /**
   * @return true if and only if its end equals to the other in the ordering.
   */
  def endEquals(other: Option[K]): Boolean = end.fold(other.isEmpty) {
    e => other.fold(false)(ord.equiv(e, _))
  }

  /**
   * @return true if and only if the end of other range is the same.
   */
  def endEquals(other: Range[K]): Boolean = endEquals(other.end)

  /**
   * @return true if and only if the end is greater or equal to the `k` in the ordering.
   */
  def endGteq(k: K): Boolean = end.fold(true)(ord.gteq(_, k))

  /**
   * @return true if and only if the end is greater or equal to the `other` in the ordering.
   */
  def endGteq(other: Option[K]): Boolean = end.fold(true) {
    thisEnd => other.fold(false)(ord.gteq(thisEnd, _))
  }

  /**
   * @return true if and only if the end is greater or equal to the end of the `other` range.
   */
  def endGteq(other: Range[K]): Boolean = endGteq(other.end)

  /**
   * @return true if and only if the other range has the same type and has the same begin and end
   * in the ordering.
   */
  def ==(other: Range[K]): Boolean

  /**
   * The opposite of ==.
   */
  def !=(other: Range[K]): Boolean = ! ==(other)

  /**
   * Expand the Range. The begin of the new range is f(begin)._1 and the end of the new range is
   * f(end)._2. The type of the range is unchanged.
   */
  def expand(f: K => (K, K)): Range[K] = ???

  /**
   * Shift the Range. The begin of the new range is f(begin) and the end of the new range is f(end).
   * The type of the range is unchanged
   */
  def shift(f: K => K): Range[K] = ???

}

/**
 * Represents a close-close range of [k, k].
 */
case class CloseSingleton[K](begin: K)(implicit val ord: Ordering[K]) extends Range[K] {
  def end: Some[K] = Some(begin)
  def intersects(other: Range[K]): Boolean = other.contains(begin)
  def contains(k: K): Boolean = ord.equiv(begin, k)
  def ==(other: Range[K]): Boolean =
    other.isInstanceOf[CloseSingleton[K]] && beginEquals(other)
}

/**
 * Represents a close-close range of [begin, end] where begin is not equal to end.
 */
case class CloseClose[K](begin: K, end: Some[K])(implicit val ord: Ordering[K]) extends Range[K] {
  require(end.forall(ord.gt(_, begin)), s"end $end is not strictly greater than begin $begin")

  def intersects(other: Range[K]): Boolean = other match {
    case CloseSingleton(k) => contains(k)
    case CloseClose(b, e) => e.forall(ord.gteq(_, begin)) && end.forall(ord.gteq(_, b))
    case CloseOpen(b, e) => e.forall(ord.gt(_, begin)) && end.forall(ord.gteq(_, b))
  }

  def contains(k: K): Boolean = ord.gteq(k, begin) && ord.gteq(end.get, k)

  def ==(other: Range[K]): Boolean =
    other.isInstanceOf[CloseClose[K]] && beginEquals(other) && endEquals(other)
}

/**
 * Represents a close-open range for [begin, end) where begin is not equal to end and the
 * end could be the greatest boundary of the universe.
 *
 * @param begin The begin of the range.
 * @param end   The end of the range. If it is None, it will be treated as the greatest possible
 *              value of type K.
 */
case class CloseOpen[K](begin: K, end: Option[K])(
  implicit
  val ord: Ordering[K]
) extends Range[K] {
  require(end.forall(ord.gt(_, begin)), s"end $end is not strictly greater than begin $begin")

  override def intersects(other: Range[K]): Boolean = other match {
    case CloseOpen(b, e) => e.forall(ord.gt(_, begin)) && end.forall(ord.gt(_, b))
    case CloseSingleton(k) => contains(k)
    case CloseClose(b, e) => e.forall(ord.gteq(_, begin)) && end.forall(ord.gt(_, b))
  }

  override def contains(k: K): Boolean = ord.gteq(k, begin) && end.forall(ord.lt(k, _))

  override def ==(other: Range[K]): Boolean =
    other.isInstanceOf[CloseOpen[K]] && beginEquals(other) && endEquals(other)

  override def expand(f: K => (K, K)): CloseOpen[K] =
    CloseOpen(f(begin)._1, end.map{ e => f(e)._2 })

  override def shift(f: K => K): CloseOpen[K] =
    CloseOpen(f(begin), end.map(f(_)))
}
