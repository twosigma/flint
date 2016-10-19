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

package com.twosigma.flint.rdd

// TODO: possibly use google.common.collection.range,
//       or twitter.algebird.Interval.scala,
//       or spire.math.Interval.scala instead.
object Range {
  /**
   * @return a close-open range if begin is not equal to end or a close range if the begin equals
   * to the end.
   */
  def apply[K](begin: K, end: Option[K])(implicit ord: Ordering[K]): Range[K] =
    closeOpen(begin, end)

  /**
   * @return a close-open range if the begin is not equal to the end or a close range if the
   * begin equals to the end.
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
  def closeClose[K](begin: K, end: K)(implicit ord: Ordering[K]): Range[K] = {
    if (ord.equiv(begin, end)) CloseSingleton(begin) else CloseClose(begin, Some(end))
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
   * @return true if and only if the end is greater or equal to the k in the ordering.
   */
  def endGteq(k: K): Boolean = end.fold(true)(ord.gteq(_, k))

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
  def expand(f: K => (K, K)): Range[K] = {
    throw new UnsupportedOperationException()
  }

  /**
   * Shift the Range. The begin of the new range is f(begin) and the end of the new range is f(end).
   * The type of the range is unchanged
   */
  def shift(f: K => K): Range[K] = {
    throw new UnsupportedOperationException()
  }
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
  require(end.forall(ord.gt(_, begin)), s"end ${end} is not strictly greater than begin ${begin}")

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
    case CloseClose(b, e) => e.forall(ord.gteq(_, begin)) && e.forall(ord.gt(_, b))
  }

  override def contains(k: K): Boolean = ord.gteq(k, begin) && end.forall(ord.lt(k, _))

  override def ==(other: Range[K]): Boolean =
    other.isInstanceOf[CloseOpen[K]] && beginEquals(other) && endEquals(other)

  override def expand(f: K => (K, K)): CloseOpen[K] =
    CloseOpen(f(begin)._1, end.map{ e => f(e)._2 })

  override def shift(f: K => K): CloseOpen[K] =
    CloseOpen(f(begin), end.map(f(_)))
}
