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

import org.scalatest.FlatSpec

import scala.util.Random

class RangeSpec extends FlatSpec {
  "Range" should "intersect correctly for Range" in {

    // bounded CloseOpen vs bounded CloseOpen
    assert(Range(2, Some(4)).intersects(Range(2, Some(4))))
    assert(Range(2, Some(4)).intersects(Range(3, Some(5))))
    assert(Range(2, Some(4)).intersects(Range(1, Some(3))))
    assert(Range(2, Some(4)).intersects(Range(1, Some(5))))
    assert(!Range(2, Some(4)).intersects(Range(0, Some(1))))
    assert(!Range(2, Some(4)).intersects(Range(1, Some(2))))
    assert(!Range(2, Some(4)).intersects(Range(4, Some(5))))

    // bounded CloseOpen vs unbounded CloseOpen
    assert(Range(2, Some(4)).intersects(Range(1, None)))
    assert(Range(2, Some(4)).intersects(Range(2, None)))
    assert(Range(2, Some(4)).intersects(Range(3, None)))
    assert(!Range(2, Some(4)).intersects(Range(4, None)))
    assert(!Range(2, Some(4)).intersects(Range(5, None)))

    // unbounded CloseOpen vs unbounded CloseOpen
    assert(Range(2, None).intersects(Range(2, None)))
    assert(Range(2, None).intersects(Range(1, None)))

    // unbounded CloseOpen vs bounded CloseOpen
    assert(!Range(2, None).intersects(Range(0, Some(1))))
    assert(!Range(2, None).intersects(Range(0, Some(2))))
    assert(Range(2, None).intersects(Range(0, Some(3))))

    // bounded CloseOpen vs bounded CloseOpen
    assert(!Range(2, Some(4)).intersects(Range(1, Some(1))))
    assert(Range(2, Some(4)).intersects(Range(2, Some(2))))
    assert(Range(2, Some(4)).intersects(Range(3, Some(3))))
    assert(!Range(2, Some(4)).intersects(Range(4, Some(4))))

    // unbounded CloseOpen vs CloseSingleton
    assert(!Range(2, None).intersects(Range(1, Some(1))))
    assert(Range(2, None).intersects(Range(2, Some(2))))
    assert(Range(2, None).intersects(Range(3, Some(3))))

    // CloseSingleton vs bounded CloseOpen
    assert(!Range(3, Some(3)).intersects(Range(1, Some(2))))
    assert(!Range(3, Some(3)).intersects(Range(2, Some(3))))
    assert(Range(3, Some(3)).intersects(Range(3, Some(4))))
    assert(!Range(3, Some(3)).intersects(Range(4, Some(5))))

    // CloseSingleton vs unbounded CloseOpen
    assert(Range(3, Some(3)).intersects(Range(3, None)))
    assert(Range(3, Some(3)).intersects(Range(1, None)))
    assert(!Range(3, Some(3)).intersects(Range(4, None)))

    // CloseSingleton vs CloseSingleton
    assert(!Range(3, Some(3)).intersects(Range(4, Some(4))))
    assert(Range(3, Some(3)).intersects(Range(3, Some(3))))

    // CloseClose vs CloseClose
    assert(Range(2, 4).intersects(Range(2, 4)))
    assert(Range(2, 4).intersects(Range(3, 5)))
    assert(Range(2, 4).intersects(Range(1, 3)))
    assert(Range(2, 4).intersects(Range(1, 5)))
    assert(!Range(2, 4).intersects(Range(0, 1)))
    assert(Range(2, 4).intersects(Range(1, 2)))
    assert(Range(2, 4).intersects(Range(4, 5)))

    // CloseClose vs bounded CloseOpen
    assert(Range(2, 4).intersects(Range(2, Some(4))))
    assert(Range(2, 4).intersects(Range(3, Some(5))))
    assert(Range(2, 4).intersects(Range(1, Some(3))))
    assert(Range(2, 4).intersects(Range(1, Some(5))))
    assert(Range(2, 4).intersects(Range(4, Some(5))))
    assert(!Range(2, 4).intersects(Range(1, Some(2))))
    assert(!Range(2, 4).intersects(Range(0, Some(1))))

    // CloseClose vs unbounded CloseOpen
    assert(Range(2, 4).intersects(Range(2, None)))
    assert(Range(2, 4).intersects(Range(3, None)))
    assert(Range(2, 4).intersects(Range(1, None)))
    assert(Range(2, 4).intersects(Range(4, None)))
    assert(!Range(2, 4).intersects(Range(5, None)))

    // CloseClose vs CloseSingleton
    assert(Range(2, 4).intersects(Range(2, Some(2))))
    assert(Range(2, 4).intersects(Range(3, Some(3))))
    assert(Range(2, 4).intersects(Range(4, Some(4))))
    assert(!Range(2, 4).intersects(Range(5, Some(5))))
  }

  it should "contain correctly for Range" in {
    // bounded CloseOpen
    assert(Range(1, Some(3)).contains(1))
    assert(Range(1, Some(3)).contains(2))
    assert(!Range(1, Some(3)).contains(3))
    assert(!Range(1, Some(3)).contains(0))

    // unbounded CloseOpen
    assert(Range(3, None).contains(3))
    assert(Range(3, None).contains(4))
    assert(!Range(3, None).contains(2))

    // CloseSingleton
    assert(Range(3, 3).contains(3))
    assert(!Range(3, 3).contains(4))
    assert(!Range(3, 3).contains(2))

    // CloseClose
    assert(Range(2, 4).contains(2))
    assert(Range(2, 4).contains(3))
    assert(Range(2, 4).contains(4))
    assert(!Range(2, 4).contains(1))
    assert(!Range(2, 4).contains(5))
  }

  it should "beginGt correctly" in {
    val range = Range(2, Some(4))
    assert(!(range beginGt 3))
    assert(!(range beginGt 2))
    assert(range beginGt 1)
  }

  it should "beginEquals correctly" in {
    val range = Range(2, Some(4))
    assert(!(range beginEquals 3))
    assert(range beginEquals 2)
    assert(!(range beginEquals 1))
  }

  it should "equals correctly for close-open range" in {
    val range1 = Range.closeOpen(2, Some(4))
    assert(range1 == Range.closeOpen(2, Some(4)))
    assert(range1 != Range.closeOpen(2, Some(5)))
    assert(range1 != Range.closeOpen(2, Some(5)))
    assert(range1 != Range.closeOpen(2, None))
    assert(range1 != Range.closeOpen(2, None))
    assert(range1 != Range.closeClose(2, 4))

    val range2 = Range.closeOpen(2, None)
    assert(range2 == Range.closeOpen(2, None))
    assert(range2 != Range.closeOpen(2, Some(4)))
    assert(range2 != Range.closeOpen(2, Some(2)))
    assert(range2 != Range.closeClose(2, 4))
  }

  it should "equals correctly for close-close range" in {
    val range1 = Range.closeClose(2, 4)
    assert(range1 == Range.closeClose(2, 4))
    assert(range1 != Range.closeClose(2, 5))
    assert(range1 != Range.closeClose(3, 5))
    assert(range1 != Range.closeClose(3, 4))
    assert(range1 != Range.closeOpen(2, Some(4)))
    assert(range1 != Range.closeOpen(2, None))
    assert(range1 != Range.closeOpen(2, Some(2)))
    assert(range1 != Range.closeClose(2, 2))
  }

  it should "endGteq correctly for close-open range" in {
    var range = CloseOpen(2, Some(3))
    assert(range.endGteq(3))
    assert(range.endGteq(2))
    assert(!range.endGteq(4))

    assert(range.endGteq(Some(3)))
    assert(range.endGteq(Some(2)))
    assert(!range.endGteq(Some(4)))
    assert(!range.endGteq(None))

    range = CloseOpen(2, None)
    assert(range.endGteq(1))
    assert(range.endGteq(2))
    assert(range.endGteq(3))
    assert(range.endGteq(4))
    assert(range.endGteq(Some(1)))
    assert(range.endGteq(Some(2)))
    assert(range.endGteq(Some(3)))
    assert(range.endGteq(Some(4)))
    assert(range.endGteq(None))
  }

  it should "endGteq correctly for close-close range" in {
    val range = CloseClose(2, Some(3))
    assert(range.endGteq(1))
    assert(range.endGteq(2))
    assert(range.endGteq(3))
    assert(!range.endGteq(4))

    assert(range.endGteq(Some(1)))
    assert(range.endGteq(Some(2)))
    assert(range.endGteq(Some(3)))
    assert(!range.endGteq(Some(4)))
    assert(!range.endGteq(None))
  }

  it should "endGteq correctly for close singleton range" in {
    val range = CloseSingleton(2)
    assert(range.endGteq(1))
    assert(range.endGteq(2))
    assert(!range.endGteq(3))

    assert(range.endGteq(Some(1)))
    assert(range.endGteq(Some(2)))
    assert(!range.endGteq(Some(3)))
    assert(!range.endGteq(None))
  }

  it should "test isSorted correctly" in {
    var ranges: Seq[Range[Int]] = Seq.empty
    assert(Range.isSorted(ranges))

    ranges = Array(Range.closeOpen(1, Some(2)))
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(2)),
      Range.closeOpen(1, Some(2))
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(4)),
      Range.closeOpen(2, Some(4))
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(2, Some(4)),
      Range.closeOpen(1, Some(4))
    )
    assert(!Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(4)),
      Range.closeOpen(2, Some(5))
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(4)),
      Range.closeOpen(1, Some(5))
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(5)),
      Range.closeOpen(1, Some(4))
    )
    assert(!Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(5)),
      Range.closeOpen(1, None),
      Range.closeOpen(1, None),
      Range.closeOpen(2, None)
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(5)),
      Range.closeOpen(2, Some(6)),
      Range.closeOpen(2, Some(5))
    )
    assert(!Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, Some(5)),
      Range.closeOpen(2, Some(5)),
      Range.closeOpen(2, None)
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, None),
      Range.closeOpen(2, None),
      Range.closeOpen(2, None)
    )
    assert(Range.isSorted(ranges))

    ranges = Array(
      Range.closeOpen(1, None),
      Range.closeOpen(2, Some(5)),
      Range.closeOpen(2, None)
    )
    assert(!Range.isSorted(ranges))
  }

  def assertIntersectsWith(range: Range[Int], ranges: IndexedSeq[Range[Int]]): Unit = {
    assert(range.intersectsWith(ranges, true).toArray.deep
      == range.intersectsWith(ranges, false).toArray.deep)
    assert(range.intersectsWith(ranges, true).toArray.deep
      == ranges.zipWithIndex.filter { case (r, i) => range.intersects(r) }.map(_._2).toArray.deep)
  }

  it should "intersectsWith correctly" in {
    var ranges: IndexedSeq[Range[Int]] = Array[Range[Int]]()
    var range = Range.closeOpen(1, Some(4))
    assert(range.intersectsWith(ranges, true).isEmpty)
    assert(range.intersectsWith(ranges, false).isEmpty)

    ranges = Array(
      Range.closeOpen(1, Some(5)),
      Range.closeOpen(2, Some(5)),
      Range.closeOpen(2, Some(6)),
      Range.closeOpen(4, Some(8)),
      Range.closeOpen(6, None),
      Range.closeOpen(8, None)
    )
    range = Range.closeOpen(1, Some(8))
    assert(Range.isSorted(ranges))
    assert(range.intersectsWith(ranges, true).toArray.deep
      == Array(0, 1, 2, 3, 4).deep)
    assertIntersectsWith(range, ranges)

    assertIntersectsWith(Range.closeOpen(3, Some(6)), ranges)
    assertIntersectsWith(Range.closeOpen(3, None), ranges)
    assertIntersectsWith(Range.closeOpen(3, None), ranges)
    assertIntersectsWith(Range.closeOpen(1, None), ranges)
    assertIntersectsWith(Range.closeOpen(2, Some(3)), ranges)
    assertIntersectsWith(Range.closeOpen(2, Some(4)), ranges)
    assertIntersectsWith(Range.closeOpen(2, Some(5)), ranges)
    assertIntersectsWith(Range.closeOpen(-5, Some(1)), ranges)
  }

  it should "intersectsWith for random ranges correctly" in {
    val size = 100
    val begins = IndexedSeq.fill(size)(Random.nextInt(size * 10)).sorted
    val ranges = begins.map { b => CloseOpen(b, Some(b + 10)) }
    assert(Range.isSorted(ranges))
    (1 to size).map { b => assertIntersectsWith(CloseOpen(b, Some(b + 10)), ranges) }
  }
}