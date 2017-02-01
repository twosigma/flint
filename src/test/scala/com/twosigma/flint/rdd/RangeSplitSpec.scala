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

import org.apache.spark.Partition
import org.scalatest.FlatSpec

class RangeSplitSpec extends FlatSpec {
  case class Split(override val index: Int) extends Partition

  val rangeSplits = IndexedSeq(
    RangeSplit(Split(0), CloseOpen(2, Option(3))),
    RangeSplit(Split(1), CloseOpen(3, Option(4))),
    RangeSplit(Split(2), CloseOpen(5, Option(7))),
    RangeSplit(Split(3), CloseOpen(7, Option(10)))
  )

  "The RangeSplit" should "getNextBegin correctly" in {
    val begins = rangeSplits.map(_.range.begin)
    assert(RangeSplit.getNextBegin(2, begins) == Some(3))
    assert(RangeSplit.getNextBegin(4, begins) == Some(5))
    assert(RangeSplit.getNextBegin(6, begins) == Some(7))
    assert(RangeSplit.getNextBegin(8, begins).isEmpty)
    assert(RangeSplit.getNextBegin(1, Vector[Int]()).isEmpty)
  }

  it should "getSplitsWithinRange correctly" in {
    assert(RangeSplit.getIntersectingSplits(CloseOpen(4, Some(6)), rangeSplits) ==
      List(RangeSplit(Split(2), CloseOpen(5, Some(7)))))

    assert(RangeSplit.getIntersectingSplits(CloseOpen(4, None), rangeSplits) ==
      List(RangeSplit(Split(2), CloseOpen(5, Some(7))), RangeSplit(Split(3), CloseOpen(7, Some(10)))))

  }
}
