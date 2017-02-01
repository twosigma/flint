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

import com.twosigma.flint.rdd.function.join.RangeMergeJoin
import org.apache.spark.Partition
import org.scalatest.FlatSpec

class RangeMergeJoinSpec extends FlatSpec {
  case class Split(override val index: Int) extends Partition

  val thisSplits = IndexedSeq(
    RangeSplit(Split(0), CloseOpen(1, Some(2))),
    RangeSplit(Split(1), CloseOpen(2, Some(3))),
    RangeSplit(Split(2), CloseOpen(3, Some(4))),
    RangeSplit(Split(3), CloseOpen(4, Some(5))),
    RangeSplit(Split(4), CloseOpen(5, None))
  )

  val thatSplits = IndexedSeq(
    RangeSplit(Split(0), CloseOpen(1, Some(3))),
    RangeSplit(Split(1), CloseOpen(3, Some(7))),
    RangeSplit(Split(2), CloseOpen(7, None))
  )

  "The RangeMergeJoin" should "`mergeSplits` with no tolerance correctly" in {
    val benchmark = List(
      RangeMergeJoin(
        CloseOpen(1, Some(2)),
        List(RangeSplit(Split(0), CloseOpen(1, Some(2)))),
        List(RangeSplit(Split(0), CloseOpen(1, Some(3))))
      ),
      RangeMergeJoin(
        CloseOpen(2, Some(3)),
        List(RangeSplit(Split(1), CloseOpen(2, Some(3)))),
        List(RangeSplit(Split(0), CloseOpen(1, Some(3))))
      ),
      RangeMergeJoin(
        CloseOpen(3, Some(4)),
        List(RangeSplit(Split(2), CloseOpen(3, Some(4)))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(4, Some(5)),
        List(RangeSplit(Split(3), CloseOpen(4, Some(5)))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(5, Some(7)),
        List(RangeSplit(Split(4), CloseOpen(5, None))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(7, None),
        List(RangeSplit(Split(4), CloseOpen(5, None))),
        List(RangeSplit(Split(2), CloseOpen(7, None)))
      )
    )
    assertResult(benchmark) { RangeMergeJoin.mergeSplits(thisSplits, thatSplits) }
  }

  it should "`mergeSplits` with some tolerance correctly" in {
    val benchmark = List(
      RangeMergeJoin(
        CloseOpen(1, Some(2)),
        List(RangeSplit(Split(0), CloseOpen(1, Some(2)))),
        List(RangeSplit(Split(0), CloseOpen(1, Some(3))))
      ),
      RangeMergeJoin(
        CloseOpen(2, Some(3)),
        List(RangeSplit(Split(0), CloseOpen(1, Some(2))), RangeSplit(Split(1), CloseOpen(2, Some(3)))),
        List(RangeSplit(Split(0), CloseOpen(1, Some(3))))
      ),
      RangeMergeJoin(
        CloseOpen(3, Some(4)),
        List(RangeSplit(Split(1), CloseOpen(2, Some(3))), RangeSplit(Split(2), CloseOpen(3, Some(4)))),
        List(RangeSplit(Split(0), CloseOpen(1, Some(3))), RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(4, Some(5)),
        List(RangeSplit(Split(2), CloseOpen(3, Some(4))), RangeSplit(Split(3), CloseOpen(4, Some(5)))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(5, Some(7)),
        List(RangeSplit(Split(3), CloseOpen(4, Some(5))), RangeSplit(Split(4), CloseOpen(5, None))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))))
      ),
      RangeMergeJoin(
        CloseOpen(7, None),
        List(RangeSplit(Split(4), CloseOpen(5, None))),
        List(RangeSplit(Split(1), CloseOpen(3, Some(7))), RangeSplit(Split(2), CloseOpen(7, None)))
      )
    )

    assertResult(benchmark) { RangeMergeJoin.mergeSplits(thisSplits, thatSplits, { x: Int => x - 1 }) }
  }
}
