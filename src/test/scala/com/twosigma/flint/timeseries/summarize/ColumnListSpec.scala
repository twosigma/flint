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

package com.twosigma.flint.timeseries.summarize

import org.scalatest.FlatSpec

class ColumnListSpec extends FlatSpec {

  "ColumnList" should "union column lists correctly" in {
    val allUnion = ColumnList.union(ColumnList.All, ColumnList.All)
    assert(allUnion == ColumnList.All)

    val list1 = ColumnList.Sequence(Seq("a", "b", "c"))
    val list2 = ColumnList.Sequence(Seq("a", "d"))

    val leftUnion = ColumnList.union(list1, ColumnList.All)
    assert(leftUnion == ColumnList.All)

    val rightUnion = ColumnList.union(ColumnList.All, list2)
    assert(rightUnion == ColumnList.All)

    val union = ColumnList.union(list1, list2)
    assert(union == ColumnList.Sequence(Seq("a", "b", "c", "d")))
  }
}
