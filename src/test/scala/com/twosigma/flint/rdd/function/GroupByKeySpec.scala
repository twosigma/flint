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

package com.twosigma.flint.rdd.function

import com.twosigma.flint.rdd.function.group.GroupByKeyIterator
import org.scalatest.FlatSpec

class GroupByKeySpec extends FlatSpec {

  val data = Array(
    (1000L, (1, 0.01)),
    (1000L, (2, 0.02)),
    (1000L, (1, 0.03)),
    (1000L, (2, 0.04)),
    (1010L, (1, 0.05)),
    (1010L, (2, 0.06)),
    (1010L, (1, 0.07)),
    (1010L, (2, 0.08)),
    (1020L, (2, 0.09)),
    (1020L, (1, 0.10)),
    (1020L, (2, 0.11)),
    (1020L, (1, 0.12))
  )

  "GroupByKeyIterator" should "group key correctly " in {
    var iter = GroupByKeyIterator(data.iterator, (x: Any) => None).map {
      case (k, v) => (k, v.toList)
    }
    assert(iter.next == (1000L, List((1, 0.01), (2, 0.02), (1, 0.03), (2, 0.04))))
    assert(iter.next == (1010L, List((1, 0.05), (2, 0.06), (1, 0.07), (2, 0.08))))
    assert(iter.next == (1020L, List((2, 0.09), (1, 0.10), (2, 0.11), (1, 0.12))))
    assert(!iter.hasNext)

    iter = GroupByKeyIterator(data.iterator, (x: Any) => None).map {
      case (k, v) => (k, v.toList)
    }
    for (i <- 0 to 2) {
      assert(iter.hasNext)
      iter.next()
    }
    assert(!iter.hasNext)
  }

  it should "handle one key case correctly " in {
    val iter = GroupByKeyIterator(data.take(4).iterator, (x: Any) => None).map {
      case (k, v) => (k, v.toList)
    }
    assert(iter.next == (1000L, List((1, 0.01), (2, 0.02), (1, 0.03), (2, 0.04))))
    assert(!iter.hasNext)
  }

  it should "handle empty iterator case correctly " in {
    val iter = GroupByKeyIterator(data.take(0).iterator, (x: Any) => None)
    assert(!iter.hasNext)
  }

  it should "group key with sk correctly " in {
    var iter = GroupByKeyIterator(data.iterator, (x: (Int, Double)) => x._1).map {
      case (k, v) => (k, v.toList)
    }
    assert(iter.next == (1000L, List((2, 0.02), (2, 0.04))))
    assert(iter.next == (1000L, List((1, 0.01), (1, 0.03))))
    assert(iter.next == (1010L, List((2, 0.06), (2, 0.08))))
    assert(iter.next == (1010L, List((1, 0.05), (1, 0.07))))
    assert(iter.next == (1020L, List((2, 0.09), (2, 0.11))))
    assert(iter.next == (1020L, List((1, 0.10), (1, 0.12))))
    assert(!iter.hasNext)

    iter = GroupByKeyIterator(data.iterator, (x: (Int, Double)) => x._1).map {
      case (k, v) => (k, v.toList)
    }
    for (i <- 0 to 5) {
      assert(iter.hasNext)
      iter.next()
    }
    assert(!iter.hasNext)
  }

  it should "group one key with multiple sk correctly " in {
    val iter = GroupByKeyIterator(data.take(4).iterator, (x: (Int, Double)) => x._1).map {
      case (k, v) => (k, v.toList)
    }
    assert(iter.next == (1000L, List((2, 0.02), (2, 0.04))))
    assert(iter.next == (1000L, List((1, 0.01), (1, 0.03))))
    assert(!iter.hasNext)
  }
}
