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

package com.twosigma.flint.rdd.function

import com.twosigma.flint.rdd.function.group.SummarizeByKeyIterator
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.RowsSummarizer
import org.scalatest.FlatSpec

class SummarizeByKeySpec extends FlatSpec {

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

  "SummarizeByKeyIterator" should "collect rows key correctly " in {
    var iter =
      new SummarizeByKeyIterator(
        data.iterator,
        (_: Any) => None,
        new RowsSummarizer[(Int, Double)]
      ).map {
        case (k, (_, v)) => (k, v.toList)
      }

    assert(
      iter.next == (1000L, List((1, 0.01), (2, 0.02), (1, 0.03), (2, 0.04)))
    )
    assert(
      iter.next == (1010L, List((1, 0.05), (2, 0.06), (1, 0.07), (2, 0.08)))
    )
    assert(
      iter.next == (1020L, List((2, 0.09), (1, 0.10), (2, 0.11), (1, 0.12)))
    )
    assert(!iter.hasNext)

    iter = new SummarizeByKeyIterator(
      data.iterator,
      (_: Any) => None,
      new RowsSummarizer[(Int, Double)]
    ).map {
      case (k, (_, v)) => (k, v.toList)
    }

    for (_ <- 0 to 2) {
      assert(iter.hasNext)
      iter.next()
    }
    assert(!iter.hasNext)
  }

  it should "handle one key case correctly " in {
    val iter =
      new SummarizeByKeyIterator(
        data.take(4).iterator,
        (_: Any) => None,
        new RowsSummarizer[(Int, Double)]
      ).map {
        case (k, (_, v)) => (k, v.toList)
      }
    assert(
      iter.next == (1000L, List((1, 0.01), (2, 0.02), (1, 0.03), (2, 0.04)))
    )
    assert(!iter.hasNext)
  }

  it should "handle empty iterator case correctly " in {
    val iter = new SummarizeByKeyIterator(
      data.take(0).iterator,
      (_: Any) => None,
      new RowsSummarizer[(Int, Double)]
    )
    assert(!iter.hasNext)
  }

  it should "group key with sk correctly " in {
    var iter = new SummarizeByKeyIterator(
      data.iterator,
      (x: (Int, Double)) => x._1,
      new RowsSummarizer[(Int, Double)]
    ).map {
      case (k, (sk, v)) => (k, sk, v.toList)
    }

    assert(iter.next == (1000L, 1, List((1, 0.01), (1, 0.03))))
    assert(iter.next == (1000L, 2, List((2, 0.02), (2, 0.04))))
    assert(iter.next == (1010L, 1, List((1, 0.05), (1, 0.07))))
    assert(iter.next == (1010L, 2, List((2, 0.06), (2, 0.08))))
    assert(iter.next == (1020L, 2, List((2, 0.09), (2, 0.11))))
    assert(iter.next == (1020L, 1, List((1, 0.10), (1, 0.12))))

    assert(!iter.hasNext)

    iter = new SummarizeByKeyIterator(
      data.iterator,
      (x: (Int, Double)) => x._1,
      new RowsSummarizer[(Int, Double)]
    ).map {
      case (k, (sk, v)) => (k, sk, v.toList)
    }

    for (i <- 0 to 5) {
      assert(iter.hasNext)
      iter.next()
    }
    assert(!iter.hasNext)
  }

  it should "group one key with multiple sk correctly " in {
    val iter = new SummarizeByKeyIterator(
      data.take(4).iterator,
      (x: (Int, Double)) => x._1,
      new RowsSummarizer[(Int, Double)]
    ).map { case (k, (_, v)) => (k, v.toList) }

    assert(iter.next == (1000L, List((1, 0.01), (1, 0.03))))
    assert(iter.next == (1000L, List((2, 0.02), (2, 0.04))))
    assert(!iter.hasNext)
  }
}
