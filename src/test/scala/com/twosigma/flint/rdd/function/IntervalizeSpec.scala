/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.rdd.function.group.Intervalize
import org.scalatest.FlatSpec

import com.twosigma.flint.SharedSparkContext
import Intervalize._
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }

class IntervalizeSpec extends FlatSpec with SharedSparkContext {

  val data = Array(
    (1000L, (1, 0.01)),
    (1000L, (2, 0.02)),
    (1005L, (1, 0.03)),
    (1005L, (2, 0.04)),
    (1010L, (1, 0.05)),
    (1010L, (2, 0.06)),
    (1015L, (1, 0.07)),
    (1015L, (2, 0.08)),
    (1020L, (2, 0.11)),
    (1020L, (1, 0.12)),
    (1025L, (2, 0.13)),
    (1025L, (1, 0.14)),
    (1030L, (2, 0.15)),
    (1030L, (1, 0.16)),
    (1035L, (1, 0.17))
  )

  val expectedBeginBegin = List(
    (1000, (1000, (1, 0.01))),
    (1000, (1000, (2, 0.02))),
    (1000, (1005, (1, 0.03))),
    (1000, (1005, (2, 0.04))),
    (1010, (1010, (1, 0.05))),
    (1010, (1010, (2, 0.06))),
    (1010, (1015, (1, 0.07))),
    (1010, (1015, (2, 0.08))),
    // ---------------------
    (1020, (1020, (2, 0.11))),
    (1020, (1020, (1, 0.12))),
    (1020, (1025, (2, 0.13))),
    (1020, (1025, (1, 0.14)))
  )

  val expectedBeginEnd = List(
    (1010, (1000, (1, 0.01))),
    (1010, (1000, (2, 0.02))),
    (1010, (1005, (1, 0.03))),
    (1010, (1005, (2, 0.04))),
    (1020, (1010, (1, 0.05))),
    (1020, (1010, (2, 0.06))),
    (1020, (1015, (1, 0.07))),
    (1020, (1015, (2, 0.08))),
    // ---------------------
    (1030, (1020, (2, 0.11))),
    (1030, (1020, (1, 0.12))),
    (1030, (1025, (2, 0.13))),
    (1030, (1025, (1, 0.14)))
  )

  val expectedEndBegin = List(
    (1000, (1005, (1, 0.03))),
    (1000, (1005, (2, 0.04))),
    (1000, (1010, (1, 0.05))),
    (1000, (1010, (2, 0.06))),
    (1010, (1015, (1, 0.07))),
    (1010, (1015, (2, 0.08))),
    // ---------------------
    (1010, (1020, (2, 0.11))),
    (1010, (1020, (1, 0.12))),
    (1020, (1025, (2, 0.13))),
    (1020, (1025, (1, 0.14))),
    (1020, (1030, (2, 0.15))),
    (1020, (1030, (1, 0.16)))
  )

  val expectedEndEnd = List(
    (1010, (1005, (1, 0.03))),
    (1010, (1005, (2, 0.04))),
    (1010, (1010, (1, 0.05))),
    (1010, (1010, (2, 0.06))),
    (1020, (1015, (1, 0.07))),
    (1020, (1015, (2, 0.08))),
    // ---------------------
    (1020, (1020, (2, 0.11))),
    (1020, (1020, (1, 0.12))),
    (1030, (1025, (2, 0.13))),
    (1030, (1025, (1, 0.14))),
    (1030, (1030, (2, 0.15))),
    (1030, (1030, (1, 0.16)))
  )

  val clock = Array(1000L, 1010L, 1020L, 1030L)

  var orderedRDD: OrderedRDD[Long, (Int, Double)] = _
  var clockRDD: OrderedRDD[Long, Long] = _

  override def beforeAll() {
    super.beforeAll()
    orderedRDD = OrderedRDD.fromRDD(sc.parallelize(data, 4), KeyPartitioningType.Sorted)
    clockRDD = OrderedRDD.fromRDD(sc.parallelize(clock.map { k => (k, k) }, 2), KeyPartitioningType.Sorted)
  }

  "Intervalize" should "round correctly" in {
    val clock = Array(2, 4, 6)

    val roundBeginBegin = roundFn[Int]("begin", "begin")
    assert(roundBeginBegin(0, clock).isEmpty)
    assert(roundBeginBegin(2, clock) == Some(2))
    assert(roundBeginBegin(3, clock) == Some(2))
    assert(roundBeginBegin(4, clock) == Some(4))
    assert(roundBeginBegin(5, clock) == Some(4))
    assert(roundBeginBegin(6, clock).isEmpty)
    assert(roundBeginBegin(7, clock).isEmpty)

    val roundBeginEnd = roundFn[Int]("begin", "end")
    assert(roundBeginEnd(0, clock).isEmpty)
    assert(roundBeginEnd(2, clock) == Some(4))
    assert(roundBeginEnd(3, clock) == Some(4))
    assert(roundBeginEnd(4, clock) == Some(6))
    assert(roundBeginEnd(5, clock) == Some(6))
    assert(roundBeginEnd(6, clock).isEmpty)
    assert(roundBeginEnd(7, clock).isEmpty)

    val roundEndBegin = roundFn[Int]("end", "begin")
    assert(roundEndBegin(0, clock).isEmpty)
    assert(roundEndBegin(2, clock).isEmpty)
    assert(roundEndBegin(3, clock) == Some(2))
    assert(roundEndBegin(4, clock) == Some(2))
    assert(roundEndBegin(5, clock) == Some(4))
    assert(roundEndBegin(6, clock) == Some(4))
    assert(roundEndBegin(7, clock).isEmpty)

    val roundEndEnd = roundFn[Int]("end", "end")
    assert(roundEndEnd(0, clock).isEmpty)
    assert(roundEndEnd(2, clock).isEmpty)
    assert(roundEndEnd(3, clock) == Some(4))
    assert(roundEndEnd(4, clock) == Some(4))
    assert(roundEndEnd(5, clock) == Some(6))
    assert(roundEndEnd(6, clock) == Some(6))
    assert(roundEndEnd(7, clock).isEmpty)
  }

  it should "intervalize OrderedRDD using array clock with (begin, begin) correctly" in {
    var intervalized = intervalize(orderedRDD, clock, "begin", "begin")
    assert(intervalized.collect().toList == expectedBeginBegin)

    intervalized = intervalize(orderedRDD, clock, "begin", "end")
    assert(intervalized.collect().toList == expectedBeginEnd)

    intervalized = intervalize(orderedRDD, clock, "end", "begin")
    assert(intervalized.collect().toList == expectedEndBegin)

    intervalized = intervalize(orderedRDD, clock, "end", "end")
    assert(intervalized.collect().toList == expectedEndEnd)
  }
}
