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

import org.scalatest.FlatSpec

class RangeDependencySpec extends FlatSpec {
  // partition 0: [1, 1, 2, ..., 4]
  // partition 1: [4, ..., 4]
  // partition 2: [4, 4, 5, ..., 7]
  // partition 3: [7, 8, 8, ..., 12]
  // partition 4: [13, 14, ..., 20]
  val headers = Seq(
    OrderedPartitionHeader(OrderedRDDPartition(0), 1, Some(2)),
    OrderedPartitionHeader(OrderedRDDPartition(1), 4, None),
    OrderedPartitionHeader(OrderedRDDPartition(2), 4, Some(5)),
    OrderedPartitionHeader(OrderedRDDPartition(3), 7, Some(8)),
    OrderedPartitionHeader(OrderedRDDPartition(4), 13, Some(14))
  ).reverse // Make it disorder.

  val singleKeyHeaders = Seq(
    OrderedPartitionHeader(OrderedRDDPartition(0), 5, None),
    OrderedPartitionHeader(OrderedRDDPartition(1), 5, None),
    OrderedPartitionHeader(OrderedRDDPartition(2), 5, None),
    OrderedPartitionHeader(OrderedRDDPartition(3), 5, None)
  ).reverse

  "RangeDependency" should "normalize a sequence of single header without second key correctly " in {
    val headers = Seq(OrderedPartitionHeader(OrderedRDDPartition(0), 1, None))
    val dep = RangeDependency.normalize(headers)
    assert(dep.size == 1)
    assert(dep.head.parents.size == 1)
    assert(dep.head.parents.head.index == 0)
    assert(dep.head.range == Range.closeOpen(1, None))
  }

  it should "normalize a sequence of single header with at least two different keys correctly " in {
    val headers = Seq(OrderedPartitionHeader(OrderedRDDPartition(0), 1, Some(2)))
    val dep = RangeDependency.normalize(headers)
    assert(dep.size == 1)
    assert(dep.head.parents.size == 1)
    assert(dep.head.parents.head.index == 0)
    assert(dep.head.range == Range.closeOpen(1, None))
  }

  it should "normalize single key headers " in {
    val intervals = HeavyKeysNormalizationStrategy.normalize(singleKeyHeaders)

    val expectedIntervals = Seq(CloseOpen(5, None))

    assert(intervals === expectedIntervals)
  }

  it should "correctly normalize one partition" in {
    val intervals =
      HeavyKeysNormalizationStrategy.normalize(Seq(OrderedPartitionHeader(OrderedRDDPartition(0), 5, None)))

    val expectedIntervals = Seq(CloseOpen(5, None))

    assert(intervals === expectedIntervals)
  }

  it should "correctly normalize partitions with one key in the first partition" in {
    val headers = Seq(
      OrderedPartitionHeader(OrderedRDDPartition(0), 1, None),
      OrderedPartitionHeader(OrderedRDDPartition(1), 4, Some(5)),
      OrderedPartitionHeader(OrderedRDDPartition(2), 7, Some(8))
    ).reverse

    val intervals = HeavyKeysNormalizationStrategy.normalize(headers)

    val expectedIntervals = Seq(CloseOpen(1, Some(5)), CloseOpen(5, Some(8)), CloseOpen(8, None))

    assert(intervals === expectedIntervals)
  }

  it should "correctly normalize partitions with one key in the last two partitions" in {
    val headers = Seq(
      OrderedPartitionHeader(OrderedRDDPartition(0), 1, Some(3)),
      OrderedPartitionHeader(OrderedRDDPartition(1), 4, None),
      OrderedPartitionHeader(OrderedRDDPartition(2), 4, None)
    ).reverse

    val intervals = HeavyKeysNormalizationStrategy.normalize(headers)

    val expectedIntervals = Seq(CloseOpen(1, Some(4)), CloseOpen(4, None))

    assert(intervals === expectedIntervals)
  }

  it should "normalize headers correctly " in {
    val intervals = HeavyKeysNormalizationStrategy.normalize(headers)

    val expectedIntervals = Seq(CloseOpen(1, Some(4)), CloseOpen(4, Some(5)),
      CloseOpen(5, Some(8)), CloseOpen(8, Some(14)), CloseOpen(14, None))

    assert(intervals === expectedIntervals)
  }

  it should "return correct range and dependencies " in {
    // [1, 4) depends on partitions 0
    // [4, 5) depends on partitions 0, 1, 2
    // [5, 8) depends on partitions 2, 3
    // [8, 14) depends on partitions 3, 4
    // [14, +infinity) depends on partitions 4
    val dep = RangeDependency.normalize(headers).toVector
    assert(dep.size == 5)
    assert(dep(0) == RangeDependency(
      0,
      CloseOpen(1, Some(4)),
      List(
        OrderedRDDPartition(0)
      )
    ))

    assert(dep(1) == RangeDependency(
      1,
      CloseOpen(4, Some(5)),
      List(
        OrderedRDDPartition(0),
        OrderedRDDPartition(1),
        OrderedRDDPartition(2)
      )
    ))

    assert(dep(2) == RangeDependency(
      2,
      CloseOpen(5, Some(8)),
      List(
        OrderedRDDPartition(2),
        OrderedRDDPartition(3)
      )
    ))

    assert(dep(3) == RangeDependency(
      3,
      CloseOpen(8, Some(14)),
      List(
        OrderedRDDPartition(3),
        OrderedRDDPartition(4)
      )
    ))

    assert(dep(4) == RangeDependency(
      4,
      CloseOpen(14, None),
      List(
        OrderedRDDPartition(4)
      )
    ))
  }

  it should "return correct range and dependencies when use BasicNormalizationStrategy" in {
    // [1, 5) depends on partitions 0, 1, 2
    // [5, 8) depends on partitions 2, 3
    // [8, 14) depends on partitions 3, 4
    // [14, +infinity) depends on partitions 4
    val dep = RangeDependency.normalize(headers, BasicNormalizationStrategy).toVector

    assert(dep.size == 4)
    assert(dep(0) == RangeDependency(
      0,
      CloseOpen(1, Some(5)),
      List(
        OrderedRDDPartition(0),
        OrderedRDDPartition(1),
        OrderedRDDPartition(2)
      )
    ))

    assert(dep(1) == RangeDependency(
      1,
      CloseOpen(5, Some(8)),
      List(
        OrderedRDDPartition(2),
        OrderedRDDPartition(3)
      )
    ))

    assert(dep(2) == RangeDependency(
      2,
      CloseOpen(8, Some(14)),
      List(
        OrderedRDDPartition(3),
        OrderedRDDPartition(4)
      )
    ))

    assert(dep(3) == RangeDependency(
      3,
      CloseOpen(14, None),
      List(
        OrderedRDDPartition(4)
      )
    ))
  }
}
