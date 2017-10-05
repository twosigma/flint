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

package com.twosigma.flint.timeseries

import com.twosigma.flint.rdd.CloseOpen
import com.twosigma.flint.timeseries.PartitionStrategy._

class PartitionStrategySpec extends TimeSeriesSuite with TimeSeriesTestData {

  private def getRanges(rdd: TimeSeriesRDD): List[CloseOpen[Long]] = rdd.partInfo.get.splits.map(_.range).toList

  it should "OnePartition correctly" in {
    val rdd = OnePartition.repartitionEnsureValid(testData)
    val testDataBegin = testData.first().getAs[Long]("time")
    val ranges = getRanges(rdd)
    val expectedRanges = CloseOpen(testDataBegin, None) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "OneTimestampTightBound" in {
    val rdd = OneTimestampTightBound.repartitionEnsureValid(testData)
    val timestamps = testData.rdd.map(_.getAs[Long]("time")).distinct().collect().sorted
    val ranges = getRanges(rdd)
    val expectedRanges = timestamps.map(t => CloseOpen(t, Some(t + 1))).toList
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimestampNormalized" in {
    val rdd = MultiTimestampNormalized.repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(1000L, Some(2001L)) ::
        CloseOpen(3000L, Some(4001L)) ::
        CloseOpen(5000L, Some(5001L)) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimeststampUnnormalized" in {
    val rdd = MultiTimestampUnnormailzed.repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(1000L, Some(3000L)) ::
        CloseOpen(3000L, Some(4000L)) ::
        CloseOpen(4000L, Some(5000L)) ::
        CloseOpen(5000L, None) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimestampNormalized :: ExtendBegin" in {
    val rdd = (MultiTimestampNormalized :: ExtendBegin).repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(Long.MinValue, Some(2001L)) ::
        CloseOpen(2001L, Some(4001L)) ::
        CloseOpen(4001L, Some(5001L)) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimestampNormalized :: ExtendEnd" in {
    val rdd = (MultiTimestampNormalized :: ExtendEnd).repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(1000L, Some(3000L)) ::
        CloseOpen(3000L, Some(5000L)) ::
        CloseOpen(5000L, None) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimestampNormalized :: ExtendHalfBeginHalfEnd" in {
    val rdd = (MultiTimestampNormalized :: ExtendHalfBeginHalfEnd).repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(Long.MinValue, Some(2500L)) ::
        CloseOpen(2500L, Some(4500L)) ::
        CloseOpen(4500L, None) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

  it should "MultiTimestampNormalized :: FillWithEmptyPartition" in {
    val rdd = (MultiTimestampNormalized :: FillWithEmptyPartition).repartitionEnsureValid(testData)
    val ranges = getRanges(rdd)
    val expectedRanges =
      CloseOpen(Long.MinValue, Some(1000L)) ::
        CloseOpen(1000L, Some(2001L)) ::
        CloseOpen(2001L, Some(3000L)) ::
        CloseOpen(3000L, Some(4001L)) ::
        CloseOpen(4001L, Some(5000L)) ::
        CloseOpen(5000L, Some(5001L)) ::
        CloseOpen(5001L, None) :: Nil
    assert(ranges == expectedRanges)
    assertEquals(testData, rdd)
  }

}
