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

import com.twosigma.flint.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class ConversionSpec extends FlatSpec with SharedSparkContext {

  var sortedRDDWithEmptyPartitions: RDD[(Long, (Int, Double))] = _
  var sortedNonNormalizedRDD: RDD[(Long, (Int, Double))] = _

  val sortedData = Array(
    (1000L, (3, 8.90)),
    (1001L, (9, 8.91)),
    (1005L, (3, 8.92)),
    (1006L, (9, 8.93)),
    (1030L, (3, 8.94)),
    (1031L, (7, 8.95)),
    (1105L, (3, 8.96)),
    (1430L, (3, 8.97))
  )

  val nonNormalizedData = Array(
    (1000L, (3, 8.90)),
    (1000L, (9, 8.91)),
    (1001L, (3, 8.90)),
    (1002L, (9, 8.91))
  )

  override def beforeAll() {
    super.beforeAll()
    // RDD with 2 empty partitions
    sortedRDDWithEmptyPartitions = sc.parallelize(sortedData, sortedData.length + 2)
    sortedNonNormalizedRDD = sc.parallelize(nonNormalizedData, nonNormalizedData.length)
  }

  "Conversion" should "handle normalized RDD with empty partitions" in {
    assert(sortedRDDWithEmptyPartitions.getNumPartitions == 10)

    val orderedRdd = Conversion.fromNormalizedSortedRDD(sortedRDDWithEmptyPartitions)
    assert(orderedRdd.getNumPartitions == 8)
  }

  it should "fail in `fromNormalizedSortedRDD` if the given RDD is non normalized" in {
    intercept[IllegalArgumentException] {
      Conversion.fromNormalizedSortedRDD(sortedNonNormalizedRDD)
    }
  }
}
