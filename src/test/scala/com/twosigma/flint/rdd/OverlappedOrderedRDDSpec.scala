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

class OverlappedOrderedRDDSpec extends FlatSpec with SharedSparkContext {

  val numSlices: Int = 3

  val sliceLength: Int = 4

  var rdd: RDD[(Int, Int)] = _

  var orderedRdd: OrderedRDD[Int, Int] = _

  var overlappedOrderedRdd: OverlappedOrderedRDD[Int, Int] = _

  private def window(t: Int): (Int, Int) = (t - 2, t)

  override def beforeAll() {
    super.beforeAll()
    val s = sliceLength
    rdd = sc.parallelize(0 until numSlices, numSlices).flatMap {
      i => (1 to s).map { j => i * s + j }
    }.map { x => (x, x) }
    orderedRdd = OrderedRDD.fromRDD(rdd, KeyPartitioningType.Sorted)
    overlappedOrderedRdd = OverlappedOrderedRDD(orderedRdd, window)
  }

  "The OverlappedOrderedRDD" should "be constructed from `OrderedRDD` correctly" in {
    assert(overlappedOrderedRdd.rangeSplits.deep == orderedRdd.rangeSplits.deep)
    val benchmark = Array(1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9, 8, 9, 10, 11, 12).map { x => (x, x) }
    assert(overlappedOrderedRdd.collect().deep == benchmark.deep)
  }

  it should "be able to remove overlapped rows to get an `OrderedRDD` correctly" in {
    assert(overlappedOrderedRdd.rangeSplits.deep == orderedRdd.rangeSplits.deep)
    assert(overlappedOrderedRdd.nonOverlapped().collect().deep == orderedRdd.collect().deep)
  }

  it should "`mapPartitionsWithIndexOverlapped` correctly" in {
    val mapped = overlappedOrderedRdd.mapPartitionsWithIndexOverlapped(
      (index, iterator) => iterator.map { case (k, v) => (k, v * 2) }
    )
    val benchmark = Array(1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9, 8, 9, 10, 11, 12).map { x => (x, 2 * x) }
    assert(mapped.collect().deep == benchmark.deep)
  }
}
