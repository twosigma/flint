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

package com.twosigma.flint.rdd.function.join

import org.scalatest.FlatSpec

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }

class LeftJoinSpec extends FlatSpec with SharedSparkContext {

  val left = Array(
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

  val right = Array(
    (1000L, (1, 0.011)),
    (1000L, (2, 0.021)),
    (1005L, (1, 0.031)),
    (1005L, (2, 0.041)),
    (1010L, (1, 0.051)),
    (1010L, (2, 0.061)),
    (1015L, (1, 0.071)),
    (1015L, (2, 0.081)),
    (1020L, (2, 0.111)),
    (1020L, (1, 0.121)),
    (1025L, (2, 0.131)),
    (1025L, (1, 0.141)),
    (1030L, (2, 0.151)),
    (1030L, (1, 0.161)),
    (1035L, (1, 0.171))
  )

  var leftRdd: OrderedRDD[Long, (Int, Double)] = _
  var rightRdd: OrderedRDD[Long, (Int, Double)] = _

  override def beforeAll() {
    super.beforeAll()
    leftRdd = OrderedRDD.fromRDD(sc.parallelize(left, 4), KeyPartitioningType.Sorted)
    rightRdd = OrderedRDD.fromRDD(sc.parallelize(right, 4), KeyPartitioningType.Sorted)
  }

  "LeftJoin" should "`futureJoin` per tid with strictForward correctly" in {
    val expected = Array(
      (1000, ((1, 0.01), Some((1005, (1, 0.031))))),
      (1000, ((2, 0.02), Some((1005, (2, 0.041))))),
      (1005, ((1, 0.03), Some((1010, (1, 0.051))))),
      (1005, ((2, 0.04), Some((1010, (2, 0.061))))),
      (1010, ((1, 0.05), Some((1015, (1, 0.071))))),
      (1010, ((2, 0.06), Some((1015, (2, 0.081))))),
      (1015, ((1, 0.07), Some((1020, (1, 0.121))))),
      (1015, ((2, 0.08), Some((1020, (2, 0.111))))),
      (1020, ((2, 0.11), Some((1025, (2, 0.131))))),
      (1020, ((1, 0.12), Some((1025, (1, 0.141))))),
      (1025, ((2, 0.13), Some((1030, (2, 0.151))))),
      (1025, ((1, 0.14), Some((1030, (1, 0.161))))),
      (1030, ((2, 0.15), None)),
      (1030, ((1, 0.16), Some((1035, (1, 0.171))))),
      (1035, ((1, 0.17), None))
    )

    val skFn = { case ((sk, v)) => sk }: ((Int, Double)) => Int
    val joined = FutureLeftJoin.apply(
      leftRdd, rightRdd,
      (t: Long) => t + 10,
      skFn, skFn, strictForward = true
    ).collect
    assert(joined.deep == expected.deep)
  }

  it should "`futureJoin` per tid without strictForward correctly" in {
    val expected = Array(
      (1000, ((1, 0.01), Some((1000, (1, 0.011))))),
      (1000, ((2, 0.02), Some((1000, (2, 0.021))))),
      (1005, ((1, 0.03), Some((1005, (1, 0.031))))),
      (1005, ((2, 0.04), Some((1005, (2, 0.041))))),
      (1010, ((1, 0.05), Some((1010, (1, 0.051))))),
      (1010, ((2, 0.06), Some((1010, (2, 0.061))))),
      (1015, ((1, 0.07), Some((1015, (1, 0.071))))),
      (1015, ((2, 0.08), Some((1015, (2, 0.081))))),
      (1020, ((2, 0.11), Some((1020, (2, 0.111))))),
      (1020, ((1, 0.12), Some((1020, (1, 0.121))))),
      (1025, ((2, 0.13), Some((1025, (2, 0.131))))),
      (1025, ((1, 0.14), Some((1025, (1, 0.141))))),
      (1030, ((2, 0.15), Some((1030, (2, 0.151))))),
      (1030, ((1, 0.16), Some((1030, (1, 0.161))))),
      (1035, ((1, 0.17), Some((1035, (1, 0.171)))))
    )

    val skFn = { case ((sk, v)) => sk }: ((Int, Double)) => Int
    val joined = FutureLeftJoin.apply(
      leftRdd, rightRdd,
      (t: Long) => t + 10,
      skFn, skFn, strictForward = false
    ).collect
    assert(joined.deep == expected.deep)
  }

  it should "`futureJoin` without strictForward correctly" in {
    val expected = Array(
      (1000, ((1, 0.01), Some((1000, (1, 0.011))))),
      (1000, ((2, 0.02), Some((1000, (1, 0.011))))),
      (1005, ((1, 0.03), Some((1005, (1, 0.031))))),
      (1005, ((2, 0.04), Some((1005, (1, 0.031))))),
      (1010, ((1, 0.05), Some((1010, (1, 0.051))))),
      (1010, ((2, 0.06), Some((1010, (1, 0.051))))),
      (1015, ((1, 0.07), Some((1015, (1, 0.071))))),
      (1015, ((2, 0.08), Some((1015, (1, 0.071))))),
      (1020, ((2, 0.11), Some((1020, (2, 0.111))))),
      (1020, ((1, 0.12), Some((1020, (2, 0.111))))),
      (1025, ((2, 0.13), Some((1025, (2, 0.131))))),
      (1025, ((1, 0.14), Some((1025, (2, 0.131))))),
      (1030, ((2, 0.15), Some((1030, (2, 0.151))))),
      (1030, ((1, 0.16), Some((1030, (2, 0.151))))),
      (1035, ((1, 0.17), Some((1035, (1, 0.171)))))
    )

    val skFn = { case ((sk, v)) => None }: ((Int, Double)) => Option[Nothing]
    val joined = FutureLeftJoin.apply(
      leftRdd, rightRdd,
      (t: Long) => t + 10,
      skFn, skFn, strictForward = false
    ).collect
    assert(joined.deep == expected.deep)
  }

  it should "`futureJoin` with strictForward correctly" in {
    val expected = Array(
      (1000, ((1, 0.01), Some((1005, (1, 0.031))))),
      (1000, ((2, 0.02), Some((1005, (1, 0.031))))),
      (1005, ((1, 0.03), Some((1010, (1, 0.051))))),
      (1005, ((2, 0.04), Some((1010, (1, 0.051))))),
      (1010, ((1, 0.05), Some((1015, (1, 0.071))))),
      (1010, ((2, 0.06), Some((1015, (1, 0.071))))),
      (1015, ((1, 0.07), Some((1020, (2, 0.111))))),
      (1015, ((2, 0.08), Some((1020, (2, 0.111))))),
      (1020, ((2, 0.11), Some((1025, (2, 0.131))))),
      (1020, ((1, 0.12), Some((1025, (2, 0.131))))),
      (1025, ((2, 0.13), Some((1030, (2, 0.151))))),
      (1025, ((1, 0.14), Some((1030, (2, 0.151))))),
      (1030, ((2, 0.15), Some((1035, (1, 0.171))))),
      (1030, ((1, 0.16), Some((1035, (1, 0.171))))),
      (1035, ((1, 0.17), None))
    )

    val skFn = { case ((sk, v)) => None }: ((Int, Double)) => Option[Nothing]
    val joined = FutureLeftJoin.apply(
      leftRdd, rightRdd,
      (t: Long) => t + 10,
      skFn, skFn, strictForward = true
    ).collect
    assert(joined.deep == expected.deep)
  }

}
