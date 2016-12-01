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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.LeftSubtractableSummarizer
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ SumSummarizer => SumSum }

import scala.Serializable
import org.scalatest.FlatSpec
import org.scalactic.{ TolerantNumerics, Equality }
import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }

class SummarizationsSpec extends FlatSpec with SharedSparkContext {

  val data = Array(
    (1000L, (1, 0.01)),
    (1000L, (2, 0.01)),
    (1005L, (1, 0.01)),
    (1005L, (2, 0.01)),
    (1010L, (1, 0.01)),
    (1010L, (2, 0.01)),
    (1015L, (1, 0.01)),
    (1015L, (2, 0.01)),
    (1020L, (1, 0.01)),
    (1020L, (2, 0.01)),
    (1025L, (1, 0.01)),
    (1025L, (2, 0.01)),
    (1030L, (1, 0.01)),
    (1030L, (2, 0.01)),
    (1035L, (1, 0.01)),
    (1035L, (2, 0.01)),
    (1040L, (1, 0.01)),
    (1040L, (2, 0.01)),
    (1045L, (1, 0.01)),
    (1045L, (2, 0.01))
  )

  val expected = List(
    (1000, ((1, 0.01), 0.01)),
    (1000, ((2, 0.01), 0.02)),
    (1005, ((1, 0.01), 0.03)),
    (1005, ((2, 0.01), 0.04)),
    (1010, ((1, 0.01), 0.05)),
    (1010, ((2, 0.01), 0.06)),
    (1015, ((1, 0.01), 0.07)),
    (1015, ((2, 0.01), 0.08)),
    (1020, ((1, 0.01), 0.09)),
    (1020, ((2, 0.01), 0.10)),
    (1025, ((1, 0.01), 0.11)),
    (1025, ((2, 0.01), 0.12)),
    (1030, ((1, 0.01), 0.13)),
    (1030, ((2, 0.01), 0.14)),
    (1035, ((1, 0.01), 0.15)),
    (1035, ((2, 0.01), 0.16)),
    (1040, ((1, 0.01), 0.17)),
    (1040, ((2, 0.01), 0.18)),
    (1045, ((1, 0.01), 0.19)),
    (1045, ((2, 0.01), 0.20))
  )

  val expectedPerSK = List(
    (1000, ((1, 0.01), 0.01)),
    (1000, ((2, 0.01), 0.01)),
    (1005, ((1, 0.01), 0.02)),
    (1005, ((2, 0.01), 0.02)),
    (1010, ((1, 0.01), 0.03)),
    (1010, ((2, 0.01), 0.03)),
    (1015, ((1, 0.01), 0.04)),
    (1015, ((2, 0.01), 0.04)),
    (1020, ((1, 0.01), 0.05)),
    (1020, ((2, 0.01), 0.05)),
    (1025, ((1, 0.01), 0.06)),
    (1025, ((2, 0.01), 0.06)),
    (1030, ((1, 0.01), 0.07)),
    (1030, ((2, 0.01), 0.07)),
    (1035, ((1, 0.01), 0.08)),
    (1035, ((2, 0.01), 0.08)),
    (1040, ((1, 0.01), 0.09)),
    (1040, ((2, 0.01), 0.09)),
    (1045, ((1, 0.01), 0.10)),
    (1045, ((2, 0.01), 0.10))
  )

  var orderedRDD: OrderedRDD[Long, (Int, Double)] = _
  var orderedRDD1: OrderedRDD[Long, (Int, Double)] = _

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1.0e-6)
  implicit val equality = new Equality[List[Double]] {
    override def areEqual(a: List[Double], b: Any): Boolean = {
      (a, b) match {
        case (Nil, Nil) => true
        case (x :: xs, y :: ys) => x === y && areEqual(xs, ys)
        case _ => false
      }
    }
  }

  override def beforeAll() {
    super.beforeAll()
    orderedRDD = OrderedRDD.fromRDD(sc.parallelize(data, 4), KeyPartitioningType.Sorted)
    orderedRDD1 = OrderedRDD.fromRDD(sc.parallelize(data, 1), KeyPartitioningType.Sorted)
  }

  "Summarizations" should "apply correctly" in {
    val summarizer = KVSumSummarizer()
    val key = { case ((sk, v)) => None }: ((Int, Double)) => Option[Nothing]
    val ret = Summarizations(orderedRDD, summarizer, key)
    val ret1 = Summarizations(orderedRDD1, summarizer, key)

    assert(ret.collect().toList.map(_._2._2) === expected.map(_._2._2))
    assert(ret1.collect().toList.map(_._2._2) === expected.map(_._2._2))
  }

  it should "apply perSecondaryKey == true correctly" in {
    val summarizer = KVSumSummarizer()
    val key = { case ((sk, v)) => sk }: ((Int, Double)) => Int

    val ret = Summarizations(orderedRDD, summarizer, key)
    val ret1 = Summarizations(orderedRDD1, summarizer, key)

    assert(ret.collect().toList.map(_._2._2) === expectedPerSK.map(_._2._2))
    assert(ret1.collect().toList.map(_._2._2) === expectedPerSK.map(_._2._2))
  }
}
