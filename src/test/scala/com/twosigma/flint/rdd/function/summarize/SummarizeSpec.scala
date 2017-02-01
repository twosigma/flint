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

package com.twosigma.flint.rdd.function.summarize

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ SumSummarizer => SumSum }
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.LeftSubtractableSummarizer
import org.scalatest.FlatSpec
import org.scalactic.{ TolerantNumerics, Equality }
import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }

case class KVSumSummarizer() extends LeftSubtractableSummarizer[(Int, Double), Double, Double] {
  val sum = SumSum[Double]()

  def toT(input: (Int, Double)): Double = input._2

  override def zero(): Double = sum.zero()
  override def add(u: Double, t: (Int, Double)): Double = sum.add(u, toT(t))
  override def subtract(u: Double, t: (Int, Double)): Double = sum.subtract(u, toT(t))
  override def merge(u1: Double, u2: Double): Double = sum.merge(u1, u2)
  override def render(u: Double): Double = sum.render(u)
}

class SummarizeSpec extends FlatSpec with SharedSparkContext {

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

  val summarizer = new KVSumSummarizer()

  var orderedRDD: OrderedRDD[Long, (Int, Double)] = _

  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1.0e-6)

  override def beforeAll() {
    super.beforeAll()
    orderedRDD = OrderedRDD.fromRDD(sc.parallelize(data, 4), KeyPartitioningType.Sorted)
  }

  "Summarize" should "apply correctly" in {
    val skFn = { case ((sk, v)) => None }: ((Int, Double)) => Option[Nothing]
    val ret = Summarize(orderedRDD, summarizer, skFn)
    assert(ret.size == 1)
    assert(ret.head._2 === data.length * 0.01)
  }

  it should "apply with sk correctly" in {
    val skFn = { case ((sk, v)) => sk }: ((Int, Double)) => Int
    val ret = Summarize(orderedRDD, summarizer, skFn)
    assert(ret.size == 2)
    assert(ret.head._2 === 0.1)
  }
}
