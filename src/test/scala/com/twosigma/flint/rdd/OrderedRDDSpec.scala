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
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.RowsSummarizer
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class OrderedRDDSpec extends FlatSpec with SharedSparkContext {
  val numSlices = 4
  /**
   * There are 9 rows and they are not ordered for the first column (the primary key).
   */
  val unsortedData = Array(
    (1200L, (3, 9.98)),
    (1200L, (7, 9.99)),
    (1000L, (7, 9.90)), // Disorder the secondary key at 1000L.
    (1000L, (3, 9.91)),
    (1030L, (3, 9.92)),
    (1030L, (7, 9.93)),
    (1100L, (3, 9.94)), // Miss 7 at 1100L.
    (1130L, (3, 9.96)),
    (1130L, (7, 9.97))
  )

  /**
   * There are 9 rows and they are ordered for the first column (the primary key).
   */
  val sortedData1 = Array(
    (1000L, (3, 8.90)),
    (1000L, (9, 8.91)),
    (1005L, (3, 8.92)),
    (1005L, (9, 8.93)),
    (1030L, (3, 8.94)),
    (1030L, (7, 8.95)),
    (1105L, (3, 8.96)),
    (1430L, (3, 8.97)),
    (1430L, (7, 8.98))
  )

  val sortedData2 = Array(
    (1000L, (7, 9.90)), // Disorder the secondary key at 1000L.
    (1000L, (3, 9.91)),
    (1030L, (3, 9.92)),
    (1030L, (7, 9.93)),
    (1100L, (3, 9.94)), // Miss 7 at 1100L.
    (1130L, (3, 9.96)),
    (1130L, (7, 9.97))
  )

  val sortedData3 = Array(
    (1000L, (7, 9.90)),
    (1000L, (3, 9.91)),
    (1030L, (3, 9.92)),
    (1030L, (7, 9.93)),
    (1100L, (3, 9.94)),
    (1100L, (7, 9.95)),
    (1130L, (3, 9.96)),
    (1130L, (7, 9.97))
  )

  val sortedData4 = Array(
    (1000L, (7, 9.90)),
    (1000L, (3, 9.91)),
    (1030L, (3, 9.92)),
    (1030L, (7, 9.93)),
    (1100L, (3, 9.94)),
    (1100L, (7, 9.95)),
    (1130L, (3, 9.96)),
    (1130L, (7, 9.97))
  )

  var unsortedRDD: RDD[(Long, (Int, Double))] = _
  var sortedRDD1: RDD[(Long, (Int, Double))] = _
  var sortedRDD2: RDD[(Long, (Int, Double))] = _
  var sortedRDD3: RDD[(Long, (Int, Double))] = _

  var orderedRDD1: OrderedRDD[Long, (Int, Double)] = _
  var orderedRDD2: OrderedRDD[Long, (Int, Double)] = _

  override def beforeAll() {
    super.beforeAll()
    // Make sure that we don't have the same number of partitions.
    unsortedRDD = sc.parallelize(unsortedData, numSlices)
    sortedRDD1 = sc.parallelize(sortedData1, numSlices - 1)
    sortedRDD2 = sc.parallelize(sortedData2, numSlices)
    sortedRDD3 = sc.parallelize(sortedData2, 1)

    orderedRDD1 = OrderedRDD.fromRDD(unsortedRDD, KeyPartitioningType.UnSorted)
    orderedRDD2 = OrderedRDD.fromRDD(sortedRDD1, KeyPartitioningType.Sorted)
  }

  "The OrderedRDD" should "be constructed by `fromUnsortedRDD` correctly" in {
    // Check the length.
    assert(orderedRDD1.count() == unsortedData.length)
    // Check the ordering.
    assert(
      orderedRDD1.collect().foldLeft((Long.MinValue, true)) {
      case ((k1, isOrdered), (k2, _)) => (k2, isOrdered && k1 <= k2)
    }._2
    )
    val orderedRDD = OrderedRDD.fromRDD(sc.parallelize(unsortedData, unsortedData.length * 10), KeyPartitioningType.UnSorted)
    assert(orderedRDD.partitions.length <= unsortedData.length)
  }

  it should "be constructed by `fromSortedRDD` correctly" in {
    // Check the length.
    assert(orderedRDD1.count() == unsortedData.length)
    // Check the ordering.
    assert(
      orderedRDD1.collect().foldLeft((Long.MinValue, true)) {
      case ((k1, isOrdered), (k2, _)) => (k2, isOrdered && k1 <= k2)
    }._2
    )
    val orderedRDD = OrderedRDD.fromRDD(sc.parallelize(unsortedData, unsortedData.length * 10), KeyPartitioningType.UnSorted)
    assert(orderedRDD.partitions.length <= unsortedData.length)
  }

  it should "be able to take the 1st row" in {
    val orderedRDD = OrderedRDD.fromRDD(sortedRDD2, KeyPartitioningType.Sorted)
    assert(orderedRDD.take(1).length == 1)
  }

  it should "be able to take the 1st row from an OrderedRDD converted from " +
    "an RDD with only one partition" in {
      val orderedRDD = OrderedRDD.fromRDD(sortedRDD3, KeyPartitioningType.Sorted)
      assert(sortedRDD3.partitions.length == 1)
      assert(orderedRDD.take(1).length == 1)
    }

  it should "`zipWithIndexOrdered` correctly" in {
    val benchmark = unsortedData.sortBy(_._1)
    orderedRDD1.zipWithIndexOrdered.collect().foreach {
      case (k, (v, idx)) =>
        val i = idx.toInt
        assert(k == benchmark(i)._1)
        assert(v == benchmark(i)._2)
    }
  }

  it should "`mapValues` correctly" in {
    val benchmark = unsortedData.sortBy(_._1).map { case (k, v) => (k, Math.sqrt(v._2)) }
    val test = orderedRDD1.mapValues {
      case (k, v) => Math.sqrt(v._2)
    }.collect()
    assert(benchmark.sameElements(test))
  }

  it should "`flatMapValues` correctly" in {
    val benchmark = unsortedData.sortBy(_._1).flatMap {
      case (k, v) => Seq(0.001, 0.002, 0.003).map { x => (k, x + v._2) }
    }
    val test = orderedRDD1.flatMapValues {
      case (k, v) => Seq(0.001, 0.002, 0.003).map { _ + v._2 }
    }.collect()
    assert(benchmark.sameElements(test))
  }

  it should "`filterOrdered` correctly" in {
    val mean = unsortedData.map { case (_, (_, v)) => v }.sum / unsortedData.length
    val benchmark = unsortedData.filter { case (_, (_, v)) => v > mean }.sortBy(_._1)
    val test = orderedRDD1.filterOrdered { case (k, (_, v)) => v > mean }.collect()
    // Check there must be some rows filtered and some rows left.
    assert(benchmark.length < unsortedData.length)
    assert(benchmark.length > 0)
    assert(benchmark.sameElements(test))
  }

  it should "`collectOrdered` correctly" in {
    val pf: PartialFunction[(Long, (Int, Double)), Double] = {
      case (k: Long, (sk: Int, v: Double)) if sk == 3 => v
    }
    val pf2: PartialFunction[(Long, (Int, Double)), (Long, Double)] = {
      case (k: Long, (sk: Int, v: Double)) if sk == 3 => (k, v)
    }
    val test = orderedRDD2.collectOrdered(pf).collect()
    val benchmark = sortedData1.collect(pf2)
    assert(test.deep == benchmark.deep)
  }

  it should "`coalesce` correctly" in {
    val newLength = orderedRDD1.partitions.length / 2
    val test = orderedRDD1.coalesce(newLength)
    assert(test.partitions.length == newLength)
    assert(orderedRDD1.collect.sameElements(test.collect))
  }

  it should "`repartition` correctly" in {
    val numPartitions = 5
    val targetNumPartitions = 50
    val data = (1 to 10000) zip (1 to 10000)
    val rdd = OrderedRDD.fromRDD(
      sc.parallelize(data, numPartitions), KeyPartitioningType.Sorted
    ).repartition(targetNumPartitions)
    assert(rdd.partitions.length == targetNumPartitions)
    assertResult(data.size) { rdd.count() }
    assert(rdd.collect().sameElements(data))
  }

  it should "`++` correctly" in {
    val benchmark = (unsortedData ++ sortedData1).sortBy(_._1)
    val test1 = (orderedRDD1 ++ orderedRDD2).collect().map {
      case (k, e) => (k, e.fold(l => l, r => r))
    }
    assert(benchmark.sameElements(test1))
    val benchmark2 = (sortedData1 ++ unsortedData).sortBy { _._1 }
    val test2 = (orderedRDD2 ++ orderedRDD1).collect().map {
      case (k, e) => (k, e.fold(l => l, r => r))
    }
    assert(benchmark2.sameElements(test2))
  }

  it should "`leftJoin` correctly" in {
    val lookback = 30
    val benchmark = unsortedData.sortBy(_._1).map {
      case (k1, (sk1, v1)) =>
        (k1, ((sk1, v1), sortedData1.filter {
          case (k2, (sk2, v2)) => k2 >= k1 - lookback && k2 <= k1 && sk1 == sk2
        }.sortBy(_._1).lastOption.map { case (k2, v2) => (k2, v2) }))
    }

    val skFn = { case ((sk, _)) => sk }: ((Int, Double)) => Int
    val test = orderedRDD1.leftJoin(orderedRDD2, { _ - lookback }, skFn, skFn).collect()
    assert(benchmark.deep == test.deep)
  }

  it should "`symmetricJoin` correctly" in {
    val lookback = 30
    val skFn = { case ((sk, _)) => sk }: ((Int, Double)) => Int

    val rddLeft = orderedRDD1.leftJoin(orderedRDD2, { _ - lookback }, skFn, skFn)
    val rdd1 = rddLeft.mapValues{ case (k, (v, v2)) => (Option(k, v), v2) }
    val rddRight = orderedRDD2.leftJoin(orderedRDD1, { _ - lookback }, skFn, skFn)
    val rdd2 = rddRight.mapValues{ case (k, (v, v2)) => (v2, Option(k, v)) }
    val benchmark = rdd1.merge(rdd2).collect()

    val test = orderedRDD1.symmetricJoin(orderedRDD2, { _ - lookback }, skFn, skFn).collect()
    assert(benchmark.deep == test.deep)
  }

  it should "`groupByKey` correctly" in {
    val rdd = sc.parallelize(sortedData3, 4)
    val orderedRDD = OrderedRDD.fromRDD(rdd, KeyPartitioningType.Sorted)
    val orderedRDD2 = orderedRDD.groupByKey({ _ => None })
    val orderedRDD2Values = orderedRDD2.collect()

    val expectedResult = Array(
      (1000L, Array((7, 9.90), (3, 9.91))),
      (1030L, Array((3, 9.92), (7, 9.93))),
      (1100L, Array((3, 9.94), (7, 9.95))),
      (1130L, Array((3, 9.96), (7, 9.97)))
    )

    assert(orderedRDD2Values.length == expectedResult.length)
    orderedRDD2Values.zipWithIndex.foreach {
      case ((k, v), index) =>
        assert(k == expectedResult(index)._1)
        assert(v.deep == expectedResult(index)._2.deep)
    }
  }

  it should "`summaryWindows` correctly" in {
    val data = Array(
      (1000L, (7, 9.90)),
      (1000L, (3, 9.91)),
      (1030L, (3, 9.92)),
      (1030L, (7, 9.93)),
      (1100L, (3, 9.94)),
      (1100L, (7, 9.95)),
      (1130L, (3, 9.96)),
      (1130L, (7, 9.97))
    )

    val rdd = sc.parallelize(data, 2)
    val orderedRDD = OrderedRDD.fromRDD(rdd, KeyPartitioningType.Sorted)

    val noneSkFn: ((Int, Double)) => Any = { _ => None }
    val skFn = { case ((sk, _)) => sk }: ((Int, Double)) => Int
    val sum = RowsSummarizer[(Int, Double)]()

    val window1 = { t: Long => (t - 30, t) }
    val result1 = orderedRDD.summarizeWindows(window1, sum, skFn).collect()
    val expected1 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90)))),
      (1000L, ((3, 9.91), Vector((3, 9.91)))),
      (1030L, ((3, 9.92), Vector((3, 9.91), (3, 9.92)))),
      (1030L, ((7, 9.93), Vector((7, 9.90), (7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94)))),
      (1100L, ((7, 9.95), Vector((7, 9.95)))),
      (1130L, ((3, 9.96), Vector((3, 9.94), (3, 9.96)))),
      (1130L, ((7, 9.97), Vector((7, 9.95), (7, 9.97))))
    )
    assert(result1.deep == expected1.deep)

    val window2 = { t: Long => (t, t + 30) }
    val result2 = orderedRDD.summarizeWindows(window2, sum, skFn).collect()
    val expected2 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90), (7, 9.93)))),
      (1000L, ((3, 9.91), Vector((3, 9.91), (3, 9.92)))),
      (1030L, ((3, 9.92), Vector((3, 9.92)))),
      (1030L, ((7, 9.93), Vector((7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94), (3, 9.96)))),
      (1100L, ((7, 9.95), Vector((7, 9.95), (7, 9.97)))),
      (1130L, ((3, 9.96), Vector((3, 9.96)))),
      (1130L, ((7, 9.97), Vector((7, 9.97))))
    )
    assert(result2.deep == expected2.deep)

    val window3 = { t: Long => (t - 30, t + 30) }
    val result3 = orderedRDD.summarizeWindows(window3, sum, skFn).collect()
    val expected3 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90), (7, 9.93)))),
      (1000L, ((3, 9.91), Vector((3, 9.91), (3, 9.92)))),
      (1030L, ((3, 9.92), Vector((3, 9.91), (3, 9.92)))),
      (1030L, ((7, 9.93), Vector((7, 9.90), (7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94), (3, 9.96)))),
      (1100L, ((7, 9.95), Vector((7, 9.95), (7, 9.97)))),
      (1130L, ((3, 9.96), Vector((3, 9.94), (3, 9.96)))),
      (1130L, ((7, 9.97), Vector((7, 9.95), (7, 9.97))))
    )
    assert(result3.deep == expected3.deep)

    val window4 = { t: Long => (t - 30, t) }
    val result4 = orderedRDD.summarizeWindows(window4, sum, noneSkFn).collect()
    val expected4 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90), (3, 9.91)))),
      (1000L, ((3, 9.91), Vector((7, 9.90), (3, 9.91)))),
      (1030L, ((3, 9.92), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1030L, ((7, 9.93), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94), (7, 9.95)))),
      (1100L, ((7, 9.95), Vector((3, 9.94), (7, 9.95)))),
      (1130L, ((3, 9.96), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1130L, ((7, 9.97), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97))))
    )
    assert(result4.deep == expected4.deep)

    val window5 = { t: Long => (t, t + 30) }
    val result5 = orderedRDD.summarizeWindows(window5, sum, noneSkFn).collect()
    val expected5 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1000L, ((3, 9.91), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1030L, ((3, 9.92), Vector((3, 9.92), (7, 9.93)))),
      (1030L, ((7, 9.93), Vector((3, 9.92), (7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1100L, ((7, 9.95), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1130L, ((3, 9.96), Vector((3, 9.96), (7, 9.97)))),
      (1130L, ((7, 9.97), Vector((3, 9.96), (7, 9.97))))
    )
    assert(result5.deep == expected5.deep)

    val window6 = { t: Long => (t - 30, t + 30) }
    val result6 = orderedRDD.summarizeWindows(window6, sum, noneSkFn).collect()
    val expected6 = Array(
      (1000L, ((7, 9.90), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1000L, ((3, 9.91), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1030L, ((3, 9.92), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1030L, ((7, 9.93), Vector((7, 9.90), (3, 9.91), (3, 9.92), (7, 9.93)))),
      (1100L, ((3, 9.94), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1100L, ((7, 9.95), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1130L, ((3, 9.96), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97)))),
      (1130L, ((7, 9.97), Vector((3, 9.94), (7, 9.95), (3, 9.96), (7, 9.97))))
    )
    assert(result6.deep == expected6.deep)
  }

  it should "`shift correctly`" in {
    val fn = { x: Long => x + 1000L }
    val result = orderedRDD2.shift(fn)
    val expected = orderedRDD2.collect().map{ case (t, v) => (fn(t), v) }
    assert(result.collect().deep == expected.deep)

    val fn1 = { x: Long => x - 1000L }
    val result1 = orderedRDD2.shift(fn1)
    val expected1 = orderedRDD2.collect().map{ case (t, v) => (fn1(t), v) }
    assert(result1.collect().deep == expected1.deep)
  }
}
