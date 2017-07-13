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

package com.twosigma.flint.rdd

import java.util.Random
import java.util.concurrent.LinkedBlockingDeque

import com.twosigma.flint.SharedSparkContext
import org.apache.spark.NarrowDependency
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.{ Seconds, Span }

import scala.concurrent.Future

class ConversionSpec extends FlatSpec with SharedSparkContext with Timeouts {

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

  def genRandomSortedRdd(numRows: Int, max: Int, numPartitions: Int): RDD[(Long, Long)] = {
    val rand = new Random()

    val times = (0 until numRows).map{ _ => rand.nextInt(max).toLong }.sorted
    val rdd = sc.parallelize(times, numPartitions).map{ t => (t, t) }
    rdd
  }

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

  it should "convert RDD with deps and ranges to OrderedRDD" in {
    val rddA = genRandomSortedRdd(1234567, 100, 123)
    val orddA = OrderedRDD.fromRDD(rddA, KeyPartitioningType.Sorted)

    val depsA = orddA.deps
    val rangeSplitsA = orddA.rangeSplits

    val rddC = rddA.filter(_._1 % 2 == 0)

    val orddC1 = Conversion.fromRDD(rddC, depsA, rangeSplitsA)
    val orddC2 = OrderedRDD.fromRDD(rddC, KeyPartitioningType.Sorted)
    assert(orddC1.collect().deep == orddC2.collect().deep)
    assert(orddC1.rangeSplits.size == orddA.rangeSplits.size)

    val depA = orddA.deps.head.asInstanceOf[NarrowDependency[_]]
    val depC = orddC1.deps.head.asInstanceOf[NarrowDependency[_]]

    (orddC1.rangeSplits zip orddA.rangeSplits).foreach {
      case (splitC, splitA) =>
        assert(
          splitC.range.equals(splitA.range),
          s"Ranges are not the same. Range1: ${splitC.range} Range2: ${splitA.range}"
        )
        assert(
          splitC.partition.index == splitA.partition.index,
          s"Partition indices is not the same. index1: ${splitC.partition.index} index2: ${splitA.partition.index}"
        )
        val parentsC = depC.getParents(splitC.partition.index)

        val parentsA = depA.getParents(splitA.partition.index)
        assert(parentsC == parentsA, s"Dependencies are not the same. Parents1: $parentsC, Parent2: $parentsA")
    }
  }

  import scala.util.{ Success, Failure }

  it should "be able to be interrupted correctly" in {
    import scala.concurrent.ExecutionContext.Implicits.global
    val parallelism = sc.defaultParallelism
    val rdd = sc.parallelize(1 to 100 * parallelism, 10 * parallelism).map {
      case i => (i.toLong, 1)
    }

    val elapse = 3

    val slowOrderedRDD = OrderedRDD.fromRDD(rdd, KeyPartitioningType.Sorted).mapValues {
      case kv =>
        Thread.sleep(elapse * 1000L)
        kv
    }

    val allowCancellation = new LinkedBlockingDeque[Long](1)

    val future = Future {
      allowCancellation.push(1L)
      slowOrderedRDD.count()
    }

    allowCancellation.take()
    // Make sure the jobs are actually started.
    while (sc.statusTracker.getActiveStageIds().isEmpty) {
      Thread.sleep(100)
    }
    val stageIds = sc.statusTracker.getActiveStageIds()
    assert(stageIds.length == 1)
    val stageId = stageIds(0)

    // Make sure there are some active tasks.
    while (sc.statusTracker.getStageInfo(stageId).get.numActiveTasks() < 1) {
      Thread.sleep(100)
    }

    // Cancel the slow computation.
    sc.cancelAllJobs()
    failAfter(Span(2 * elapse, Seconds)) {
      while (sc.statusTracker.getStageInfo(stageId).get.numActiveTasks() > 0) {
        Thread.sleep(500)
      }
    }

    future onComplete {
      case Success(_) =>
        assert(false, "Should not completed as the job has been killed.")
      case Failure(_) =>
    }
  }

  "fromSortedRDD" should "sort partitions, and have partition indexes increasing" in {

    // Create an RDD with data data sorted within partitions, but partitions not sorted
    // Data is:
    // Partition 0: 100, 130, 160, 199
    // Partition 1:   0,  30,  60,  99
    val data = Seq(100, 130, 160, 199, 0, 30, 60, 99)
    val kvData = data.zip(data)
    val rdd = sc.makeRDD(kvData, 2)

    val orderedRdd = Conversion.fromSortedRDD(rdd)
    assert(orderedRdd.partitions(0).index == 0)
    assert(orderedRdd.partitions(1).index == 1)

    assert(orderedRdd.collect().toSeq == kvData.sorted)
  }
}
