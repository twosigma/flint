/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.rdd._
import com.twosigma.flint.timeseries.time.types.TimeType
import org.apache.spark.NarrowDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.prop.PropertyChecks

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap
import scala.concurrent.duration.NANOSECONDS

private[flint] sealed trait PartitionStrategy {

  def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD

  /**
   * Ensure a TimeSeriesRDD is valid. This checks:
   * (1) data within each partition is within partition range
   * (2) data is ordered within each partition
   * (3) ranges are ordered and non-overlapping
   */
  private def ensureValid(rdd: TimeSeriesRDD): TimeSeriesRDD = {
    rdd.validate()
    rdd
  }

  def repartitionEnsureValid(rdd: TimeSeriesRDD): TimeSeriesRDD = ensureValid(repartition(rdd))
  def ::(other: PartitionStrategy): PartitionStrategy = {
    val self = this
    new PartitionStrategy {
      override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = self.repartition(other.repartition(rdd))

      override def toString() = {
        s"${other.toString} :: ${self.toString}"
      }
    }
  }
}

private[flint] object PartitionStrategy {
  case object Origin extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = rdd
  }

  /**
   * Partition a TimeSeriesRDD into one partition with range CloseOpen(firstT, None)
   * where firstT is the first timestamp of the data
   */
  case object OnePartition extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      rdd.repartition(1)
    }
  }

  /**
   * One timestamp per partition with the range [t, t + 1000) (if timestamp type) or [t, t + 1) (if long type)
   * for each partition (t is the timestamp of the rows in the partition)
   */
  case object OneTimestampTightBound extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {

      val timeType = TimeType.get(rdd.sparkSession)

      val expandEnd = timeType match {
        case TimeType.LongType => (t: Long) => t + 1
        case TimeType.TimestampType => (t: Long) => t + 1000
      }

      val timeIndex = rdd.schema.fieldIndex("time")
      val rowGroupMap = TreeMap(rdd.toDF.queryExecution.executedPlan.executeCollect().groupBy{
        r => timeType.internalToNanos(r.getLong(timeIndex))
      }.toArray: _*)
      val rowGroupArray = rowGroupMap.values.toArray

      val rangeSplits = rowGroupMap.zipWithIndex.map {
        case ((time: Long, rows), index) => RangeSplit(
          OrderedRDDPartition(index), CloseOpen(time, Some(expandEnd(time)))
        )
      }.toSeq

      val orderedRdd = new OrderedRDD[Long, InternalRow](rdd.orderedRdd.sparkContext, rangeSplits, Nil)(
        (part, context) => rowGroupArray(part.index).map{ row =>
          (timeType.internalToNanos(row.getLong(timeIndex)), row)
        }.toIterator
      )

      new TimeSeriesRDDImpl(TimeSeriesStore(orderedRdd, rdd.schema))
    }
  }

  def withRangeChange(partInfo: Option[PartitionInfo])(
    fn: (Int, CloseOpen[Long]) => CloseOpen[Long]
  ): Option[PartitionInfo] = {
    partInfo.map {
      case PartitionInfo(splits, deps) =>
        val newSplits = splits.map{
          case RangeSplit(partition, range) =>
            RangeSplit(OrderedRDDPartition(partition.index), fn(partition.index, range))
        }
        PartitionInfo(newSplits, deps)
    }
  }

  type NeighbourRange = Seq[CloseOpen[Long]]
  def getIndexToNeighbourRanges(rdd: TimeSeriesRDD): SortedMap[Int, NeighbourRange] = {
    val inputORdd = rdd.orderedRdd
    val inputRanges = inputORdd.rangeSplits.map(_.range).toSeq
    val inputRangesWithNeighourRange = (null +: inputRanges :+ null).sliding(3, 1).toList
    SortedMap[Int, NeighbourRange]() ++
      (inputORdd.rangeSplits.map(_.partition.index) zip inputRangesWithNeighourRange).toMap
  }

  /**
   * Extend the start of each partition range to the end of the previous range. For example:
   * Before:
   * [0, 1000) [2000, 3000) [3000, 4000)
   * After:
   * [Long.MinValue, 1000) [1000, 3000) [3000, 4000)
   */
  case object ExtendBegin extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      val indexToNeighbourRanges = getIndexToNeighbourRanges(rdd)

      val newPartInfo = withRangeChange(rdd.partInfo) {
        case (index, range) =>
          val neighourRanges = indexToNeighbourRanges(index)
          neighourRanges match {
            case Seq(null, current, _) =>
              CloseOpen(Long.MinValue, current.end)
            case Seq(previous, current, _) =>
              CloseOpen(previous.end.get, current.end)
          }
      }

      TimeSeriesRDD.fromDFWithPartInfo(rdd.toDF, newPartInfo)
    }
  }

  /**
   * Extend the end of each partition range to the begin of the next range. For example:
   * Before:
   * [0, 1000) [2000, 3000) [3000, 4000)
   * After:
   * [0, 2000) [2000, 3000) [3000, None)
   */
  case object ExtendEnd extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      val indexToNeighbourRanges = getIndexToNeighbourRanges(rdd)

      val newPartInfo = withRangeChange(rdd.partInfo) {
        case (index, range) =>
          val neighourRanges = indexToNeighbourRanges(index)
          neighourRanges match {
            case Seq(previous, current, null) =>
              CloseOpen(current.begin, None)
            case Seq(previous, current, next) =>
              CloseOpen(current.begin, Some(next.begin))
          }
      }

      TimeSeriesRDD.fromDFWithPartInfo(rdd.toDF, newPartInfo)
    }
  }

  /**
   * Extend the begin, end of each partition by half the distance to the neighbor range. For example:
   * Before:
   * [0, 1000) [2000, 3000) [3000, 4000)
   * After:
   * [Long.MinValue, 1500) [1500, 3000) [3000, None)
   */
  case object ExtendHalfBeginHalfEnd extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      val indexToNeighbourRanges = getIndexToNeighbourRanges(rdd)

      def middle(range1: CloseOpen[Long], range2: CloseOpen[Long]): Long = {
        (range1.end.get + range2.begin) / 2
      }

      val newPartInfo = withRangeChange(rdd.partInfo) {
        case (index, range) =>
          val neighbourRanges = indexToNeighbourRanges(index)

          neighbourRanges match {
            case Seq(null, current, null) =>
              CloseOpen(Long.MinValue, None)
            case Seq(null, current, next) =>
              CloseOpen(Long.MinValue, Some(middle(current, next)))
            case Seq(previous, current, null) =>
              CloseOpen(middle(previous, current), None)
            case Seq(previous, current, next) =>
              CloseOpen(middle(previous, current), Some(middle(current, next)))
          }
      }

      TimeSeriesRDD.fromDFWithPartInfo(rdd.toDF, newPartInfo)
    }
  }

  /**
   * Repartition into n partitions where each partition has roughly the same amount of cycles.
   */
  case object MultiTimestampNormalized extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {

      val timeType = TimeType.get(rdd.sparkSession)
      val expandEnd = timeType match {
        case TimeType.LongType => (t: Long) => t + 1
        case TimeType.TimestampType => (t: Long) => t + 1000
      }

      val timeIndex = rdd.schema.fieldIndex("time")
      val rows = rdd.toDF.queryExecution.sparkPlan.executeCollect()

      val groupedRows = SortedMap[Long, Array[InternalRow]]() ++
        rows.groupBy(row => timeType.internalToNanos(row.getLong(timeIndex)))

      val count = groupedRows.size
      val targetPartitionNumber = math.sqrt(count).toLong
      val timestampsPerPartition = count / targetPartitionNumber

      val partitionedRows = groupedRows.grouped(timestampsPerPartition.toInt).toArray

      val rangeSplits = partitionedRows.zipWithIndex.map{
        case (timestampToRows, index) =>
          val begin = timestampToRows.keys.min
          val end = expandEnd(timestampToRows.keys.max)
          RangeSplit(
            OrderedRDDPartition(index),
            CloseOpen(begin, Some(end))
          )
      }.toSeq

      val orderedRdd = new OrderedRDD[Long, InternalRow](rdd.orderedRdd.sparkContext, rangeSplits, Nil)(
        (part, context) => partitionedRows(part.index).toArray.flatMap{
          case (time, rs) =>
            rs.map((time, _))
        }.toIterator
      )

      new TimeSeriesRDDImpl(TimeSeriesStore(orderedRdd, rdd.schema))
    }
  }

  /**
   * Repartition into n partitions where each partition has roughly the same amount of rows, then run a normalization.
   */
  case object MultiTimestampUnnormailzed extends PartitionStrategy {
    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      val rows = rdd.collect()

      val count = rows.length
      val targetPartitionNumber = math.sqrt(count).toLong
      val rowsPerPartition = count / targetPartitionNumber

      val partitionedRows = rows.grouped(rowsPerPartition.toInt).toArray

      val sc = rdd.rdd.sparkContext

      val newRdd = sc.parallelize(partitionedRows, partitionedRows.length).flatMap(_.toIterator)
      require(newRdd.getNumPartitions == partitionedRows.length)
      TimeSeriesRDD.fromRDD(newRdd, rdd.schema)(isSorted = true, timeUnit = NANOSECONDS)
    }
  }

  /**
   * Fill gaps in current partition with empty ranges. For example:
   * Before:
   * [0, 1000) [2000, 3000) [3000, 4000)
   * After:
   * [Long.MinValue, 0) [0, 1000) [1000, 2000) [2000, 3000) [3000, 4000)
   */
  case object FillWithEmptyPartition extends PartitionStrategy {
    private def fillRangeGap(ranges: Array[RangeSplit[Long]]): Array[(CloseOpen[Long], Option[Int])] = {
      ((null +: ranges) zip (ranges :+ null)).flatMap {
        case (null, RangeSplit(OrderedRDDPartition(_), right)) =>
          if (right.begin == Long.MinValue) {
            Iterator.empty
          } else {
            Iterator((CloseOpen(Long.MinValue, Some(right.begin)), None))
          }
        case (RangeSplit(OrderedRDDPartition(index), left), null) =>
          if (left.end.isEmpty) {
            Iterator((left, Some(index)))
          } else {
            Iterator((left, Some(index)), (CloseOpen(left.end.get, None), None))
          }
        case (RangeSplit(OrderedRDDPartition(leftIndex), left), RangeSplit(_, right)) =>
          val leftEnd = left.end.get
          val rightBegin = right.begin

          if (leftEnd == rightBegin) {
            Iterator((left, Some(leftIndex)))
          } else {
            Iterator((left, Some(leftIndex)), (CloseOpen(leftEnd, Some(right.begin)), None))
          }
      }
    }

    override def repartition(rdd: TimeSeriesRDD): TimeSeriesRDD = {
      val parentRdd = rdd.orderedRdd

      val (ranges, parentIndices) = fillRangeGap(
        parentRdd.rangeSplits
      ).unzip

      val rangeSplits = ranges.zipWithIndex.map {
        case (range, index) =>
          RangeSplit(OrderedRDDPartition(index), range)
      }

      val deps = new NarrowDependency(parentRdd) {
        override def getParents(partitionId: Int): Seq[Int] = {
          parentIndices(partitionId).map(Seq(_)).getOrElse(Seq.empty)
        }
      }
      val parentPartitions = parentRdd.partitions

      val orderedRdd = new OrderedRDD[Long, InternalRow](parentRdd.sparkContext, rangeSplits, Seq(deps))({
        (part, context) =>
          deps.getParents(part.index) match {
            case Seq() => Iterator.empty
            case Seq(parentIndex) => parentRdd.iterator(parentPartitions(parentIndex), context)
          }
      })

      new TimeSeriesRDDImpl(TimeSeriesStore(orderedRdd, rdd.schema))
    }
  }
}

class MultiPartitionSuite extends TimeSeriesSuite with PropertyChecks {
  import PartitionStrategy._
  val NONE = Seq(Origin)
  val ALL = Seq(
    Origin,
    OnePartition,
    OneTimestampTightBound,
    OneTimestampTightBound :: ExtendBegin,
    OneTimestampTightBound :: ExtendEnd,
    OneTimestampTightBound :: ExtendHalfBeginHalfEnd,
    OneTimestampTightBound :: FillWithEmptyPartition,
    MultiTimestampNormalized,
    MultiTimestampNormalized :: ExtendBegin,
    MultiTimestampNormalized :: ExtendEnd,
    MultiTimestampNormalized :: ExtendHalfBeginHalfEnd,
    MultiTimestampNormalized :: FillWithEmptyPartition,
    MultiTimestampUnnormailzed,
    MultiTimestampUnnormailzed :: ExtendBegin,
    MultiTimestampUnnormailzed :: ExtendEnd,
    MultiTimestampUnnormailzed :: ExtendHalfBeginHalfEnd,
    MultiTimestampUnnormailzed :: FillWithEmptyPartition
  )

  val DEFAULT = ALL

  def withPartitionStrategy(rdd: TimeSeriesRDD)(strategies: Seq[PartitionStrategy])(
    test: TimeSeriesRDD => Unit
  ): Unit = {
    for (s <- strategies) {
      // println(s"PartitionStrategy: ${s}")
      // println(s"Range: ${s.repartition(rdd).partInfo.get.splits}")
      test(s.repartitionEnsureValid(rdd))
    }
  }

  def withPartitionStrategy(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD)(strategies: Seq[PartitionStrategy])(
    test: (TimeSeriesRDD, TimeSeriesRDD) => Unit
  ): Unit = {
    for (s1 <- strategies; s2 <- strategies) {
      // info(s"PartitionStrategy: ${s1} ${s2}")
      // info(s"Range1: ${s1.partition(rdd1).partInfo.get.splits}")
      // info(s"Range2: ${s2.partition(rdd2).partInfo.get.splits}")
      test(s1.repartition(rdd1), s2.repartition(rdd2))
    }
  }

  def withPartitionStrategyCompare(rdd: TimeSeriesRDD)(strategies: Seq[PartitionStrategy])(
    fn: (TimeSeriesRDD) => TimeSeriesRDD
  ): Unit = {
    val baseline = fn(OnePartition.repartition(rdd))
    val strategiesWithoutOnePartition = strategies.filter(_ != OnePartition)
    for (s <- strategiesWithoutOnePartition) {
      val result = fn(s.repartition(rdd))
      assertEquals(result, baseline)
    }
  }

  /**
   * Run tests with different params and partition strategies. Compare the result to OnePartition strategy.
   * This creates one test for each param/partition combo, so it's easier to find which param/partition fails the test.
   * However, this removes type information in the test function.
   */
  def withPartitionStrategyAndParams(
    input: () => TimeSeriesRDD
  )(
    name: String
  )(
    strategies: Seq[PartitionStrategy]
  )(
    params: Seq[Seq[Any]]
  )(
    fn: (TimeSeriesRDD, Seq[Any]) => TimeSeriesRDD
  ): Unit = {
    val strategiesWithoutOnePartition = strategies.filter(_ != OnePartition)
    val strategiesTable = Table("strategies", strategiesWithoutOnePartition: _*)
    val paramsTable = Table("params", params: _*)
    forAll(paramsTable) { params =>
      forAll(strategiesTable) { strategy =>
        it should s"pass with strategy = $strategy and param = $params" in {
          val rdd = input()
          val baseline = fn(OnePartition.repartition(rdd), params)
          val result = fn(strategy.repartition(rdd), params)
          assertEquals(result, baseline)
        }
      }
    }
  }

  def withPartitionStrategyAndParams(
    input1: () => TimeSeriesRDD,
    input2: () => TimeSeriesRDD
  )(
    name: String
  )(
    strategies1: Seq[PartitionStrategy],
    strategies2: Seq[PartitionStrategy]
  )(
    params: Seq[Seq[Any]]
  )(
    test: (TimeSeriesRDD, TimeSeriesRDD, Seq[Any]) => Unit
  ): Unit = {
    val strategies1Table = Table("strategies", strategies1: _*)
    val strategies2Table = Table("strategies", strategies2: _*)
    val paramsTable = Table("params", params: _*)
    forAll(paramsTable) { params =>
      forAll(strategies1Table) { strategy1 =>
        forAll(strategies2Table) { strategy2 =>
          it should s"pass $name with strategy1 = $strategy1 and strategy2 = $strategy2 and param = $params" in {
            val rdd1 = input1()
            val rdd2 = input2()
            test(strategy1.repartition(rdd1), strategy2.repartition(rdd2), params)
          }
        }
      }
    }
  }

  def withPartitionStrategyCompare(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD)(strategies: Seq[PartitionStrategy])(
    fn: (TimeSeriesRDD, TimeSeriesRDD) => TimeSeriesRDD
  ): Unit = {
    val baseline = fn(OnePartition.repartition(rdd1), OnePartition.repartition(rdd2))
    val strategiesWithoutOnePartition = strategies.filter(_ != OnePartition)
    for (s1 <- strategiesWithoutOnePartition; s2 <- strategiesWithoutOnePartition) {
      val result = fn(s1.repartitionEnsureValid(rdd1), s2.repartitionEnsureValid(rdd2))
      assertEquals(result, baseline)
    }
  }
}
