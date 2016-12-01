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

package com.twosigma.flint.timeseries

import java.util.concurrent.TimeUnit

import com.twosigma.flint.timeseries.row.Schema
import org.scalatest.FlatSpec
import org.scalatest.tagobjects.Slow
import com.twosigma.flint.{ SharedSparkContext, SpecUtils }
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }
import org.apache.spark.sql.functions.{ col, udf }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericRowWithSchema => ExternalRow }
import org.scalactic.TolerantNumerics

import scala.collection.mutable
import scala.concurrent.duration._

class TimeSeriesRDDSpec extends FlatSpec with SharedSparkContext {
  val additivePrecision: Double = 1.0e-8
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(additivePrecision)

  val priceSchema = Schema("time" -> LongType, "tid" -> IntegerType, "price" -> DoubleType)
  val forecastSchema = Schema("time" -> LongType, "tid" -> IntegerType, "forecast" -> DoubleType)
  val forecastSwitchColumnSchema = Schema("time" -> LongType, "forecast" -> DoubleType, "tid" -> IntegerType)
  val volSchema = Schema("time" -> LongType, "tid" -> IntegerType, "volume" -> LongType)
  val clockSchema = Schema("time" -> LongType)

  val clockData = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L), clockSchema)),
    (1100L, new ExternalRow(Array(1100L), clockSchema)),
    (1200L, new ExternalRow(Array(1200L), clockSchema)),
    (1300L, new ExternalRow(Array(1300L), clockSchema))
  )

  val forecastData = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L, 7, 3.0), forecastSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 5.0), forecastSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, -1.5), forecastSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 2.0), forecastSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, -2.4), forecastSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 6.4), forecastSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 1.5), forecastSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, -7.9), forecastSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 4.6), forecastSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1.4), forecastSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, -9.6), forecastSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 6.0), forecastSchema))
  )

  val forecastSwitchColumnData = forecastData.map {
    case (time, row) =>
      val data = row.toSeq.toArray
      val newData = Array(data(0), data(2), data(1))

      (time, new ExternalRow(newData, forecastSwitchColumnSchema): Row)
  }

  val priceData = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L, 7, 0.5), priceSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 1.0), priceSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 1.5), priceSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 2.0), priceSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 2.5), priceSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 3.0), priceSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 3.5), priceSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 4.0), priceSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 4.5), priceSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 5.0), priceSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 5.5), priceSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 6.0), priceSchema))
  )

  val volData = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L, 7, 100L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 200L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 300L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 400L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 500L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 600L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 700L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 800L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 900L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1000L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 1100L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 1200L), volSchema))
  )

  val vol2Data = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L, 7, 100L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 7, 100L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 200L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 200L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 300L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 300L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 400L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 400L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 500L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 600L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 500L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 600L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 700L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 800L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 700L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 800L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 900L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1000L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 900L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1000L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 1100L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 1200L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 1100L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 1200L), volSchema))
  )

  val vol3Data = Array[(Long, Row)](
    (1000L, new ExternalRow(Array(1000L, 7, 100L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 7, 101L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 200L), volSchema)),
    (1000L, new ExternalRow(Array(1000L, 3, 201L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 300L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 3, 301L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 400L), volSchema)),
    (1050L, new ExternalRow(Array(1050L, 7, 401L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 500L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 600L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 3, 501L), volSchema)),
    (1100L, new ExternalRow(Array(1100L, 7, 601L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 700L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 800L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 3, 701L), volSchema)),
    (1150L, new ExternalRow(Array(1150L, 7, 801L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 900L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1000L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 3, 901L), volSchema)),
    (1200L, new ExternalRow(Array(1200L, 7, 1001L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 1100L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 1200L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 3, 1101L), volSchema)),
    (1250L, new ExternalRow(Array(1250L, 7, 1201L), volSchema))
  )

  val defaultNumPartitions = 5

  var priceTSRdd: TimeSeriesRDD = _
  var forecastTSRdd: TimeSeriesRDD = _
  var forecastSwitchColumnTSRdd: TimeSeriesRDD = _
  var volTSRdd: TimeSeriesRDD = _
  var clockTSRdd: TimeSeriesRDD = _
  var vol2TSRdd: TimeSeriesRDD = _
  var vol3TSRdd: TimeSeriesRDD = _

  override def beforeAll() {
    super.beforeAll()
    priceTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(priceData, defaultNumPartitions), KeyPartitioningType.Sorted),
      priceSchema
    )
    forecastTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(forecastData, defaultNumPartitions), KeyPartitioningType.Sorted),
      forecastSchema
    )
    forecastSwitchColumnTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(forecastSwitchColumnData, defaultNumPartitions), KeyPartitioningType.Sorted),
      forecastSwitchColumnSchema
    )
    volTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(volData, defaultNumPartitions), KeyPartitioningType.Sorted),
      volSchema
    )
    clockTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(clockData, defaultNumPartitions), KeyPartitioningType.Sorted),
      clockSchema
    )
    vol2TSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(vol2Data, defaultNumPartitions), KeyPartitioningType.Sorted),
      volSchema
    )
    vol3TSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(vol3Data, defaultNumPartitions), KeyPartitioningType.Sorted),
      volSchema
    )
  }

  "TimeSeriesRDD" should "`select data between correctly`" in {
    val df = priceTSRdd.toDF
    val df2 = df.filter(df("time") >= 1000L && df("time") < 1100L)
    val df3 = TimeSeriesRDD.DFBetween(df, "19700101 00:16:40", "19700101 00:18:20", timeUnit = SECONDS)

    assert(df2.collect().deep == df3.collect().deep)
  }

  "TimeSeriesRDD" should "`addColumns` correctly" in {
    val expectedSchema = Schema(
      "tid" -> IntegerType, "price" -> DoubleType, "price2" -> DoubleType, "price3" -> DoubleType
    )
    val expectedData = Array[Row](
      new ExternalRow(Array(1000L, 7, 0.5, 0.5, 1.0), expectedSchema),
      new ExternalRow(Array(1000L, 3, 1.0, 1.0, 2.0), expectedSchema),
      new ExternalRow(Array(1050L, 3, 1.5, 1.5, 3.0), expectedSchema),
      new ExternalRow(Array(1050L, 7, 2.0, 2.0, 4.0), expectedSchema),
      new ExternalRow(Array(1100L, 3, 2.5, 2.5, 5.0), expectedSchema),
      new ExternalRow(Array(1100L, 7, 3.0, 3.0, 6.0), expectedSchema),
      new ExternalRow(Array(1150L, 3, 3.5, 3.5, 7.0), expectedSchema),
      new ExternalRow(Array(1150L, 7, 4.0, 4.0, 8.0), expectedSchema),
      new ExternalRow(Array(1200L, 3, 4.5, 4.5, 9.0), expectedSchema),
      new ExternalRow(Array(1200L, 7, 5.0, 5.0, 10.0), expectedSchema),
      new ExternalRow(Array(1250L, 3, 5.5, 5.5, 11.0), expectedSchema),
      new ExternalRow(Array(1250L, 7, 6.0, 6.0, 12.0), expectedSchema)
    )

    val expectedSchema2 = Schema(
      "tid" -> IntegerType, "price" -> DoubleType, "price2" -> DoubleType
    )

    val expectedData2 = Array[Row](
      new ExternalRow(Array(1000L, 14, 1.0, 0.5), expectedSchema2),
      new ExternalRow(Array(1000L, 6, 2.0, 1.0), expectedSchema2),
      new ExternalRow(Array(1050L, 6, 3.0, 1.5), expectedSchema2),
      new ExternalRow(Array(1050L, 14, 4.0, 2.0), expectedSchema2),
      new ExternalRow(Array(1100L, 6, 5.0, 2.5), expectedSchema2),
      new ExternalRow(Array(1100L, 14, 6.0, 3.0), expectedSchema2),
      new ExternalRow(Array(1150L, 6, 7.0, 3.5), expectedSchema2),
      new ExternalRow(Array(1150L, 14, 8.0, 4.0), expectedSchema2),
      new ExternalRow(Array(1200L, 6, 9.0, 4.5), expectedSchema2),
      new ExternalRow(Array(1200L, 14, 10.0, 5.0), expectedSchema2),
      new ExternalRow(Array(1250L, 6, 11.0, 5.5), expectedSchema2),
      new ExternalRow(Array(1250L, 14, 12.0, 6.0), expectedSchema2)
    )

    val result = priceTSRdd.addColumns(
      "price2" -> DoubleType ->
        { r: Row => r.getAs[Double]("price") },

      "price3" -> DoubleType ->
        { r: Row => r.getAs[Double]("price") * 2 }
    ).collect()

    val result2 = priceTSRdd.addColumns(
      "price" -> DoubleType ->
        { r: Row => r.getAs[Double]("price") * 2 },

      "price2" -> DoubleType ->
        { r: Row => r.getAs[Double]("price") },

      "tid" -> IntegerType ->
        { r: Row => r.getAs[Integer]("tid") * 2 }
    ).collect()

    assert(result.deep == expectedData.deep)
    assert(result2.deep == expectedData2.deep)
  }

  it should "`toDF` correctly" in {
    val df = priceTSRdd.toDF
    assert(df.schema == priceTSRdd.schema)
    assert(df.collect().deep == priceTSRdd.collect().deep)
  }

  it should "`repartition` correctly" in {
    assert(vol2TSRdd.repartition(1).collect().deep == vol2TSRdd.collect().deep)
    assert(vol2TSRdd.repartition(defaultNumPartitions * 2).collect().deep == vol2TSRdd.collect().deep)
  }

  it should "`coalesce` correctly" in {
    assert(vol2TSRdd.coalesce(1).collect().deep == vol2TSRdd.collect().deep)
    assert(vol2TSRdd.coalesce(defaultNumPartitions / 2).collect().deep == vol2TSRdd.collect().deep)
  }

  it should "`groupByCycle` correctly" in {
    val innerRowSchema = Schema("tid" -> IntegerType, "volume" -> LongType)
    val expectedSchema = Schema("rows" -> ArrayType(innerRowSchema))

    val rows = volData.map(_._2)

    val expectedData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, Array(rows(0), rows(1))), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, Array(rows(2), rows(3))), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, Array(rows(4), rows(5))), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, Array(rows(6), rows(7))), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, Array(rows(8), rows(9))), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, Array(rows(10), rows(11))), expectedSchema))
    )

    val result = volTSRdd.groupByCycle().collect()
    val expectedResult = expectedData.map(_._2)

    expectedResult.indices.foreach {
      index =>
        assert(result(index).getAs[mutable.WrappedArray[Row]]("rows").deep ==
          expectedResult(index).getAs[Array[Row]]("rows").deep)
    }
    assert(result.map(_.schema).deep == expectedResult.map(_.schema).deep)
  }

  it should "`addSummaryColumns` correctly" in {
    val expectedSchema = Schema("tid" -> IntegerType, "volume" -> LongType, "volume_sum" -> DoubleType)
    val expectedData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, 7, 100, 100.0), expectedSchema)),
      (1000L, new ExternalRow(Array(1000L, 3, 200, 300.0), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 3, 300, 600.0), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 7, 400, 1000.0), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 3, 500, 1500.0), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 7, 600, 2100.0), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 3, 700, 2800.0), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 7, 800, 3600.0), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 3, 900, 4500.0), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 7, 1000, 5500.0), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 3, 1100, 6600.0), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 7, 1200, 7800.0), expectedSchema))
    )

    val expectedData2 = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, 7, 100, 100.0), expectedSchema)),
      (1000L, new ExternalRow(Array(1000L, 3, 200, 200.0), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 3, 300, 500.0), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 7, 400, 500.0), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 3, 500, 1000.0), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 7, 600, 1100.0), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 3, 700, 1700.0), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 7, 800, 1900.0), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 3, 900, 2600.0), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 7, 1000, 2900.0), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 3, 1100, 3700.0), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 7, 1200, 4100.0), expectedSchema))
    )

    val result1 = volTSRdd.addSummaryColumns(Summarizers.sum("volume"))
    assert(result1.collect().deep == expectedData.map(_._2).deep)

    val result2 = volTSRdd.addSummaryColumns(Summarizers.sum("volume"), Seq("tid"))
    assert(result2.collect().deep == expectedData2.map(_._2).deep)
  }

  it should "`addWindows` correctly" in {

    val windowLength = "50ns"
    val windowColumnName = s"window_past_$windowLength"

    val innerRowSchema = Schema("tid" -> IntegerType, "volume" -> LongType)
    val expectedSchema = Schema(
      "tid" -> IntegerType, "volume" -> LongType, windowColumnName -> ArrayType(innerRowSchema)
    )

    val rows = volData.map(_._2)

    val expectedData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, 7, 100, Array(rows(0), rows(1))), expectedSchema)),
      (1000L, new ExternalRow(Array(1000L, 3, 200, Array(rows(0), rows(1))), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 3, 300, Array(rows(0), rows(1), rows(2), rows(3))), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 7, 400, Array(rows(0), rows(1), rows(2), rows(3))), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 3, 500, Array(rows(2), rows(3), rows(4), rows(5))), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 7, 600, Array(rows(2), rows(3), rows(4), rows(5))), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 3, 700, Array(rows(4), rows(5), rows(6), rows(7))), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 7, 800, Array(rows(4), rows(5), rows(6), rows(7))), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 3, 900, Array(rows(6), rows(7), rows(8), rows(9))), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 7, 1000, Array(rows(6), rows(7), rows(8), rows(9))), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 3, 1100, Array(rows(8), rows(9), rows(10), rows(11))), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 7, 1200, Array(rows(8), rows(9), rows(10), rows(11))), expectedSchema))
    )
    val result1 = volTSRdd.addWindows(Windows.pastAbsoluteTime(windowLength)).collect()
    val expectedResult = expectedData.map(_._2)

    rows.indices.foreach {
      index =>
        assert(result1(index).getAs[mutable.WrappedArray[Row]](windowColumnName).deep ==
          expectedResult(index).getAs[Array[Row]](windowColumnName).deep)
    }
    assert(result1.map(_.schema).deep == expectedResult.map(_.schema).deep)
  }

  it should "`addWindows` correctly with secondary key" in {
    val lookback = 99
    val windowLength = s"${lookback}ns"
    val windowColumnName = s"window_past_${windowLength}"

    val resultWindows = forecastTSRdd.addWindows(
      Windows.pastAbsoluteTime(windowLength), key = Seq("tid")
    ).collect().map(_.getAs[mutable.WrappedArray[Row]](windowColumnName))

    val expectedWindows = forecastData.map(_._2).map {
      row =>
        val rowTime = row.getAs[Long]("time")
        val rowKey = row.getAs[Int]("tid")
        forecastData.map(_._2).filter {
          windowRow =>
            val windowRowTime = windowRow.getAs[Long]("time")
            val windowRowKey = windowRow.getAs[Int]("tid")
            rowTime - lookback <= windowRowTime && windowRowTime <= rowTime && windowRowKey == rowKey
        }
    }

    assert(resultWindows.deep == expectedWindows.deep)
  }

  it should "`keepRows` correctly" in {
    val expectedData = volData.filter { case (t: Long, r: Row) => r.getAs[Long]("volume") > 900 }
    val result = volTSRdd.keepRows { row: Row => row.getAs[Long]("volume") > 900 }
    assert(result.collect().deep == expectedData.map(_._2).deep)

    val result2 = volTSRdd.addColumns("volume2" -> LongType -> { _ => null }).keepRows(_.getAs[Any]("volume2") != null)
    assert(result2.count() == 0)
  }

  it should "`deleteRows` correctly" in {
    val expectedData = volData.filterNot { case (t: Long, r: Row) => r.getAs[Long]("volume") > 900 }
    val result = volTSRdd.deleteRows { row: Row => row.getAs[Long]("volume") > 900 }
    assert(result.collect().deep == expectedData.map(_._2).deep)
  }

  it should "`keepColumns` correctly" in {
    val expectedSchema = Schema("tid" -> IntegerType)
    val expectedData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, 7), expectedSchema)),
      (1000L, new ExternalRow(Array(1000L, 3), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 3), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 7), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 3), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 7), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 3), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 7), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 3), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 7), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 3), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 7), expectedSchema))
    )

    var result = volTSRdd.keepColumns("tid")
    assert(result.collect().deep == expectedData.map(_._2).deep)
    result = volTSRdd.keepColumns("time", "tid")
    assert(result.collect().deep == expectedData.map(_._2).deep)
  }

  it should "`deleteColumns` correctly" in {
    val expectedSchema = StructType(
      StructField("time", LongType) ::
        StructField("tid", IntegerType) :: Nil
    )
    val expectedData = Array[(Long, Row)](
      (1000L, new ExternalRow(Array(1000L, 7), expectedSchema)),
      (1000L, new ExternalRow(Array(1000L, 3), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 3), expectedSchema)),
      (1050L, new ExternalRow(Array(1050L, 7), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 3), expectedSchema)),
      (1100L, new ExternalRow(Array(1100L, 7), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 3), expectedSchema)),
      (1150L, new ExternalRow(Array(1150L, 7), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 3), expectedSchema)),
      (1200L, new ExternalRow(Array(1200L, 7), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 3), expectedSchema)),
      (1250L, new ExternalRow(Array(1250L, 7), expectedSchema))
    )

    val result = volTSRdd.deleteColumns("volume")
    assert(result.collect().deep == expectedData.map(_._2).deep)
  }

  it should "`lookBackwardClock` correctly" in {
    val result = priceTSRdd.lookBackwardClock("1000ns")
    val expectedData = priceTSRdd.collect().map {
      r =>
        val values = r.toSeq.toArray
        values(0) = r.getLong(0) - 1000L
        new ExternalRow(values, r.schema): Row
    }
    assert(result.collect().deep == expectedData.deep)
  }

  it should "`lookForwardClock` correctly" in {
    val result = priceTSRdd.lookForwardClock("1000ns")
    val expectedData = priceTSRdd.collect().map {
      r =>
        val values = r.toSeq.toArray
        values(0) = r.getLong(0) + 1000L
        new ExternalRow(values, r.schema): Row
    }
    assert(result.collect().deep == expectedData.deep)
  }

  it should "cast columns correctly" in {
    val resultTSRdd = forecastTSRdd.cast("tid" -> ShortType, "forecast" -> IntegerType)
    assertResult(
      Schema.of("time" -> LongType, "tid" -> ShortType, "forecast" -> IntegerType),
      "Verify schema"
    )(resultTSRdd.schema)

    val result = resultTSRdd.collect()
    assert(result.forall(row => row.get(1).isInstanceOf[Short]))
    assert(result.forall(row => row.get(2).isInstanceOf[Int]))
    assertResult(-1, "Casting -1.5 to integer")(result(2).get(2))
  }

  it should "return the same RDD if there's nothing to cast" in {
    val resultTSRdd = forecastTSRdd.cast("tid" -> IntegerType)
    assert(forecastTSRdd eq resultTSRdd)
  }

  it should "set time and return an ordered rdd" in {
    val updatedRdd = clockTSRdd.setTime {
      row: Row =>
        val time = row.getAs[Long]("time") / 100
        if (time % 2 == 1) {
          time * 2
        } else {
          time
        }
    }.collect()

    assert(updatedRdd(0).getAs[Long]("time") === 10)
    assert(updatedRdd(1).getAs[Long]("time") === 12)
    assert(updatedRdd(2).getAs[Long]("time") === 22)
    assert(updatedRdd(3).getAs[Long]("time") === 26)
  }

  it should "support adding nested data types" in {
    val columnSchema = Schema.of("subcolumn_A" -> DoubleType, "subcolumn_B" -> StringType)
    val resultTSRdd = forecastTSRdd.addColumns("nested_column" -> columnSchema -> {
      _ => new GenericRow(Array(1.0, "test"))
    })
    val filteredResult = resultTSRdd.keepRows(_.getAs[ExternalRow]("nested_column").length == 2)
    filteredResult.collect()

    assert(filteredResult.count() == 12)
  }

  it should "support converting nested data types" in {
    val df = forecastTSRdd.toDF

    val makeTuple = udf((time: Long, long: Double) => Tuple2(time.toString, long))
    val updatedDf = df.withColumn("test", makeTuple(col("time"), col("forecast")))
    val ts = TimeSeriesRDD.fromDF(updatedDf)(isSorted = true, TimeUnit.NANOSECONDS)
    val tsFiltered = ts.keepRows(_.getAs[ExternalRow]("test").length == 2)

    assert(tsFiltered.count() == 12)
  }

  // This test is temporarily tagged as "Slow" so that scalatest runner could exclude this test optionally.
  it should "read parquet files" taggedAs (Slow) in {
    SpecUtils.withResource("/timeseries/parquet/PriceWithHeader.parquet") { source =>
      val expectedSchema = Schema("tid" -> IntegerType, "price" -> DoubleType, "info" -> StringType)
      val tsrdd = TimeSeriesRDD.fromParquet(sc, "file://" + source + "/*")(true, NANOSECONDS)
      val rows = tsrdd.collect()

      assert(tsrdd.schema == expectedSchema)
      assert(rows(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(rows(0).getAs[Integer]("tid") == 7)
      assert(rows(0).getAs[Double]("price") == 0.5)
      assert(rows(0).getAs[String]("info") == "test")
      assert(rows.length == 12)
    }
  }

  // This test is temporarily tagged as "Slow" so that scalatest runner could exclude this test optionally.
  it should "not modify original rows during conversions/modifications" taggedAs (Slow) in {
    SpecUtils.withResource("/timeseries/parquet/PriceWithHeader.parquet") { source =>
      val tsrdd = TimeSeriesRDD.fromParquet(sc, "file://" + source + "/*")(true, NANOSECONDS)
      // fromParquet outputs UnsafeRows. Recording the initial state.
      val rows = tsrdd.collect()

      // conversions and modifications shouldn't affect the original RDD
      val convertedTSRDD = TimeSeriesRDD.fromDF(tsrdd.toDF)(isSorted = true, NANOSECONDS)
      convertedTSRDD.setTime(_.getAs[Long](TimeSeriesRDD.timeColumnName) + 1).count()
      convertedTSRDD.leftJoin(priceTSRdd, key = Seq("tid"), leftAlias = "left", rightAlias = "right").count()

      val finalRows = convertedTSRDD.collect()
      assert(rows.deep == finalRows.deep)
    }
  }

}
