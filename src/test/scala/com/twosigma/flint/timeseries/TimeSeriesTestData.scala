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

import com.twosigma.flint.rdd.{ CloseOpen, ParallelCollectionRDD }
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ IntegerType, LongType, StructField, StructType }
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{ col, round }

import scala.concurrent.duration.NANOSECONDS
import scala.util.Random

private[flint] trait TimeSeriesTestData {
  protected def sqlContext: SQLContext

  // Helper object to import SQL implicits without a concrete SQLContext
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = sqlContext
  }

  import internalImplicits._
  import TimeSeriesTestData._

  protected lazy val testData: TimeSeriesRDD = {
    val df = sqlContext.sparkContext.parallelize(
      TestData(1000) ::
        TestData(1000) ::
        TestData(1000) ::
        TestData(2000) ::
        TestData(2000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(4000) ::
        TestData(5000) ::
        TestData(5000) :: Nil
    ).toDF()
    TimeSeriesRDD.fromDF(df)(isSorted = true, timeUnit = NANOSECONDS)
  }

  protected lazy val forecastData: TimeSeriesRDD = {
    val df = sqlContext.sparkContext.parallelize(
      ForecastData(1000L, 7, Some(3.0)) ::
        ForecastData(1000L, 3, Some(5.0)) ::
        ForecastData(1050L, 3, Some(-1.5)) ::
        ForecastData(1050L, 7, Some(2.0)) ::
        ForecastData(1100L, 3, Some(-2.4)) ::
        ForecastData(1100L, 7, Some(6.4)) ::
        ForecastData(1150L, 3, Some(1.5)) ::
        ForecastData(1150L, 7, Some(-7.9)) ::
        ForecastData(1200L, 3, Some(4.6)) ::
        ForecastData(1200L, 7, Some(1.4)) ::
        ForecastData(1250L, 3, Some(-9.6)) ::
        ForecastData(1250L, 7, Some(6.0)) :: Nil
    ).toDF()

    TimeSeriesRDD.fromDF(df)(isSorted = true, timeUnit = NANOSECONDS)
  }

  protected lazy val cycleMetaData1 = CycleMetaData(1000000L, 1000000L * 10)
  protected lazy val cycleMetaData2 = CycleMetaData(1000000L, 1000000L * 10)
  protected lazy val cycleData1: TimeSeriesRDD = generateCycleData(0, cycleMetaData1)
  protected lazy val cycleData2: TimeSeriesRDD = generateCycleData(1, cycleMetaData2)

  /**
   * Meta data for the generated cycle data. Metadata is used by test to decide arguments for various functions.
   * See below for how the data looks like.
   */
  case class CycleMetaData(cycleWidth: Long, intervalWidth: Long)

  /**
   * Generate cycle data. Generated data has multiple intervals, each interval has the multiple cycles.
   *
   * For instance, for cycleWidth = 1000, intervalWidth = 10000, beginCycleOffset = 3, endCycleOffset = 8:
   *
   * Interval 1:
   * [3000, 4000, 5000, 6000, 7000]
   * Interval 2:
   * [13000, 14000, 15000, 16000, 17000]
   * ...
   *
   */
  private def generateCycleData(salt: Long, metadata: CycleMetaData): TimeSeriesRDD = {
    val begin = 0L
    val numIntervals = 13
    val beginCycleOffset = 3
    val endCycleOffset = 8
    val cycleWidth = metadata.cycleWidth
    val intervalWitdh = metadata.intervalWidth
    val seed = 123456789L

    var df = new TimeSeriesGenerator(
      sqlContext.sparkContext,
      begin = begin,
      end = numIntervals * cycleWidth, frequency = cycleWidth
    )(
      columns = Seq(
        "v1" -> { (_: Long, _: Int, r: Random) => r.nextDouble() },
        "v2" -> { (_: Long, _: Int, r: Random) => r.nextDouble() },
        "v3" -> { (_: Long, _: Int, r: Random) => r.nextDouble() }
      ),
      ids = 1 to 10,
      ratioOfCycleSize = 0.8,
      seed = seed + salt
    ).generate().toDF

    df = df.withColumn("index", (df("time") / intervalWitdh).cast(IntegerType))
      .withColumn("cycleOffset", (df("time") % intervalWitdh / cycleWidth).cast(IntegerType))
      .where(col("cycleOffset") > beginCycleOffset)
      .where(col("cycleOffset") < endCycleOffset)
      .drop("cycleOffset")

    val rows = df.queryExecution.executedPlan.executeCollect().toSeq
    val indexColumn = df.schema.fieldIndex("index")
    val groupedRows = rows.groupBy(_.getInt(indexColumn)).toSeq.sortBy(_._1).map(_._2)

    val rdd = new ParallelCollectionRDD[InternalRow](sqlContext.sparkContext, groupedRows)
    val ranges = df.select("index").distinct().collect().map(_.getInt(0)).sorted.map{
      case index =>
        CloseOpen(index * intervalWitdh, Some((index + 1) * intervalWitdh))
    }
    TimeSeriesRDD.fromDFWithRanges(DFConverter.toDataFrame(rdd, df.schema), ranges)
  }
}

private[flint] object TimeSeriesTestData {
  case class TestData(time: Long)
  case class ForecastData(time: Long, id: Int, forecast: Option[Double])
}
