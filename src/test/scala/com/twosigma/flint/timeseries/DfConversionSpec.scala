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

package com.twosigma.flint.timeseries

import java.util.concurrent.TimeUnit

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD }
import com.twosigma.flint.timeseries.row.Schema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ LongType, StructType }
import org.scalatest.FlatSpec

class DfConversionSpec extends FlatSpec with SharedSparkContext {
  val defaultNumPartitions = 5
  val clockSchema: StructType = Schema("time" -> LongType)
  val clockData = (0L to 100L).map(ts => (ts, new GenericRowWithSchema(Array(ts), clockSchema).asInstanceOf[Row]))

  var clockTSRdd: TimeSeriesRDD = _

  override def beforeAll() {
    super.beforeAll()
    clockTSRdd = TimeSeriesRDD.fromOrderedRDD(
      OrderedRDD.fromRDD(sc.parallelize(clockData, defaultNumPartitions), KeyPartitioningType.Sorted), clockSchema
    )
  }

  "DfConversionSpec" should "convert timestamps correctly" in {
    val df = clockTSRdd.toDF
    val convertedDf = TimeSeriesRDD.convertDfTimestamps(df, TimeUnit.MICROSECONDS)

    val dfRows = df.collect()
    val convertedRows = convertedDf.collect()

    val zipped = dfRows.zip(convertedRows)
    assert(zipped.forall {
      case (row, convertedRow) => row.getAs[Long]("time") * 1000 == convertedRow.getAs[Long]("time")
    })
  }

  it should "correctly sort dataframes" in {
    val df = clockTSRdd.toDF
    val revertedDf = df.withColumn("time", -col("time") + 100)

    val tsrdd = TimeSeriesRDD.fromDF(revertedDf)(isSorted = false, TimeUnit.NANOSECONDS)
    val tsRows = tsrdd.collect()

    val zipped = tsRows.map(row => row.getAs[Long]("time")).zipWithIndex
    assert(zipped.forall {
      case (ts, index) => ts == index
    })
  }
}
