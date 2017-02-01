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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.{ SharedSparkContext, SpecUtils }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DoubleType, IntegerType, LongType, StringType, StructType }
import org.scalatest.FlatSpec

class AddColumnsForCycleSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/addcolumnsforcycle"

  private def from(filename: String, schema: StructType): TimeSeriesRDD =
    SpecUtils.withResource(s"$resourceDir/$filename") { source =>
      CSV.from(
        sqlContext,
        s"file://$source",
        header = true,
        sorted = true,
        schema = schema
      ).repartition(defaultPartitionParallelism)
    }

  "AddColumnsForCycle" should "pass `AddAdjustedPrice` test" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val resultTSRdd = from(
      "AddAdjustedPrice.results",
      Schema("tid" -> IntegerType, "price" -> DoubleType, "adjustedPrice" -> DoubleType)
    )
    val adjustedPriceTSRdd = priceTSRdd.addColumnsForCycle(
      "adjustedPrice" -> DoubleType ->
        { rows: Seq[Row] =>
          val size = rows.size
          rows.map { row => row -> row.getDouble(2) * size }.toMap
        }
    )
    assert(adjustedPriceTSRdd.schema == resultTSRdd.schema)
    assert(adjustedPriceTSRdd.collect().deep == resultTSRdd.collect().deep)
  }

  it should "support non-primitive types" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val resultTSRdd = from(
      "AddAdjustedPrice.results",
      Schema("tid" -> IntegerType, "price" -> DoubleType, "adjustedPrice" -> StringType)
    )
    val adjustedPriceTSRdd = priceTSRdd.addColumnsForCycle(
      "adjustedPrice" -> StringType ->
        { rows: Seq[Row] =>
          val size = rows.size
          rows.map { row => row -> (row.getDouble(2) * size).toString }.toMap
        }
    )
    assert(adjustedPriceTSRdd.schema == resultTSRdd.schema)
    assert(adjustedPriceTSRdd.collect().deep == resultTSRdd.collect().deep)
  }

  it should "pass `AddTotalVolumePerKey` test, i.e. with additional a single key. " in {
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val resultTSRdd = from(
      "AddTotalVolumePerKey.results",
      Schema("tid" -> IntegerType, "volume" -> LongType, "totalVolume" -> LongType)
    )
    val totalVolumeTSRdd = volumeTSRdd.addColumnsForCycle(
      Seq(
        "totalVolume" -> LongType -> { rows: Seq[Row] =>
          val sum = rows.map(_.getAs[Long]("volume")).sum
          rows.zipWithIndex.map { case (row, idx) => row -> (idx + sum) }.toMap
        }
      ),
      Seq("tid")
    )
    assert(totalVolumeTSRdd.schema == resultTSRdd.schema)

    // TODO: we should do this instead of the following 3 asserts
    // assert(totalVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    assert(totalVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 3).collect().deep)
    assert(totalVolumeTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep ==
      resultTSRdd.keepRows(_.getAs[Int]("tid") == 7).collect().deep)
    assert(totalVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
      resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
  }

  it should "pass `AddTotalVolumePerSeqOfKeys` test, i.e. with additional a sequence of keys." in {
    val volumeTSRdd = from(
      "VolumeWithIndustryGroup.csv",
      Schema("tid" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
    )
    val resultTSRdd = from(
      "AddTotalVolumePerSeqOfKeys.results",
      Schema("tid" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "totalVolume" -> LongType)
    )
    val totalVolumeTSRdd = volumeTSRdd.addColumnsForCycle(
      Seq(
        "totalVolume" -> LongType -> { rows: Seq[Row] =>
          val sum = rows.map(_.getAs[Long]("volume")).sum
          rows.zipWithIndex.map { case (row, idx) => row -> (idx + sum) }.toMap
        }
      ),
      Seq("tid", "group")
    )
    assert(totalVolumeTSRdd.schema == resultTSRdd.schema)
    assert(totalVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
  }
}
