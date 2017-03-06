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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AddColumnsForCycleSpec extends MultiPartitionSuite {

  override val defaultResourceDir: String = "/timeseries/addcolumnsforcycle"

  "AddColumnsForCycle" should "pass `AddAdjustedPrice` test" in {
    val resultTSRdd = fromCSV(
      "AddAdjustedPrice.results",
      Schema("id" -> IntegerType, "price" -> DoubleType, "adjustedPrice" -> DoubleType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val adjustedPriceTSRdd = rdd.addColumnsForCycle(
        "adjustedPrice" -> DoubleType ->
          { rows: Seq[Row] =>
            val size = rows.size
            rows.map { row => row -> row.getDouble(2) * size }.toMap
          }
      )
      assert(adjustedPriceTSRdd.schema == resultTSRdd.schema)
      assert(adjustedPriceTSRdd.collect().deep == resultTSRdd.collect().deep)
    }

    {
      val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
      withPartitionStrategy(priceTSRdd)(DEFAULT)(test)
    }

  }

  it should "support non-primitive types" in {
    val resultTSRdd = fromCSV(
      "AddAdjustedPrice.results",
      Schema("id" -> IntegerType, "price" -> DoubleType, "adjustedPrice" -> StringType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val adjustedPriceTSRdd = rdd.addColumnsForCycle(
        "adjustedPrice" -> StringType ->
          { rows: Seq[Row] =>
            val size = rows.size
            rows.map { row => row -> (row.getDouble(2) * size).toString }.toMap
          }
      )
      assert(adjustedPriceTSRdd.schema == resultTSRdd.schema)
      assert(adjustedPriceTSRdd.collect().deep == resultTSRdd.collect().deep)
    }

    {
      val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
      withPartitionStrategy(priceTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `AddTotalVolumePerKey` test, i.e. with additional a single key. " in {
    val resultTSRdd = fromCSV(
      "AddTotalVolumePerKey.results",
      Schema("id" -> IntegerType, "volume" -> LongType, "totalVolume" -> LongType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val totalVolumeTSRdd = rdd.addColumnsForCycle(
        Seq(
          "totalVolume" -> LongType -> { rows: Seq[Row] =>
            val sum = rows.map(_.getAs[Long]("volume")).sum
            rows.zipWithIndex.map { case (row, idx) => row -> (idx + sum) }.toMap
          }
        ),
        Seq("id")
      )
      assert(totalVolumeTSRdd.schema == resultTSRdd.schema)

      // TODO: we should do this instead of the following 3 asserts
      // assert(totalVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
      assert(totalVolumeTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 3).collect().deep)
      assert(totalVolumeTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep ==
        resultTSRdd.keepRows(_.getAs[Int]("id") == 7).collect().deep)
      assert(totalVolumeTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep ==
        resultTSRdd.keepColumns(TimeSeriesRDD.timeColumnName).collect().deep)
    }

    {
      val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }

  it should "pass `AddTotalVolumePerSeqOfKeys` test, i.e. with additional a sequence of keys." in {
    val resultTSRdd = fromCSV(
      "AddTotalVolumePerSeqOfKeys.results",
      Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType, "totalVolume" -> LongType)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val totalVolumeTSRdd = rdd.addColumnsForCycle(
        Seq(
          "totalVolume" -> LongType -> { rows: Seq[Row] =>
            val sum = rows.map(_.getAs[Long]("volume")).sum
            rows.zipWithIndex.map { case (row, idx) => row -> (idx + sum) }.toMap
          }
        ),
        Seq("id", "group")
      )
      assert(totalVolumeTSRdd.schema == resultTSRdd.schema)
      assert(totalVolumeTSRdd.collect().deep == resultTSRdd.collect().deep)
    }

    {
      val volumeTSRdd = fromCSV(
        "VolumeWithIndustryGroup.csv",
        Schema("id" -> IntegerType, "group" -> IntegerType, "volume" -> LongType)
      )
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }
}
