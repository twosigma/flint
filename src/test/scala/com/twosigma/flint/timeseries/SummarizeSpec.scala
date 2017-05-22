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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ LongType, IntegerType, DoubleType }

class SummarizeSpec extends MultiPartitionSuite {

  override val defaultResourceDir: String = "/timeseries/summarize"

  it should "`summarize` correctly" in {
    val expectedSchema = Schema("volume_sum" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 7800.0), expectedSchema))

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.summarize(Summarizers.sum("volume"))
      assert(results.schema == expectedSchema)
      assert(results.collect().deep == expectedResults.deep)
    }

    {
      val volumeRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
      withPartitionStrategy(volumeRdd)(DEFAULT)(test)
    }

  }

  it should "`summarize` per key correctly" in {
    val expectedSchema = Schema("id" -> IntegerType, "volume_sum" -> DoubleType)
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(0L, 7, 4100.0), expectedSchema),
      new GenericRowWithSchema(Array(0L, 3, 3700.0), expectedSchema)
    )

    def test(rdd: TimeSeriesRDD): Unit = {
      val results = rdd.summarize(Summarizers.sum("volume"), Seq("id"))
      assert(results.schema == expectedSchema)
      assert(results.collect().sortBy(_.getAs[Int]("id")).deep == expectedResults.sortBy(_.getAs[Int]("id")).deep)
    }

    {
      val volumeTSRdd = fromCSV("Volume.csv", Schema("id" -> IntegerType, "volume" -> LongType))
      withPartitionStrategy(volumeTSRdd)(DEFAULT)(test)
    }
  }
}
