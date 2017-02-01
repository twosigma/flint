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
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ LongType, IntegerType, DoubleType, StructType }
import org.scalatest.FlatSpec

class SummarizeSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarize"

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

  it should "`summarize` correctly" in {
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val expectedSchema = Schema("volume_sum" -> DoubleType)
    val expectedResults = Array[Row](new GenericRowWithSchema(Array(0L, 7800.0), expectedSchema))
    val results = volumeTSRdd.summarize(Summarizers.sum("volume"))
    assert(results.schema == expectedSchema)
    assert(results.collect().deep == expectedResults.deep)
  }

  it should "`summarize` per key correctly" in {
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val expectedSchema = Schema("tid" -> IntegerType, "volume_sum" -> DoubleType)
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(0L, 7, 4100.0), expectedSchema),
      new GenericRowWithSchema(Array(0L, 3, 3700.0), expectedSchema)
    )
    val results = volumeTSRdd.summarize(Summarizers.sum("volume"), Seq("tid"))
    assert(results.schema == expectedSchema)
    assert(results.collect().sortBy(_.getAs[Int]("tid")).deep == expectedResults.sortBy(_.getAs[Int]("tid")).deep)
  }
}
