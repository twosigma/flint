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
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.scalatest.FlatSpec

class MergeSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/merge"

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

  "Merge" should "pass `Merge` test." in {
    val priceTSRdd1 = from("Price1.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val priceTSRdd2 = from("Price2.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val resultsTSRdd = from("Merge.results", Schema("tid" -> IntegerType, "price" -> DoubleType))
    val mergedTSRdd = priceTSRdd1.merge(priceTSRdd2)
    assert(resultsTSRdd.schema == mergedTSRdd.schema)
    assert(resultsTSRdd.collect().deep == mergedTSRdd.collect().deep)
  }
}
