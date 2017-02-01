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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

import scala.collection.mutable

class GroupByIntervalSpec extends FlatSpec with SharedSparkContext {

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/groupbyinterval"

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

  "GroupByInterval" should "group by clock correctly." in {
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val clockTSRdd = from("Clock.csv", Schema())
    val expectedSchema = Schema("rows" -> ArrayType(Schema("tid" -> IntegerType, "volume" -> LongType)))

    val rows = volumeTSRdd.rdd.collect()
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(1000L, Array(rows(0), rows(1), rows(2), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, Array(rows(4), rows(5), rows(6), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, Array(rows(8), rows(9), rows(10), rows(11))), expectedSchema)
    )

    val results = volumeTSRdd.groupByInterval(clockTSRdd).collect()

    expectedResults.indices.foreach {
      index =>
        assert(results(index).getAs[mutable.WrappedArray[Row]]("rows").deep ==
          expectedResults(index).getAs[Array[Row]]("rows").deep)
    }

    assert(results.map(_.schema).deep == expectedResults.map(_.schema).deep)
  }

  it should "`groupByInterval` per key correctly" in {
    val volumeTSRdd = from("Volume.csv", Schema("tid" -> IntegerType, "volume" -> LongType))
    val clockTSRdd = from("Clock.csv", Schema())
    val expectedSchema = Schema("tid" -> IntegerType, "rows" -> ArrayType(Schema("tid" -> IntegerType, "volume" -> LongType)))

    val rows = volumeTSRdd.rdd.collect()
    val expectedResults = Array[Row](
      new GenericRowWithSchema(Array(1000L, 3, Array(rows(1), rows(2))), expectedSchema),
      new GenericRowWithSchema(Array(1000L, 7, Array(rows(0), rows(3))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 3, Array(rows(4), rows(6))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 7, Array(rows(5), rows(7))), expectedSchema),
      new GenericRowWithSchema(Array(1100L, 3, Array(rows(8), rows(10))), expectedSchema),
      new GenericRowWithSchema(Array(1200L, 7, Array(rows(9), rows(11))), expectedSchema)
    )

    val results = volumeTSRdd.groupByInterval(clockTSRdd, Seq("tid")).collect()

    expectedResults.indices.foreach {
      index =>
        assert(results(index).getAs[mutable.WrappedArray[Row]]("rows").deep ==
          expectedResults(index).getAs[Array[Row]]("rows").deep)
    }

    assert(results.map(_.schema).deep == expectedResults.map(_.schema).deep)
  }
}
