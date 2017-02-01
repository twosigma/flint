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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.{ SpecUtils, SharedSparkContext }
import com.twosigma.flint.timeseries.{ Summarizers, CSV, TimeSeriesRDD }
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }
import org.scalactic.TolerantNumerics
import org.scalatest.FlatSpec

class NthMomentSummarizerSpec extends FlatSpec with SharedSparkContext {

  private implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(1.0e-8)

  private val defaultPartitionParallelism: Int = 5

  private val resourceDir: String = "/timeseries/summarize/summarizer/nthmomentsummarizer"

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

  "NthMomentSummarizer" should "`computeNthMoment` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    var results = priceTSRdd.summarize(Summarizers.nthMoment("price", 0), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_0thMoment") === 1.0)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_0thMoment") === 1.0)

    results = priceTSRdd.summarize(Summarizers.nthMoment("price", 1), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_1thMoment") === 3.0833333333333335)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_1thMoment") === 3.416666666666667)

    results = priceTSRdd.summarize(Summarizers.nthMoment("price", 2), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_2thMoment") === 12.041666666666668)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_2thMoment") === 15.041666666666666)

    results = priceTSRdd.summarize(Summarizers.nthMoment("price", 3), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_3thMoment") === 53.39583333333333)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_3thMoment") === 73.35416666666667)

    results = priceTSRdd.summarize(Summarizers.nthMoment("price", 4), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_4thMoment") === 253.38541666666669)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_4thMoment") === 379.0104166666667)
  }

  it should "`computeNthCentralMoment` correctly" in {
    val priceTSRdd = from("Price.csv", Schema("tid" -> IntegerType, "price" -> DoubleType))
    var results = priceTSRdd.summarize(Summarizers.nthMoment("price", 0), Seq("tid")).collect()
    results = priceTSRdd.summarize(Summarizers.nthCentralMoment("price", 1), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_1thCentralMoment") === 0d)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_1thCentralMoment") === 0d)

    results = priceTSRdd.summarize(Summarizers.nthCentralMoment("price", 2), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_2thCentralMoment") === 2.534722222222222)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_2thCentralMoment") === 3.3680555555555554)

    results = priceTSRdd.summarize(Summarizers.nthCentralMoment("price", 3), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_3thCentralMoment") === 0.6365740740740735)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_3thCentralMoment") === -1.0532407407407405)

    results = priceTSRdd.summarize(Summarizers.nthCentralMoment("price", 4), Seq("tid")).collect()
    assert(results.find(_.getAs[Int]("tid") == 3).head.getAs[Double]("price_4thCentralMoment") === 10.567563657407407)
    assert(results.find(_.getAs[Int]("tid") == 7).head.getAs[Double]("price_4thCentralMoment") === 21.227285879629633)
  }
}
