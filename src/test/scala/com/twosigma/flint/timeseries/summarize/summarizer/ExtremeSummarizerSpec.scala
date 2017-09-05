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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.rdd.function.summarize.summarizer.Summarizer
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ SummarizerFactory, SummarizerSuite }
import com.twosigma.flint.timeseries.{ CSV, Summarizers, TimeSeriesRDD, TimeSeriesSuite }
import org.apache.spark.sql.types.{ DataType, DoubleType, FloatType, IntegerType, LongType, StructType }
import java.util.Random

import org.apache.spark.sql.Row

class ExtremeSummarizerSpec extends SummarizerSuite {

  override val defaultResourceDir: String = "/timeseries/summarize/summarizer/meansummarizer"

  private def test[T](
    dataType: DataType,
    randValue: Row => Any,
    summarizer: String => SummarizerFactory,
    reduceFn: (T, T) => T,
    inputColumn: String,
    outputColumn: String
  ): Unit = {
    val priceTSRdd = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType)).addColumns(
      inputColumn -> dataType -> randValue
    )

    val data = priceTSRdd.collect().map{ row => row.getAs[T](inputColumn) }

    val trueExtreme = data.reduceLeft[T]{ case (x, y) => reduceFn(x, y) }

    val result = priceTSRdd.summarize(summarizer(inputColumn))

    val extreme = result.first().getAs[T](outputColumn)
    val outputType = result.schema(outputColumn).dataType

    assert(outputType == dataType, s"$outputType")
    assert(trueExtreme === extreme, s"extreme: $extreme, trueExtreme: $trueExtreme, data: ${data.toSeq}")
  }

  "MaxSummarizer" should "compute double max correctly" in {
    val rand = new Random()
    test[Double](DoubleType, { _: Row => rand.nextDouble() }, Summarizers.max, math.max, "x", "x_max")
  }

  it should "compute long max correctly" in {
    val rand = new Random()
    test[Long](LongType, { _: Row => rand.nextLong() }, Summarizers.max, math.max, "x", "x_max")
  }

  it should "compute float max correctly" in {
    val rand = new Random()
    test[Float](FloatType, { _: Row => rand.nextFloat() }, Summarizers.max, math.max, "x", "x_max")
  }

  it should "compute int max correctly" in {
    val rand = new Random()
    test[Int](IntegerType, { _: Row => rand.nextInt() }, Summarizers.max, math.max, "x", "x_max")
  }

  "MinSummarizer" should "compute double min correctly" in {
    val rand = new Random()
    test[Double](DoubleType, { _: Row => rand.nextDouble() }, Summarizers.min, math.min, "x", "x_min")
  }

  it should "compute long min correctly" in {
    val rand = new Random()
    test[Long](LongType, { _: Row => rand.nextLong() }, Summarizers.min, math.min, "x", "x_min")
  }

  it should "compute float min correctly" in {
    val rand = new Random()
    test[Float](FloatType, { _: Row => rand.nextFloat() }, Summarizers.min, math.min, "x", "x_min")
  }

  it should "compute int min correctly" in {
    val rand = new Random()
    test[Int](IntegerType, { _: Row => rand.nextInt() }, Summarizers.min, math.min, "x", "x_min")
  }

  it should "pass summarizer property test" in {
    summarizerPropertyTest(AllProperties)(Summarizers.max("x1"))
    summarizerPropertyTest(AllProperties)(Summarizers.min("x2"))
  }

  it should "ignore null values" in {
    val input = fromCSV("Price.csv", Schema("id" -> IntegerType, "price" -> DoubleType))
    val inputWithNull = insertNullRows(input, "price")

    assertEquals(
      input.summarize(Summarizers.min("price")),
      inputWithNull.summarize(Summarizers.min("price"))
    )
  }
}
