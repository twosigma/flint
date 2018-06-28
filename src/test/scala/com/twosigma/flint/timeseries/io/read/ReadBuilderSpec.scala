/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.timeseries.io.read

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{ functions => F }
import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.timeseries.io.read.ReadBuilder._
import com.twosigma.flint.timeseries.time.TimeFormat
import com.twosigma.flint.timeseries.time.TimeFormat.parseNano
import org.scalatest.FlatSpec

import scala.concurrent.duration.Duration

class ReadBuilderSpec extends FlatSpec with SharedSparkContext {

  behavior of "ReadBuilder"

  it should "set options" in {
    val b = new ReadBuilder()

    // Set an option
    b.option(COLUMNS, "x,y,z")
    assert(b.parameters.columns === Some(Seq("x", "y", "z")))

    // Reset an option
    b.option(COLUMNS, null)
    assert(b.parameters.columns === None)
  }

  it should "set begin and end range" in {
    val b1 = new ReadBuilder().range("20170101", "20170201", "UTC")
    val b2 = new ReadBuilder().range(parseNano("20170101"), parseNano("20170201"))
    val b3 = new ReadBuilder().range("20161231 19:00:00", "20170131 19:00:00", "America/New_York")

    assert(b1.parameters.range === b2.parameters.range)
    assert(b1.parameters.range === b3.parameters.range)

    val b4 = new ReadBuilder().range(null, "20170201")
    assert(b4.parameters.range == BeginEndRange(None, Some(parseNano("20170201"))))
  }

  it should "set default options" in {
    val builder = new ReadBuilder()

    assert(builder.parameters.columns === None)
    assert(builder.parameters.extraOptions(TIME_COLUMN) === "time")
    assert(builder.parameters.extraOptions(TIME_UNIT) === "ns")
  }

  behavior of "Parquet reader"

  private val priceWithHeaderParquetPath = "src/test/resources/timeseries/parquet/PriceWithHeader.parquet"
  private val priceWithHeaderUnsortedParquetPath =
    "src/test/resources/timeseries/parquet/PriceWithHeaderUnsorted.parquet"
  private val priceWithHeaderTimeRenamedParquetPath =
    "src/test/resources/timeseries/parquet/PriceWithHeaderTimeRenamed.parquet"

  it should "read a parquet file" in {
    val tsrdd = new ReadBuilder()
      .parquet(priceWithHeaderParquetPath)

    val actualColumns = tsrdd.schema.fields.map(_.name)
    assert(actualColumns === Seq("time", "id", "price", "info"))
    assert(tsrdd.count() === 12)
  }

  it should "support a time range" in {
    val all = new ReadBuilder()
      .parquet(priceWithHeaderParquetPath)
      .collect()

    val beginAndEnd = new ReadBuilder()
      .range(1100L, 1250L)
      .parquet(priceWithHeaderParquetPath)
    val beginAndEndExpected = all.filter { row =>
      row.getAs[Long]("time") >= 1100L && row.getAs[Long]("time") < 1250L
    }
    assert(beginAndEnd.collect() === beginAndEndExpected)

    val endOnly = new ReadBuilder()
      .range(null, 1100L)
      .parquet(priceWithHeaderParquetPath)
    val endOnlyExpected = all.filter { row =>
      row.getAs[Long]("time") < 1100L
    }
    assert(endOnly.collect() === endOnlyExpected)

    val beginOnly = new ReadBuilder()
      .range(1100L, null)
      .parquet(priceWithHeaderParquetPath)
    val beginOnlyExpected = all.filter { row =>
      row.getAs[Long]("time") >= 1100L
    }
    assert(beginOnly.collect() === beginOnlyExpected)
  }

  it should "support the 'columns' option" in {
    val tsrdd = new ReadBuilder()
      .option("columns", "time, id")
      .parquet(priceWithHeaderParquetPath)

    assert(tsrdd.schema.fields.map(_.name) === Seq("time", "id"))
  }

  it should "always place the 'time' column first" in {
    val tsrdd = new ReadBuilder()
      .option("columns", "id, time")
      .parquet(priceWithHeaderParquetPath)

    assert(tsrdd.schema.fields.map(_.name) === Seq("time", "id"))
  }

  it should "support 'timeUnit' option" in {
    val withoutTimeUnit = new ReadBuilder()
      .parquet(priceWithHeaderParquetPath)

    // Test the 'timeUnit' option
    val withTimeUnit = new ReadBuilder()
      .option("timeUnit", "s")
      .parquet(priceWithHeaderParquetPath)

    val expectedTime = withoutTimeUnit.collect().map { row =>
      val time = row.getAs[Long]("time")
      TimeUnit.NANOSECONDS.convert(time, TimeUnit.SECONDS)
    }
    val actualTime = withTimeUnit.toDF.select("time").collect().map(_.getAs[Long]("time"))
    assert(actualTime === expectedTime)
  }

  it should "support 'timeColumn' option" in {
    val actual = new ReadBuilder()
      .option("timeColumn", "timeRenamed")
      .parquet(priceWithHeaderTimeRenamedParquetPath)
      .toDF
      .drop("timeRenamed")

    val df = sqlContext.read.parquet(priceWithHeaderTimeRenamedParquetPath)
      .withColumn("time", F.col("timeRenamed"))
      .drop("timeRenamed")

    val expected = df.select("time", df.columns.filter(_ != "time"): _*)

    assert(actual.collect() === expected.collect())
  }

  it should "support 'isSorted' option" in {
    val actual = new ReadBuilder()
      .option("isSorted", "false")
      .parquet(priceWithHeaderUnsortedParquetPath)

    val expected = sqlContext.read.parquet(priceWithHeaderUnsortedParquetPath).sort("time")

    assert(actual.collect() === expected.collect())
  }

  behavior of "BeginEndRange"

  it should "return null for beginNanosOrNull when beginNanosOpt is None" in {
    val range = BeginEndRange(None, Some(parseNano("20170201")))
    assert(range.beginNanosOrNull === null)
  }

  it should "return null for endNanosOrNull when endNanosOpt is None" in {
    val range = BeginEndRange(Some(parseNano("20170101")), None)
    assert(range.endNanosOrNull === null)
  }

  it should "return beginNanos and endNanos when set" in {
    val expectedBeginNanos = parseNano("20170101")
    val expectedEndNanos = parseNano("20170201")
    val reader = new ReadBuilder()
      .range(expectedBeginNanos, expectedEndNanos)

    assert(reader.parameters.range.beginNanosOrNull === expectedBeginNanos)
    assert(reader.parameters.range.endNanosOrNull === expectedEndNanos)

    assert(reader.parameters.range.beginNanos === expectedBeginNanos)
    assert(reader.parameters.range.endNanos === expectedEndNanos)
  }

  it should "return expanded begin and end when expand is set" in {
    val rawBeginNanos = parseNano("20170101")
    val rawEndNanos = parseNano("20170201")
    val expandBegin = "1day"
    val expandEnd = "2day"
    val expectedBeginNanos = rawBeginNanos - Duration(expandBegin).toNanos
    val expectedEndNanos = rawEndNanos + Duration(expandEnd).toNanos
    val reader = new ReadBuilder()
      .range(rawBeginNanos, rawEndNanos)
      .expand(expandBegin, expandEnd)

    assert(reader.parameters.range.beginNanosOrNull === expectedBeginNanos)
    assert(reader.parameters.range.endNanosOrNull === expectedEndNanos)

    assert(reader.parameters.range.beginNanos === expectedBeginNanos)
    assert(reader.parameters.range.endNanos === expectedEndNanos)
  }

  it should "override previous expand calls" in {
    val rawBeginNanos = parseNano("20170101")
    val rawEndNanos = parseNano("20170201")
    val expandBegin = "1day"
    val expandEnd = "2day"
    val expectedBeginNanos = rawBeginNanos - Duration(expandBegin).toNanos
    val expectedEndNanos = rawEndNanos + Duration(expandEnd).toNanos

    val reader = new ReadBuilder()
      .range(rawBeginNanos, rawEndNanos)
      .expand("60days", "30days")
      .expand("30days", "60days")
      .expand(expandBegin, expandEnd)

    assert(reader.parameters.range.beginNanosOrNull === expectedBeginNanos)
    assert(reader.parameters.range.endNanosOrNull === expectedEndNanos)

    assert(reader.parameters.range.beginNanos === expectedBeginNanos)
    assert(reader.parameters.range.endNanos === expectedEndNanos)
  }

  it should "has no effect if range is not called" in {
    val expandBegin = "1day"
    val expandEnd = "2day"

    val reader = new ReadBuilder()
      .expand(expandBegin, expandEnd)

    assert(reader.parameters.range.beginNanosOrNull === null)
    assert(reader.parameters.range.endNanosOrNull === null)
  }

  it should "support calling expand before range" in {
    val rawBeginNanos = parseNano("20170101")
    val rawEndNanos = parseNano("20170201")
    val expandBegin = "1day"
    val expandEnd = "2day"
    val expectedBeginNanos = rawBeginNanos - Duration(expandBegin).toNanos
    val expectedEndNanos = rawEndNanos + Duration(expandEnd).toNanos
    val reader = new ReadBuilder()
      .expand(expandBegin, expandEnd)
      .range(rawBeginNanos, rawEndNanos)

    assert(reader.parameters.range.beginNanosOrNull === expectedBeginNanos)
    assert(reader.parameters.range.endNanosOrNull === expectedEndNanos)

    assert(reader.parameters.range.beginNanos === expectedBeginNanos)
    assert(reader.parameters.range.endNanos === expectedEndNanos)
  }

  it should "support expanding only one side" in {
    val rawBeginNanos = parseNano("20170101")
    val rawEndNanos = parseNano("20170201")
    val expandBegin = "1day"
    val expandEnd = "2day"
    val expectedBeginNanos = rawBeginNanos - Duration(expandBegin).toNanos
    val expectedEndNanos = rawEndNanos + Duration(expandEnd).toNanos
    val reader1 = new ReadBuilder()
      .range(rawBeginNanos, rawEndNanos)
      .expand(begin = expandBegin)

    assert(reader1.parameters.range.beginNanosOrNull === expectedBeginNanos)
    assert(reader1.parameters.range.endNanosOrNull === rawEndNanos)

    assert(reader1.parameters.range.beginNanos === expectedBeginNanos)
    assert(reader1.parameters.range.endNanos === rawEndNanos)

    val reader2 = new ReadBuilder()
      .range(rawBeginNanos, rawEndNanos)
      .expand(end = expandEnd)

    assert(reader2.parameters.range.beginNanosOrNull === rawBeginNanos)
    assert(reader2.parameters.range.endNanosOrNull === expectedEndNanos)

    assert(reader2.parameters.range.beginNanos === rawBeginNanos)
    assert(reader2.parameters.range.endNanos === expectedEndNanos)

  }
}
