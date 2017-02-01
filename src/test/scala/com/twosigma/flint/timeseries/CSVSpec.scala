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
import org.scalatest.FlatSpec

import org.apache.spark.sql.types._

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.SpecUtils

class CSVSpec extends FlatSpec with SharedSparkContext {

  "CSV" should "read a CSV file without header." in {
    SpecUtils.withResource("/timeseries/csv/Price.csv") { source =>
      val expectedSchema = Schema("C1" -> IntegerType, "C2" -> DoubleType)
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, sorted = true)
      val ts = timeseriesRdd.collect()
      assert(timeseriesRdd.schema == expectedSchema)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("C1") == 7)
      assert(ts(0).getAs[Double]("C2") == 0.5)
      assert(ts.length == 12)
    }
  }

  it should "read a CSV file with header." in {
    SpecUtils.withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val expectedSchema = Schema("tid" -> IntegerType, "price" -> DoubleType, "info" -> StringType)
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, header = true, sorted = true)
      val ts = timeseriesRdd.collect()

      assert(timeseriesRdd.schema == expectedSchema)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("tid") == 7)
      assert(ts(0).getAs[Double]("price") == 0.5)
      assert(ts.length == 12)
    }
  }

  it should "read a CSV file with header and keep origin time column." in {
    SpecUtils.withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val expectedSchema = Schema(
        "time_" -> IntegerType,
        "tid" -> IntegerType,
        "price" -> DoubleType,
        "info" -> StringType
      )
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source,
        header = true, keepOriginTimeColumn = true, sorted = true)
      val ts = timeseriesRdd.collect()
      // we want to keep the time column first, but the order isn't guaranteed
      assert(timeseriesRdd.schema.fieldIndex(TimeSeriesRDD.timeColumnName) == 0)
      assert(timeseriesRdd.schema.fields.toSet == expectedSchema.fields.toSet)
      assert(ts(0).getAs[Long](TimeSeriesRDD.timeColumnName) == 1000L)
      assert(ts(0).getAs[Integer]("tid") == 7)
      assert(ts(0).getAs[Double]("price") == 0.5)
      assert(ts.forall(_.getAs[String]("info") == "test"))
      assert(ts.length == 12)
    }
  }

  it should "read an unsorted CSV file with header" in {
    val ts1 = SpecUtils.withResource("/timeseries/csv/PriceWithHeaderUnsorted.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, header = true, sorted = false)
      timeseriesRdd.collect()
    }
    val ts2 = SpecUtils.withResource("/timeseries/csv/PriceWithHeader.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source, header = true, sorted = true)
      timeseriesRdd.collect()
    }
    assert(ts1.length == ts2.length)
    assert(ts1.deep == ts2.deep)
  }

  it should "correctly convert SQL TimestampType" in {
    val ts1 = SpecUtils.withResource("/timeseries/csv/TimeStampsWithHeader.csv") { source =>
      val timeseriesRdd = CSV.from(sqlContext, "file://" + source,
        header = true, sorted = false, dateFormat = "yyyyMMdd HH:mm:ss.SSS")
      val first = timeseriesRdd.first()

      // 02 Jan 2008 00:00:00 GMT
      assert(first.getAs[Long]("time") == 1199232000000000000L)
    }
  }
}
