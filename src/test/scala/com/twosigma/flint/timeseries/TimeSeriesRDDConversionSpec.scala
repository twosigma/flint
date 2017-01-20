/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.SharedSparkContext
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{ GenericRowWithSchema => ExternalRow }
import org.scalatest.tagobjects.Slow

class TimeSeriesRDDConversionSpec extends FlatSpec with SharedSparkContext {

  // The largest prime < 100
  private val defaultNumPartitions = 97

  // The 10000-th prime.
  private val defaultNumRows = 104729

  private def createDataFrame(isSorted: Boolean = true)(implicit sqlContext: SQLContext): DataFrame = {
    val n = defaultNumRows
    val schema = Schema("value" -> DoubleType)
    val rdd: RDD[Row] = sqlContext.sparkContext.parallelize(1 to n, defaultNumPartitions).map { i =>
      val data: Array[Any] = if (isSorted) {
        Array((i / 100).toLong, i.toDouble)
      } else {
        Array(((i + 1 - n) / 100).toLong, i.toDouble)
      }
      new ExternalRow(data, schema)
    }
    sqlContext.createDataFrame(rdd, schema)
  }

  "TimeSeriesRDD" should "convert from a sorted DataFrame correctly" taggedAs (Slow) in {
    implicit val _sqlContext = sqlContext
    (1 to 10).foreach {
      i =>
        val tsRdd = TimeSeriesRDD.fromDF(createDataFrame(isSorted = true))(isSorted = true, TimeUnit.NANOSECONDS)
        assert(tsRdd.count() == defaultNumRows)
    }
    (1 to 10).foreach {
      i =>
        val tsRdd = TimeSeriesRDD.fromDF(createDataFrame(isSorted = true))(isSorted = false, TimeUnit.NANOSECONDS)
        assert(tsRdd.count() == defaultNumRows)
    }
    (1 to 10).foreach {
      i =>
        val tsRdd = TimeSeriesRDD.fromDF(createDataFrame(isSorted = false))(isSorted = false, TimeUnit.NANOSECONDS)
        assert(tsRdd.count() == defaultNumRows)
    }
    (1 to 10).foreach {
      i =>
        val tsRdd = TimeSeriesRDD.fromDF(
          createDataFrame(isSorted = false).sort("time")
        )(
            isSorted = true, TimeUnit.NANOSECONDS
          )
        assert(tsRdd.count() == defaultNumRows)
    }
  }
}
