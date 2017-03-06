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

import org.apache.spark.sql.types.{ StructField, LongType, StructType }
import org.apache.spark.sql.{ DataFrame, SQLImplicits, SQLContext }
import scala.concurrent.duration.NANOSECONDS

private[flint] trait TimeSeriesTestData {
  protected def sqlContext: SQLContext

  // Helper object to import SQL implicits without a concrete SQLContext
  private object internalImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = sqlContext
  }

  import internalImplicits._
  import TimeSeriesTestData._

  private def changeTimeNotNull(df: DataFrame): DataFrame = {
    val schema = StructType(df.schema.map {
      case StructField("time", LongType, false, meta) => StructField("time", LongType, true, meta)
      case t => t
    })

    sqlContext.createDataFrame(df.rdd, schema)
  }

  protected lazy val testData: TimeSeriesRDD = {
    val df = sqlContext.sparkContext.parallelize(
      TestData(1000) ::
        TestData(1000) ::
        TestData(1000) ::
        TestData(2000) ::
        TestData(2000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(3000) ::
        TestData(4000) ::
        TestData(5000) ::
        TestData(5000) :: Nil
    ).toDF()
    TimeSeriesRDD.fromDF(changeTimeNotNull(df))(isSorted = true, timeUnit = NANOSECONDS)
  }
}

private[flint] object TimeSeriesTestData {
  case class TestData(time: Long)
}
