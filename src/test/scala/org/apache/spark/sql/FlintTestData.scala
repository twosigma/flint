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

package org.apache.spark.sql
import org.apache.spark.sql.functions.{ udf, sum }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.percent_rank

trait FlintTestData {
  protected def sqlContext: SQLContext

  private object internalImplicits extends SQLImplicits {
    override protected def _sqlContext: SQLContext = sqlContext
  }

  import internalImplicits._
  import FlintTestData._

  protected lazy val testData: DataFrame = {
    val df = sqlContext.sparkContext.parallelize(
      (0 to 97).map(i => TestData(i.toLong, i.toDouble))
    ).toDF()
    df
  }

  protected lazy val testData2: DataFrame = {
    val df = sqlContext.sparkContext.parallelize(
      (0 to 101).map(i => TestData2(i.toLong, i.toDouble, -i.toDouble))
    ).toDF()
    df
  }

  protected lazy val testDataCached: DataFrame = {
    val df = DFConverter.newDataFrame(testData)
    df.cache
    df.count
    df
  }

  protected val withTime2Column = { df: DataFrame => df.withColumn("time2", df("time") * 2) }
  protected val withTime3ColumnUdf = { df: DataFrame =>
    val testUdf = udf({ time: Long => time * 2 })
    df.withColumn("time3", testUdf(df("time")))
  }
  protected val selectV = { df: DataFrame => df.select("v") }
  protected val selectExprVPlusOne = { df: DataFrame => df.selectExpr("v + 1 as v") }
  protected val filterV = { df: DataFrame => df.filter(df("v") > 0) }

  protected val orderByTime = { df: DataFrame => df.orderBy("time") }
  protected val orderByV = { df: DataFrame => df.orderBy("v") }
  protected val addRankColumn = { df: DataFrame =>
    df.withColumn("rank", percent_rank().over(Window.partitionBy("time").orderBy("v")))
  }
  protected val selectSumV = { df: DataFrame => df.select(sum("v")) }
  protected val selectExprSumV = { df: DataFrame => df.selectExpr("sum(v)") }
  protected val groupByTimeSumV = { df: DataFrame => df.groupBy("time").agg(sum("v").alias("v")) }
  protected val repartition = { df: DataFrame => df.repartition(10) }
  protected val coalesce = { df: DataFrame => df.coalesce(5) }

  protected val cache = { df: DataFrame => df.cache(); df.count(); df }
  protected val unpersist = { df: DataFrame => df.unpersist() }
}

object FlintTestData {
  case class TestData(time: Long, v: Double)
  case class TestData2(time: Long, v: Double, v2: Double)
}
