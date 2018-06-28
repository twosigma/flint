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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.CycleColumnSpec._
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DataType, DoubleType, IntegerType, LongType }

import scala.concurrent.duration.NANOSECONDS

class CycleColumnSpec extends MultiPartitionSuite with TimeSeriesTestData {

  behavior of "addColumnsForCycle"

  it should "allow inline tuple Seq[Row] => Map[Row, T] syntax" in {
    val df = testData.addColumns("test1" -> DoubleType -> { _: Row => 1.0 })
    val actualDf = df.addColumnsForCycle(
      "test2" -> DoubleType -> { rows: Seq[Row] => rows.map(row => row -> (row.getAs[Double]("test1") + 1)).toMap }
    )
    val actualValues = actualDf.collect().map(_.getAs[Double]("test2"))
    assert(actualValues.forall(_ === 2.0))
  }

  it should "allow inline tuple Seq[Row] => Seq[T] syntax" in {
    val df = testData.addColumns("test1" -> DoubleType -> { _: Row => 1.0 })
    val actualDf = df.addColumnsForCycle(
      "test2" -> DoubleType -> { rows: Seq[Row] => rows.map(_.getAs[Double]("test1") + 1) }
    )
    val actualValues = actualDf.collect().map(_.getAs[Double]("test2"))
    assert(actualValues.forall(_ === 2.0))
  }

  it should "support a predefined case object" in {
    val df = testData.addColumns("test1" -> DoubleType -> { _: Row => 1.0 })
    val actualDf = df.addColumnsForCycle(FoobarCycleColumn)
    val actualValues = actualDf.collect().map(_.getAs[Int]("test"))
    assert(actualValues === Seq(1, 2, 3, 1, 2, 1, 2, 3, 4, 5, 1, 1, 2))
  }

  it should "support applying a name to an UnnamedCycleColumn" in {
    val df = testData.addColumns("test1" -> DoubleType -> { _: Row => 1.0 })

    val actualDf = df.addColumnsForCycle("test2" -> addOne("test1"))
    val actualValues = actualDf.collect().map(_.getAs[Double]("test2"))
    assert(actualValues.forall(_ === 2.0))
  }

  it should "return null for any missing rows in a map-form CycleColumn" in {
    val df = TimeSeriesRDD.fromDF(
      sqlContext.createDataFrame(sc.parallelize(Seq(
        Row(1L, 1.0),
        Row(2L, 2.0),
        Row(2L, 3.0),
        Row(3L, 4.0),
        Row(3L, 5.0),
        Row(3L, 6.0)
      )), Schema.of("time" -> LongType, "value" -> DoubleType))
    )(isSorted = true, NANOSECONDS)

    // A cycle function that returns a value for every other row.
    // mapFormToSeqForm should fill in the undefined rows in the map with null.
    val udf = CycleColumn.unnamed(DoubleType, CycleColumn.mapFormToSeqForm({ rows: Seq[Row] =>
      rows.zipWithIndex
        .filter { case (_, i) => i % 2 == 0 }
        .map { case (row, _) => row -> 100.0 }
        .toMap
    }))

    val actualDf = df.addColumnsForCycle("newColumn" -> udf)
    val actualValues = actualDf.collect().map(_.getAs[java.lang.Double]("newColumn"))
    assert(actualValues === Seq(100.0, 100.0, null, 100.0, null, 100.0))
  }

  it should "return null for any missing rows in a seq-form CycleColumn" in {
    val df = TimeSeriesRDD.fromDF(
      sqlContext.createDataFrame(sc.parallelize(Seq(
        Row(1L, 1.0),
        Row(2L, 2.0),
        Row(2L, 3.0),
        Row(3L, 4.0),
        Row(3L, 5.0),
        Row(3L, 6.0)
      )), Schema.of("time" -> LongType, "value" -> DoubleType))
    )(isSorted = true, NANOSECONDS)

    // A cycle function that returns only a single row
    val udf = CycleColumn.unnamed(DoubleType, { rows: Seq[Row] => Seq(1.0) })
    val actualDf = df.addColumnsForCycle("newColumn" -> udf)
    val actualValues = actualDf.collect().map(_.getAs[java.lang.Double]("newColumn"))
    assert(actualValues === Seq(1.0, 1.0, null, 1.0, null, null))
  }

}

object CycleColumnSpec {

  /**
   * An example of an [[CycleColumn.UnnamedCycleColumn]].
   */
  def addOne(sourceColumn: String): CycleColumn.UnnamedCycleColumn =
    CycleColumn.unnamed(DoubleType, { rows: Seq[Row] =>
      rows.map { row =>
        val idx = row.fieldIndex(sourceColumn)
        row.getAs[Double](idx) + 1
      }
    })

  /**
   * An example of a built-in column that returns integers as its result.
   */
  case object FoobarCycleColumn extends CycleColumn {
    val name: String = "test"
    val dataType: DataType = IntegerType
    def applyCycle(input: Seq[Row]): Seq[Any] = 1 to input.size
  }
}
