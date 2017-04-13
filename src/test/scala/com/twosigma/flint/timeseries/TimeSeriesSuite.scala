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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.FlintSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.scalactic.{ Equality, TolerantNumerics }
import play.api.libs.json.{ JsValue, Json }

import scala.collection.mutable
import scala.io.Source

object TimeSeriesSuite {}

trait TimeSeriesSuite extends FlintSuite {

  /**
   * The default partition parallelism
   */
  val defaultPartitionParallelism: Int = 5

  /**
   * The default relative path of resource directory
   */
  val defaultResourceDir: String = ""

  /**
   * The default additive precision
   */
  val defaultAdditivePrecision: Double = 1.0e-8

  implicit val doubleEquality: Equality[Double] =
    TolerantNumerics.tolerantDoubleEquality(defaultAdditivePrecision)

  /**
   * Assert if two arrays of doubles are equal within additive precision `defaultAdditivePrecision`.
   */
  def assertAlmostEquals(thisArray: Array[Double], thatArray: Array[Double]): Unit =
    (thisArray zip thatArray).map { case (x, y) => assertAlmostEquals(x, y) }

  /**
   * Assert if two doubles are equal within additive precision `defaultAdditivePrecision`.
   */
  def assertAlmostEquals(x: Double, y: Double): Unit = assert(x.isNaN && y.isNaN || x === y)

  /**
   * Assert if two rows have the same values within additive precision `defaultAdditivePrecision`.
   *
   * Only support columns of types [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]] and
   * [[ArrayType]] of [[IntegerType]], [[LongType]], [[FloatType]], [[DoubleType]] respectively.
   */
  def assertAlmostEquals(thisRow: Row, thatRow: Row): Unit = {
    assert(thisRow.schema == thatRow.schema)
    thisRow.schema.foreach { col =>
      col.dataType match {
        case BooleanType =>
          assert(thisRow.getAs[Boolean](col.name) == thatRow.getAs[Boolean](col.name))
        case IntegerType =>
          assert(thisRow.getAs[Int](col.name) == thatRow.getAs[Int](col.name))
        case LongType =>
          assert(
            thisRow.getAs[Long](col.name) == thatRow.getAs[Long](col.name)
          )
        case FloatType =>
          assert(
            thisRow.getAs[Float](col.name) === thatRow.getAs[Float](col.name)
          )
        case DoubleType =>
          assertAlmostEquals(thisRow.getAs[Double](col.name), thatRow.getAs[Double](col.name))
        case ArrayType(IntegerType, _) =>
          assert(
            thisRow.getAs[mutable.WrappedArray[Int]](col.name).deep ==
              thatRow.getAs[mutable.WrappedArray[Int]](col.name).deep
          )
        case ArrayType(LongType, _) =>
          assert(
            thisRow.getAs[mutable.WrappedArray[Long]](col.name).deep ==
              thatRow.getAs[mutable.WrappedArray[Long]](col.name).deep
          )
        case ArrayType(FloatType, _) =>
          assertAlmostEquals(
            thisRow
              .getAs[mutable.WrappedArray[Float]](col.name)
              .toArray
              .map(_.toDouble),
            thatRow
              .getAs[mutable.WrappedArray[Float]](col.name)
              .toArray
              .map(_.toDouble)
          )
        case ArrayType(DoubleType, _) =>
          assertAlmostEquals(
            thisRow.getAs[mutable.WrappedArray[Double]](col.name).toArray,
            thatRow.getAs[mutable.WrappedArray[Double]](col.name).toArray
          )
        case ArrayType(StringType, _) =>
          assert(
            thisRow.getAs[mutable.WrappedArray[String]](col.name).deep ==
              thatRow.getAs[mutable.WrappedArray[String]](col.name).deep
          )
        case dataType =>
          assert(
            false,
            s"Not supported $dataType for comparing two sql rows under column ${col.name}."
          )
      }
    }
  }

  /**
   * Assert two [[TimeSeriesRDD]] are equal row by row in order.
   */
  def assertAlmostEquals(thisTSRdd: TimeSeriesRDD, otherTSRdd: TimeSeriesRDD): Unit =
    (thisTSRdd.collect() zip otherTSRdd.collect()).foreach {
      case (thisRow, thatRow) => assertAlmostEquals(thisRow, thatRow)
    }

  /**
   * Read a data set of time series from a CSV file in resource as a [[TimeSeriesRDD]]
   * where the number of partitions is `defaultPartitionParallelism`.
   *
   * @param filepath   The file path relative to `defaultResourceDir`. If the file path ends with ".gz",
   *                   it will try to parse the CSV file with codec "gzip".
   * @param schema     The schema of the data in the CSV file.
   * @param header     Whether the CSV file has a header.
   * @param sorted     Whether the data set of time series in CSV file has been sorted by their timestamps.
   * @param dateFormat Format that will be used to check if values of columns are date time and then parse
   *                   accordingly.
   * @return a [[TimeSeriesRDD]] parsed from the CSV file in the resource.
   */
  def fromCSV(
    filepath: String,
    schema: StructType = null,
    header: Boolean = true,
    sorted: Boolean = true,
    dateFormat: String = "yyyy-MM-dd HH:mm:ss.S"
  ): TimeSeriesRDD = withResource(s"$defaultResourceDir/$filepath") { source =>
    var codec: String = null
    if (filepath.endsWith(".gz")) {
      codec = "gzip"
    }
    CSV.from(
      sqlContext,
      s"file://$source",
      header = header,
      sorted = sorted,
      schema = schema,
      dateFormat = dateFormat,
      codec = codec
    ).repartition(defaultPartitionParallelism)
  }

  def fromParquet(
    filepath: String,
    sorted: Boolean = true
  ): TimeSeriesRDD = withResource(s"$defaultResourceDir/$filepath") { source =>
    var codec: String = null
    if (filepath.endsWith(".gz")) {
      codec = "gzip"
    }
    TimeSeriesRDD.fromParquet(
      sc,
      s"file://$source"
    )(
        isSorted = sorted,
        timeUnit = TimeUnit.NANOSECONDS
      ).repartition(defaultPartitionParallelism)
  }

  /**
   * Parse a file as json.
   *
   * @param filepath The json file path relative to `defaultResourceDir`.
   * @return the parsed [[JsValue]]
   */
  def asJson(filepath: String): JsValue =
    withResource(s"$defaultResourceDir/$filepath") { source =>
      Json.parse(Source.fromFile(source).mkString)
    }

  /**
   * Assert two [[TimeSeriesRDD]] are equal. Works with comparing rows with nested structure.
   */
  def assertEquals(rdd1: TimeSeriesRDD, rdd2: TimeSeriesRDD): Unit = {
    assert(rdd1.schema == rdd2.schema)
    assert(rdd1.rdd.collect.map(prepareRow).toSeq == rdd2.rdd.collect.map(prepareRow).toSeq)
  }

  /**
   * Taken from ttps://github.com/apache/spark/blob/f48461ab2bdb91cd00efa5a5ec4b0b2bc361e7a2/sql/core/src/test/scala/org/apache/spark/sql/QueryTest.scala#L299
   */
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  protected def insertNullRows(rdd: TimeSeriesRDD, cols: String*): TimeSeriesRDD = {
    val schema = rdd.schema
    val newRdd = rdd.rdd.flatMap{
      case row: Row =>
        val nullRows = cols.map(rdd.schema.fieldIndex).map {
          case index =>
            new GenericRowWithSchema(row.toSeq.updated(index, null).toArray, schema)
        }
        row +: nullRows
    }
    TimeSeriesRDD.fromRDD(newRdd, schema)(true, scala.concurrent.duration.NANOSECONDS)
  }
}
