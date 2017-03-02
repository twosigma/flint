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

import com.twosigma.flint.FlintSuite
import org.apache.spark.sql.types.StructType
import org.scalactic.{ Equality, TolerantNumerics }
import play.api.libs.json.{ Json, JsValue }

import scala.io.Source

object TimeSeriesSuite {

}

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

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(defaultAdditivePrecision)

  /**
   * Assert if two arrays of doubles are equal within additive precision `defaultAdditivePrecision`.
   */
  def assertEquals(a: Array[Double], b: Array[Double]): Unit = assert(a.corresponds(b)(_ === _))

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
  ): TimeSeriesRDD = withResource(s"$defaultResourceDir/$filepath") {
    source =>
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

  /**
   * Parse a file as json.
   *
   * @param filepath The json file path relative to `defaultResourceDir`.
   * @return the parsed [[JsValue]]
   */
  def asJson(filepath: String): JsValue = withResource(s"$defaultResourceDir/$filepath") {
    source => Json.parse(Source.fromFile(source).mkString)
  }
}
