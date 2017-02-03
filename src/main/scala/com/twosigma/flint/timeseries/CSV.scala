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

import java.nio.charset.Charset
import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types._

import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

/**
 * Parse a CSV file into a [[TimeSeriesRDD]].
 *
 * Example usage:
 *
 * Read and parse a gzipped CSV file with header and specific time format into a [[TimeSeriesRDD]].
 * {{{
 * import com.twosigma.flint.timeseries.CSV
 *
 * CSV.from(
 *   sqlContext,
 *   s"file://foo/bar/data.csv",
 *   header = true,
 *   dateFormat = "yyyyMMdd HH:mm:ss.SSS",
 *   codec = "gzip",
 *   sorted = true
 * )
 * }}}
 */
object CSV {
  // scalastyle:off parameter.number
  /**
   * Parse a CSV file into a [[TimeSeriesRDD]].
   *
   * @param sqlContext               The Spark SQLContext.
   * @param filePath                 Location of file. Similar to Spark can accept standard Hadoop globbing
   *                                 expressions or a local file.
   * @param sorted                   A flag that specifies if rows in the CSV file have been sorted by time column.
   * @param timeColumnName           Column name that specifies which column is the time column when
   *                                 there is header or when a schema is given.
   * @param timeUnit                 Time unit when a time column is given with a numeric type like
   *                                 Long or Integer, with default value [[scala.concurrent.duration.NANOSECONDS]].
   * @param header                   When set to true the first line of files will be used to name columns and will
   *                                 not be included in data. Default value is false.
   * @param delimiter                Delimiter that columns are delimited by which could be any character. Default
   *                                 is ','.
   * @param quote                    By default the quote character is `"`, but can be set to any character.
   *                                 Delimiters inside quotes are ignored.
   * @param escape                   By default the escape character is `\`, but can be set to any character.
   *                                 Escaped quote characters are ignored.
   * @param parserLib                By default it is "commons" can be set to "univocity" to use that library for
   *                                 CSV parsing.
   * @param mode                     Determines the parsing mode. By default it is PERMISSIVE. Possible values are:
   *                                 PERMISSIVE: tries to parse all lines: nulls are inserted for missing tokens and
   *                                 extra tokens are ignored; DROPMALFORMED: drops lines which have fewer or more
   *                                 tokens than expected or tokens which do not match the schema;
   *                                 FAILFAST: aborts with a RuntimeException if encounters any malformed line.
   * @param charset                  Defaults to 'UTF-8' but can be set to other valid charset names.
   * @param comment                  Skip lines beginning with this character. Default is "#".
   * @param ignoreLeadingWhiteSpace  By default false to ignore the leading white space.
   * @param ignoreTrailingWhiteSpace By default false to ignore the trailing white space.
   * @param schema                   The schema for the CSV file. If the schema is given, use it otherwise infer the
   *                                 schema from the data itself.
   * @param dateFormat               The pattern string to parse the date time string under the time column.
   *                                 Defaults to "yyyy-MM-dd'T'HH:mm:ss.SSSZZ". For example, "2016-01-01T12:00:00+00:00"
   * @param keepOriginTimeColumn     The schema of return [[TimeSeriesRDD]] will always have a column named "time"
   *                                 with LongType. The original time column will not be kept by default.
   * @param codec                    compression codec to use when reading from file. Should be the fully qualified
   *                                 name of a class implementing [[org.apache.hadoop.io.compress.CompressionCodec]] or
   *                                 one of case-insensitive shorten names (bzip2, gzip, lz4, and snappy).
   *                                 Defaults to no compression when a codec is not specified.
   */
  def from(
    sqlContext: SQLContext,
    filePath: String,
    // TimeSeriesRDD specific parameters
    sorted: Boolean,
    timeColumnName: String = TimeSeriesRDD.timeColumnName,
    timeUnit: TimeUnit = NANOSECONDS,
    // Spark-csv specific parameters
    header: Boolean = false,
    delimiter: Char = ',',
    quote: Char = '"',
    escape: Char = '\\',
    comment: Char = '#',
    mode: String = "PERMISSIVE",
    parserLib: String = "COMMONS",
    ignoreLeadingWhiteSpace: Boolean = false,
    ignoreTrailingWhiteSpace: Boolean = false,
    charset: String = Charset.forName("UTF-8").name(),
    schema: StructType = null,
    dateFormat: String = null,
    keepOriginTimeColumn: Boolean = false,
    codec: String = null
  ): TimeSeriesRDD = {
    // scalastyle:on parameter.number
    // TODO: In Spark 2, we should use the following CSV reader instead of databricks one
    //       "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat"
    val reader = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", header.toString)
      .option("delimiter", delimiter.toString)
      .option("quote", quote.toString)
      .option("escape", escape.toString)
      .option("comment", comment.toString)
      .option("mode", mode)
      .option("parserLib", parserLib)
      .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace.toString)
      .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace.toString)
      .option("charset", charset)

    if (dateFormat != null) {
      reader.option("dateFormat", dateFormat)
      reader.option("timestampFormat", dateFormat)
    }

    // If the schema is given, use it otherwise infer the schema from the data itself.
    if (schema == null) {
      reader.option("inferSchema", "true")
    } else {
      reader.schema(schema)
    }
    // Set the codec if provided.
    if (codec != null) {
      reader.option("codec", codec)
    }
    var df = reader.load(filePath)

    // If there is no header and there is no schema given, it will just use the first column
    // as the `time` column.
    if (!header && schema == null) {
      // If the CSV file doesn't include a header, the infer columns will be
      // named as `C0`, `C1`, `C2`, ...
      df = df.withColumnRenamed(df.schema.fields(0).name, timeColumnName)
    }

    require(
      df.schema.fields.exists(_.name == timeColumnName),
      s"The row schema does not have a column named $timeColumnName"
    )

    val timeColumnType = df.schema(timeColumnName).dataType
    val timeColumnUnit = if (timeColumnType == DataTypes.TimestampType) { MILLISECONDS } else { timeUnit }

    if (keepOriginTimeColumn) {
      val renamedTimeColumnName = s"${timeColumnName}_"
      df = df.withColumn(renamedTimeColumnName, df(timeColumnName))
    }

    val dfConverted = convertTimeToLong(df, timeColumnName)
    TimeSeriesRDD.fromDF(dfConverted)(isSorted = sorted, timeColumnUnit, timeColumn = timeColumnName)
  }

  private[this] def convertTimeToLong(dataFrame: DataFrame, timeColumnName: String): DataFrame = {
    val dataType = dataFrame.schema(timeColumnName).dataType

    val intToLong = udf[Long, Int](_.toLong)
    val timestampToLong = udf[Long, Timestamp](_.getTime)

    dataType match {
      case DataTypes.LongType =>
        dataFrame
      case DataTypes.IntegerType =>
        dataFrame.withColumn(timeColumnName, intToLong(dataFrame(timeColumnName)))
      case DataTypes.TimestampType =>
        dataFrame.withColumn(timeColumnName, timestampToLong(dataFrame(timeColumnName)))

      case _ => sys.error(s"Columns of ${dataType.typeName} type are not supported as a `time` column.")
    }
  }
}
