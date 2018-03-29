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

package com.twosigma.flint.timeseries.io.read

import java.util.concurrent.TimeUnit
import javax.annotation.Nullable

import org.joda.time.DateTimeZone
import com.twosigma.flint.annotation.PythonApi
import com.twosigma.flint.timeseries.TimeSeriesRDD
import com.twosigma.flint.timeseries.time.TimeFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration

/**
 * Reader builder for Flint data sources. Used by the Python bindings
 * to maintain the same API across both Scala and Python.
 */
class ReadBuilder(
  @transient protected val sc: SparkContext,
  val parameters: ReadBuilder.ReadBuilderParameters
) extends Serializable {

  /**
   * Explicitly defined primary constructor instead of default parameters for
   * access from Python.
   */
  @PythonApi
  def this() =
    this(
      SparkSession.builder().getOrCreate().sparkContext,
      new ReadBuilder.ReadBuilderParameters()
    )

  // This class is marked with the Serializable interface to ensure closures created
  // within the class pass the closure Serializable check within Spark.

  /**
   * Add an input option for the underlying data source.
   *
   * If the value is null, then the option for `key` will be unset.
   */
  @PythonApi
  def option(key: String, @Nullable value: String): this.type = {
    parameters.option(key, Option(value))
    this
  }

  /**
   * Add an input option for the underlying data source.
   */
  def option(key: String, value: Int): this.type =
    option(key, value.toString)

  /**
   * Add multiple input options for the underlying data source using a map.
   */
  def options(map: Map[String, String]): this.type = {
    map.foreach { case (key, value) => option(key, value) }
    this
  }

  /**
   * Set the begin / end date range using strings.
   *
   * @param begin The inclusive begin time, or null for none, e.g., "20170101"
   * @param end   The exclusive end time, or null for none, e.g., "20170201"
   * @param timeZone Optional time zone. Default: UTC.
   * @return this [[ReadBuilder]]
   *
   * @see [[TimeFormat.parseNano()]] for parsing string to nanoseconds.
   */
  def range(
    @Nullable begin: String,
    @Nullable end: String,
    timeZone: String = DateTimeZone.UTC.getID
  ): this.type = {
    val tz = DateTimeZone.forID(timeZone)
    val beginNanos = Option(begin).map(TimeFormat.parseNano(_, tz))
    val endNanos = Option(end).map(TimeFormat.parseNano(_, tz))
    range(beginNanos, endNanos)
  }

  /**
   * Set the begin / end date range using nullable [[java.lang.Long]] parameters.
   *
   * @param beginNanos Inclusive begin time in nanoseconds, or null for none, e.g., "20170101"
   * @param endNanos   Exclusive end time in nanoseconds, or null for none, e.g., "20170201"
   * @return this [[ReadBuilder]]
   */
  @PythonApi
  def range(@Nullable beginNanos: java.lang.Long, @Nullable endNanos: java.lang.Long): this.type =
    range(Option(beginNanos).map(_.toLong), Option(endNanos).map(_.toLong))

  /**
   * Set the begin / end date range using ``Option[Long]``.
   *
   * @param beginNanosOpt If defined, the inclusive end time in nanoseconds.
   * @param endNanosOpt If defined, the exclusive end time in nanoseconds.
   * @return this [[ReadBuilder]]
   */
  private[read] def range(beginNanosOpt: Option[Long], endNanosOpt: Option[Long]): this.type = {
    parameters.range = parameters.range.copy(rawBeginNanosOpt = beginNanosOpt, rawEndNanosOpt = endNanosOpt)
    this
  }

  /**
   * Set the time distance to expand begin / end date range.
   *
   * If called multiple times, only the last call is effective.
   *
   * @param begin A string that specifies the time distance to expand the begin time, e.g., "7days"
   * @param end A string that specifies the time distance to expand the end time, e.g., "7days"
   * @return
   */
  def expand(@Nullable begin: String = null, @Nullable end: String = null): this.type = {
    val expandBeginNanos = Option(begin).map(begin => Long.box(Duration(begin).toNanos)).orNull
    val expandEndNanos = Option(end).map(end => Long.box(Duration(end).toNanos)).orNull
    expand(expandBeginNanos, expandEndNanos)
    this
  }

  @PythonApi
  def expand(@Nullable beginNanos: java.lang.Long, @Nullable endNanos: java.lang.Long): this.type = {
    parameters.range = parameters.range.copy(
      expandBeginNanosOpt = Option(beginNanos).map(_.toLong),
      expandEndNanosOpt = Option(endNanos).map(_.toLong)
    )
    this
  }

  /**
   * Read a parquet file into a [[TimeSeriesRDD]].
   *
   * Supported options:
   * <dl>
   *   <dt>range (optional)</dt>
   *   <dd>
   *     Set the inclusive-begin and exclusive-end time range. Begin and end are
   *     optional and either begin, end, or both begin and end can be omitted.
   *     If omitted, no boundary on the underlying Parquet files will be set.
   *   </dd>
   *   <dt>isSorted (optional)</dt>
   *   <dd>Whether the Parquet files are sorted by time or not. Default: "true"</dd>
   *   <dt>timeUnit (optional)</dt>
   *   <dd>Time unit of the time column. Default: "ns"</dd>
   *   <dt>timeColumn (optional)</dt>
   *   <dd>Column in parquet table that specifies time. Default: "time".</dd>
   *   <dt>columns (optional)</dt>
   *   <dd>
   *     A subset of columns to retain from the parquet table separated by commas.
   *     Specifying a subset of columns can greatly improve performance by 10x
   *     compared to reading all columns in a set of parquet files.
   *     Default: all columns are retained.
   *   </dd>
   * </dl>
   *
   * Example usage:
   * {{{
   *   read.builder(sc)
   *    .option("columns", "x,y,z")
   *    .parquet("/path/to/parquet", "/path/to/more/parquet")
   * }}}
   *
   * @param paths One or more paths containing parquet files. Paths can contain wildcards.
   */
  @PythonApi
  def parquet(paths: String*): TimeSeriesRDD =
    TimeSeriesRDD.fromParquet(
      sc,
      paths,
      isSorted = parameters.extraOptions.getOrElse("isSorted", "true").toBoolean,
      beginNanos = parameters.range.beginNanosOpt,
      endNanos = parameters.range.endNanosOpt,
      columns = parameters.columns,
      timeUnit = parameters.timeUnit,
      timeColumn = parameters.timeColumn
    )

}

object ReadBuilder {

  val TIME_UNIT: String = "timeUnit"
  val TIME_COLUMN: String = "timeColumn"
  val COLUMNS: String = "columns"

  /**
   * Map storing default options and their values.
   */
  private[read] val defaultOptions: Map[String, String] = Map(
    TIME_UNIT -> "ns",
    TIME_COLUMN -> "time"
  )

  /**
   * Subclass of [[Parameters]] with accessor methods that are used by the methods
   * in [[ReadBuilder]] and in the Python API.
   */
  private[read] class ReadBuilderParameters(additionalDefaults: Map[String, String] = Map.empty)
    extends Parameters(defaultOptions ++ additionalDefaults) {

    /** Returns the value of the `TIME_UNIT` option as a [[TimeUnit]] */
    def timeUnit: TimeUnit = Duration("1" + timeUnitString).unit

    /** Returns the value of the `TIME_UNIT` option as a string */
    @PythonApi
    def timeUnitString: String = extraOptions(TIME_UNIT)

    /** Returns the value of the 'TIME_COLUMN' option */
    @PythonApi
    def timeColumn: String = extraOptions(TIME_COLUMN)

    /**
     * Returns a sequence of columns if defined, split at ',' and with any whitespace trimmed.
     */
    def columns: Option[Seq[String]] =
      extraOptions.get(COLUMNS).map(_.split(',').map(_.trim))

    /**
     * Returns `columns` as a Scala sequence if defined, otherwise null.
     *
     * Used by the Python API.
     */
    @PythonApi
    def columnsOrNull: Seq[String] = columns.orNull
  }

}
