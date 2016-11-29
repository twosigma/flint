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

import com.twosigma.flint.annotation.PythonApi
import com.twosigma.flint.rdd.{ CloseOpen, Conversion, KeyPartitioningType, OrderedRDD, RangeSplit }
import com.twosigma.flint.timeseries.row.{ InternalRowUtils, Schema }
import com.twosigma.flint.timeseries.summarize.{ OverlappableSummarizer, OverlappableSummarizerFactory, SummarizerFactory }
import com.twosigma.flint.timeseries.time.TimeFormat
import com.twosigma.flint.timeseries.window.{ ShiftTimeWindow, TimeWindow, Window }

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Dependency, SparkContext }
import org.apache.spark.sql.{ CatalystTypeConvertersWrapper, DFConverter, DataFrame, Row, SQLContext }
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, GenericRowWithSchema => ERow }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration._

object TimeSeriesRDD {
  /**
   * The name of timestamp column. A [[TimeSeriesRDD]] always has such a column.
   */
  val timeColumnName: String = "time"

  /**
   * The field for the time column.
   */
  private[flint] val timeField: StructField = StructField(timeColumnName, LongType)

  /**
   * Checks if the schema has a field "time" with long type
   */
  private def requireSchema(schema: StructType): Unit = {
    require(
      schema.fields.contains(timeField),
      s"Schema $schema doesn't contain a column $timeField"
    )
  }

  /**
   * Provides a function to extract the timestamp from [[org.apache.spark.sql.Row]] as NANOSECONDS and convert
   * an external row object into an internal object. The input schema is expected
   * to contain a time column with Long type whose time unit is specified by `timeUnit`.
   *
   * @param schema   The schema of the rows as input of returning function.
   * @param timeUnit The unit of time (as Long type) under the time column in the rows as input of the returning
   *                 function.
   * @return a function to extract the timestamps from [[org.apache.spark.sql.Row]].
   * @note  if timeUnit is different from [[NANOSECONDS]] then the function needs to make an extra copy
   *        of the row value.
   */
  private[this] def getExternalRowConverter(
    schema: StructType,
    timeUnit: TimeUnit
  ): Row => (Long, InternalRow) = {
    val timeColumnIndex = schema.fieldIndex(timeColumnName)
    val toInternalRow = CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)

    if (timeUnit == NANOSECONDS) {
      (row: Row) =>
        (row.getLong(timeColumnIndex), toInternalRow(row))
    } else {
      (row: Row) =>
        val values = row.toSeq.toArray
        val t = TimeUnit.NANOSECONDS.convert(row.getLong(timeColumnIndex), timeUnit)
        values.update(timeColumnIndex, t)
        val updatedRow = new GenericRow(values)

        (t, toInternalRow(updatedRow))
    }
  }

  /**
   * Similar to getRowConverter(), but used to convert a [[DataFrame]] into a [[TimeSeriesRDD]]
   *
   * @param schema          The schema of the input rows.
   * @param timeUnit        The unit of time (as Long type) under the time column in the rows as input of the returning
   *                        function.
   * @param requireCopy     Whether to require new row objects or reuse the existing ones.
   * @return                a function to convert [[InternalRow]] into a tuple.
   * @note                  if `requireNewCopy` is true or timeUnit is different from [[NANOSECONDS]] then the function
   *                        makes an extra copy of the row value. Otherwise it makes no copies of the row.
   */
  private[this] def getInternalRowConverter(
    schema: StructType,
    timeUnit: TimeUnit,
    requireCopy: Boolean
  ): InternalRow => (Long, InternalRow) = {
    val timeColumnIndex = schema.fieldIndex(timeColumnName)

    if (timeUnit == NANOSECONDS) {
      if (requireCopy) {
        (internalRow: InternalRow) => (internalRow.getLong(timeColumnIndex), internalRow.copy())
      } else {
        (internalRow: InternalRow) => (internalRow.getLong(timeColumnIndex), internalRow)
      }
    } else {
      (internalRow: InternalRow) =>
        val t = TimeUnit.NANOSECONDS.convert(internalRow.getLong(timeColumnIndex), timeUnit)
        // the line below creates a new InternalRow object
        val updatedRow = InternalRowUtils.update(internalRow, schema, timeColumnIndex -> t)
        (t, updatedRow)
    }
  }

  private[timeseries] def fromSeq(
    sc: SparkContext,
    rows: Seq[InternalRow],
    schema: StructType,
    isSorted: Boolean,
    numSlices: Int = 1
  ): TimeSeriesRDD = {
    requireSchema(schema)
    val timeIndex = schema.fieldIndex(timeColumnName)
    val rdd = sc.parallelize(
      rows.map { row => (row.getLong(timeIndex), row) }, numSlices
    )
    TimeSeriesRDD.fromInternalOrderedRDD(Conversion.fromSortedRDD(rdd), schema)
  }

  /**
   * Convert an [[OrderedRDD]] to a [[TimeSeriesRDD]].
   *
   * @param rdd    An [[OrderedRDD]] whose ordered keys are of Long types representing timestamps in NANOSECONDS.
   * @param schema The schema of rows in the give ordered `rdd`.
   * @return a [[TimeSeriesRDD]].
   */
  private[flint] def fromOrderedRDD(
    rdd: OrderedRDD[Long, Row],
    schema: StructType
  ): TimeSeriesRDD = {
    val converter = CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)
    TimeSeriesRDD.fromInternalOrderedRDD(rdd.mapValues {
      case (_, row) => converter(row)
    }, schema)
  }

  /**
   * Filter a [[org.apache.spark.sql.DataFrame]] to contain data within a time range
   *
   * @param dataFrame  A [[org.apache.spark.sql.DataFrame]].
   * @param begin      Begin time of the returned [[DataFrame]], inclusive
   * @param end        End time of the returnred [[DataFrame]], exclusive
   * @param timeUnit   Optional. The time unit under time column which could be
   *                   [[scala.concurrent.duration.NANOSECONDS]],[[scala.concurrent.duration.MILLISECONDS]], etc.
   * @param timeColumn Optional. The name of column in `df` that specifies the column name for time. Default: "time"
   * @return a [[org.apache.spark.sql.DataFrame]].
   */
  @PythonApi
  private[flint] def DFBetween(
    dataFrame: DataFrame,
    begin: String,
    end: String,
    timeUnit: TimeUnit = NANOSECONDS,
    timeColumn: String = timeColumnName
  ): DataFrame = {
    var df = dataFrame

    if (begin != null) {
      df = df.filter(df(timeColumn) >= timeUnit.convert(TimeFormat.parseNano(begin), NANOSECONDS))
    }
    if (end != null) {
      df = df.filter(df(timeColumn) < timeUnit.convert(TimeFormat.parseNano(end), NANOSECONDS))
    }

    df
  }

  /**
   * Convert an [[org.apache.spark.rdd.RDD]] to a [[TimeSeriesRDD]]. The input schema is
   * expected to contain a time column of Long type.
   *
   * @param rdd           An [[org.apache.spark.rdd.RDD]] expected to convert into a [[TimeSeriesRDD]].
   * @param schema        The schema of rows in the given `rdd`.
   * @param isSorted      A flag specifies if the rows in given `rdd` have been sorted by their timestamps. If it
   *                      is not sorted, it will involve sorting and thus shuffling.
   * @param timeUnit      The time unit under time column which could be [[scala.concurrent.duration.NANOSECONDS]],
   *                      [[scala.concurrent.duration.MILLISECONDS]], etc.
   * @param timeColumn    Optional. The name of column in `df` that specifies the column name for time.
   * @param isNormalized  Whether the `rdd` is normalized (it means sorted and partitioned in a such way that
   *                      all rows with timestamp `ts` (for each ts) belong to one partition).
   * @return a [[TimeSeriesRDD]].
   * @example
   * {{{
   * import com.twosigma.flint.timeseries.Schema
   * import scala.concurrent.duration.MILLISECONDS
   * val rdd = ... // A RDD whose rows have been sorted by their timestamps under "time" column.
   * val tsRdd = TimeSeriesRDD.fromRDD(
   *   rdd,
   *   schema = Schema("time" -> LongType, "price" -> DoubleType)
   * )(isSorted = true,
   *   MILLISECONDS
   * )
   * }}}
   */
  def fromRDD(
    rdd: RDD[Row],
    schema: StructType
  )(
    isSorted: Boolean,
    timeUnit: TimeUnit,
    timeColumn: String = timeColumnName,
    isNormalized: Boolean = false
  ): TimeSeriesRDD = {
    val newSchema = Schema.rename(schema, Seq(timeColumn -> timeColumnName))
    requireSchema(newSchema)
    val converter = getExternalRowConverter(newSchema, timeUnit)
    val pairRdd = rdd.map(converter)
    // TODO We should use KeyPartitioningType for the TimeSeriesRDD level APIs as well.
    TimeSeriesRDD.fromInternalOrderedRDD(
      OrderedRDD.fromRDD(pairRdd, KeyPartitioningType(isSorted, isNormalized)),
      newSchema
    )
  }

  /**
   * Convert an [[OrderedRDD]] with internal rows to a [[TimeSeriesRDD]].
   *
   * @param orderedRdd  An [[OrderedRDD]] with timestamps in NANOSECONDS and internal rows.
   * @param schema      The schema of the input `rdd`.
   * @return a [[TimeSeriesRDD]].
   */
  def fromInternalOrderedRDD(
    orderedRdd: OrderedRDD[Long, InternalRow],
    schema: StructType
  ): TimeSeriesRDD = {
    val sc = orderedRdd.sparkContext
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = DFConverter.toDataFrame(sqlContext, schema, orderedRdd)
    val ranges = orderedRdd.rangeSplits.map(_.range).toSeq

    new TimeSeriesRDDImpl(df, ranges)
  }

  /**
   * Creates a [[TimeSeriesRDD]] from a [[DataFrame]] and ranges.
   *
   * @param dataFrame   A dataframe.
   * @param ranges      Partition ranges.
   * @return a [[TimeSeriesRDD]].
   */
  private[flint] def fromDfWithRanges(
    dataFrame: DataFrame,
    ranges: Seq[CloseOpen[Long]]
  ): TimeSeriesRDD = new TimeSeriesRDDImpl(dataFrame, ranges)

  /**
   * Convert a [[org.apache.spark.sql.DataFrame]] into a pair RDD.
   *
   * @param dataFrame   The given [[org.apache.spark.sql.DataFrame]].
   * @param timeUnit    The time unit of the time column.
   * @param requireCopy Whether to require new row objects or reuse the existing ones.
   *
   * @return a pair RDD.
   */
  private[this] def dfToPairRdd(
    dataFrame: DataFrame,
    timeUnit: TimeUnit,
    requireCopy: Boolean
  ): RDD[(Long, InternalRow)] = {

    val schema = dataFrame.schema
    val internalRows = dataFrame.queryExecution.toRdd
    internalRows.mapPartitions { rows =>
      val converter = getInternalRowConverter(schema, timeUnit, requireCopy)
      rows.map(converter)
    }
  }

  /**
   * Unsafe method to create a [[OrderedRDD]] from a [[org.apache.spark.sql.DataFrame]] with timestamps
   * in NANOSECONDS.
   *
   * This method takes time ranges of the dataframe and avoid data scanning to find out the ranges.
   */
  private[timeseries] def dfToOrderedRdd(
    dataFrame: DataFrame,
    ranges: Seq[CloseOpen[Long]],
    requireCopy: Boolean
  ): OrderedRDD[Long, InternalRow] = {
    val pairRdd = dfToPairRdd(dataFrame, NANOSECONDS, requireCopy)
    OrderedRDD.fromRDD(pairRdd, ranges)
  }

  /**
   * Convert a [[org.apache.spark.sql.DataFrame]] to a [[TimeSeriesRDD]].
   *
   * @param dataFrame  The [[org.apache.spark.sql.DataFrame]] expected to convert. Its schema is expected to have a
   *                   column named "time" and of type Long.
   * @param isSorted   A flag specifies if the rows in given `dataFrame` have been sorted by their timestamps under
   *                   their time column.
   * @param timeUnit   The time unit under time column which could be [[scala.concurrent.duration.NANOSECONDS]],
   *                   [[scala.concurrent.duration.MILLISECONDS]], etc.
   * @param timeColumn Optional. The name of column in `df` that specifies the column name for time.
   * @return a [[TimeSeriesRDD]].
   * @example
   * {{{
   * val df = ... // A DataFrame whose rows have been sorted by their timestamps under "time" column.
   * val tsRdd = TimeSeriesRDD.from(df)(
   *   isSorted = true,
   *   timeUnit = scala.concurrent.duration.MILLISECONDS,
   *   begin = "20150101",
   *   end = "20160101"
   * )
   * }}}
   */
  def fromDF(
    dataFrame: DataFrame
  )(
    isSorted: Boolean,
    timeUnit: TimeUnit,
    timeColumn: String = timeColumnName
  ): TimeSeriesRDD = {
    // TODO: convert timestamps on the dataframe if we need to
    val df = dataFrame.withColumnRenamed(timeColumn, timeColumnName)
    requireSchema(df.schema)

    // pairRdd needs to be normalized, this is why we require row copies
    val pairRdd = dfToPairRdd(dataFrame, timeUnit, requireCopy = true)
    val keyRdd = dfToPairRdd(dataFrame.select(timeColumnName), timeUnit, requireCopy = false).map(_._1)
    TimeSeriesRDD.fromInternalOrderedRDD(
      OrderedRDD.fromRDD(pairRdd, KeyPartitioningType(isSorted, false), keyRdd),
      df.schema
    )
  }

  @PythonApi
  private[flint] def fromDFUnSafe(
    dataFrame: DataFrame
  )(
    timeUnit: TimeUnit,
    timeColumn: String,
    deps: Seq[Dependency[_]],
    rangeSplits: Array[RangeSplit[Long]]
  ): TimeSeriesRDD = {
    val df = dataFrame.withColumnRenamed(timeColumn, timeColumnName)
    requireSchema(df.schema)

    val pairRdd = dfToPairRdd(dataFrame, timeUnit, requireCopy = false)
    TimeSeriesRDD.fromInternalOrderedRDD(OrderedRDD.fromRDD(pairRdd, deps, rangeSplits), df.schema)
  }

  /**
   * Read a parquet file into a [[TimeSeriesRDD]].
   *
   * @param sc         [[org.apache.spark.SparkContext]].
   * @param paths      The paths of the parquet file.
   * @param isSorted   flag specifies if the rows in the file have been sorted by their timestamps.
   * @param timeUnit   The unit of time under time column which could be NANOSECONDS, MILLISECONDS, etc.
   * @param begin      Optional. Inclusive. Support most common date format. Default timeZone is UTC.
   * @param end        Optional. Exclusive. Support most common date format. Default timeZone is UTC.
   * @param columns    Optional. Column in the parquet file to read into [[TimeSeriesRDD]]. IMPORTANT: This is critical
   *                   for performance. Reading small amounts of columns can easily increase performance by 10x
   *                   comparing to reading all columns in the file.
   * @param timeColumn Optional. Column in parquet file that specifies time.
   * @return a [[TimeSeriesRDD]]
   * @example
   * {{{
   * val tsRdd = TimeSeriesRDD.fromParquet(
   *   sqlContext,
   *   path = "hdfs://foo/bar/"
   * )(isSorted = true,
   *   timeUnit = scala.concurrent.duration.MILLISECONDS,
   *   columns = Seq("time", "tid", "price"),  // By default, null for all columns
   *   begin = "20100101",                     // By default, null for no boundary at begin
   *   end = "20150101"                        // By default, null for no boundary at end
   * )
   * }}}
   */
  def fromParquet(
    sc: SparkContext,
    paths: String*
  )(
    isSorted: Boolean,
    timeUnit: TimeUnit,
    begin: String = null,
    end: String = null,
    columns: Seq[String] = null,
    timeColumn: String = timeColumnName
  ): TimeSeriesRDD = {
    val sqlContext = SQLContext.getOrCreate(sc)
    var df = sqlContext.read.parquet(paths: _*)
    if (columns != null) {
      df = df.select(columns.head, columns.tail: _*)
    }
    fromDF(DFBetween(df, begin, end, timeUnit, timeColumn))(
      isSorted = isSorted,
      timeUnit = timeUnit,
      timeColumn = timeColumn
    )
  }

  // A function taking any row as input and just return `Seq[Any]()`.
  private[flint] val emptyKeyFn: InternalRow => Seq[Any] = {
    _: InternalRow => Seq[Any]()
  }

  private[flint] def getAsAny(schema: StructType, cols: Seq[String])(row: InternalRow): Seq[Any] = {
    val columns = cols.map(column => (schema.fieldIndex(column), schema(column).dataType))
    InternalRowUtils.selectIndices(columns)(row)
  }

  private[flint] def safeGetAsAny(schema: StructType, cols: Seq[String]): InternalRow => Seq[Any] =
    if (cols.isEmpty) {
      emptyKeyFn
    } else {
      getAsAny(schema, cols)
    }
}

trait TimeSeriesRDD extends Serializable {

  /**
   * The schema of this [[TimeSeriesRDD]].
   *
   * It always keeps a column named "time" and of type Long and the time unit is NANOSECONDS.
   */
  val schema: StructType

  /**
   * An [[org.apache.spark.rdd.RDD RDD]] representation of this [[TimeSeriesRDD]].
   */
  val rdd: RDD[Row]

  /**
   * An [[com.twosigma.flint.rdd.OrderedRDD]] representation of this [[TimeSeriesRDD]]. Used to efficiently
   * perform join-like operations.
   */
  private[flint] val orderedRdd: OrderedRDD[Long, InternalRow]

  /**
   * Convert the this [[TimeSeriesRDD]] to a [[org.apache.spark.sql.DataFrame]].
   */
  val toDF: DataFrame

  /**
   * @return the number of rows.
   */
  def count(): Long

  /**
   * @return the first row.
   */
  def first(): Row

  /**
   * Persists this [[TimeSeriesRDD]] with default storage level (MEMORY_ONLY).
   *
   * @return the cached [[TimeSeriesRDD]].
   */
  def cache(): TimeSeriesRDD

  /**
   * Persists this [[TimeSeriesRDD]] with default storage level (MEMORY_ONLY).
   *
   * @return the cached [[TimeSeriesRDD]].
   */
  def persist(): TimeSeriesRDD

  /**
   * Sets storage level to persist the values across operations after the first time it is computed.
   *
   * @param newLevel The level of persist level.
   * @return the persisted [[TimeSeriesRDD]].
   */
  def persist(newLevel: StorageLevel): TimeSeriesRDD

  /**
   * Marks the it as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   * @return the un-persisted [[TimeSeriesRDD]].
   */
  def unpersist(blocking: Boolean = true): TimeSeriesRDD

  /**
   * Return a new [[TimeSeriesRDD]] that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this [[TimeSeriesRDD]]. Internally, this uses
   * a shuffle to redistribute data if it tries to increase the numPartitions.
   *
   * If you are decreasing the number of partitions in this [[TimeSeriesRDD]], consider using
   * `coalesce`, which can avoid performing a shuffle.
   *
   * @param numPartitions The expected number of partitions.
   */
  def repartition(numPartitions: Int): TimeSeriesRDD

  /**
   * Return a new [[TimeSeriesRDD]] that is reduced to numPartitions partitions.
   *
   * @param numPartitions Must be positive and less then the number of partitions of this [[TimeSeriesRDD]].
   * @return A new [[TimeSeriesRDD]] with the desired number of partitions.
   */
  def coalesce(numPartitions: Int): TimeSeriesRDD

  /**
   * @return an Array of [[org.apache.spark.sql.Row]]s in this [[TimeSeriesRDD]].
   */
  def collect(): Array[Row]

  /**
   * @param fn A predicate.
   * @return a new [[TimeSeriesRDD]] containing only the rows that satisfy a predicate.
   */
  def keepRows(fn: Row => Boolean): TimeSeriesRDD

  /**
   * @param fn A predicate.
   * @return a new [[TimeSeriesRDD]] filtering out the rows that satisfy a predicate.
   */
  def deleteRows(fn: Row => Boolean): TimeSeriesRDD

  /**
   * Returns a [[TimeSeriesRDD]] with the only specified columns. The time column is always kept.
   *
   * @param columns A list of column names which is not necessary to have the "time" column name.
   * @return a [[TimeSeriesRDD]] with the only specified columns.
   * @example
   * {{{
   * val priceTSRdd = ... // A TimeSeriesRDD with columns "time", "id", and "price"
   * val result = priceTSRdd.keepColumns("time") // A TimeSeriesRDD with only "time" column
   * }}}
   */
  def keepColumns(columns: String*): TimeSeriesRDD

  /**
   * Returns a [[TimeSeriesRDD]] without the specified columns. The time column is always kept.
   *
   * @param columns A sequence of column names.
   * @return a [[TimeSeriesRDD]] without the specified columns.
   * @example
   * {{{
   * val priceTSRdd = ... // A TimeSeriesRDD with columns "time", "id", and "price"
   * val result = priceTSRdd.deleteColumns("id") // A TimeSeriesRDD with only "time" and "price" columns
   * }}}
   */
  def deleteColumns(columns: String*): TimeSeriesRDD

  /**
   * Renames columns by providing a mapping to each column name.
   *
   * @param fromTo A mapping to each column name. Note that duplicated column names are not accepted.
   * @return a [[TimeSeriesRDD]] with renamed column names.
   * @example
   * {{{
   * val priceTSRdd = ... // A TimeSeriesRDD with columns "time", "id", and "price"
   * val result = priceTSRdd.renameColumns("id" -> "ticker", "price" -> "highPrice")
   * }}}
   */
  def renameColumns(fromTo: (String, String)*): TimeSeriesRDD

  /**
   * Adds new columns using a list of provided names, data types and functions.
   *
   * @param columns A list of tuple(s). For each tuple, the left specifies the name and the data type of a column to
   *                be added; while the right is a function that takes a row as input and calculates the value under
   *                that added column. The function should return an array for struct objects. Note that duplicated
   *                column names are not accepted.
   * @return a [[TimeSeriesRDD]] with added columns.
   * @example
   * {{{
   * val priceTSRdd = ... // A TimeSeriesRDD with columns "time", "highPrice", and "lowPrice"
   * val results = priceTSRdd.addColumns(
   *   "diff" -> DoubleType -> {
   *     r: Row => r.getAs[Double]("highPrice") - r.getAs[Double]("lowPrice")
   *   }
   * )
   * // A TimeSeriesRDD with a new column "diff" = "highPrice" - "lowPrice"
   * }}}
   */
  def addColumns(columns: ((String, DataType), Row => Any)*): TimeSeriesRDD

  /**
   * Adds new columns using a sequence of provided names, data types and functions for each cycle.
   *
   * A cycle is defined as a sequence of rows that share exactly the same timestamps.
   *
   * @param columns A list of tuple(s). For each tuple, the left specifies the name and the data type of a column to
   *                be added; while the right is a function that takes a cycle of rows as input and outputs a map from
   *                rows to their calculated values respectively. Note that duplicated column names are not accepted.
   * @return a [[TimeSeriesRDD]] with added columns.
   * @example
   * {{{
   * val priceTSRdd = ...
   * // A TimeSeriesRDD with columns "time", "id", and "sellingPrice"
   * // time  id  sellingPrice
   * // ----------------------
   * // 1000L 0   1.0
   * // 1000L 1   2.0
   * // 1000L 1   3.0
   * // 2000L 0   3.0
   * // 2000L 0   4.0
   * // 2000L 1   5.0
   * // 2000L 2   6.0
   *
   * val results = priceTSRdd.addColumnsForCycle(
   *   "adjustedSellingPrice" -> DoubleType -> { rows: Seq[Row] =>
   *     rows.map { row => (row, row.getDouble(2) * rows.size) }.toMap
   *  }
   * )
   * // time  id  sellingPrice adjustedSellingPrice
   * // -------------------------------------------
   * // 1000L 0   1.0          3.0
   * // 1000L 1   2.0          6.0
   * // 1000L 1   3.0          9.0
   * // 2000L 0   3.0         12.0
   * // 2000L 0   4.0         16.0
   * // 2000L 1   5.0         20.0
   * // 2000L 2   6.0         24.0
   * }}}
   */
  def addColumnsForCycle(columns: ((String, DataType), Seq[Row] => Map[Row, Any])*): TimeSeriesRDD

  /**
   * Adds new columns using a list of provided names, data types and functions for each cycle.
   *
   * A cycle is defined as a sequence of rows that share exactly the same timestamps.
   *
   * @param columns A sequence of tuple(s). For each tuple, the left specifies the name and the data type of a column to
   *                be added; while the right is a function that takes a cycle of rows as input and outputs a map from
   *                rows to their calculated values respectively. Note that duplicated column names are not accepted.
   * @param key     If non-empty, rows are further grouped by the columns specified by `key` in addition to row time.
   * @return a [[TimeSeriesRDD]] with added columns.
   * @example
   * {{{
   * val priceTSRdd = ...
   * // A TimeSeriesRDD with columns "time", "id", and "sellingPrice"
   * // time  id  sellingPrice
   * // ----------------------
   * // 1000L 0   1.0
   * // 1000L 1   2.0
   * // 1000L 1   3.0
   * // 2000L 0   3.0
   * // 2000L 0   4.0
   * // 2000L 1   5.0
   * // 2000L 2   6.0
   *
   * val results = priceTSRdd.addColumnsForCycle(
   *   Seq("adjustedSellingPrice" -> DoubleType -> { rows: Seq[Row] =>
   *     rows.map { row => (row, row.getDouble(2) * rows.size) }.toMap
   *   }),
   *   key = Seq("id")
   * )
   * // time  id  sellingPrice adjustedSellingPrice
   * // ---------------------------------------------
   * // 1000L 0   1.0          1.0
   * // 1000L 1   2.0          4.0
   * // 1000L 1   3.0          6.0
   * // 2000L 0   3.0          6.0
   * // 2000L 0   4.0          8.0
   * // 2000L 1   5.0          5.0
   * // 2000L 2   6.0          6.0
   * }}}
   */
  def addColumnsForCycle(
    columns: Seq[((String, DataType), Seq[Row] => Map[Row, Any])],
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD

  private[flint] def addColumnsForCycle(
    columns: Seq[((String, DataType), Seq[Row] => Map[Row, Any])],
    key: String
  ): TimeSeriesRDD = addColumnsForCycle(columns, Option(key).toSeq)

  /**
   * Groups rows within a cycle, i.e. rows with exactly the same timestamps. If a `key` is provided, rows within a
   * cycle will be further partitioned into sub-groups by the given `key`.
   *
   * @param key If non-empty, rows are further grouped by the columns specified by `key` in addition to row time.
   * @return a [[TimeSeriesRDD]].
   * @example
   * {{{
   * val priceTSRdd = ...
   * // A TimeSeriesRDD with columns "time" and "price"
   * // time  price
   * // -----------
   * // 1000L 1.0
   * // 1000L 2.0
   * // 2000L 3.0
   * // 2000L 4.0
   * // 2000L 5.0
   *
   * val results = priceTSRdd.groupByCycle()
   * // time  rows
   * // ------------------------------------------------
   * // 1000L [[1000L, 1.0], [1000L, 2.0]]
   * // 2000L [[2000L, 3.0], [2000L, 4.0], [2000L, 5.0]]
   * }}}
   */
  def groupByCycle(key: Seq[String] = Seq.empty): TimeSeriesRDD

  private[flint] def groupByCycle(key: String): TimeSeriesRDD = groupByCycle(Option(key).toSeq)

  /**
   * Groups rows whose timestamps falling into an interval. If a `key` is provided, rows within an interval will be
   * further partitioned into sub-groups by the given `key`.
   *
   * @param clock          A [[TimeSeriesRDD]] whose timestamps will be used to defined intervals, i.e. two sequential
   *                       timestamps define an interval.
   * @param key            If non-empty, rows within an interval should be further partitioned into sub-groups
   *                       specified by 'key' in addition to row time.
   * @param beginInclusive For rows within an interval, it will round the timestamps of rows to the begin of the
   *                       interval by default.
   * @return a [[TimeSeriesRDD]].
   * @example
   * {{{
   * val priceTSRdd = ...
   * // A TimeSeriesRDD with columns "time" and "price"
   * // time  price
   * // -----------
   * // 1000L 1.0
   * // 1500L 2.0
   * // 2000L 3.0
   * // 2500L 4.0
   *
   * val clockTSRdd = ...
   * // A TimeSeriesRDD with only column "time"
   * // time
   * // -----
   * // 1000L
   * // 2000L
   * // 3000L
   *
   * val results = priceTSRdd.groupByInterval(clockTSRdd)
   * // time  rows
   * // ----------------------------------
   * // 1000L [[1000L, 1.0], [1500L, 2.0]]
   * // 2000L [[2000L, 3.0], [2500L, 4.0]]
   * }}}
   */
  def groupByInterval(clock: TimeSeriesRDD, key: Seq[String] = Seq.empty, beginInclusive: Boolean = true): TimeSeriesRDD

  private[flint] def groupByInterval(clock: TimeSeriesRDD, key: String, beginInclusive: Boolean): TimeSeriesRDD =
    groupByInterval(clock, Option(key).toSeq, beginInclusive)

  /**
   * For each row, adds a new column `window` whose value is a list of rows within the specified window.
   *
   * @param window   A window specifies a time range for any timestamp of a row.
   * @param key      For a particular row and those rows whose timestamps within its window, those rows will be
   *                 considered to be within that window iff they share the same key. By default,
   *                 the keys is empty, i.e not specified and it thus includes all rows of that window.
   * @return a [[TimeSeriesRDD]] with added column of windows.
   * @example
   * {{{
   * val priceTSRdd = ...
   * // A TimeSeriesRDD with columns "time" and "price"
   * // time  price
   * // -----------
   * // 1000L 1.0
   * // 1500L 2.0
   * // 2000L 3.0
   * // 2500L 4.0
   *
   * val result = priceTSRdd.addWindows(Window.pastAbsoluteTime("1000ns"))
   * // time  price window
   * // ------------------------------------------------------
   * // 1000L 1.0   [[1000L, 1.0]]
   * // 1500L 2.0   [[1000L, 1.0], [1500L, 2.0]]
   * // 2000L 3.0   [[1000L, 1.0], [1500L, 2.0], [2000L, 3.0]]
   * // 2500L 4.0   [[1500L, 2.0], [2000L, 3.0], [2500L, 4.0]]
   * }}}
   */
  def addWindows(window: Window, key: Seq[String] = Seq.empty): TimeSeriesRDD

  private[flint] def addWindows(window: Window, key: String): TimeSeriesRDD =
    addWindows(window, Option(key).toSeq)

  /**
   * Merge this [[TimeSeriesRDD]] and the other [[TimeSeriesRDD]] with the same schema. The merged
   * [[TimeSeriesRDD]] include sall rows from each in temporal order. If there is a timestamp ties,
   * the rows in this [[TimeSeriesRDD]] will be returned earlier than those from the other
   * [[TimeSeriesRDD]].
   *
   * @param other The other [[TimeSeriesRDD]] expected to merge.
   * @return a merged  [[TimeSeriesRDD]] with rows from each [[TimeSeriesRDD]] in temporal order.
   * @example
   * {{{
   * val thisTSRdd = ...
   * // +----+---+-----+
   * // |time|tid|price|
   * // +----+---+-----+
   * // |1000|  3|  1.0|
   * // |1050|  3|  1.5|
   * ...
   *
   * val otherTSRdd = ...
   * // +----+---+-----+
   * // |time|tid|price|
   * // +----+---+-----+
   * // |1000|  7|  0.5|
   * // |1050|  7|  2.0|
   * ...
   *
   * val mergedTSRdd = thisTSRdd.merge(otherTSRdd)
   * // +----+---+-----+
   * // |time|tid|price|
   * // +----+---+-----+
   * // |1000|  3|  1.0|
   * // |1000|  7|  0.5|
   * // |1050|  3|  1.5|
   * // |1050|  7|  2.0|
   * ...
   * }}}
   */
  def merge(other: TimeSeriesRDD): TimeSeriesRDD

  /**
   * Performs the temporal left-join to the right [[TimeSeriesRDD]], i.e. left-join using inexact timestamp matches.
   * For each row in the left, append the most recent row from the right at or before the same time. The result is null
   * in the right side when there is no match.
   *
   * @param right      The [[TimeSeriesRDD]] to find the past row.
   * @param tolerance  The most recent row from the right will only be appended if it was within the specified
   *                   time of the row from the left. The default tolerance is "Ons" which provides the exact
   *                   left-join. Examples of possible values are "1ms", "2s", "5m", "10h", "25d" etc.
   * @param key        Columns that could be used as the matching key. If non-empty, the most recent row from the
   *                   right that shares the same key with the current row from the left will be appended.
   * @param leftAlias  The prefix name for columns from left after join.
   * @param rightAlias The prefix name for columns from right after join.
   * @return a joined [[TimeSeriesRDD]].
   * @example
   * {{{
   * val leftTSRdd = ...
   * val rightTSRdd = ...
   * val joinedTSRdd = leftTSRdd.leftJoin(rightTSRdd, "2h")
   * }}}
   */
  def leftJoin(
    right: TimeSeriesRDD,
    tolerance: String = "0ns",
    key: Seq[String] = Seq.empty,
    leftAlias: String = null,
    rightAlias: String = null
  ): TimeSeriesRDD

  private[flint] def leftJoin(
    right: TimeSeriesRDD,
    tolerance: String,
    key: String,
    leftAlias: String,
    rightAlias: String
  ): TimeSeriesRDD = leftJoin(right, tolerance, Option(key).toSeq, leftAlias, rightAlias): TimeSeriesRDD

  /**
   * Performs the temporal future left-outer-join to the right [[TimeSeriesRDD]], i.e. left-join using inexact timestamp
   * matches. For each row in the left, appends the closest future row from the right at or after the same time. The
   * result is null in the right side when there is no match.
   *
   * @param right           The [[TimeSeriesRDD]] to find the future row.
   * @param tolerance       The closest future row from the right will only be appended if it was within the specified
   *                        time of the row from the left. The default tolerance is "Ons" which provides the exact
   *                        left-join. Examples of possible values are "1ms", "2s", "5m", "10h", "25d" etc.
   * @param key             Columns that could be used as the matching key. If non-empty, the closest future row
   *                        from right that shares a key with the current row from the left will be appended.
   * @param leftAlias       The prefix name for columns from left after join.
   * @param rightAlias      The prefix name for columns from right after join.
   * @param strictLookahead When performing a future left join, whether to join rows where timestamps exactly match.
   *                        Default is false (rows in the right will match rows in the left with exactly matching
   *                        timestamp). True implies that rows in the left table only be joined with rows in the right
   *                        that have strictly larger timestamps.
   * @return a joined [[TimeSeriesRDD]].
   * @example
   * {{{
   * val leftTSRdd = ...
   * val rightTSRdd = ...
   * val joinedTSRdd = leftTSRdd.futureLeftJoin(rightTSRdd, "2h")
   * }}}
   */
  def futureLeftJoin(
    right: TimeSeriesRDD,
    tolerance: String = "0ns",
    key: Seq[String] = Seq.empty,
    leftAlias: String = null,
    rightAlias: String = null,
    strictLookahead: Boolean = false
  ): TimeSeriesRDD

  private[flint] def futureLeftJoin(
    right: TimeSeriesRDD,
    tolerance: String,
    key: String,
    leftAlias: String,
    rightAlias: String,
    strictLookahead: Boolean
  ): TimeSeriesRDD = futureLeftJoin(right, tolerance, Option(key).toSeq, leftAlias, rightAlias, strictLookahead)

  /**
   * Computes aggregate statistics of rows that are within a cycle, i.e. rows share a timestamp.
   *
   * An example of calculating the summations over rows within a cycle is given as follows. The summations are over
   * the values under the specified column name "col".
   *
   * @param summarizer A summarizer expected to perform aggregation. See [[Summarizers]] for supported summarizers.
   * @param key        Columns that could be used as the grouping key and aggregations will be performed per
   *                   key per cycle level if specified.
   * @return a [[TimeSeriesRDD]] with summarized column.
   * @example
   * {{{
   * val resultTimeSeriesRdd = timeSeriesRdd.summarizeCycles(Summary.sum("col"))
   * }}}
   */
  def summarizeCycles(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD

  private[flint] def summarizeCycles(summarizer: SummarizerFactory, key: String): TimeSeriesRDD =
    summarizeCycles(summarizer, Option(key).toSeq)

  /**
   * Computes aggregate statistics of rows whose timestamps falling into an interval.
   *
   * An example of calculating the summations over rows within an interval is given as follows. The intervals are
   * defined by the provided `clock` and the summations are over values under the specified column name "col".
   *
   * @param clock          A [[TimeSeriesRDD]] whose timestamps will be used to defined intervals, i.e. two sequential
   *                       timestamps define an interval.
   * @param summarizer     A summarizer expected to perform aggregation. See [[Summarizers]] for supported summarizers.
   * @param key            Columns that could be used as the grouping key and aggregations will be performed per
   *                       key per cycle level if specified.
   * @param beginInclusive For rows within an interval, it will round the timestamps of rows to the begin of the
   *                       interval by default.
   * @return a [[TimeSeriesRDD]] with summarized column.
   * @example
   * {{{
   * import com.twosigma.flint.timeseries.summarize.Summary
   * val clockTimeSeriesRDD = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.summarizeIntervals(clockTimeSeriesRDD, Summary.sum("col"))
   * }}}
   */
  def summarizeIntervals(
    clock: TimeSeriesRDD,
    summarizer: SummarizerFactory,
    key: Seq[String] = Seq.empty,
    beginInclusive: Boolean = true
  ): TimeSeriesRDD

  private[flint] def summarizeIntervals(
    clock: TimeSeriesRDD,
    summarizer: SummarizerFactory,
    key: String,
    beginInclusive: Boolean
  ): TimeSeriesRDD = summarizeIntervals(clock, summarizer, Option(key).toSeq, beginInclusive)

  /**
   * For each row, computes aggregate statistics of rows within its window.
   *
   * An example of calculating the summations over windows is given as follows. A window of a row is provided by
   * `window` and the summations are over values under the specified column name "col".
   *
   * @param window     A window specifies a time range for any time stamp of a row.
   * @param summarizer A summarizer expected to perform aggregation. See [[Summarizers]] for supported summarizers.
   * @param key        For a particular row and those rows whose timestamps within its window, those rows will be
   *                   considered to be within that window iff they share the same keys. By default, the it is empty,
   *                   i.e. not specified and it will include all rows of that window.
   * @return a [[TimeSeriesRDD]] with summarized column.
   * @example
   * {{{
   * import com.twosigma.flint.timeseries.summarize.Summary
   * val timeSeriesRdd = ...
   * val window = Window.pastAbsoluteTime("100ms")
   * val resultTimeSeriesRdd = timeSeriesRdd.summarizeWindows(window, Summary.sum("col"))
   * }}}
   */
  def summarizeWindows(
    window: Window,
    summarizer: SummarizerFactory,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD

  private[flint] def summarizeWindows(
    window: Window,
    summarizer: SummarizerFactory,
    key: String
  ): TimeSeriesRDD = summarizeWindows(window, summarizer)

  /**
   * Computes aggregate statistics of all rows.
   *
   * @param summarizer A summarizer expected to perform aggregation.
   * @param key        Columns that could be used as the grouping key and the aggregations will be performed
   *                   per key level.
   * @return a [[TimeSeriesRDD]] with summarized column.
   * @example
   * {{{
   * import com.twosigma.flint.timeseries.summarize.Summary
   * val timeSeriesRdd = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.summarize(Summary.sum("col"))
   * }}}
   */
  def summarize(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD

  private[flint] def summarize(summarizer: SummarizerFactory, key: String): TimeSeriesRDD =
    summarize(summarizer, Option(key).toSeq)

  /**
   * Adds a summary column that summarizes one or more columns for each row.
   *
   * @param summarizer A summarizer expected to perform aggregation. See [[Summarizers]] for supported summarizers.
   * @param key        Columns that could be used as the grouping key and the aggregations will be performed
   *                   per key level.
   * @return a [[TimeSeriesRDD]] with an additional summarized column.
   * @example
   * {{{
   * import com.twosigma.flint.timeseries.summarize.Summary
   * val timeSeriesRdd = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.addSummaryColumns(Summary.sum("col"))
   * }}}
   */
  def addSummaryColumns(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD

  private[flint] def addSummaryColumns(summarizer: SummarizerFactory, key: String): TimeSeriesRDD =
    addSummaryColumns(summarizer, Option(key).toSeq)

  /**
   * Shifts the timestamps of rows backward by a given amount.
   *
   * @param shiftAmount An amount of shift, e.g. "1ms", "2s", "5m", "10h", "25d" etc.
   * @return a [[TimeSeriesRDD]] with adjusted timestamps.
   * @example
   * {{{
   * val timeSeriesRdd = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.lookBackwardClock("10h")
   * }}}
   */
  @deprecated("Use shift(Windows.pastAbsoluteTime(shiftAmount)) instead")
  def lookBackwardClock(shiftAmount: String): TimeSeriesRDD

  /**
   * Shifts the timestamps of rows forward by a given amount.
   *
   * @param shiftAmount An amount of shift, e.g. "1ms", "2s", "5m", "10h", "25d" etc.
   * @return a [[TimeSeriesRDD]] with adjusted timestamps.
   * @example
   * {{{
   * val timeSeriesRdd = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.lookForwardClock("10h")
   * }}}
   *
   */
  @deprecated("Use shift(Windows.futureAbsoluteTime(shiftAmount)) instead")
  def lookForwardClock(shiftAmount: String): TimeSeriesRDD

  /**
   * Casts numeric columns to a different numeric type (e.g. DoubleType to IntegerType).
   *
   * @param columns A sequence of tuples specifying a column name and a data type to cast the column to.
   * @return a [[TimeSeriesRDD]] with adjusted schema and values.
   * @example
   * {{{
   * // A TimeSeriesRDD with schema Schema("time" -> LongType, "price" -> DoubleType)
   * val timeSeriesRdd = ...
   * val resultTimeSeriesRdd = timeSeriesRdd.cast("price" -> IntegerType)
   * }}}
   */
  def cast(columns: (String, NumericType)*): TimeSeriesRDD

  /**
   * Changes the timestamp of each row by specifying a function that computes new timestamps.
   * A non-default window value will be used to optimize RDD sorting.
   *
   * @param fn A function that computes new timestamps in NANOSECONDS.
   * @param window A time window that limits timestamp updates, i.e. no row's timestamp will change
   *               more than window forward or backward. Null by default.
   * @return a [[TimeSeriesRDD]] with adjusted timestamps.
   * @example
   * {{{
   * val clockTSRdd = ...
   * val updatedRdd = clockTSRdd.setTime { row: Row =>
   *   val time = row.getAs[Long]("time")
   *   if (time % 2 == 1) {
   *      time - 1L
   *   } else {
   *      time
   *   }
   * }
   * }}}
   */
  def setTime(fn: Row => Long, window: String = null): TimeSeriesRDD

  /**
   * Shift the timestamp of each row by a length defined a [[ShiftTimeWindow]].
   *
   * @example
   * {{{
   * val timeSeriesRdd = ...
   * // Shift timestamp of each row backward for one day
   * val shifted = timeSeriesRdd.shift(Windows.pastAbsoluteTime("1day"))
   * }}}
   * @param window A [[ShiftTimeWindow]] that specfies the shift amount for each row
   * @return a [[TimeSeriesRDD]] with shifted timestamps.
   */
  def shift(window: ShiftTimeWindow): TimeSeriesRDD
}

class TimeSeriesRDDImpl private[timeseries] (
  val dataFrame: DataFrame,
  val ranges: Seq[CloseOpen[Long]]
) extends TimeSeriesRDD {

  override val schema: StructType = dataFrame.schema

  private[flint] override lazy val orderedRdd = TimeSeriesRDD.dfToOrderedRdd(dataFrame, ranges, requireCopy = true)

  // you can't store references to row objects from `unsafeOrderedRdd`, or use rdd.collect() without making
  // safe copies of rows. The current implementation might use the same memory buffer to process all rows
  // per partition, so the referenced bytes might be overwritten by the next row.
  private[timeseries] lazy val unsafeOrderedRdd =
    TimeSeriesRDD.dfToOrderedRdd(dataFrame, ranges, requireCopy = false)

  private[flint] def safeGetAsAny(cols: Seq[String]): InternalRow => Seq[Any] =
    TimeSeriesRDD.safeGetAsAny(schema, cols)

  private def prependTimeAndKey(t: Long, inputRow: InternalRow, outputRow: InternalRow,
    outputSchema: StructType, key: Seq[String]): InternalRow =
    InternalRowUtils.prepend(outputRow, outputSchema, t +: key.map {
      fieldName =>
        val fieldIndex = schema.fieldIndex(fieldName)
        inputRow.get(fieldIndex, schema.fields(fieldIndex).dataType)
    }: _*)

  /**
   * Values converter for this rdd.
   *
   * If the row is not nested, returns a identity function.
   *
   * If the row is nested, returns a function that knows how to convert internal row to external row
   */
  private val toExternalRow: InternalRow => ERow = CatalystTypeConvertersWrapper.toScalaRowConverter(schema)

  override val rdd: RDD[Row] = dataFrame.rdd

  override val toDF: DataFrame = dataFrame

  def count(): Long = dataFrame.count()

  def first(): Row = dataFrame.first()

  def collect(): Array[Row] = dataFrame.collect()

  def cache(): TimeSeriesRDD = {
    dataFrame.cache(); this
  }

  def persist(): TimeSeriesRDD = {
    dataFrame.persist(); this
  }

  def persist(newLevel: StorageLevel): TimeSeriesRDD = {
    dataFrame.persist(newLevel); this
  }

  def unpersist(blocking: Boolean = true): TimeSeriesRDD = {
    dataFrame.unpersist(blocking); this
  }

  def repartition(numPartitions: Int): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.repartition(numPartitions), schema)

  def coalesce(numPartitions: Int): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.coalesce(numPartitions), schema)

  def keepRows(fn: Row => Boolean): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.filterOrdered {
      (t: Long, r: InternalRow) => fn(toExternalRow(r))
    }, schema)

  def deleteRows(fn: Row => Boolean): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.filterOrdered {
      (t: Long, r: InternalRow) => !fn(toExternalRow(r))
    }, schema)

  def keepColumns(columns: String*): TimeSeriesRDD = {
    // TODO: implement DF-based version
    val (select, newSchema) = InternalRowUtils.select(schema, Schema.prependTimeIfMissing(columns))
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.mapValues { (_, row) => select(row) }, newSchema)
  }

  def deleteColumns(columns: String*): TimeSeriesRDD = {
    require(!columns.contains(TimeSeriesRDD.timeColumnName), "You can't delete the time column!")

    val columnSet = columns.toSet
    val remainingColumns = schema.fields.map(_.name).filterNot(name => columnSet.contains(name))
    val newDf = dataFrame.select(remainingColumns.head, remainingColumns.tail: _*)

    TimeSeriesRDD.fromDfWithRanges(newDf, ranges)
  }

  def renameColumns(fromTo: (String, String)*): TimeSeriesRDD = {
    // TODO: implement DF-based renameColumns()
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd, Schema.rename(this.schema, fromTo))
  }

  def addColumns(columns: ((String, DataType), Row => Any)*): TimeSeriesRDD = {
    val (add, newSchema) = InternalRowUtils.addOrUpdate(schema, columns.map(_._1))

    TimeSeriesRDD.fromInternalOrderedRDD(
      unsafeOrderedRdd.mapValues {
        (_, row) =>
          val extRow = toExternalRow(row)
          add(row, columns.map {
            case (_, columnFunc) => columnFunc(extRow)
          })
      },
      newSchema
    )
  }

  def addColumnsForCycle(
    columns: ((String, DataType), Seq[Row] => Map[Row, Any])*
  ): TimeSeriesRDD = addColumnsForCycle(columns.toSeq, key = Seq.empty)

  def addColumnsForCycle(
    columns: Seq[((String, DataType), Seq[Row] => Map[Row, Any])],
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = {
    val (add, newSchema) = InternalRowUtils.addOrUpdate(schema, columns.map(_._1))

    val newRdd = orderedRdd.groupByKey(safeGetAsAny(key)).flatMapValues {
      (_, rows) =>
        val eRows = rows.map(toExternalRow)
        val nameWithTypeToRowMaps = Map(columns.map { case (k, fn) => k -> fn(eRows) }: _*)
        (rows zip eRows).map {
          case (row, eRow) => add(row, columns.map { case (k, m) => nameWithTypeToRowMaps(k)(eRow) })
        }
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def groupByCycle(key: Seq[String] = Seq.empty): TimeSeriesRDD = summarizeCycles(Summarizers.rows("rows"), key)

  def groupByInterval(
    clock: TimeSeriesRDD,
    key: Seq[String] = Seq.empty,
    beginInclusive: Boolean = true
  ): TimeSeriesRDD = summarizeIntervals(clock, Summarizers.rows("rows"), key, beginInclusive)

  def addWindows(
    window: Window,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = summarizeWindows(window, Summarizers.rows(s"window_${window.name}"), key)

  def merge(other: TimeSeriesRDD): TimeSeriesRDD = {
    require(
      schema == other.schema,
      s"Cannot merge this TimeSeriesRDD of schema $schema with other TimeSeriesRDD with schema ${other.schema} "
    )
    val otherOrderedRdd = other.orderedRdd
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.merge(otherOrderedRdd), schema)
  }

  def leftJoin(
    right: TimeSeriesRDD,
    tolerance: String = "0ns",
    key: Seq[String] = Seq.empty,
    leftAlias: String = null,
    rightAlias: String = null
  ): TimeSeriesRDD = {
    val toleranceNum = Duration(tolerance).toNanos
    val toleranceFn = (t: Long) => t - toleranceNum
    val joinedRdd = orderedRdd.leftJoin(
      right.orderedRdd, toleranceFn, safeGetAsAny(key), TimeSeriesRDD.safeGetAsAny(right.schema, key)
    )

    val (concat, newSchema) = InternalRowUtils.concat(
      IndexedSeq(this.schema, right.schema),
      Seq(leftAlias, rightAlias),
      (key :+ TimeSeriesRDD.timeColumnName).toSet
    )

    val rightNullRow = InternalRow.fromSeq(Array.fill[Any](right.schema.size)(null))

    val newRdd = joinedRdd.collectOrdered {
      case (k, (r1, Some((_, r2)))) => concat(Seq(r1, r2))
      case (k, (r1, None)) => concat(Seq(r1, rightNullRow))
    }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def futureLeftJoin(
    right: TimeSeriesRDD,
    tolerance: String = "0ns",
    key: Seq[String] = Seq.empty,
    leftAlias: String = null,
    rightAlias: String = null,
    strictLookahead: Boolean = false
  ): TimeSeriesRDD = {
    val toleranceNum = Duration(tolerance).toNanos
    val toleranceFn = (t: Long) => t + toleranceNum
    val joinedRdd = orderedRdd.futureLeftJoin(
      right.orderedRdd, toleranceFn, safeGetAsAny(key),
      TimeSeriesRDD.safeGetAsAny(right.schema, key), strictForward = strictLookahead
    )

    val (concat, newSchema) = InternalRowUtils.concat(
      IndexedSeq(this.schema, right.schema),
      Seq(leftAlias, rightAlias),
      (key :+ TimeSeriesRDD.timeColumnName).toSet
    )

    val rightNullRow = InternalRow.fromSeq(Array.fill[Any](right.schema.size)(null))
    val newRdd = joinedRdd.collectOrdered {
      case (k, (r1, Some((_, r2)))) => concat(Seq(r1, r2))
      case (k, (r1, None)) => concat(Seq(r1, rightNullRow))
    }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def summarizeCycles(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD = {
    val sum = summarizer(schema)
    val groupByRdd = orderedRdd.groupByKey(safeGetAsAny(key))

    val newSchema = Schema.prependTimeAndKey(sum.outputSchema, key.map(schema(_)))

    val newRdd: OrderedRDD[Long, InternalRow] = groupByRdd.collectOrdered{
      case (t: Long, rows: Array[InternalRow]) if !rows.isEmpty =>
        val result = rows.foldLeft(sum.zero()) {
          case (state, r) => sum.add(state, r)
        }
        val row = sum.render(result)
        prependTimeAndKey(t, rows.head, row, sum.outputSchema, key)
    }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def summarizeIntervals(
    clock: TimeSeriesRDD,
    summarizer: SummarizerFactory,
    key: Seq[String] = Seq.empty,
    beginInclusive: Boolean = true
  ): TimeSeriesRDD = {
    val sum = summarizer(schema)
    val intervalized = orderedRdd.intervalize(clock.orderedRdd, beginInclusive).mapValues {
      case (_, v) => v._2
    }
    val grouped = intervalized.groupByKey(safeGetAsAny(key))
    val newSchema = Schema.prependTimeAndKey(sum.outputSchema, key.map(schema(_)))

    TimeSeriesRDD.fromInternalOrderedRDD(
      grouped.collectOrdered {
        // Rows must have the same key if a key is provided
        case (t: Long, rows: Array[InternalRow]) if !rows.isEmpty =>
          val result = rows.foldLeft(sum.zero()) {
            case (state, row) => sum.add(state, row)
          }
          val iRow = sum.render(result)
          prependTimeAndKey(t, rows.head, iRow, sum.outputSchema, key)
      },
      newSchema
    )
  }

  def summarizeWindows(
    window: Window,
    summarizer: SummarizerFactory,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = {
    val sum = summarizer(schema)
    val keyFn = safeGetAsAny(key)

    val (concat, newSchema) = InternalRowUtils.concat2(schema, sum.outputSchema)

    val summarizedRdd = window match {
      case w: TimeWindow => orderedRdd.summarizeWindows(w.of, sum, keyFn)
      case _ => sys.error(s"Unsupported window type: $window")
    }

    val newRdd = summarizedRdd.mapValues { case (_, (row1, row2)) => concat(row1, row2) }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def summarize(summarizerFactory: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD = {
    val summarizer = summarizerFactory(schema)
    val summarized = summarizerFactory match {
      case factory: OverlappableSummarizerFactory =>
        orderedRdd.summarize(factory(schema).asInstanceOf[OverlappableSummarizer], factory.window.of, safeGetAsAny(key))
      case _ =>
        orderedRdd.summarize(summarizer, safeGetAsAny(key))
    }
    val rows = summarized.map {
      case (keyValues, row) => InternalRowUtils.prepend(row, summarizer.outputSchema, (0L +: keyValues): _*)
    }

    val newSchema = Schema.prependTimeAndKey(summarizer.outputSchema, key.map(schema(_)))
    TimeSeriesRDD.fromSeq(orderedRdd.sc, rows.toSeq, newSchema, true, 1)
  }

  def addSummaryColumns(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD = {
    val sum = summarizer(schema)
    val reductionsRdd = orderedRdd.summarizations(sum, safeGetAsAny(key))
    val (concat, newSchema) = InternalRowUtils.concat2(schema, sum.outputSchema)
    val rowRdd = reductionsRdd.mapValues {
      case (_: Long, (row1, row2)) => concat(row1, row2)
    }
    TimeSeriesRDD.fromInternalOrderedRDD(rowRdd, newSchema)
  }

  def lookBackwardClock(shiftAmount: String): TimeSeriesRDD = shift(Windows.pastAbsoluteTime(shiftAmount))

  def lookForwardClock(shiftAmount: String): TimeSeriesRDD = shift(Windows.futureAbsoluteTime(shiftAmount))

  def cast(updates: (String, NumericType)*): TimeSeriesRDD = {
    val (castRow, newSchema) = InternalRowUtils.cast(schema, updates: _*)
    if (schema == newSchema) {
      this
    } else {
      TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.mapValues{ case (_, row) => castRow(row) }, newSchema)
    }
  }

  @Experimental
  def setTime(fn: Row => Long, window: String): TimeSeriesRDD = {
    if (window == null) {
      val timeIndex = schema.fieldIndex(TimeSeriesRDD.timeColumnName)
      TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.mapOrdered {
        case (t: Long, r: InternalRow) =>
          val timeStamp = fn(toExternalRow(r))
          (timeStamp, InternalRowUtils.update(r, schema, timeIndex -> timeStamp))
      }, schema)
    } else {
      throw new IllegalArgumentException(s"Non-default window isn't supported at this moment.")
    }
  }

  def shift(window: ShiftTimeWindow): TimeSeriesRDD = {
    val timeIndex = schema.fieldIndex(TimeSeriesRDD.timeColumnName)

    val newRdd = unsafeOrderedRdd.shift(window.shift).mapValues {
      case (t, iRow) => InternalRowUtils.update(iRow, schema, timeIndex -> t)
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, schema)
  }
}
