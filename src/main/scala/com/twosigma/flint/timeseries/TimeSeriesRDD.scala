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

package com.twosigma.flint.timeseries

import java.util.concurrent.TimeUnit
import javax.annotation.Nullable

import com.twosigma.flint.FlintConf
import com.twosigma.flint.annotation.PythonApi
import com.twosigma.flint.rdd._
import com.twosigma.flint.timeseries.row.{ InternalRowUtils, Schema }
import com.twosigma.flint.timeseries.summarize.{ ColumnList, OverlappableSummarizer, OverlappableSummarizerFactory, SummarizerFactory }
import com.twosigma.flint.timeseries.time.TimeFormat
import com.twosigma.flint.timeseries.window.summarizer.ArrowWindowBatchSummarizer
import com.twosigma.flint.timeseries.window.{ ShiftTimeWindow, TimeWindow, Window }
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Dependency, OneToOneDependency, SparkContext, TaskContext }
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{ GenericInternalRow, GenericRow, GenericRowWithSchema => ERow }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions._
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
   * Checks if the schema is legal.
   */
  private def requireSchema(schema: StructType): Unit = {
    require(
      schema.length == schema.fieldNames.toSet.size,
      s"Schema $schema contains duplicate field names"
    )
    require(
      schema.exists {
        case StructField("time", LongType, _, _) => true
        case _ => false
      },
      s"Schema $schema doesn't contain a valid time column"
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

  private[flint] def convertDfTimestamps(
    dataFrame: DataFrame,
    timeUnit: TimeUnit
  ): DataFrame = {
    if (timeUnit == NANOSECONDS) {
      dataFrame
    } else {
      val converter: Long => Long = TimeUnit.NANOSECONDS.convert(_, timeUnit)
      val udfConverter = udf(converter)

      dataFrame.withColumn(timeColumnName, udfConverter(col(timeColumnName)))
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
   * @param begin      Optional begin time of the returned [[DataFrame]], inclusive
   * @param end        Optional end time of the returned [[DataFrame]], exclusive
   * @param timeUnit   Optional. The time unit under time column which could be
   *                   [[scala.concurrent.duration.NANOSECONDS]],[[scala.concurrent.duration.MILLISECONDS]], etc.
   * @param timeColumn Optional. The name of column in `df` that specifies the column name for time. Default: "time"
   * @return a [[org.apache.spark.sql.DataFrame]].
   */
  @deprecated("0.3.4", "No longer used by Python bindings")
  @PythonApi
  private[flint] def DFBetween(
    dataFrame: DataFrame,
    @Nullable begin: String,
    @Nullable end: String,
    timeUnit: TimeUnit = NANOSECONDS,
    timeColumn: String = timeColumnName
  ): DataFrame = {
    val beginNanos = Option(begin).map(TimeFormat.parse(_, timeUnit = timeUnit))
    val endNanos = Option(end).map(TimeFormat.parse(_, timeUnit = timeUnit))

    DFBetween(dataFrame, beginNanos, endNanos, timeColumn = timeColumn)
  }

  /**
   *
   * @param dataFrame     A [[org.apache.spark.sql.DataFrame]].
   * @param beginNanosOpt Optional begin time of the returned [[DataFrame]] in nanoseconds, inclusive
   * @param endNanosOpt   Optional end time of the returned [[DataFrame]] in nanoseconds, exclusive
   * @param timeColumn    Optional. The name of column in `dataFrame` that specifies the column name for time.
   * @return a [[org.apache.spark.sql.DataFrame]].
   */
  private[flint] def DFBetween(
    dataFrame: DataFrame,
    beginNanosOpt: Option[Long],
    endNanosOpt: Option[Long],
    timeColumn: String
  ): DataFrame = {
    var df = dataFrame

    df = beginNanosOpt match {
      case Some(nanos) => df.filter(df(timeColumn) >= nanos)
      case None => df
    }

    df = endNanosOpt match {
      case Some(nanos) => df.filter(df(timeColumn) < nanos)
      case None => df
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
   * Convert a [[org.apache.spark.sql.DataFrame]] to a [[TimeSeriesRDD]].
   *
   * @param dataFrame  The [[org.apache.spark.sql.DataFrame]] expected to convert. Its schema is expected to have a
   *                   column named "time" and of type Long.
   * @param isSorted   A flag specifies if the rows in given `dataFrame` are sorted by their timestamps under
   *                   their time column. If a given [[DataFrame]] is already sorted, and Catalyst knows about it,
   *                   then the flag will be ignored.
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
    val canonizedDf = canonizeDF(dataFrame, isSorted, timeUnit, timeColumn)
    fromDFWithPartInfo(canonizedDf, None)
  }

  /**
   * Prepare [[DataFrame]] for conversion to [[TimeSeriesRDD]]. The list of requirements:
   * 1) Time column should exist, and it should be named `time`.
   * 2) [[DataFrame]] should be sorted by `time`.
   * 3) Timestamps should be converted to nanoseconds.
   * 4) `time` should be the first column.
   * We try to avoid overwriting or renaming `time` column, because these operations change it's attribute id, and
   * Catalyst partitioning metadata no longer matches the attribute, causing issues in TimeSeriesStore.isNormalized:
   * {{{
   * df("time").expr                             // time#25L
   * val df2 = df.withColumn("time", df("time"))
   * df2("time").expr                            // time#67L
   * df2.queryExecution.executedPlan.outputPartitioning  //rangepartitioning(time#25L ASC, 200) - original time column
   * }}}
   *
   * @return canonized [[DataFrame]]
   */
  private[flint] def canonizeDF(
    dataFrame: DataFrame,
    isSorted: Boolean,
    timeUnit: TimeUnit,
    timeColumn: String
  ): DataFrame = {
    require(
      !dataFrame.columns.contains(timeColumnName) || timeColumn == timeColumnName,
      "Cannot use another column as timeColumn while a column with name `time` exists"
    )
    val df = if (timeColumn == timeColumnName) {
      dataFrame
    } else {
      dataFrame.withColumnRenamed(timeColumn, timeColumnName)
    }
    requireSchema(df.schema)

    val convertedDf = convertDfTimestamps(df, timeUnit)
    // we want to keep time column first, but no code should rely on that
    val timeFirstDf = if (convertedDf.schema.fieldIndex(timeColumnName) == 0) {
      convertedDf
    } else {
      val nonTimeColumns = convertedDf.schema.fieldNames.filterNot(_ == TimeSeriesRDD.timeColumnName)
      convertedDf.select(timeColumnName, nonTimeColumns: _*)
    }

    if (isSorted || TimeSeriesStore.isSorted(timeFirstDf.queryExecution.executedPlan)) {
      timeFirstDf
    } else {
      timeFirstDf.sort(timeColumnName)
    }
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
    val canonizedDf = canonizeDF(dataFrame, isSorted = true, timeUnit, timeColumn)
    val partitionInfo = PartitionInfo(rangeSplits, deps)
    TimeSeriesRDD.fromDFWithPartInfo(canonizedDf, Some(partitionInfo))
  }

  /**
   * Read a parquet file into a [[TimeSeriesRDD]].
   *
   * @param sc         The [[org.apache.spark.SparkContext]].
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
   * )(
   *   isSorted = true,
   *   timeUnit = scala.concurrent.duration.MILLISECONDS,
   *   columns = Seq("time", "id", "price"),  // By default, null for all columns
   *   begin = "20100101",                    // By default, null for no boundary at begin
   *   end = "20150101"                       // By default, null for no boundary at end
   * )
   * }}}
   */
  def fromParquet(
    sc: SparkContext,
    paths: String*
  )(
    isSorted: Boolean,
    timeUnit: TimeUnit,
    @Nullable begin: String = null,
    @Nullable end: String = null,
    @Nullable columns: Seq[String] = null,
    timeColumn: String = timeColumnName
  ): TimeSeriesRDD = {
    fromParquet(
      sc,
      paths,
      isSorted = isSorted,
      beginNanos = Option(begin).map(TimeFormat.parse(_, timeUnit = timeUnit)),
      endNanos = Option(end).map(TimeFormat.parse(_, timeUnit = timeUnit)),
      columns = Option(columns),
      timeUnit = timeUnit,
      timeColumn = timeColumn
    )
  }

  /**
   * Read a Parquet file into a [[TimeSeriesRDD]] using optional nanoseconds for begin and end.
   *
   * Used by [[com.twosigma.flint.timeseries.io.read.ReadBuilder]].
   *
   * @param sc         The [[org.apache.spark.SparkContext]].
   * @param paths      The paths of the parquet file.
   * @param isSorted   flag specifies if the rows in the file have been sorted by their timestamps.
   * @param beginNanos Optional. Inclusive nanoseconds.
   * @param endNanos   Optional. Exclusive nanoseconds.
   * @param columns    Optional. Column in the parquet file to read into [[TimeSeriesRDD]]. IMPORTANT: This is critical
   *                   for performance. Reading small amounts of columns can easily increase performance by 10x
   *                   comparing to reading all columns in the file.
   * @param timeColumn Optional. Column in parquet file that specifies time.
   * @return a [[TimeSeriesRDD]]
   */
  private[timeseries] def fromParquet(
    sc: SparkContext,
    paths: Seq[String],
    isSorted: Boolean,
    beginNanos: Option[Long],
    endNanos: Option[Long],
    columns: Option[Seq[String]],
    timeUnit: TimeUnit,
    timeColumn: String
  ): TimeSeriesRDD = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = sqlContext.read.parquet(paths: _*)

    val prunedDf = columns.map { columnNames =>
      df.select(columnNames.map(col): _*)
    }.getOrElse(df)

    fromDF(DFBetween(prunedDf, beginNanos, endNanos, timeColumn))(
      isSorted = isSorted,
      timeUnit = timeUnit,
      timeColumn = timeColumn
    )
  }

  // Two functions below are factory methods. Only they call TimeSeriesRDDImpl constructor directly.

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
    val dataStore = TimeSeriesStore(orderedRdd, schema)

    new TimeSeriesRDDImpl(dataStore)
  }

  /**
   * Create a [[TimeSeriesRDD]] from a sorted [[DataFrame]] and partition time ranges.
   *
   * @param dataFrame The sorted [[DataFrame]] expected to convert
   * @param ranges    Time ranges for each partition
   * @return a [[TimeSeriesRDD]]
   */
  def fromDFWithRanges(
    dataFrame: DataFrame,
    ranges: Array[CloseOpen[Long]]
  ): TimeSeriesRDD = {
    val rangeSplits = ranges.zipWithIndex.map {
      case (range, index) => RangeSplit(OrderedRDDPartition(index), range)
    }
    val partInfo = PartitionInfo(rangeSplits, Seq(new OneToOneDependency(null)))
    TimeSeriesRDD.fromDFWithPartInfo(dataFrame, Some(partInfo))
  }

  /**
   * Creates a [[TimeSeriesRDD]] from a sorted [[DataFrame]] and partition info.
   *
   * @param dataFrame   A sorted [[DataFrame]].
   * @param partInfo    Partition info.
   * @return a [[TimeSeriesRDD]].
   */
  private[flint] def fromDFWithPartInfo(
    dataFrame: DataFrame,
    partInfo: Option[PartitionInfo]
  ): TimeSeriesRDD = new TimeSeriesRDDImpl(TimeSeriesStore(dataFrame, partInfo))

  // A function taking any row as input and just return `Seq[Any]()`.
  private[flint] val emptyKeyFn: InternalRow => Seq[Any] = {
    _: InternalRow => Seq[Any]()
  }

  private[flint] def createSafeGetAsAny(schema: StructType): Seq[String] => (InternalRow => Seq[Any]) = {
    (cols: Seq[String]) =>
      {
        if (cols.isEmpty) {
          emptyKeyFn
        } else {
          val columns = cols.map(column => (schema.fieldIndex(column), schema(column).dataType))
          (row: InternalRow) => InternalRowUtils.selectIndices(columns)(row)
        }
      }
  }

  // this function is used to select only the columns that are required by an operation
  private[timeseries] def pruneColumns(
    input: TimeSeriesRDD,
    requiredColumns: ColumnList,
    keys: Seq[String]
  ): TimeSeriesRDD = requiredColumns match {
    case ColumnList.All => input
    case ColumnList.Sequence(columnSeq) =>
      val columns = input.schema.fieldNames.toSet
      val neededColumns = (Seq(TimeSeriesRDD.timeColumnName) ++ columnSeq ++ keys).distinct
      neededColumns.foreach(column => require(columns.contains(column), s"Column $column doesn't exist."))

      if (neededColumns.toSet.size == input.schema.size) {
        input
      } else {
        input.keepColumns(neededColumns: _*)
      }
  }

  private[flint] def mergeSchema(schemaA: StructType, schemaB: StructType): StructType = {
    require(schemaA.length == schemaB.length, "Tables should have the same number of columns.")
    val zipped = schemaA.zip(schemaB)
    val isCompatible = zipped.forall {
      case (columnA, columnB) =>
        columnA.name == columnB.name && columnA.dataType == columnB.dataType
    }
    require(isCompatible, s"Schema $schemaA isn't compatible with $schemaB. Can't merge the tables.")
    val newFields = zipped.map {
      case (fieldA, fieldB) => StructField(fieldA.name, fieldA.dataType, fieldA.nullable | fieldB.nullable)
    }
    StructType(newFields)
  }
}

trait TimeSeriesRDD extends Serializable {

  val sparkSession: SparkSession

  /**
   * The schema of this [[TimeSeriesRDD]].
   *
   * It always keeps a column named "time" and of type Long and the time unit is NANOSECONDS.
   */
  val schema: StructType

  private[flint] val safeGetAsAny: Seq[String] => (InternalRow => Seq[Any])

  /**
   * Partition info of this [[TimeSeriesRDD]]
   */
  private[flint] def partInfo: Option[PartitionInfo]

  /**
   * An [[org.apache.spark.rdd.RDD RDD]] representation of this [[TimeSeriesRDD]].
   */
  def rdd: RDD[Row]

  /**
   * An [[com.twosigma.flint.rdd.OrderedRDD]] representation of this [[TimeSeriesRDD]]. Used to efficiently
   * perform join-like operations.
   */
  private[flint] def orderedRdd: OrderedRDD[Long, InternalRow]

  /**
   * Convert the this [[TimeSeriesRDD]] to a [[org.apache.spark.sql.DataFrame]].
   */
  def toDF: DataFrame

  /**
   * Validate the data is ordered and within each partition range.
   *
   * @note This is an expensive operation and should not be called unless for debugging.
   * @throws Exception if data is not ordered or not in partition range
   */
  def validate(): Unit

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
   * The column names provided should only be names of top-level columns. In particular, trying to specify a subcolumn
   * of a column of [[StructType]] will result in an exception.
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
   * Convenience method for `addColumnsForCycle` when an additional key is not needed.
   *
   * @see [[addColumnsForCycle(cycleColumns:Seq[CycleColumn],key:Seq[String]):TimeSeriesRDD*]]
   */
  def addColumnsForCycle(cycleColumns: CycleColumn*): TimeSeriesRDD

  /**
   * Adds new columns for each cycle using a seq of [[CycleColumn]], with convenient implicits for converting tuples to
   * [[CycleColumn]]s. A cycle is defined as a sequence of rows that share exactly the same timestamps.
   *
   * For each column, the user can use one of the following form, which will be converted to a
   * [[CycleColumn]] implicitly:
   *
   *   <ul>
   *    <li>(1) `"columnName" -> DataType -> Seq[Row] => Seq[Any]`</li>
   *    <li>(2) `"columnName" -> DataType -> Seq[Row] => Map[Row, Any]`</li>
   *    <li>(3) Using a built-in function, like: `"columnName" -> Ranker.percentile("price")`</li>
   *   </ul>
   *
   *  In (1), if the length of the returned `Seq[Any]` is less than the length of the input
   *  `Seq[Row]`, `null` is used to fill the remainder of rows.
   *
   *  In (2), if the `Map[Row, Any]` omits a row provided in the `Seq[Row]`, a value of
   *  `null` is used as the value for that row.
   *
   *  Note that duplicate column names are not accepted.
   *
   * @param cycleColumns A seq of [[CycleColumn CycleColumns]].
   * @param key If non-empty, rows are further grouped by the columns specified by `key` in addition to row time.
   * @see [[CycleColumnImplicits!]] for implicit conversions.
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
   *   Seq(
   *    "adjusted1" -> DoubleType -> { rows: Seq[Row] =>
   *       rows.map { row => row.getDouble(2) * rows.size) }
   *     },
   *    "adjusted2" -> DoubleType -> { rows: Seq[Row] =>
   *       rows.map { row => (row, row.getDouble(2) * rows.size) }.toMap
   *     },
   *    "adjusted3" -> CycleColumn.unnamed(DoubleType, { rows: Seq[Row] =>
   *      rows.map { row => row.getDouble(2) * rows.size }
   *      })
   *  ),
   *   key = Seq("id")
   * )
   * // time  id  sellingPrice adjusted1 adjusted2 adjusted3
   * // ----------------------------------------------------
   * // 1000L 0   1.0          1.0       1.0       1.0
   * // 1000L 1   2.0          4.0       4.0       4.0
   * // 1000L 1   3.0          6.0       6.0       6.0
   * // 2000L 0   3.0          6.0       6.0       6.0
   * // 2000L 0   4.0          8.0       8.0       8.0
   * // 2000L 1   5.0          5.0       5.0       5.0
   * // 2000L 2   6.0          6.0       6.0       6.0
   * }}}
   */
  def addColumnsForCycle(
    cycleColumns: Seq[CycleColumn],
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD

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
   * @param inclusion      Defines the shape of the intervals, i.e, whether intervals are [begin, end) or (begin, end].
   *                       "begin" causes rows that are at the exact beginning of an interval to be included and
   *                       rows that fall on the exact end to be excluded, as represented by the interval [begin, end).
   *                       "end" causes rows that are at the exact beginning of an interval to be excluded and rows that
   *                       fall on the exact end to be included, as represented by the interval (begin, end].
   *                       Defaults to "begin".
   * @param rounding       Determines how timestamps of input rows are rounded to timestamps of intervals.
   *                       "begin" causes the input rows to be rounded to the beginning timestamp of
   *                       an interval. "end" causes the input rows to be rounded to the ending timestamp of an
   *                       interval. Defaults to "end".
   *
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
   * // 2000L [[1000L, 1.0], [1500L, 2.0]]
   * // 3000L [[2000L, 3.0], [2500L, 4.0]]
   * }}}
   */
  def groupByInterval(
    clock: TimeSeriesRDD,
    key: Seq[String] = Seq.empty,
    inclusion: String = "begin",
    rounding: String = "end"
  ): TimeSeriesRDD

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
   * [[TimeSeriesRDD]] includes all rows from each in temporal order. If there is a timestamp ties,
   * the rows in this [[TimeSeriesRDD]] will be returned earlier than those from the other
   * [[TimeSeriesRDD]].
   *
   * @param other The other [[TimeSeriesRDD]] expected to merge.
   * @return a merged  [[TimeSeriesRDD]] with rows from each [[TimeSeriesRDD]] in temporal order.
   * @example
   * {{{
   * val thisTSRdd = ...
   * // +----+---+-----+
   * // |time|id|price|
   * // +----+---+-----+
   * // |1000|  3|  1.0|
   * // |1050|  3|  1.5|
   * ...
   *
   * val otherTSRdd = ...
   * // +----+---+-----+
   * // |time|id|price|
   * // +----+---+-----+
   * // |1000|  7|  0.5|
   * // |1050|  7|  2.0|
   * ...
   *
   * val mergedTSRdd = thisTSRdd.merge(otherTSRdd)
   * // +----+---+-----+
   * // |time|id|price|
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
   * @param inclusion      Defines the shape of the intervals, i.e, whether intervals are [begin, end) or (begin, end].
   *                       "begin" causes rows that are at the exact beginning of an interval to be included and
   *                       rows that fall on the exact end to be excluded, as represented by the interval [begin, end).
   *                       "end" causes rows that are at the exact beginning of an interval to be excluded and rows that
   *                       fall on the exact end to be included, as represented by the interval (begin, end].
   *                       Defaults to "begin".
   * @param rounding       Determines how timestamps of input rows are rounded to timestamps of intervals.
   *                       "begin" causes the input rows to be rounded to the beginning timestamp of
   *                       an interval. "end" causes the input rows to be rounded to the ending timestamp of an
   *                       interval. Defaults to "end".
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
    inclusion: String = "begin",
    rounding: String = "end"
  ): TimeSeriesRDD

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

  private[flint] def summarizeWindowBatches(
    window: Window,
    columns: Seq[String] = null,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD

  private[flint] def concatArrowAndExplode(
    baseRowsColumnName: String,
    schemaColumNames: Seq[String],
    dataColumnNames: Seq[String]
  ): TimeSeriesRDD

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
   * Casts columns to a different data type (e.g. from StringType to IntegerType).
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
  def cast(columns: (String, DataType)*): TimeSeriesRDD

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

  /**
   * Apply a transformation on the underlying Spark DataFrame without altering partitioning info.
   *
   * This assumes the transformation truly does not alter the partition info, and does not check this fact.
   * Be careful when using this method, when you do, you are assuming responsibility for ensuring this fact.
   *
   * @example
   * {{{
   *   val tsrdd = ...
   *   val tsrdd2 = tsrdd.withPartitionsPreserved { df =>
   *     df.withColumn("id", F.explode(df("ids")))
   *   }
   * }}}
   * @param xform A function transforming a [[DataFrame]] which does not change its partition information.
   * @return a [[TimeSeriesRDD]] whose underlying [[DataFrame]] is the result of the transformation.
   */
  def withPartitionsPreserved(xform: DataFrame => DataFrame): TimeSeriesRDD
}

/**
 * The implementation uses two assumptions:
 *     - dataFrame is sorted by time;
 *     - if partInfo is defined - then it should correctly represent dataFrame partitioning.
 */
class TimeSeriesRDDImpl private[timeseries] (
  val dataStore: TimeSeriesStore
) extends TimeSeriesRDD {

  val sparkSession = dataStore.dataFrame.sparkSession
  import sparkSession.implicits._
  import TimeSeriesRDD.timeColumnName

  override val schema: StructType = dataStore.schema
  override val safeGetAsAny: Seq[String] => (InternalRow => Seq[Any]) = TimeSeriesRDD.createSafeGetAsAny(schema)

  /**
   * Get a key function from a list of column names.
   */
  // TODO: This should return a object that is associated with this TimeSeriesRDD, this can
  //       help us catch bugs where we are passing key function to the wrong OrderedRDD
  override def partInfo: Option[PartitionInfo] = dataStore.partInfo

  private[flint] override def orderedRdd = dataStore.orderedRdd

  // you can't store references to row objects from `unsafeOrderedRdd`, or use rdd.collect() without making
  // safe copies of rows. The current implementation might use the same memory buffer to process all rows
  // per partition, so the referenced bytes might be overwritten by the next row.
  private[timeseries] def unsafeOrderedRdd = dataStore.unsafeOrderedRdd

  /**
   * Values converter for this rdd.
   *
   * If the row is not nested, returns a identity function.
   *
   * If the row is nested, returns a function that knows how to convert internal row to external row
   */
  private val toExternalRow: InternalRow => ERow = CatalystTypeConvertersWrapper.toScalaRowConverter(schema)

  override def rdd: RDD[Row] = dataStore.rdd

  override def toDF: DataFrame = dataStore.dataFrame

  def count(): Long = dataStore.dataFrame.count()

  def first(): Row = dataStore.dataFrame.first()

  def collect(): Array[Row] = dataStore.dataFrame.collect()

  def cache(): TimeSeriesRDD = {
    dataStore.cache(); this
  }

  def persist(): TimeSeriesRDD = {
    dataStore.persist(); this
  }

  def persist(newLevel: StorageLevel): TimeSeriesRDD = {
    dataStore.persist(newLevel); this
  }

  def unpersist(blocking: Boolean = true): TimeSeriesRDD = {
    dataStore.unpersist(blocking); this
  }

  def repartition(numPartitions: Int): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.repartition(numPartitions), schema)

  def coalesce(numPartitions: Int): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.coalesce(numPartitions), schema)

  def keepRows(fn: Row => Boolean): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.filterOrdered {
      (_: Long, r: InternalRow) => fn(toExternalRow(r))
    }, schema)

  def deleteRows(fn: Row => Boolean): TimeSeriesRDD =
    TimeSeriesRDD.fromInternalOrderedRDD(unsafeOrderedRdd.filterOrdered {
      (_: Long, r: InternalRow) => !fn(toExternalRow(r))
    }, schema)

  def keepColumns(columns: String*): TimeSeriesRDD = withUnshuffledDataFrame {
    val nonTimeColumns = columns.filterNot(_ == timeColumnName)

    val newColumns = (timeColumnName +: nonTimeColumns).map {
      // We need to escape the column name to ensure that columns with names like "a.b" are handled well.
      columnName => dataStore.dataFrame.col(s"`$columnName`")
    }

    dataStore.dataFrame.select(newColumns: _*)
  }

  def deleteColumns(columns: String*): TimeSeriesRDD = withUnshuffledDataFrame {
    require(!columns.contains(timeColumnName), "You can't delete the time column!")

    dataStore.dataFrame.drop(columns: _*)
  }

  def renameColumns(fromTo: (String, String)*): TimeSeriesRDD = withUnshuffledDataFrame {
    val fromToMap = fromTo.toMap
    require(!fromToMap.contains(timeColumnName), "You can't rename the time column!")
    require(fromToMap.size == fromTo.size, "Repeating column names are not allowed!")

    val newColumns = dataStore.dataFrame.schema.fieldNames.map {
      columnName =>
        if (fromToMap.contains(columnName)) {
          dataStore.dataFrame.col(s"`$columnName`").as(fromToMap(columnName))
        } else {
          dataStore.dataFrame.col(s"`$columnName`")
        }
    }

    dataStore.dataFrame.select(newColumns: _*)
  }

  def addColumns(columns: ((String, DataType), Row => Any)*): TimeSeriesRDD = {
    val (add, newSchema) = InternalRowUtils.addOrUpdate(schema, columns.map(_._1))

    TimeSeriesRDD.fromInternalOrderedRDD(
      orderedRdd.mapValues {
        (_, row) =>
          val extRow = toExternalRow(row)
          add(row, columns.map {
            case (_, columnFunc) => columnFunc(extRow)
          })
      },
      newSchema
    )
  }

  def addColumnsForCycle(columns: CycleColumn*): TimeSeriesRDD =
    addColumnsForCycle(columns.toSeq, key = Seq.empty)

  def addColumnsForCycle(
    columns: Seq[CycleColumn],
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = {
    val targetNameDataTypes = columns.map(c => c.name -> c.dataType)
    val (add, newSchema) = InternalRowUtils.addOrUpdate(schema, targetNameDataTypes)

    val newRdd = orderedRdd.groupByKey(safeGetAsAny(key)).flatMapValues { (_, rows: Array[InternalRow]) =>
      val eRows = rows.map(toExternalRow)

      val rowColumnValues: Seq[IndexedSeq[Any]] = columns.map { cycleColumn =>
        // Pad returned sequences to the same length as the input rows with `null`
        cycleColumn.applyCycle(eRows).padTo(eRows.length, null)(scala.collection.breakOut)
      }

      rows.zipWithIndex.map {
        case (row, idx) =>
          add(row, rowColumnValues.map(value => value(idx)))
      }
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def groupByCycle(key: Seq[String] = Seq.empty): TimeSeriesRDD = summarizeCycles(Summarizers.rows("rows"), key)

  // For python compatibility. Do not use this.
  @PythonApi(until = "0.4.0")
  private[flint] def groupByInterval(
    clock: TimeSeriesRDD,
    key: Seq[String],
    beginInclusive: Boolean
  ): TimeSeriesRDD =
    if (beginInclusive) {
      groupByInterval(clock, key, "begin", "begin")
    } else {
      groupByInterval(clock, key, "end", "end")
    }

  def groupByInterval(
    clock: TimeSeriesRDD,
    key: Seq[String] = Seq.empty,
    inclusion: String = "begin",
    rounding: String = "end"
  ): TimeSeriesRDD = summarizeIntervals(clock, Summarizers.rows("rows"), key, inclusion, rounding)

  def addWindows(
    window: Window,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = summarizeWindows(window, Summarizers.rows(s"window_${window.name}"), key)

  def merge(other: TimeSeriesRDD): TimeSeriesRDD = {
    val newSchema = TimeSeriesRDD.mergeSchema(schema, other.schema)
    val otherOrderedRdd = other.orderedRdd
    TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.merge(otherOrderedRdd), newSchema)
  }

  def leftJoin(
    right: TimeSeriesRDD,
    tolerance: String = "0ns",
    key: Seq[String] = Seq.empty,
    leftAlias: String = null,
    rightAlias: String = null
  ): TimeSeriesRDD = {
    val window = Windows.pastAbsoluteTime(tolerance)
    val toleranceFn = window.shift _
    val joinedRdd = orderedRdd.leftJoin(
      right.orderedRdd, toleranceFn, safeGetAsAny(key), right.safeGetAsAny(key)
    )

    val (concat, newSchema) = InternalRowUtils.concat2(
      this.schema, right.schema,
      Option(leftAlias), Option(rightAlias),
      (key :+ timeColumnName).toSet
    )

    val rightNullRow = InternalRow.fromSeq(Array.fill[Any](right.schema.size)(null))

    val newRdd = joinedRdd.collectOrdered {
      case (_, (r1, Some((_, r2)))) => concat(r1, r2)
      case (_, (r1, None)) => concat(r1, rightNullRow)
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
    val window = Windows.futureAbsoluteTime(tolerance)
    val toleranceFn = window.shift _
    val joinedRdd = orderedRdd.futureLeftJoin(
      right.orderedRdd, toleranceFn, safeGetAsAny(key),
      right.safeGetAsAny(key), strictForward = strictLookahead
    )

    val (concat, newSchema) = InternalRowUtils.concat2(
      this.schema, right.schema,
      Option(leftAlias), Option(rightAlias),
      (key :+ timeColumnName).toSet
    )

    val rightNullRow = InternalRow.fromSeq(Array.fill[Any](right.schema.size)(null))
    val newRdd = joinedRdd.collectOrdered {
      case (_, (r1, Some((_, r2)))) => concat(r1, r2)
      case (_, (r1, None)) => concat(r1, rightNullRow)
    }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  def summarizeCycles(summarizer: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD = {

    // TODO: investigate the performance of the following implementation of summarizeCycles for supporting
    //       OverlappableSummarizer.
    //       - extract timestamps for each cycle as a new time series
    //       - summarizeWindows the above time series to this time series with OverlappableSummarizer.
    require(
      !summarizer.isInstanceOf[OverlappableSummarizerFactory],
      s"Function summarizeCycles currently does not support OverlappableSummarizer $summarizer"
    )

    val pruned = TimeSeriesRDD.pruneColumns(this, summarizer.requiredColumns, key)
    val sum = summarizer(pruned.schema)
    val newSchema = Schema.prependTimeAndKey(sum.outputSchema, key.map(pruned.schema(_)))
    val numColumns = newSchema.length

    TimeSeriesRDD.fromInternalOrderedRDD(
      pruned.orderedRdd.summarizeByKey(pruned.safeGetAsAny(key), sum)
        .mapValues { (k, v) =>
          InternalRowUtils.concatTimeWithValues(k, numColumns, v._1, v._2.toSeq(pruned.schema))
        },
      newSchema
    )
  }

  // For python compatibility. Do not use this.
  @PythonApi(until = "0.4.0")
  private[flint] def summarizeIntervals(
    clock: TimeSeriesRDD,
    summarizer: SummarizerFactory,
    key: Seq[String],
    beginInclusive: Boolean
  ): TimeSeriesRDD = {
    if (beginInclusive) {
      summarizeIntervals(clock, summarizer, key, "begin", "begin")
    } else {
      summarizeIntervals(clock, summarizer, key, "end", "end")
    }
  }

  def summarizeIntervals(
    clock: TimeSeriesRDD,
    summarizer: SummarizerFactory,
    key: Seq[String] = Seq.empty,
    inclusion: String = "begin",
    rounding: String = "end"
  ): TimeSeriesRDD = {
    require(Seq("begin", "end").contains(inclusion), "inclusion must be \"begin\" or \"end\"")
    require(Seq("begin", "end").contains(rounding), "rounding must be \"begin\" or \"end\"")

    val pruned = TimeSeriesRDD.pruneColumns(this, summarizer.requiredColumns, key)
    val sum = summarizer(pruned.schema)
    val clockLocal = clock.toDF.select("time").as[(Long)].collect()
    val intervalized = pruned.orderedRdd.intervalize(clockLocal, inclusion, rounding).mapValues {
      case (_, v) => v._2
    }

    val newSchema = Schema.prependTimeAndKey(sum.outputSchema, key.map(pruned.schema(_)))
    val numColumns = newSchema.length
    TimeSeriesRDD.fromInternalOrderedRDD(
      intervalized.summarizeByKey(pruned.safeGetAsAny(key), sum)
        .mapValues { (k, v) =>
          InternalRowUtils.concatTimeWithValues(k, numColumns, v._1, v._2.toSeq(pruned.schema))
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
      case w: TimeWindow =>
        (sum, summarizer) match {
          case (s: OverlappableSummarizer, sf: OverlappableSummarizerFactory) =>
            orderedRdd.summarizeWindows(w.of, s, keyFn, sf.window.of)
          case (s, sf: SummarizerFactory) =>
            orderedRdd.summarizeWindows(w.of, s, keyFn)
        }
      case _ => sys.error(s"Unsupported window type: $window")
    }

    val newRdd = summarizedRdd.mapValues { case (_, (row1, row2)) => concat(row1, row2) }
    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  /**
   * Summarize window batches.
   *
   * `summarizeWindows` with python udf consists of three steps:
   *  1. [[summarizeWindowBatches]]
   *  This step breaks the left table and right table into multiple batches and computes indices for each left row.
   *
   *  2. withColumn with PySpark UDF to compute each batch
   *  This is done on PySpark side. Once we have each batch in arrow format, we can now send the bytes
   *  to python worker using regular PySpark UDF, compute rolling windows in python using precomputed indices, and
   *  return the result in arrow format.
   *  See dataframe.py/summarizeWindows
   *
   *  3. [[concatArrowAndExplode()]]
   *  The final step concatenates new columns to the original rows, and explodes each batch back to multiple rows.
   *
   * This function is used for the first step of `summarizeWindows` with python udf.
   *
   * This function divides each left table partition into multiple batches, and for each batch,
   * produces one nested row.
   *
   * The schema of the nested row is defined in [[ArrowWindowBatchSummarizer]] and it contains
   * the original left rows (used for concatenation later), left and right batches (Arrow record batches
   * to be passed to Python worker for udf evaluation) and the indices (also Arrow record batches, used
   * for defining windows for each row in the left)
   *
   * Each nested row is also prepended with a timestamp. the timestamp is the first timestamp in the left rows.
   * Because each nested row represents a batch of rows, the timestamp is merely a place holder and isn't used
   * for anything real.
   *
   * Finally, the partition ranges of the output [[TimeSeriesRDD]] is the same as the left table. This is important
   * because summarizeWindows doesn't change row order or partitioning. We maintain this invariance throughout
   * different steps of summarizeWindows with python udf.
   *
   * To give an concrete example, here is how output looks like:
   *
   * +----+--------------------+--------------------+---------------------+---------------------+
   * |time|   __window_baseRows|  __window_leftBatch| __window_rightBatch | __window_indices    |
   * +----+--------------------+--------------------+--------------------+--------------------+--
   * |1000|[[1000,1,100], ...]]|[41 52 52 4F 57 3...||[41 52 52 4F 57 3...||[41 52 52 4F 57 3...|
   * |2500|[[2500,1,4], ...]]  |[41 52 52 4F 57 3...||[41 52 52 4F 57 3...||[41 52 52 4F 57 3...|
   * +----+--------------------+--------------------+---------------------+---------------------+
   *
   * @param columns: Required columns in leftBatch.
   *                 If null, all columns from the left table will be included in left batches.
   *                 If empty seq, leftBatches will be nulls.
   * @see [[concatArrowAndExplode()]]
   */
  @PythonApi
  private[flint] override def summarizeWindowBatches(
    window: Window,
    columns: Seq[String] = null,
    key: Seq[String] = Seq.empty
  ): TimeSeriesRDD = {
    val batchSize = sparkSession.conf.get(
      FlintConf.WINDOW_BATCH_MAXSIZE_CONF,
      FlintConf.WINDOW_BATCH_MAXSIZE_DEFAULT
    ).toInt

    val otherRdd: TimeSeriesRDD = null
    val otherColumns: Seq[String] = null

    val prunedSchema = Option(columns).map(cols =>
      StructType(cols.map(c => this.schema(this.schema.fieldIndex(c))))).getOrElse(this.schema)

    val (otherORdd, otherSchema, otherPrunedSchema, otherSk) =
      if (otherRdd == null) {
        (this.orderedRdd, this.schema, prunedSchema, this.safeGetAsAny(key))
      } else {
        val otherPrunedSchema = Option(otherColumns).map{ cols =>
          require(cols.nonEmpty, "otherColumns cannot be empty")
          StructType(cols.map(c => otherRdd.schema(otherRdd.schema.fieldIndex(c))))
        }.getOrElse(otherRdd.schema)

        (otherRdd.orderedRdd, otherRdd.schema, otherPrunedSchema, otherRdd.safeGetAsAny(key))
      }

    val sk = this.safeGetAsAny(key)

    val sum = new ArrowWindowBatchSummarizer(
      this.schema, prunedSchema, otherSchema, otherPrunedSchema
    )

    val summarizedRdd = window match {
      case w: TimeWindow =>
        orderedRdd.summarizeWindowBatches(w.of, sum, sk, otherORdd, otherSk, batchSize)
    }

    val (concat, newSchema) = InternalRowUtils.concat2(
      StructType(Seq(StructField(timeColumnName, LongType))),
      sum.schema
    )

    val newRdd = summarizedRdd.mapValues{
      case (k, row) =>
        val values: Array[Any] = Array(k)
        concat(new GenericInternalRow(values), row)
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, newSchema)
  }

  /**
   * Computes aggregate statistics of all rows in multi-level tree aggregation fashion.
   *
   * @param summarizerFactory A summarizer expected to perform aggregation.
   * @param key               Columns that could be used as the grouping key and the aggregations will be performed
   *                          per key level.
   * @param depth             The depth of tree for merging partial summarized results across different partitions
   *                          in a a multi-level tree aggregation fashion.
   * @return a [[TimeSeriesRDD]] with summarized column.
   */
  private[flint] def summarizeInternal(
    summarizerFactory: SummarizerFactory, key: Seq[String] = Seq.empty, depth: Int
  ): TimeSeriesRDD = {
    val pruned = TimeSeriesRDD.pruneColumns(this, summarizerFactory.requiredColumns, key)
    val summarizer = summarizerFactory(pruned.schema)
    val keyGetter = pruned.safeGetAsAny(key)
    val summarized = summarizerFactory match {
      case factory: OverlappableSummarizerFactory =>
        pruned.orderedRdd.summarize(
          summarizer.asInstanceOf[OverlappableSummarizer], factory.window.of, keyGetter, depth
        )
      case _ => pruned.orderedRdd.summarize(summarizer, keyGetter, depth)
    }
    val rows = summarized.map {
      case (keyValues, row) => InternalRowUtils.prepend(row, summarizer.outputSchema, 0L +: keyValues: _*)
    }

    val newSchema = Schema.prependTimeAndKey(summarizer.outputSchema, key.map(pruned.schema(_)))
    TimeSeriesRDD.fromSeq(pruned.orderedRdd.sc, rows.toSeq, newSchema, true, 1)
  }

  def summarize(summarizerFactory: SummarizerFactory, key: Seq[String] = Seq.empty): TimeSeriesRDD =
    summarizeInternal(summarizerFactory, key, 2)

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

  def cast(updates: (String, DataType)*): TimeSeriesRDD = withUnshuffledDataFrame {
    val columnsToCastMap = updates.toMap
    require(!columnsToCastMap.contains(timeColumnName), "You can't cast the time column!")

    val newColumns = dataStore.dataFrame.schema.fieldNames.map {
      columnName =>
        if (columnsToCastMap.contains(columnName)) {
          dataStore.dataFrame.col(columnName).cast(columnsToCastMap(columnName))
        } else {
          dataStore.dataFrame.col(columnName)
        }
    }

    dataStore.dataFrame.select(newColumns: _*)
  }

  @Experimental
  def setTime(fn: Row => Long, window: String): TimeSeriesRDD = {
    if (window == null) {
      val timeIndex = schema.fieldIndex(timeColumnName)
      TimeSeriesRDD.fromInternalOrderedRDD(orderedRdd.mapOrdered {
        case (_: Long, r: InternalRow) =>
          val timeStamp = fn(toExternalRow(r))
          (timeStamp, InternalRowUtils.update(r, schema, timeIndex -> timeStamp))
      }, schema)
    } else {
      throw new IllegalArgumentException(s"Non-default window isn't supported at this moment.")
    }
  }

  def shift(window: ShiftTimeWindow): TimeSeriesRDD = {
    val timeIndex = schema.fieldIndex(timeColumnName)

    // Note: Don't change this to unsafeOrderedRdd, it might cause data corruption and there is no test for this now
    //
    // table.select("col1").distinct().show()
    // table = table.shiftTime("1day")
    // table = table.withColumn('col3', udf(...)(table.col2))
    // table.select("col1").distinct().show()
    // The first and second show has different results, "col1" seems to be corrupted
    val newRdd = orderedRdd.shift(window.shift).mapValues {
      case (t, iRow) => InternalRowUtils.update(iRow, schema, timeIndex -> t)
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newRdd, schema)
  }

  // this method reuses partition information of the current TSRDD
  @inline private def withUnshuffledDataFrame(dataFrame: => DataFrame): TimeSeriesRDD = {
    val newDataStore = TimeSeriesStore(dataFrame, dataStore.partInfo)
    new TimeSeriesRDDImpl(newDataStore)
  }

  def validate(): Unit = {
    val ranges = orderedRdd.rangeSplits.map { _.range }

    rdd.mapPartitionsWithIndex {
      case (index, rows) =>
        val range = ranges(index)
        var lastTimestamp = Long.MinValue
        for (row <- rows) {
          val timestamp = row.getAs[Long](timeColumnName)
          assert(
            range.contains(timestamp),
            s"Timestamp $timestamp is not in range $range of partition $index"
          )
          assert(
            timestamp >= lastTimestamp,
            s"Timestamp $timestamp is smaller than the previous timestamp $lastTimestamp"
          )
          lastTimestamp = timestamp
        }
        Iterator.empty
    }.count()
  }

  override def withPartitionsPreserved(xform: DataFrame => DataFrame): TimeSeriesRDD = {
    val newDataFrame = xform(dataStore.dataFrame)
    new TimeSeriesRDDImpl(TimeSeriesStore(newDataFrame, dataStore.partInfo))
  }

  /**
   * This function is used as the final step of summarizeWindows and addColumnsForCycle for python udf.
   *
   * Given a nested [[TimeSeriesRDD]] contains one base column of Array[InternalRow] and one or more
   * Arrow record batch columns (in deserialized bytes form),
   * this function will concat the Arrow record batches with to the rows and then explode the results.
   *
   * Example:
   *
   * Input:
   * Row((Row(1000, 1), Row(1000, 2), Row(1050, 1)), ArrowBatchRecord(10, 20, 30))
   * Row((Row(1100, 1), Row(1150, 1)), ArrowBatchRecord(30, 40))
   *
   * Output:
   * Row(1000, 1, 10)
   * Row(1000, 2, 20)
   * Row(1050, 1, 30)
   * Row(1100, 1, 30)
   * Row(1150, 1, 40)
   *
   * For each input row, the number of elements in the base column and Arrow columns must match.
   *
   * @see [[summarizeWindowBatches()]]
   */
  private[flint] override def concatArrowAndExplode(
    baseRowsColumnName: String,
    schemaColumNames: Seq[String],
    dataColumnNames: Seq[String]
  ): TimeSeriesRDD = {

    val baseRowSchema = schema(baseRowsColumnName).dataType match {
      case ArrayType(s: StructType, _) => s
      case _ => throw new IllegalArgumentException(
        s"Cannot parse schema for base rows. Schema: ${schema(baseRowsColumnName)}"
      )
    }
    val timeColumnIndex = schema.fieldIndex(timeColumnName)

    val schemas = schemaColumNames.map(schema(_).dataType.asInstanceOf[StructType])
    val newSchema = StructType(baseRowSchema.fields ++ schemas.flatMap(_.fields))

    val baseRowsColumnIndex = schema.fieldIndex(baseRowsColumnName)
    val arrowColumnIndices = dataColumnNames.map(schema.fieldIndex)

    val newOrdd = orderedRdd.mapPartitionsWithIndexOrdered {
      case (_, rows) =>
        val allocator = new RootAllocator(Long.MaxValue)
        TaskContext.get().addTaskCompletionListener { _ =>
          allocator.close()
        }

        rows.flatMap {
          case (_, row) =>
            val baseRowsArrayData = row.getArray(baseRowsColumnIndex)
            val arrowColumns = arrowColumnIndices.map(row.getBinary)

            InternalRowUtils.concatArrowColumns(
              allocator,
              baseRowsArrayData,
              baseRowSchema,
              arrowColumns,
              timeColumnIndex,
              baseRowSchema.length,
              newSchema.length - baseRowSchema.length
            )
        }
    }

    TimeSeriesRDD.fromInternalOrderedRDD(newOrdd, newSchema)
  }
}
