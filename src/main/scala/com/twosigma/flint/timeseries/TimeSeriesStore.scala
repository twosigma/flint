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

import com.twosigma.flint.rdd.{ KeyPartitioningType, OrderedRDD, RangeSplit }
import org.apache.spark.{ Dependency, OneToOneDependency }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DFConverter, DataFrame, Row, SQLContext }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

/**
 * - a DataFrame will be normalized via DataFrame -> (normalized ) OrderedRDD -> DataFrame
 * conversion if there is no partition information provided;
 * - a DataFrame will be used directly as a backend if a partition information is provided.
 */
private[timeseries] object TimeSeriesStore {

  /**
   * Convert a [[org.apache.spark.sql.DataFrame]] to a [[TimeSeriesStore]].
   *
   * @param dataFrame  A [[org.apache.spark.sql.DataFrame]] with `time` column, and sorted by time value.
   * @param partInfo   This parameter should be either empty, or correctly represent partitioning of
   *                   the given DataFrame.
   * @return a [[TimeSeriesStore]].
   */
  def apply(dataFrame: DataFrame, partInfo: Option[PartitionInfo]): TimeSeriesStore =
    if (partInfo.isDefined) {
      new NormalizedDataFrameStore(dataFrame, partInfo.get)
    } else {
      val schema = dataFrame.schema
      val internalRows = dataFrame.queryExecution.toRdd
      val pairRdd = internalRows.mapPartitions { rows =>
        val converter = TimeSeriesStore.getInternalRowConverter(schema, requireCopy = false)
        rows.map(converter)
      }

      // Ideally, we may use dataFrame.select("time").queryExecution.toRdd to build the `keyRdd`.
      // This may allow us push down column pruning and thus reduce IO. However, there is no guarantee that
      // DataFrame.sort("time").select("time") preserves partitioning as DataFrame.sort("time").
      val timeColumnIndex = schema.fieldIndex(TimeSeriesRDD.timeColumnName)
      val keyRdd = internalRows.mapPartitions { rows => rows.map(_.getLong(timeColumnIndex)) }
      val orderedRdd = OrderedRDD.fromRDD(pairRdd, KeyPartitioningType(isSorted = true, isNormalized = false), keyRdd)
      TimeSeriesStore(orderedRdd, schema)
    }

  /**
   * Convert an [[OrderedRDD]] to a [[TimeSeriesStore]].
   *
   * @param orderedRdd  An [[OrderedRDD]] with `time` column, and sorted by time value.
   * @param schema      Schema of this [[TimeSeriesStore]].
   * @return a [[TimeSeriesStore]].
   */
  def apply(orderedRdd: OrderedRDD[Long, InternalRow], schema: StructType): TimeSeriesStore = {
    val sc = orderedRdd.sparkContext
    val sqlContext = SQLContext.getOrCreate(sc)
    val df = DFConverter.toDataFrame(sqlContext, schema, orderedRdd)

    require(orderedRdd.partitions(0).index == 0, "Partition index should start with zero.")
    // the rdd parameter is not used
    val deps = new OneToOneDependency(null)
    val partInfo = PartitionInfo(orderedRdd.rangeSplits, Seq(deps))

    new NormalizedDataFrameStore(df, partInfo)
  }

  /**
   * Similar to TimeSeriesRDD.getRowConverter(), but used to convert a [[DataFrame]] into a [[TimeSeriesRDD]]
   *
   * @param schema          The schema of the input rows.
   * @param requireCopy     Whether to require new row objects or reuse the existing ones.
   * @return                a function to convert [[InternalRow]] into a tuple.
   * @note                  if `requireNewCopy` is true then the function makes an extra copy of the row value.
   *                        Otherwise it makes no copies of the row.
   */
  private[timeseries] def getInternalRowConverter(
    schema: StructType,
    requireCopy: Boolean
  ): InternalRow => (Long, InternalRow) = {
    val timeColumnIndex = schema.fieldIndex(TimeSeriesRDD.timeColumnName)

    if (requireCopy) {
      (internalRow: InternalRow) => (internalRow.getLong(timeColumnIndex), internalRow.copy())
    } else {
      (internalRow: InternalRow) => (internalRow.getLong(timeColumnIndex), internalRow)
    }
  }
}

private[timeseries] sealed trait TimeSeriesStore extends Serializable {
  /**
   * Returns the schema of this [[TimeSeriesStore]].
   */
  def schema: StructType

  /**
   * Returns an [[RDD]] representation of this [[TimeSeriesStore]].
   */
  def rdd: RDD[Row]

  /**
   * Returns an [[OrderedRDD]] representation of this [[TimeSeriesStore]] with safe row copies.
   */
  def orderedRdd: OrderedRDD[Long, InternalRow]

  /**
   * Returns an [[OrderedRDD]] representation of this [[TimeSeriesStore]] with `unsafe` rows.
   */
  def unsafeOrderedRdd: OrderedRDD[Long, InternalRow]

  /**
   * Returns [[PartitionInfo]] if internal representation is normalized, or None otherwise.
   */
  def partInfo: Option[PartitionInfo]

  /**
   * Returns a [[org.apache.spark.sql.DataFrame]] representation of this [[TimeSeriesStore]].
   */
  def dataFrame: DataFrame

  /**
   * Persists this [[TimeSeriesStore]] with default storage level (MEMORY_ONLY).
   */
  def cache(): Unit

  /**
   * Persists this [[TimeSeriesStore]] with default storage level (MEMORY_ONLY).
   */
  def persist(): Unit

  /**
   * Persists this [[TimeSeriesStore]] with specified storage level.
   *
   * @param newLevel The storage level.
   */
  def persist(newLevel: StorageLevel): Unit

  /**
   * Marks this [[TimeSeriesStore]] as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean = true): Unit
}

/**
 * There are two ways to retrieve a DataFrame object in this class:
 * - use internalDf object (should be used for all DataFrame operations)
 * - use newDf function (should be used if you access lazy fields of DF's QueryExecution)
 */
private[timeseries] class NormalizedDataFrameStore(
  private val internalDf: DataFrame,
  private var internalPartInfo: PartitionInfo
) extends TimeSeriesStore {

  override val schema: StructType = internalDf.schema

  override def rdd: RDD[Row] = newDf.rdd

  /**
   * The field below can be used only for DataFrame operations, since they respect DF caching.
   * If you need an [[RDD]] representation - use `newDF`.
   */
  override val dataFrame: DataFrame = internalDf

  override def orderedRdd: OrderedRDD[Long, InternalRow] = toOrderedRdd(requireCopy = true)

  override def unsafeOrderedRdd: OrderedRDD[Long, InternalRow] = toOrderedRdd(requireCopy = false)

  override def partInfo: Option[PartitionInfo] = Some(internalPartInfo)

  override def persist(): Unit = internalDf.persist()

  override def persist(newLevel: StorageLevel): Unit = internalDf.persist(newLevel)

  override def cache(): Unit = internalDf.cache()

  override def unpersist(blocking: Boolean): Unit = internalDf.unpersist(blocking)

  private def toOrderedRdd(requireCopy: Boolean): OrderedRDD[Long, InternalRow] = {
    val internalRows = newDf.queryExecution.toRdd
    val pairRdd = internalRows.mapPartitions { rows =>
      val converter = TimeSeriesStore.getInternalRowConverter(schema, requireCopy)
      rows.map(converter)
    }

    OrderedRDD.fromRDD(pairRdd, internalPartInfo.deps, internalPartInfo.splits)
  }

  /**
   * We create a new DataFrame object to force reevaluation of 'lazy val' fields
   * in [[org.apache.spark.sql.execution.QueryExecution]].
   */
  private def newDf: DataFrame = {
    internalDf.withColumnRenamed(TimeSeriesRDD.timeColumnName, TimeSeriesRDD.timeColumnName)
  }
}

private[timeseries] case class PartitionInfo(
  splits: Seq[RangeSplit[Long]],
  deps: Seq[Dependency[_]]
) extends Serializable {}
