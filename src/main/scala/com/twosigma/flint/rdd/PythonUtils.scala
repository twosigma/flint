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

package com.twosigma.flint.rdd

import com.twosigma.flint.timeseries.{ TimeSeriesRDD, TimeSeriesRDDImpl }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ CatalystTypeConvertersWrapper, Row }

private[rdd] case class SchemaColumnInfo(idx: Int, clazz: Class[_ <: Ordered[_]], dataType: DataType)

case class TimeSeriesRDDWithSchema(rdd: TimeSeriesRDDImpl, schema: StructType)

object PythonUtils {
  /**
   * Add reference to the object (py4j won't do this)
   */
  def makeCopy[T](obj: T): T = obj

  /**
   * Convert RDD[row] to RDD[(key, row)]
   */
  def formatRDD[K](
    rdd: RDD[Row],
    schema: StructType,
    keyColumn: String
  ): RDD[(K, Row)] = {
    val keyIdx = schema.fieldIndex(keyColumn)
    rdd.map { row =>
      (row.getAs[K](keyIdx), row)
    }
  }

  /**
   * (py4j-friendly) Create TimeSeriesRDD through OrderedRDD's fromSorted
   */
  def fromSortedRDD(
    sc: SparkContext,
    rdd: RDD[Row],
    schema: StructType,
    keyColumn: String
  ): TimeSeriesRDDImpl = {
    val ordd = OrderedRDD.fromRDD(formatRDD[Long](rdd, schema, keyColumn), KeyPartitioningType.Sorted)
    TimeSeriesRDD.fromOrderedRDD(ordd, schema).asInstanceOf[TimeSeriesRDDImpl]
  }

  /**
   * (py4j-friendly) Create TimeSeriesRDD through OrderedRDD's fromUnsorted
   */
  def fromUnsortedRDD(
    sc: SparkContext,
    rdd: RDD[Row],
    schema: StructType,
    keyColumn: String
  ): TimeSeriesRDDImpl = {
    val orderedRdd = OrderedRDD.fromRDD(formatRDD[Long](rdd, schema, keyColumn), KeyPartitioningType.UnSorted)
    TimeSeriesRDD.fromOrderedRDD(orderedRdd, schema).asInstanceOf[TimeSeriesRDDImpl]
  }

  def toOrderedRDD(
    rdd: RDD[Row],
    schema: StructType,
    keyColumn: String,
    ranges: Seq[CloseOpen[Long]]
  ): OrderedRDD[Long, InternalRow] = {
    val keyIdx = schema.fieldIndex(keyColumn)
    val converter = CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)
    OrderedRDD.fromRDD(rdd.map(row => (row.getAs[Long](keyIdx), converter(row))), ranges)
  }
}
