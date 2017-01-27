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

package com.twosigma.flint.timeseries.row

import com.twosigma.flint.timeseries.TimeSeriesRDD
import org.apache.spark.sql.types._

import scala.collection.mutable

private[timeseries] object Schema {
  def addColumnPrefix(field: StructField, prefix: String): StructField =
    if (prefix == null) field else StructField(s"${prefix}_${field.name}", field.dataType)

  /**
   * Check if the names of columns are unique and throw [[IllegalArgumentException]] otherwise.
   *
   * @param schema The schema expected to check the uniqueness.
   */
  def requireUniqueColumnNames(schema: StructType): Unit = {
    val duplicates = schema
      .fieldNames
      .groupBy{ name => name }
      .filter{ case (name, group) => group.size > 1 }
      .map(_._1)
      .toSeq

    if (!duplicates.isEmpty) {
      throw new DuplicateColumnsException(
        s"Found duplicate columns ${duplicates} in schema $schema",
        duplicates
      )
    }
  }

  def rename(schema: StructType, fromTo: Seq[(String, String)]): StructType = {
    val newSchema = StructType(schema.map {
      field => fromTo.toMap.get(field.name).fold(field)(StructField(_, field.dataType))
    })
    requireUniqueColumnNames(newSchema)
    newSchema
  }

  def add(schema: StructType, columns: Seq[(String, DataType)]): StructType = {
    val newSchema = StructType(schema.fields ++ columns.map{ case (n, t) => StructField(n, t) })
    requireUniqueColumnNames(newSchema)
    newSchema
  }

  def addOrUpdate(schema: StructType, columns: Seq[((String, DataType), Option[Int])]): StructType = {
    val oldFields = schema.fields.clone()
    columns.foreach {
      case ((name, dataType), Some(index)) => oldFields(index) = StructField(name, dataType)
      case _ => // Nothing
    }

    val newFields = columns.collect {
      case ((name, dataType), None) => StructField(name, dataType)
    }

    val newSchema = StructType(oldFields ++ newFields)
    requireUniqueColumnNames(newSchema)
    newSchema
  }

  /**
   * Append time column and possible other provided fields to the given schema.
   *
   * @param schema    The schema expected to append the time column etc.
   * @param keyFields The other possible fields expected to append.
   * @return a new schema with the appended time column etc.
   */
  def prependTimeAndKey(schema: StructType, keyFields: Seq[StructField]): StructType = {
    val newSchema = StructType((TimeSeriesRDD.timeField +: keyFields) ++ schema.fields)
    requireUniqueColumnNames(newSchema)
    newSchema
  }

  /**
   * Prepend a "time" string to a sequence of strings if it doesn't contain one.
   *
   * @param columns The sequence of strings expected to examine.
   * @return a sequence of strings with a prepending "time" string if it doesn't contain one. Otherwise,
   *         simply return the original string .
   */
  def prependTimeIfMissing(columns: Seq[String]): Seq[String] =
    columns.find(_ == TimeSeriesRDD.timeColumnName).fold(TimeSeriesRDD.timeColumnName +: columns) { _ => columns }

  /**
   * Create a schema with the given sequence of fields.
   *
   * @param columns A sequence of tuple(s) specifying the name and the data type of a field.
   * @return a schema.
   * @note If the given fields do not contain a column of name "time", it will prepend one with LongType; However, if
   *       the given fields contain a column of name "time" whose data type is not LongType, it will throw
   *       an IllegalArgumentException.
   */
  def apply(columns: (String, DataType)*): StructType = {
    val cols: Seq[(String, DataType)] = if (!columns.map(_._1).contains(TimeSeriesRDD.timeColumnName)) {
      (TimeSeriesRDD.timeColumnName, LongType) +: columns
    } else {
      require(
        columns.exists(_ == (TimeSeriesRDD.timeColumnName, LongType)),
        s"The columns doesn't contain a field name ${TimeSeriesRDD.timeColumnName} of LongType"
      )
      columns
    }
    of(cols: _*)
  }

  /**
   * Create a schema with the given sequence of fields.
   *
   * @param columns A sequence of tuple(s) specifying the name and the data type of a field.
   * @return a schema.
   */
  def of(columns: (String, DataType)*): StructType = StructType(columns.map {
    case (columnName, dataType) => StructField(columnName, dataType)
  })

  /**
   * Create a schema with updated column data types or throw an exception if the operation isn't valid.
   *
   * @param schema current schema
   * @param updates A sequence of tuples, where a tuple contains column name and NumericType.
   * @return a new schema.
   */
  def cast(schema: StructType, updates: (String, NumericType)*): StructType = {
    require(
      !updates.exists(_._1 == TimeSeriesRDD.timeColumnName),
      s"Casting the ${TimeSeriesRDD.timeColumnName} column isn't allowed."
    )

    val nameToIndex = schema.fieldNames.zipWithIndex.toMap
    val updatesWithIndex = updates.map {
      case (name, dataType) => ((name, dataType), nameToIndex.get(name))
    }

    updatesWithIndex.foreach {
      case ((name, _), indexOpt) => require(
        indexOpt.nonEmpty,
        s"$name column isn't found in the RDD"
      )
    }

    val currentDataTypes = updatesWithIndex.map { case (_, index) => schema.fields(index.get).dataType }
    require(
      currentDataTypes.forall(_.isInstanceOf[NumericType]),
      s"Casting non-numeric columns isn't allowed."
    )

    // If all sanity checks above are passed, it will be safe to cast the columns.
    val patches = updates.toMap
    val updatedFields = schema.fields.map {
      case structField => patches.get(structField.name).fold(structField)(StructField(structField.name, _))
    }

    StructType(updatedFields)
  }

  def append(schema: StructType, columns: (String, DataType)*): StructType = {
    StructType(schema.fields ++ columns.map{
      case (columnName, dataType) => StructField(columnName, dataType)
    })
  }
}
