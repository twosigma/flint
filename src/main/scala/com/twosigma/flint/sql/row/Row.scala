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

package com.twosigma.flint.sql.row

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row => SqlRow }

import scala.collection.immutable.SortedMap
import scala.collection.mutable.{ Set => MSet }

object Schema {
  def addColumnPrefix(field: StructField, prefix: String): StructField = if (prefix == null) {
    field
  } else {
    StructField(s"${prefix}_${field.name}", field.dataType)
  }

  def requireUniqueColumnNames(schema: StructType): Unit = {
    val seen: MSet[String] = MSet()
    schema.fields.map(_.name).foreach {
      name =>
        if (seen.contains(name)) {
          throw new RuntimeException(s"Found duplicate column '$name' in schema $schema")
        }
        seen.add(name)
    }
  }

  /**
   * Create a function that concatenates a sequence of rows.
   *
   * @param schemas Row schemas
   * @param aliases Prefix for columns in each table
   * @param duplicates Columns to be removed from schemas other than the first
   */
  def concatenator(schemas: Seq[StructType], aliases: Seq[String], duplicates: Set[String]): Seq[SqlRow] => SqlRow = {
    require(schemas.length == aliases.length)

    val (firstFields, firstIndex) = (schemas, aliases).zipped.head match {
      case (s, alias) =>
        val rowFields = s.fields
        rowFields.zipWithIndex.map {
          case (field, index) => if (duplicates.contains(field.name)) {
            (field, index)
          } else {
            (addColumnPrefix(field, alias), index)
          }
        }.unzip
    }

    val (remainingFields, remainingIndex) = (schemas, aliases).zipped.tail.map {
      case (s, alias) =>
        val rowFields = s.fields
        rowFields.toSeq.zipWithIndex.collect {
          case (field, index) if !duplicates.contains(field.name) => (addColumnPrefix(field, alias), index)
        }.unzip
    }.unzip

    val schema = StructType(firstFields ++ remainingFields.flatten)
    val indices = Seq(firstIndex.toSeq) ++ remainingIndex.toSeq

    requireUniqueColumnNames(schema)

    {
      rows: Seq[SqlRow] =>
        val values = (rows, indices).zipped.flatMap {
          case (row, index) => index.map(row.get)
        }.toArray

        new GenericRowWithSchema(values, schema)
    }
  }
}

/**
 * Utility class that manipulates a DataFrame row object.
 */

// TODO: Check duplicate columns
object Row {
  /**
   * Add a new field in the row.
   *
   * @param fieldDesc Schema of the new field
   * @param fn Function that produce the new field from the original row
   */
  def add(row: SqlRow, fieldDesc: StructField, fn: SqlRow => Any): SqlRow = {
    val schema = StructType(row.schema.fields :+ fieldDesc)
    val values = (row.toSeq :+ fn(row)).toArray
    new GenericRowWithSchema(values, schema)
  }

  /**
   * Select fields in a row.
   */
  def select(row: SqlRow, fields: Seq[String]): SqlRow = {
    val newFieldsWithIndex = row.schema.zipWithIndex.filter { case (field, _) => fields.contains(field.name) }
    val schema = StructType(newFieldsWithIndex.map(_._1))
    val indexes = newFieldsWithIndex.map(_._2)
    val values = indexes.map(row.get).toArray
    new GenericRowWithSchema(values, schema)
  }

  def delete(row: SqlRow, fields: Seq[String]): SqlRow = {
    val newFieldsWithIndex = row.schema.zipWithIndex.filterNot { case (field, _) => fields.contains(field.name) }
    val schema = StructType(newFieldsWithIndex.map(_._1))
    val indexes = newFieldsWithIndex.map(_._2)
    val values = indexes.map(row.get).toArray
    new GenericRowWithSchema(values, schema)
  }

  /**
   * Rename fields in a row.
   */
  def rename(row: SqlRow, fromTo: Map[String, String]): SqlRow = {
    val schema = StructType(row.schema.map {
      field => fromTo.get(field.name).fold(field)(StructField(_, field.dataType))
    })
    new GenericRowWithSchema(row.toSeq.toArray, schema)
  }

  def concat(row1: SqlRow, row2: SqlRow): SqlRow = {
    val schema = StructType(row1.schema.fields ++ row2.schema.fields)
    val values = (row1.toSeq ++ row2.toSeq).toArray
    new GenericRowWithSchema(values, schema)
  }

  /**
   * Concatenate fields in multiple rows into a single row. Input rows should not have fields with the same name.
   */
  def concat(rows: Seq[SqlRow]): SqlRow = {
    val schema = StructType(rows.map(_.schema.fields).reduce(_ ++ _))
    val values = rows.map(_.toSeq).reduce(_ ++ _).toArray
    new GenericRowWithSchema(values, schema)
  }

  def getDataType(value: Any): DataType = value match {
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: Short => ShortType
    case _: Byte => ByteType
    case _: Boolean => BooleanType
    case _: String => StringType
    case row: SqlRow => row.schema
    case array: Array[_] =>
      if (array.length == 0) {
        ArrayType(NullType)
      } else {
        ArrayType(getDataType(array(0)))
      }
  }

  def columnsToRow(columns: Seq[(String, Any)]): SqlRow = {
    val (fields, values) = columns.map {
      case (columnName: String, value: Any) =>
        (StructField(columnName, getDataType(value)), value)
    }.unzip
    new GenericRowWithSchema(values.toArray, new StructType(fields.toArray))
  }

  /**
   * Add columns to the end of a row in order.
   */
  def addColumns(row: SqlRow, columns: Seq[((String, DataType), Any)]): SqlRow = {
    val newSize = row.size + columns.size
    val newFields = new Array[StructField](newSize)
    val newValues = new Array[Any](newSize)

    Array.copy(row.schema.fields, 0, newFields, 0, row.size)
    Array.copy(row.toSeq.toArray, 0, newValues, 0, row.size)

    // This is not very functional, but should be faster than creating intermediate object every
    // step.
    columns.scanLeft(row.size) {
      case (index, ((columnName, t), v)) =>
        newFields(index) = StructField(columnName, t)
        newValues(index) = v
        index + 1
    }
    new GenericRowWithSchema(newValues, new StructType(newFields))
  }

  /**
   * Prepend columns to the begin of a row in order.
   */
  def prependColumns(row: SqlRow, columns: Seq[(String, Any)]): SqlRow = {
    val newSize = columns.size + row.size
    val newFields = new Array[StructField](newSize)
    val newValues = new Array[Any](newSize)
    columns.scanLeft(0) {
      case (index, (columnName, v)) =>
        newFields(index) = StructField(columnName, getDataType(v))
        newValues(index) = v
        index + 1
    }
    Array.copy(row.schema.fields, 0, newFields, columns.size, row.size)
    Array.copy(row.toSeq.toArray, 0, newValues, columns.size, row.size)
    new GenericRowWithSchema(newValues, new StructType(newFields))
  }

  def prependColumn(
    row: SqlRow,
    name: String, value: Any
  ): SqlRow = prependColumns(row, Array((name, value)))

  def prepend2Columns(
    row: SqlRow,
    name1: String, value1: Any,
    name2: String, value2: Any
  ): SqlRow =
    prependColumns(row, Array((name1, value1), (name2, value2)))

  def prepend3Columns(
    row: SqlRow,
    name1: String, value1: Any,
    name2: String, value2: Any,
    name3: String, value3: Any
  ): SqlRow =
    prependColumns(row, Array((name1, value1), (name2, value2), (name3, value3)))

  def prepend4Columns(
    row: SqlRow,
    name1: String, value1: Any,
    name2: String, value2: Any,
    name3: String, value3: Any,
    name4: String, value4: Any
  ): SqlRow =
    prependColumns(row, Array((name1, value1), (name2, value2), (name3, value3), (name4, value4)))

  def swapKeys[A, B, C](m: SortedMap[A, Map[B, C]]): Map[B, Seq[(A, C)]] = {
    m.toSeq.map {
      case (a, bToC) =>
        bToC.mapValues { c => a -> c }.toSeq
    }.reduce(_ ++ _).groupBy(_._1).mapValues(_.map(_._2))
  }
}
