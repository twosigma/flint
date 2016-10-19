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

package org.apache.spark.sql

import com.twosigma.flint.timeseries.Schema

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{ ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StructField, StructType }

/**
 * A set of functions to manipulate Catalyst GenericInternalRow objects.
 */
object GenericInternalRowUtils {
  private def concatArray(xs: Array[Any]*): GenericInternalRow = concatSeq(xs.map(_.toSeq): _*)

  private def concatSeq(xs: Seq[Any]*): GenericInternalRow = {
    var size = 0
    var i = 0
    while (i < xs.size) {
      size += xs(i).size
      i += 1
    }
    val ret: Array[Any] = Array.fill(size)(null)

    i = 0
    var index = 0
    while (i < xs.size) {
      val x = xs(i)
      var j = 0
      while (j < x.size) {
        ret(index) = x(j)
        j += 1
        index += 1
      }
      i += 1
    }

    new GenericInternalRow(ret)
  }

  // updates existing elements, or appends a new element to the end if index isn't provided
  private def updateOrAppend(original: Array[Any], newValues: (Option[Int], Any)*): GenericInternalRow = {
    var i = 0
    var j = 0
    while (i < newValues.size) {
      if (newValues(i)._1.isEmpty) {
        j += 1
      }
      i += 1
    }

    val size = original.length + j
    val ret: Array[Any] = Array.fill(size)(null)

    Array.copy(original, 0, ret, 0, original.length)

    i = 0
    j = original.length
    while (i < newValues.size) {
      val newValue = newValues(i)._2
      newValues(i)._1.fold {
        ret(j) = newValue
        j += 1
      } {
        index => ret(index) = newValue
      }
      i += 1
    }

    new GenericInternalRow(ret)
  }

  def selectIndices(indices: Seq[Int])(row: GenericInternalRow): Seq[Any] = indices.map(row.values(_))

  private def selectFn(indices: Seq[Int]): GenericInternalRow => GenericInternalRow = {
    (row: GenericInternalRow) =>
      val size = indices.size
      val newValues: Array[Any] = Array.fill(size)(null)
      var i = 0

      while (i < size) {
        newValues(i) = row.values(indices(i))
        i += 1
      }

      new GenericInternalRow(newValues)
  }

  def prepend(row: GenericInternalRow, values: Any*): GenericInternalRow = concatSeq(values, row.values)

  def delete(schema: StructType, toDelete: Seq[String]): (GenericInternalRow => GenericInternalRow, StructType) = {
    val (fields, indices) = schema.zipWithIndex.filterNot {
      case (field: StructField, i) => toDelete.contains(field.name)
    }.unzip

    (selectFn(indices), StructType(fields))
  }

  def select(schema: StructType, toSelect: Seq[String]): (GenericInternalRow => GenericInternalRow, StructType) = {
    val (fields, indices) = schema.zipWithIndex.filter {
      case (field: StructField, i) => toSelect.contains(field.name)
    }.unzip

    (selectFn(indices), StructType(fields))
  }

  def add(schema: StructType, toAdd: Seq[(String, DataType)]): ((GenericInternalRow, Seq[Any]) => GenericInternalRow, StructType) =
    ({ (row, values) => concatSeq(row.values, values) }, Schema.add(schema, toAdd))

  def addOrUpdate(
    schema: StructType,
    toAdd: Seq[(String, DataType)]
  ): ((GenericInternalRow, Seq[Any]) => GenericInternalRow, StructType) = {
    val namesToIndex = schema.fieldNames.zipWithIndex.toMap
    val indices = toAdd.map{ case (name, _) => namesToIndex.get(name) }

    val newSchema = Schema.addOrUpdate(schema, toAdd.zip(indices))
    val fn = {
      (row: GenericInternalRow, values: Seq[Any]) => updateOrAppend(row.values, indices.zip(values): _*)
    }
    (fn, newSchema)
  }

  def concat2(
    schema1: StructType,
    schema2: StructType
  ): ((GenericInternalRow, GenericInternalRow) => GenericInternalRow, StructType) = {
    val newSchema = StructType(schema1.fields ++ schema2.fields)
    Schema.requireUniqueColumnNames(newSchema)
    ((row1: GenericInternalRow, row2: GenericInternalRow) => concatArray(row1.values, row2.values), newSchema)
  }

  def concat(
    schemas: StructType*
  ): (Seq[GenericInternalRow] => GenericInternalRow, StructType) = {
    val newSchema = StructType(schemas.map(_.fields).reduce(_ ++ _))
    Schema.requireUniqueColumnNames(newSchema)
    ({ rows: Seq[GenericInternalRow] => concatArray(rows.map(_.values): _*) }, newSchema)
  }

  def concat(
    schemas: Seq[StructType],
    aliases: Seq[String],
    duplicates: Set[String]
  ): (Seq[GenericInternalRow] => GenericInternalRow, StructType) = {
    require(schemas.length == aliases.length)

    val (firstFields, firstIndex) = (schemas, aliases).zipped.head match {
      case (s, alias) =>
        val rowFields = s.fields
        rowFields.zipWithIndex.map {
          case (field, index) =>
            if (duplicates.contains(field.name)) {
              (field, index)
            } else {
              (Schema.addColumnPrefix(field, alias), index)
            }
        }.unzip
    }

    val (remainingFields, remainingIndex) = (schemas, aliases).zipped.tail.map {
      case (s, alias) =>
        val rowFields = s.fields
        rowFields.toSeq.zipWithIndex.collect {
          case (field, index) if !duplicates.contains(field.name) => (Schema.addColumnPrefix(field, alias), index)
        }.unzip
    }.unzip

    val schema = StructType(firstFields ++ remainingFields.flatten)
    val indices = Seq(firstIndex.toSeq) ++ remainingIndex.toSeq
    val selectFns = indices.map {
      indices => selectFn(indices)
    }

    Schema.requireUniqueColumnNames(schema)
    val size = schemas.size

    val fn = {
      rows: Seq[GenericInternalRow] =>
        val values: Array[Array[Any]] = Array.fill(size)(null)
        var i = 0
        while (i < size) {
          values(i) = selectFns(i)(rows(i)).values
          i += 1
        }
        concatArray(values: _*)
    }

    (fn, schema)
  }

  /**
   * Build a new schema and a function to cast rows.
   *
   * @param schema current schema.
   * @param updates A sequence of tuples specifying a column name and a data type to cast the column to.
   * @return a function that should be applied to every row, and new schema.
   */
  def cast(schema: StructType, updates: (String, NumericType)*): (GenericInternalRow => GenericInternalRow, StructType) = {
    val newSchema = Schema.cast(schema, updates: _*)
    val nameToIndex = schema.fieldNames.zipWithIndex.toMap
    val indexedUpdates = updates.map {
      case (name, dataType) => (nameToIndex.get(name).get, dataType)
    }.toMap

    val fn = {
      (row: GenericInternalRow) =>
        val newValues = Array.tabulate(schema.size) {
          i => indexedUpdates.get(i).fold(row.values(i))(castNumericValue(row.values(i).asInstanceOf[Number], _))
        }
        new GenericInternalRow(newValues)
    }

    (fn, newSchema)
  }

  @inline
  private final def castNumericValue(value: Number, newType: NumericType): Number = newType match {
    case ByteType => value.byteValue()
    case DoubleType => value.doubleValue()
    case FloatType => value.floatValue()
    case IntegerType => value.intValue()
    case LongType => value.longValue()
    case ShortType => value.shortValue()
  }

  // Update values with given indices, and returns a new object
  def update(iRow: GenericInternalRow, updates: (Int, Any)*): GenericInternalRow = {
    val values = iRow.values.clone()
    var i = 0
    while (i < updates.size) {
      values(updates(i)._1) = updates(i)._2
      i += 1
    }
    new GenericInternalRow(values)
  }
}
