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

package com.twosigma.flint.timeseries.row

import org.apache.spark.sql.CatalystTypeConvertersWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{ ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, StructField, StructType }

/**
 * A set of functions to manipulate Catalyst InternalRow objects.
 */
private[timeseries] object InternalRowUtils {
  private def concat2Array(xs: Array[Any], ys: Array[Any]): InternalRow = {
    val size = xs.length + ys.length
    val ret = new Array[Any](size)
    System.arraycopy(xs, 0, ret, 0, xs.length)
    System.arraycopy(ys, 0, ret, xs.length, ys.length)
    new GenericInternalRow(ret)
  }

  private def concatSeq(xs: Seq[Any]*): InternalRow = {
    var size = 0
    var i = 0
    while (i < xs.size) {
      size += xs(i).size
      i += 1
    }
    val ret: Array[Any] = new Array[Any](size)

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
  private def updateOrAppend(original: Array[Any], newValues: Array[(Option[Int], Any)]): InternalRow = {
    var i = 0
    var j = 0
    while (i < newValues.length) {
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
    while (i < newValues.length) {
      val newValue = newValues(i)._2
      newValues(i)._1.fold {
        ret(j) = newValue
        j += 1
      } {
        index => ret(index) = newValue
      }
      i += 1
    }

    InternalRow.fromSeq(ret)
  }

  def selectIndices(columns: Seq[(Int, DataType)])(row: InternalRow): Array[Any] = {
    var i = 0
    val size = columns.length
    val ret = new Array[Any](size)
    while (i < size) {
      ret(i) = row.get(columns(i)._1, columns(i)._2)
      i += 1
    }
    ret
  }

  private def selectFn(schema: StructType, columns: Seq[Int]): InternalRow => Array[Any] = {
    val columnsWithTypes = columns.map {
      index =>
        (index, schema(index).dataType)
    }

    selectFn(columnsWithTypes)
  }

  private def selectFn(columns: Seq[(Int, DataType)]): InternalRow => Array[Any] = {
    (row: InternalRow) =>
      val size = columns.size
      val newValues = new Array[Any](size)
      var i = 0
      while (i < size) {
        newValues(i) = row.get(columns(i)._1, columns(i)._2)
        i += 1
      }

      newValues
  }

  private def selectFnRow(columns: Seq[(Int, DataType)]): InternalRow => InternalRow = {
    (row: InternalRow) =>
      InternalRow.fromSeq(selectFn(columns)(row))
  }

  def prepend(row: InternalRow, schema: StructType, values: Any*): InternalRow = concatSeq(values, row.toSeq(schema))

  def delete(schema: StructType, toDelete: Seq[String]): (InternalRow => InternalRow, StructType) = {
    val fields = schema.zipWithIndex.filterNot {
      case (field: StructField, i) => toDelete.contains(field.name)
    }
    val columns = fields.map {
      case (field, i) => (i, field.dataType)
    }

    (selectFnRow(columns), StructType(fields.unzip._1))
  }

  def select(schema: StructType, toSelect: Seq[String]): (InternalRow => InternalRow, StructType) = {
    val fields = schema.zipWithIndex.filter {
      case (field: StructField, i) => toSelect.contains(field.name)
    }
    val columns = fields.map {
      case (field, i) => (i, field.dataType)
    }

    (selectFnRow(columns), StructType(fields.unzip._1))
  }

  def add(schema: StructType, toAdd: Seq[(String, DataType)]): ((InternalRow, Seq[Any]) => InternalRow, StructType) =
    ({ (row, values) => concatSeq(row.toSeq(schema), values) }, Schema.add(schema, toAdd))

  def addOrUpdate(
    schema: StructType,
    toAdd: Seq[(String, DataType)]
  ): ((InternalRow, Seq[Any]) => InternalRow, StructType) = {
    val namesToIndex = schema.fieldNames.zipWithIndex.toMap
    val indices = toAdd.map{ case (name, _) => namesToIndex.get(name) }
    val dataTypes = toAdd.map(_._2)
    val converters = dataTypes.map(dataType => CatalystTypeConvertersWrapper.toCatalystConverter(dataType))

    val newSchema = Schema.addOrUpdate(schema, toAdd.zip(indices))
    val fn = {
      (row: InternalRow, values: Seq[Any]) =>
        val newValues = new Array[(Option[Int], Any)](values.length)
        var i = 0
        values.foreach { v =>
          newValues(i) = (indices(i), converters(i)(v))
          i = i + 1
        }

        updateOrAppend(row.toSeq(schema).toArray, newValues)
    }
    (fn, newSchema)
  }

  def concat2(
    schema1: StructType,
    schema2: StructType
  ): ((InternalRow, InternalRow) => InternalRow, StructType) = {
    val newSchema = StructType(schema1.fields ++ schema2.fields)
    Schema.requireUniqueColumnNames(newSchema)
    ((row1: InternalRow, row2: InternalRow) => concatSeq(row1.toSeq(schema1), row2.toSeq(schema2)), newSchema)
  }

  def concat2(
    schema1: StructType,
    schema2: StructType,
    alias1: Option[String],
    alias2: Option[String],
    duplicates: Set[String]
  ): ((InternalRow, InternalRow) => InternalRow, StructType) = {

    val schemas = Seq(schema1, schema2)
    val aliases = Seq(alias1, alias2)

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
    val selectFns = indices.zipWithIndex.map {
      case (rowIndices, index) =>
        selectFn(schemas(index), rowIndices)
    }

    Schema.requireUniqueColumnNames(schema)

    val fn = {
      (r1: InternalRow, r2: InternalRow) =>
        val v1 = selectFns(0)(r1)
        val v2 = selectFns(1)(r2)
        concat2Array(v1, v2)
    }

    (fn, schema)
  }

  // Update values with given indices, and returns a new object
  def update(iRow: InternalRow, schema: StructType, updates: (Int, Any)*): InternalRow = {
    val values = Array(iRow.toSeq(schema): _*)
    var i = 0
    while (i < updates.size) {
      values(updates(i)._1) = updates(i)._2
      i += 1
    }
    InternalRow.fromSeq(values)
  }

  /**
   * Takes a time and multiple value arrays and creates a row with time and then the values.
   * This is just a more efficient method to create the row, rather than concatting the seqs
   *
   * @param time the value for the first column, which will be a Long time column
   * @param totalNumColumns the number of columns, including the time column to pull out of the sequences
   * @param valuesSeqs sequences of values to be concatenated
   */
  def concatTimeWithValues(time: Long, totalNumColumns: Int, valuesSeqs: Seq[Any]*): InternalRow = {
    val array = new Array[Any](totalNumColumns)
    array(0) = time
    var currentIndex = 1
    var seqIndex = 0
    var seqValueIndex = 0
    while (seqIndex < valuesSeqs.size) {
      while (seqValueIndex < valuesSeqs(seqIndex).size) {
        array(currentIndex) = valuesSeqs(seqIndex)(seqValueIndex)
        currentIndex += 1
        seqValueIndex += 1
      }
      seqValueIndex = 0
      seqIndex += 1
    }
    InternalRow.fromSeq(array)
  }
}
