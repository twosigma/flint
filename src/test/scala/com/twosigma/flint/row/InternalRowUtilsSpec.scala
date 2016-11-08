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

package com.twosigma.flint.row

import com.twosigma.flint.timeseries.Schema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.{ DataType, DoubleType, LongType, StringType, StructField, StructType }
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class InternalRowUtilsSpec extends FlatSpec with TableDrivenPropertyChecks {

  val rows = Table(
    ("data", "schema"),
    (Array[Any](123456789L), Schema.of("time" -> LongType)),
    (
      Array[Any](1L, 2.0, UTF8String.fromString("test")),
      Schema.of("time" -> LongType, "value" -> DoubleType, "info" -> StringType)
    )
  )

  val rowProjections = Table(
    "projection",
    (data: Array[Any], _: StructType) => InternalRow.fromSeq(data),
    (data: Array[Any], schema: StructType) => UnsafeProjection.create(schema).apply(InternalRow.fromSeq(data))
  )

  def testTemplate(assertion: (Array[Any], StructType, InternalRow) => Unit): Unit = {
    forAll (rows) { (data: Array[Any], schema: StructType) =>
      forAll(rowProjections) { projection: ((Array[Any], StructType) => InternalRow) =>
        val row = projection(data, schema)

        assertion(data, schema, row)
      }
    }
  }

  "InternalRowUtils" should "select indices correctly" in {
    testTemplate { (data: Array[Any], schema: StructType, row: InternalRow) =>
      val fields = schema.zipWithIndex.map {
        case (field: StructField, i) => (i, field.dataType)
      }

      val selected = InternalRowUtils.selectIndices(fields)(row)
      assert(selected == data.toSeq)
    }
  }

  it should "prepend values correctly" in {
    testTemplate { (data: Array[Any], schema: StructType, row: InternalRow) =>
      val values = Seq(123.0, 456, UTF8String.fromString("789"))

      val newRow = InternalRowUtils.prepend(row, schema, values: _*)
      assert(newRow == InternalRow.fromSeq(values ++ data))
    }
  }

  it should "delete correctly" in {
    testTemplate { (data: Array[Any], schema: StructType, row: InternalRow) =>
      val (deleteFn, newSchema) = InternalRowUtils.delete(schema, Seq(schema.fieldNames(0)))

      assert(newSchema.fields sameElements schema.fields.tail)

      val updatedRow = deleteFn(row)
      assert(updatedRow.toSeq(newSchema) == data.tail.toSeq)
    }
  }

  it should "select correctly" in {
    testTemplate { (data: Array[Any], schema: StructType, row: InternalRow) =>
      val (selectFn, newSchema) = InternalRowUtils.select(schema, Seq(schema.fieldNames(0)))

      assert(newSchema.fields sameElements Array(schema.fields.head))
      val updatedRow = selectFn(row)
      assert(updatedRow.toSeq(newSchema) == Seq(data.head))
    }
  }

  it should "add correctly" in {
    testTemplate { (data: Array[Any], schema: StructType, row: InternalRow) =>
      val toAdd: Seq[(String, DataType)] = Seq("long" -> LongType, "double" -> DoubleType, "string" -> StringType)
      val values = Seq(5L, 3.0, UTF8String.fromString("test"))
      val (addFn, newSchema) = InternalRowUtils.add(schema, toAdd)

      val expectedFileds = schema.fields ++ toAdd.map{ case (name, dataType) => StructField(name, dataType) }
      assert(newSchema.fields sameElements expectedFileds)

      val updatedRow = addFn(row, values)
      assert(updatedRow.toSeq(newSchema) == (data ++ values).toSeq)
    }
  }
}
