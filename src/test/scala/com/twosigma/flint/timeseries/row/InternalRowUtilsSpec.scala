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

import org.apache.spark.sql.{ CatalystTypeConvertersWrapper, Row }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ GenericMutableRow, GenericRow, UnsafeProjection }
import org.apache.spark.sql.types.{ ArrayType, DataType, DoubleType, LongType, StringType, StructField, StructType }
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks

class InternalRowUtilsSpec extends FlatSpec with TableDrivenPropertyChecks {

  val rows = Table(
    ("data", "schema"),
    (new GenericRow(Array[Any](123456789L)), Schema.of("time" -> LongType)),
    (
      new GenericRow(Array[Any](1L, 2.0, "test", Array(3.0, 4.0).toSeq)),
      Schema.of("time" -> LongType, "value" -> DoubleType, "info" -> StringType, "array" -> ArrayType(DoubleType))
    ),
    (
      new GenericRow(Array[Any](10L, new GenericRow(Array[Any]("test", 1.0)))),
      Schema.of("time" -> LongType, "nestedColumn" -> Schema.of("string" -> StringType, "double" -> DoubleType))
    )
  )

  val rowProjections = Table(
    "projection",
    (row: Row, schema: StructType) => CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)(row),
    (row: Row, schema: StructType) => {
      val internalRow = CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)(row)
      UnsafeProjection.create(schema).apply(internalRow)
    },
    (row: Row, schema: StructType) => {
      val internalRow = CatalystTypeConvertersWrapper.toCatalystRowConverter(schema)(row)
      new GenericMutableRow(internalRow.toSeq(schema).toArray)
    }
  )

  def testTemplate(assertion: (Row, StructType, InternalRow) => Unit): Unit = {
    forAll (rows) { (data: GenericRow, schema: StructType) =>
      forAll(rowProjections) { projection: ((Row, StructType) => InternalRow) =>
        val row = projection(data, schema)
        val rowCopy = row.copy()

        assertion(data, schema, row)
        // operations shouldn't change the original row
        assert(row == rowCopy)
      }
    }
  }

  "InternalRowUtils" should "select indices correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val fields = schema.zipWithIndex.map {
        case (field: StructField, i) => (i, field.dataType)
      }

      // the result contains internal representation, so we can't use to compare with the original data
      val selected = InternalRowUtils.selectIndices(fields)(row)
      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(schema)(InternalRow.fromSeq(selected))

      assert(converted == data)
    }
  }

  it should "prepend values correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val values = Seq(123.0, 456, UTF8String.fromString("789"))

      val newRow = InternalRowUtils.prepend(row, schema, values: _*)
      assert(newRow == InternalRow.fromSeq(values ++ row.toSeq(schema)))
    }
  }

  it should "delete correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val (deleteFn, newSchema) = InternalRowUtils.delete(schema, Seq(schema.fieldNames(0)))

      assert(newSchema.fields sameElements schema.fields.tail)

      val updatedRow = deleteFn(row)
      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema)(updatedRow)

      assert(converted.toSeq == data.toSeq.tail)
    }
  }

  it should "select correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val (selectFn, newSchema) = InternalRowUtils.select(schema, Seq(schema.fieldNames(0)))

      assert(newSchema.fields sameElements Array(schema.fields.head))
      val updatedRow = selectFn(row)
      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema)(updatedRow)

      assert(converted.toSeq == Seq(data.toSeq.head))
    }
  }

  it should "add correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val toAdd: Seq[(String, DataType)] = Seq("long" -> LongType, "double" -> DoubleType, "string" -> StringType)
      val values = Seq(5L, 3.0, UTF8String.fromString("test"))
      val (addFn, newSchema) = InternalRowUtils.add(schema, toAdd)

      val expectedFileds = schema.fields ++ toAdd.map{ case (name, dataType) => StructField(name, dataType) }
      assert(newSchema.fields sameElements expectedFileds)

      val updatedRow = addFn(row, values)
      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema)(updatedRow)
      assert(converted.toSeq == (data.toSeq ++ Seq(5L, 3.0, "test")))
    }
  }

  it should "addOrUpdate correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val toAdd1: Seq[(String, DataType)] = Seq("time" -> LongType, "newColumnName" -> DoubleType)
      val values1 = IndexedSeq[Any](5L, 3.0)
      val (addOrUpdateFn1, newSchema1) = InternalRowUtils.addOrUpdate(schema, toAdd1)

      // we are expecting that time column exists in the rows
      val expectedFields1 = schema.fields ++ toAdd1.tail.map{ case (name, dataType) => StructField(name, dataType) }
      assert(newSchema1.fields sameElements expectedFields1)

      val updatedRow1 = addOrUpdateFn1(row, values1)
      val converted1 = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema1)(updatedRow1)

      val expected1 = Seq(values1(0)) ++ data.toSeq.tail ++ Seq(values1(1))
      assert(converted1.toSeq == expected1)
      assert(converted1.getAs[Double]("newColumnName") == 3.0)

      val toAdd2 = Seq("time" -> LongType, "newColumnName" -> LongType)
      val values2 = IndexedSeq[Any](5L, 30L)
      val (addOrUpdateFn2, newSchema2) = InternalRowUtils.addOrUpdate(newSchema1, toAdd2)
      val updatedRow2 = addOrUpdateFn2(updatedRow1, values2)
      val expectedFields2 = schema.fields ++ toAdd2.tail.map{ case (name, dataType) => StructField(name, dataType) }
      assert(newSchema2.fields sameElements expectedFields2)
      val converted2 = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema2)(updatedRow2)
      assert(converted2.getAs[Long]("newColumnName") == 30L)
    }
  }

  it should "concat2 correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val schemaToAdd = Schema.of("uniqueColumnName1" -> LongType, "uniqueColumnName2" -> DoubleType)
      val rowToAdd = InternalRow.fromSeq(Seq[Any](5L, 3.0))
      val (concatFn, concatenatedSchema) = InternalRowUtils.concat2(schema, schemaToAdd)

      val expectedFileds = schema.fields ++ schemaToAdd.fields
      val newSchema = new StructType(expectedFileds)
      assert(concatenatedSchema.fields sameElements expectedFileds)

      val updatedRow = concatFn(row, rowToAdd)
      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(newSchema)(updatedRow)

      val expected = data.toSeq ++ rowToAdd.toSeq(schemaToAdd)
      assert(converted.toSeq == expected)
    }
  }

  it should "update correctly" in {
    testTemplate { (data: Row, schema: StructType, row: InternalRow) =>
      val updatedRow = InternalRowUtils.update(row, schema, 0 -> 123L)

      val converted = CatalystTypeConvertersWrapper.toScalaRowConverter(schema)(updatedRow)
      val expected = Seq(123L) ++ data.toSeq.tail
      assert(converted.toSeq == expected)
    }
  }
}
