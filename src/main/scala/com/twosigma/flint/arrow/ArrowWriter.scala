/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.arrow

import com.twosigma.flint.arrow.ArrowUtils

import scala.collection.JavaConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.arrow.vector.util.DecimalUtility
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.types._

object ArrowWriter {

  def create(schema: StructType): ArrowWriter = create(schema, schema)

  def create(root: VectorSchemaRoot): ArrowWriter = {
    val sparkSchema = ArrowUtils.fromArrowSchema(root.getSchema)
    create(sparkSchema, sparkSchema, root)
  }

  def create(inputSchema: StructType, outputSchema: StructType, root: VectorSchemaRoot): ArrowWriter = {
    require(
      outputSchema.fields.toSet.subsetOf(inputSchema.fields.toSet),
      s"$outputSchema is no a subset of $inputSchema"
    )

    require(
      ArrowUtils.toArrowSchema(outputSchema).equals(root.getSchema),
      s"output schema doesn't match root schema"
    )

    val inputIndices = outputSchema.map(f => inputSchema.fieldIndex(f.name)).toArray

    val children = root.getFieldVectors().asScala.map { vector =>
      vector.allocateNew()
      createFieldWriter(vector)
    }
    new ArrowWriter(root, children.toArray, inputIndices)
  }

  def create(inputSchema: StructType, outputSchema: StructType): ArrowWriter = {

    val arrowSchema = ArrowUtils.toArrowSchema(outputSchema)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)

    create(inputSchema, outputSchema, root)
  }

  private def createFieldWriter(vector: ValueVector): ArrowFieldWriter = {
    val field = vector.getField()
    (ArrowUtils.fromArrowField(field), vector) match {
      case (BooleanType, vector: BitVector) => new BooleanWriter(vector)
      case (ByteType, vector: TinyIntVector) => new ByteWriter(vector)
      case (ShortType, vector: SmallIntVector) => new ShortWriter(vector)
      case (IntegerType, vector: IntVector) => new IntegerWriter(vector)
      case (LongType, vector: BigIntVector) => new LongWriter(vector)
      case (FloatType, vector: Float4Vector) => new FloatWriter(vector)
      case (DoubleType, vector: Float8Vector) => new DoubleWriter(vector)
      case (StringType, vector: VarCharVector) => new StringWriter(vector)
      case (BinaryType, vector: VarBinaryVector) => new BinaryWriter(vector)
      case (DateType, vector: DateDayVector) => new DateWriter(vector)
      case (TimestampType, vector: TimeStampMicroTZVector) => new TimestampWriter(vector)

      case (ArrayType(_, _), vector: ListVector) =>
        val elementVector = createFieldWriter(vector.getDataVector())
        new ArrayWriter(vector, elementVector)
      case (StructType(_), vector: NullableMapVector) =>
        val children = (0 until vector.size()).map { ordinal =>
          createFieldWriter(vector.getChildByOrdinal(ordinal))
        }
        new StructWriter(vector, children.toArray)
      case (dt, _) =>
        throw new UnsupportedOperationException(
          s"Unsupported data type: ${dt.simpleString} vector: $vector"
        )
    }
  }
}

class ArrowWriter(
  val root: VectorSchemaRoot,
  fields: Array[ArrowFieldWriter],
  inputIndices: Array[Int]
) {

  def outputSchema: StructType = StructType(fields.map { f =>
    StructField(f.name, f.dataType, f.nullable)
  })

  private var count: Int = 0

  def write(row: InternalRow): Unit = {
    var i = 0
    while (i < fields.size) {
      // println(s"writing original field ${inputIndices(i)} for field ${i}")
      fields(i).write(row, inputIndices(i))
      i += 1
    }
    count += 1
  }

  def finish(): Unit = {
    root.setRowCount(count)
    fields.foreach(_.finish())
  }

  def reset(): Unit = {
    root.setRowCount(0)
    count = 0
    fields.foreach(_.reset())
  }
}

abstract class ArrowFieldWriter {

  def valueVector: ValueVector

  def name: String = valueVector.getField().getName()
  def dataType: DataType = ArrowUtils.fromArrowField(valueVector.getField())
  def nullable: Boolean = valueVector.getField().isNullable()

  def setNull(): Unit
  def setValue(input: SpecializedGetters, ordinal: Int): Unit

  private[arrow] var count: Int = 0

  def write(input: SpecializedGetters, ordinal: Int): Unit = {
    if (input.isNullAt(ordinal)) {
      setNull()
    } else {
      setValue(input, ordinal)
    }
    count += 1
  }

  def finish(): Unit = {
    valueVector.setValueCount(count)
  }

  def reset(): Unit = {
    // TODO: reset() should be in a common interface
    valueVector match {
      case fixedWidthVector: BaseFixedWidthVector => fixedWidthVector.reset()
      case variableWidthVector: BaseVariableWidthVector => variableWidthVector.reset()
      case listVector: ListVector =>
        // Manual "reset" the underlying buffer.
        // TODO: When we upgrade to Arrow 0.10.0, we can simply remove this and call
        // `listVector.reset()`.
        val buffers = listVector.getBuffers(false)
        buffers.foreach(buf => buf.setZero(0, buf.capacity()))
        listVector.setValueCount(0)
        listVector.setLastSet(0)
      case _ =>
    }
    count = 0
  }
}

private[arrow] class BooleanWriter(val valueVector: BitVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, if (input.getBoolean(ordinal)) 1 else 0)
  }
}

private[arrow] class ByteWriter(val valueVector: TinyIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getByte(ordinal))
  }
}

private[arrow] class ShortWriter(val valueVector: SmallIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getShort(ordinal))
  }
}

private[arrow] class IntegerWriter(val valueVector: IntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class LongWriter(val valueVector: BigIntVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class FloatWriter(val valueVector: Float4Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getFloat(ordinal))
  }
}

private[arrow] class DoubleWriter(val valueVector: Float8Vector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getDouble(ordinal))
  }
}

private[arrow] class StringWriter(val valueVector: VarCharVector) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val utf8 = input.getUTF8String(ordinal)
    // todo: for off-heap UTF8String, how to pass in to arrow without copy?
    valueVector.setSafe(count, utf8.getBytes, 0, utf8.numBytes())
  }
}

private[arrow] class BinaryWriter(
  val valueVector: VarBinaryVector
) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val bytes = input.getBinary(ordinal)
    valueVector.setSafe(count, bytes, 0, bytes.length)
  }
}

private[arrow] class DateWriter(
  val valueVector: DateDayVector
) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getInt(ordinal))
  }
}

private[arrow] class TimestampWriter(
  val valueVector: TimeStampMicroTZVector
) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    valueVector.setSafe(count, input.getLong(ordinal))
  }
}

private[arrow] class ArrayWriter(
  val valueVector: ListVector,
  val elementWriter: ArrowFieldWriter
) extends ArrowFieldWriter {

  override def setNull(): Unit = {
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val array = input.getArray(ordinal)
    var i = 0
    valueVector.startNewValue(count)
    while (i < array.numElements()) {
      elementWriter.write(array, i)
      i += 1
    }
    valueVector.endValue(count, array.numElements())
  }

  override def finish(): Unit = {
    super.finish()
    elementWriter.finish()
  }

  override def reset(): Unit = {
    super.reset()
    elementWriter.reset()
  }
}

private[arrow] class StructWriter(
  val valueVector: NullableMapVector,
  children: Array[ArrowFieldWriter]
) extends ArrowFieldWriter {

  override def setNull(): Unit = {
    var i = 0
    while (i < children.length) {
      children(i).setNull()
      children(i).count += 1
      i += 1
    }
    valueVector.setNull(count)
  }

  override def setValue(input: SpecializedGetters, ordinal: Int): Unit = {
    val struct = input.getStruct(ordinal, children.length)
    var i = 0
    while (i < struct.numFields) {
      children(i).write(struct, i)
      i += 1
    }
    valueVector.setIndexDefined(count)
  }

  override def finish(): Unit = {
    super.finish()
    children.foreach(_.finish())
  }

  override def reset(): Unit = {
    super.reset()
    children.foreach(_.reset())
  }
}
