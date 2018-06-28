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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.arrow.vector._
import org.apache.arrow.vector.BaseValueVector
import org.apache.arrow.vector.types.{ DateUnit, FloatingPointPrecision, TimeUnit => ArrowTimeUnit }
import org.apache.arrow.vector.types.pojo.{ ArrowType, Schema }

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{ BufferHolder, UnsafeRowWriter }
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait RowFieldWriter[T <: BaseValueVector] {
  val unsafeRowWriter: UnsafeRowWriter
  val valueVector: T

  def write(rowIndex: Int)
}

abstract class PrimitiveRowFieldWriter[T <: BaseValueVector](
  val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: T
) extends RowFieldWriter[T] {
  protected def writeValue(rowIndex: Int): Unit

  override def write(rowIndex: Int): Unit = {
    if (valueVector.isNull(rowIndex)) {
      unsafeRowWriter.setNullAt(ordinal)
    } else {
      writeValue(rowIndex)
    }
  }
}

class BooleanRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: BitVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class ShortRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: SmallIntVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class IntegerRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: IntVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class LongRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: BigIntVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class FloatRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: Float4Vector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class DoubleRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: Float8Vector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class ByteRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TinyIntVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class UTF8StringRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: VarCharVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, UTF8String.fromBytes(valueVector.get(rowIndex)))
  }
}

class BinaryRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: VarBinaryVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class DateDayRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: DateDayVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class DateMilliRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: DateMilliVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(
      ordinal,
      java.util.concurrent.TimeUnit.MILLISECONDS.toDays(valueVector.get(rowIndex))
    )
  }
}

// Timestamps
class TimestampSecRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampSecVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.SECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class TimestampSecTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampSecTZVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.SECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class TimestampMilliRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampMilliVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.MILLISECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class TimestampMilliTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampMilliTZVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.MILLISECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class TimestampMicroRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampMicroVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class TimestampMicroTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampMicroTZVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, valueVector.get(rowIndex))
  }
}

class TimestampNanoRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampNanoVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.NANOSECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class TimestampNanoTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val valueVector: TimeStampNanoTZVector
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, valueVector) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.NANOSECONDS.toMicros(valueVector.get(rowIndex)))
  }
}

class ArrowBackendUnsafeRowIterator(
  root: VectorSchemaRoot,
  schema: Schema,
  rowCount: Int
) extends ClosableIterator[UnsafeRow] {
  private[this] var rowIndex = 0
  private[this] val columnCount = schema.getFields.size()
  private[this] val unsafeRow = new UnsafeRow(columnCount)
  private[this] val unsafeRowBufferHolder = new BufferHolder(unsafeRow, 0)
  private[this] val unsafeRowWriter = new UnsafeRowWriter(unsafeRowBufferHolder, columnCount)
  private[this] val valueVectors = root.getFieldVectors.asScala.toArray
  private[this] val rowFieldWriters = for (i <- 0 until columnCount)
    yield RowFieldWriter(i, unsafeRowWriter, valueVectors(i), schema.getFields.get(i).getType)

  override def hasNext: Boolean = rowIndex < rowCount

  override def next(): UnsafeRow = {
    unsafeRowBufferHolder.reset()
    unsafeRowWriter.zeroOutNullBytes()
    var i = 0
    while (i < columnCount) {
      rowFieldWriters(i).write(rowIndex)
      i += 1
    }
    rowIndex += 1
    unsafeRow.setTotalSize(unsafeRowBufferHolder.totalSize)
    unsafeRow
  }

  override def close(): Unit = root.close()
}

object RowFieldWriter {
  def apply(
    ordinal: Int,
    unsafeRowWriter: UnsafeRowWriter,
    valueVector: ValueVector,
    dataType: ArrowType
  ): RowFieldWriter[_] = {
    dataType match {
      case ArrowType.Bool.INSTANCE =>
        new BooleanRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[BitVector])

      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 =>
        new ByteRowFieldWriter(ordinal, unsafeRowWriter, valueVector
          .asInstanceOf[TinyIntVector])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 16 =>
        new ShortRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[SmallIntVector])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 32 =>
        new IntegerRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[IntVector])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 64 =>
        new LongRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[BigIntVector])

      case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.SINGLE =>
        new FloatRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[Float4Vector])
      case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.DOUBLE =>
        new DoubleRowFieldWriter(ordinal, unsafeRowWriter,
          valueVector.asInstanceOf[Float8Vector])

      case ArrowType.Utf8.INSTANCE =>
        new UTF8StringRowFieldWriter(ordinal, unsafeRowWriter, valueVector
          .asInstanceOf[VarCharVector])
      case ArrowType.Binary.INSTANCE =>
        new BinaryRowFieldWriter(ordinal, unsafeRowWriter, valueVector
          .asInstanceOf[VarBinaryVector])
      case d: ArrowType.Date =>
        d.getUnit match {
          case DateUnit.DAY =>
            new DateDayRowFieldWriter(ordinal, unsafeRowWriter, valueVector
              .asInstanceOf[DateDayVector])
          case DateUnit.MILLISECOND =>
            new DateMilliRowFieldWriter(ordinal, unsafeRowWriter, valueVector
              .asInstanceOf[DateMilliVector])
        }

      case d: ArrowType.Timestamp =>
        (d.getUnit, d.getTimezone) match {
          case (ArrowTimeUnit.SECOND, null) =>
            new TimestampSecRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampSecVector])
          case (ArrowTimeUnit.SECOND, tz) =>
            new TimestampSecTZRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampSecTZVector])
          case (ArrowTimeUnit.MILLISECOND, null) =>
            new TimestampMilliRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampMilliVector])
          case (ArrowTimeUnit.MILLISECOND, tz) =>
            new TimestampMilliTZRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampMilliTZVector])
          case (ArrowTimeUnit.MICROSECOND, null) =>
            new TimestampMicroRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampMicroVector])
          case (ArrowTimeUnit.MICROSECOND, tz) =>
            new TimestampMicroTZRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampMicroTZVector])
          case (ArrowTimeUnit.NANOSECOND, null) =>
            new TimestampNanoRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampNanoVector])
          case (ArrowTimeUnit.NANOSECOND, tz) =>
            new TimestampNanoTZRowFieldWriter(ordinal, unsafeRowWriter,
              valueVector.asInstanceOf[TimeStampNanoTZVector])
        }

      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}

