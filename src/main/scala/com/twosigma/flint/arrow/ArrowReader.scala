/*
 *  Copyright 2017 TWO SIGMA OPEN SOURCE, LLC
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
import org.apache.arrow.vector.BaseValueVector.BaseAccessor
import org.apache.arrow.vector.types.{ DateUnit, FloatingPointPrecision, TimeUnit => ArrowTimeUnit }
import org.apache.arrow.vector.types.pojo.{ ArrowType, Schema }

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{ BufferHolder, UnsafeRowWriter }
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait RowFieldWriter[T <: BaseAccessor] {
  val unsafeRowWriter: UnsafeRowWriter
  val arrowValueAccessor: T

  def write(rowIndex: Int)
}

abstract class PrimitiveRowFieldWriter[T <: BaseAccessor](
  val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: T
) extends RowFieldWriter[T] {
  protected def writeValue(rowIndex: Int): Unit

  override def write(rowIndex: Int): Unit = {
    if (arrowValueAccessor.isNull(rowIndex)) {
      unsafeRowWriter.setNullAt(ordinal)
    } else {
      writeValue(rowIndex)
    }
  }
}

class BooleanRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableBitVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class ShortRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableSmallIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class IntegerRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class LongRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableBigIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class FloatRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableFloat4Vector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class DoubleRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableFloat8Vector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class ByteRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTinyIntVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class UTF8StringRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableVarCharVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, UTF8String.fromBytes(arrowValueAccessor.get(rowIndex)))
  }
}

class BinaryRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableVarBinaryVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class DateDayRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableDateDayVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class DateMilliRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableDateMilliVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(
      ordinal,
      java.util.concurrent.TimeUnit.MILLISECONDS.toDays(arrowValueAccessor.get(rowIndex))
    )
  }
}

// Timestamps
class TimestampSecRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampSecVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.SECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
  }
}

class TimestampSecTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampSecTZVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.SECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
  }
}

class TimestampMilliRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampMilliVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.MILLISECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
  }
}

class TimestampMilliTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampMilliTZVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.MILLISECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
  }
}

class TimestampMicroRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampMicroVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class TimestampMicroTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampMicroTZVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, arrowValueAccessor.get(rowIndex))
  }
}

class TimestampNanoRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampNanoVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.NANOSECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
  }
}

class TimestampNanoTZRowFieldWriter(
  override val ordinal: Int,
  override val unsafeRowWriter: UnsafeRowWriter,
  override val arrowValueAccessor: NullableTimeStampNanoTZVector#Accessor
) extends PrimitiveRowFieldWriter(ordinal, unsafeRowWriter, arrowValueAccessor) {
  override protected def writeValue(rowIndex: Int): Unit = {
    unsafeRowWriter.write(ordinal, TimeUnit.NANOSECONDS.toMicros(arrowValueAccessor.get(rowIndex)))
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
  private[this] val accessors = root.getFieldVectors.asScala.toArray.map(_.getAccessor())
  private[this] val rowFieldWriters = for (i <- 0 until columnCount)
    yield RowFieldWriter(i, unsafeRowWriter, accessors(i), schema.getFields.get(i).getType)

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
    arrowAccessor: ValueVector.Accessor,
    dataType: ArrowType
  ): RowFieldWriter[_] = {
    dataType match {
      case ArrowType.Bool.INSTANCE =>
        new BooleanRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableBitVector#Accessor])

      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 =>
        new ByteRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableTinyIntVector#Accessor])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 16 =>
        new ShortRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableSmallIntVector#Accessor])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 32 =>
        new IntegerRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableIntVector#Accessor])
      case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 64 =>
        new LongRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableBigIntVector#Accessor])

      case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.SINGLE =>
        new FloatRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableFloat4Vector#Accessor])
      case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.DOUBLE =>
        new DoubleRowFieldWriter(ordinal, unsafeRowWriter,
          arrowAccessor.asInstanceOf[NullableFloat8Vector#Accessor])

      case ArrowType.Utf8.INSTANCE =>
        new UTF8StringRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableVarCharVector#Accessor])
      case ArrowType.Binary.INSTANCE =>
        new BinaryRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
          .asInstanceOf[NullableVarBinaryVector#Accessor])
      case d: ArrowType.Date =>
        d.getUnit match {
          case DateUnit.DAY =>
            new DateDayRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
              .asInstanceOf[NullableDateDayVector#Accessor])
          case DateUnit.MILLISECOND =>
            new DateMilliRowFieldWriter(ordinal, unsafeRowWriter, arrowAccessor
              .asInstanceOf[NullableDateMilliVector#Accessor])
        }

      case d: ArrowType.Timestamp =>
        (d.getUnit, d.getTimezone) match {
          case (ArrowTimeUnit.SECOND, null) =>
            new TimestampSecRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampSecVector#Accessor])
          case (ArrowTimeUnit.SECOND, tz) =>
            new TimestampSecTZRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampSecTZVector#Accessor])
          case (ArrowTimeUnit.MILLISECOND, null) =>
            new TimestampMilliRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampMilliVector#Accessor])
          case (ArrowTimeUnit.MILLISECOND, tz) =>
            new TimestampMilliTZRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampMilliTZVector#Accessor])
          case (ArrowTimeUnit.MICROSECOND, null) =>
            new TimestampMicroRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampMicroVector#Accessor])
          case (ArrowTimeUnit.MICROSECOND, tz) =>
            new TimestampMicroTZRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampMicroTZVector#Accessor])
          case (ArrowTimeUnit.NANOSECOND, null) =>
            new TimestampNanoRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampNanoVector#Accessor])
          case (ArrowTimeUnit.NANOSECOND, tz) =>
            new TimestampNanoTZRowFieldWriter(ordinal, unsafeRowWriter,
              arrowAccessor.asInstanceOf[NullableTimeStampNanoTZVector#Accessor])
        }

      case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dataType")
    }
  }
}

