/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.rdd.function.summarize.summarizer.{
  ExponentialSmoothingOutput,
  ExponentialSmoothingState,
  SmoothingRow,
  ExponentialSmoothingSummarizer => ESSummarizer
}
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.types._
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

object ExponentialSmoothingInterpolation extends Enumeration {
  type ExponentialSmoothingInterpolation = Value
  val PreviousPoint = Value("previous")
  val LinearInterpolation = Value("linear")
  val CurrentPoint = Value("current")
}

object ExponentialSmoothingConvention extends Enumeration {
  type ExponentialSmoothingConvention = Value
  val Core = Value("core")
  val Convolution = Value("convolution")
  val Legacy = Value("legacy")
}

case class ExponentialSmoothingSummarizerFactory(
  xColumn: String,
  timeColumn: String,
  alpha: Double,
  primingPeriods: Double,
  timestampsToPeriods: (Long, Long) => Double,
  exponentialSmoothingInterpolation: ExponentialSmoothingInterpolation.Value,
  exponentialSmoothingConvention: ExponentialSmoothingConvention.Value
) extends BaseSummarizerFactory(xColumn, timeColumn) {
  override def apply(inputSchema: StructType): ExponentialSmoothingSummarizer =
    ExponentialSmoothingSummarizer(
      inputSchema,
      prefixOpt,
      requiredColumns,
      alpha,
      primingPeriods,
      timestampsToPeriods,
      exponentialSmoothingInterpolation,
      exponentialSmoothingConvention
    )
}

case class ExponentialSmoothingSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  alpha: Double,
  primingPeriods: Double,
  timestampsToPeriods: (Long, Long) => Double,
  exponentialSmoothingType: ExponentialSmoothingInterpolation.Value,
  exponentialSmoothingConvention: ExponentialSmoothingConvention.Value
) extends FlippableSummarizer
  with FilterNullInput
  with TimeAwareSummarizer {

  private val Sequence(Seq(xColumn, timeColumn)) = requiredColumns
  private val xColumnId = inputSchema.fieldIndex(xColumn)
  private val timeColumnId = inputSchema.fieldIndex(timeColumn)

  private final val xExtractor =
    asDoubleExtractor(inputSchema(xColumnId).dataType, xColumnId)

  override type T = SmoothingRow
  override type U = ExponentialSmoothingState
  override type V = ExponentialSmoothingOutput

  override val summarizer = ESSummarizer(
    alpha,
    primingPeriods,
    timestampsToPeriods,
    exponentialSmoothingType,
    exponentialSmoothingConvention
  )

  override val schema: StructType = Schema.of(s"${xColumn}_ema" -> DoubleType)

  override def toT(r: InternalRow): SmoothingRow = {
    SmoothingRow(
      time = getTimeNanos(r, timeColumnId),
      x = xExtractor(r)
    )
  }

  override def fromV(o: ExponentialSmoothingOutput): GenericInternalRow = {
    new GenericInternalRow(
      Array[Any](
        o.es
      )
    )
  }
}
