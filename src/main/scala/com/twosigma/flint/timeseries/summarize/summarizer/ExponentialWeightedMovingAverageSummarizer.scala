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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.types._
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{
  EWMARow,
  ExponentialWeightedMovingAverageOutput,
  ExponentialWeightedMovingAverageState,
  ExponentialWeightedMovingAverageSummarizer => EWMASummarizer
}
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

object ExponentialWeightedMovingAverageConvention extends Enumeration {
  type ExponentialWeightedMovingAverageConvention = Value
  val Core = Value("core")
  val Legacy = Value("legacy")
}

case class ExponentialWeightedMovingAverageSummarizerFactory(
  xColumn: String,
  timeColumn: String,
  alpha: Double,
  timestampsToPeriods: (Long, Long) => Double,
  constantPeriods: Boolean,
  exponentialWeightedMovingAverageConvention: ExponentialWeightedMovingAverageConvention.Value
) extends BaseSummarizerFactory(xColumn, timeColumn) {
  override def apply(
    inputSchema: StructType
  ): ExponentialWeightedMovingAverageSummarizer =
    ExponentialWeightedMovingAverageSummarizer(
      inputSchema,
      prefixOpt,
      requiredColumns,
      alpha,
      timestampsToPeriods,
      constantPeriods,
      exponentialWeightedMovingAverageConvention
    )
}

case class ExponentialWeightedMovingAverageSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  alpha: Double,
  timestampsToPeriods: (Long, Long) => Double,
  constantPeriods: Boolean,
  exponentialWeightedMovingAverageConvention: ExponentialWeightedMovingAverageConvention.Value
) extends LeftSubtractableSummarizer
  with FilterNullInput
  with TimeAwareSummarizer {

  private val Sequence(Seq(xColumn, timeColumn)) = requiredColumns
  private val xColumnId = inputSchema.fieldIndex(xColumn)
  private val timeColumnId = inputSchema.fieldIndex(timeColumn)

  private final val xExtractor =
    asDoubleExtractor(inputSchema(xColumnId).dataType, xColumnId)

  override type T = EWMARow
  override type U = ExponentialWeightedMovingAverageState
  override type V = ExponentialWeightedMovingAverageOutput

  override val summarizer = new EWMASummarizer(
    alpha,
    timestampsToPeriods,
    constantPeriods,
    exponentialWeightedMovingAverageConvention
  )

  override val schema: StructType = Schema.of(s"${xColumn}_ewma" -> DoubleType)

  override def toT(r: InternalRow): EWMARow = EWMARow(
    time = getTimeNanos(r, timeColumnId),
    x = xExtractor(r)
  )

  override def fromV(
    o: ExponentialWeightedMovingAverageOutput
  ): GenericInternalRow = {
    new GenericInternalRow(
      Array[Any](
        o.ewma
      )
    )
  }
}
