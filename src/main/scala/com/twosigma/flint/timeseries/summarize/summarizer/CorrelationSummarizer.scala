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

package com.twosigma.flint.timeseries.summarize.summarizer

import com.twosigma.flint.rdd.function.summarize.summarizer.{ CorrelationOutput, CorrelationState, CorrelationSummarizer => CorrelationSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, Summarizer, SummarizerFactory, anyToDouble }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class CorrelationSummarizerFactory(columnX: String, columnY: String) extends SummarizerFactory {
  override def apply(inputSchema: StructType): CorrelationSummarizer =
    new CorrelationSummarizer(inputSchema, prefixOpt, columnX, columnY)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(columnX, columnY))
}

abstract class AbstractCorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columnX: String,
  columnY: String
) extends Summarizer {
  private final val columnXIndex = inputSchema.fieldIndex(columnX)
  private final val columnYIndex = inputSchema.fieldIndex(columnY)
  private final val xToDouble = anyToDouble(inputSchema(columnXIndex).dataType)
  private final val yToDouble = anyToDouble(inputSchema(columnYIndex).dataType)
  protected val columnPrefix = s"${columnX}_${columnY}"
  override final type T = (Double, Double)
  override final type U = CorrelationState
  override final type V = CorrelationOutput
  override final val summarizer = CorrelationSum()
  override final def toT(r: InternalRow): (Double, Double) =
    (
      xToDouble(r.get(columnXIndex, inputSchema(columnXIndex).dataType)),
      yToDouble(r.get(columnYIndex, inputSchema(columnYIndex).dataType))
    )
}

class CorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columnX: String,
  columnY: String
) extends AbstractCorrelationSummarizer(inputSchema, prefixOpt, columnX, columnY) {
  override val schema = Schema.of(
    s"${columnPrefix}_correlation" -> DoubleType,
    s"${columnPrefix}_correlationTStat" -> DoubleType
  )

  override def fromV(v: V): InternalRow = InternalRow(v.correlation, v.tStat)
}
