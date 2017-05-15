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
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class CorrelationSummarizerFactory(columnX: String, columnY: String)
  extends BaseSummarizerFactory(columnX, columnY) {
  override def apply(inputSchema: StructType): CorrelationSummarizer =
    new CorrelationSummarizer(inputSchema, prefixOpt, requiredColumns)
}

abstract class AbstractCorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList
) extends Summarizer with FilterNullInput {
  protected final val Sequence(Seq(columnX, columnY)) = requiredColumns
  protected final val columnXIndex = inputSchema.fieldIndex(columnX)
  protected final val columnYIndex = inputSchema.fieldIndex(columnY)
  protected final val xExtractor = asDoubleExtractor(inputSchema(columnXIndex).dataType, columnXIndex)
  protected final val yExtractor = asDoubleExtractor(inputSchema(columnYIndex).dataType, columnYIndex)
  protected val columnPrefix = s"${columnX}_${columnY}"
  override final type T = (Double, Double)
  override final type U = CorrelationState
  override final type V = CorrelationOutput
  override final val summarizer = CorrelationSum()
  override final def toT(r: InternalRow): (Double, Double) = (xExtractor(r), yExtractor(r))
}

class CorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList
) extends AbstractCorrelationSummarizer(inputSchema, prefixOpt, requiredColumns) {
  override val schema = Schema.of(
    s"${columnPrefix}_correlation" -> DoubleType,
    s"${columnPrefix}_correlationTStat" -> DoubleType
  )
  override def fromV(v: V): InternalRow = InternalRow(v.correlation, v.tStat)
}
