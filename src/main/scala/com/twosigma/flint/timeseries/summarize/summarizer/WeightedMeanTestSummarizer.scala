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

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ WeightedMeanTestOutput, WeightedMeanTestState, WeightedMeanTestSummarizer => WMSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class WeightedMeanTestSummarizerFactory(valueColumn: String, weightColumn: String)
  extends BaseSummarizerFactory(valueColumn, weightColumn) {
  override def apply(inputSchema: StructType): WeightedMeanTestSummarizer =
    WeightedMeanTestSummarizer(inputSchema, prefixOpt, requiredColumns)
}

case class WeightedMeanTestSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends LeftSubtractableSummarizer with FilterNullInput {
  val Sequence(Seq(valueColumn, weightColumn)) = requiredColumns
  private val valueColumnIndex = inputSchema.fieldIndex(valueColumn)
  private val weightColumnIndex = inputSchema.fieldIndex(weightColumn)
  private final val valueExtractor = asDoubleExtractor(inputSchema(valueColumnIndex).dataType, valueColumnIndex)
  private final val weightExtractor = asDoubleExtractor(inputSchema(weightColumnIndex).dataType, weightColumnIndex)
  private val columnPrefix = s"${valueColumn}_${weightColumn}"

  override type T = (Double, Double)
  override type U = WeightedMeanTestState
  override type V = WeightedMeanTestOutput
  override val summarizer = WMSummarizer()

  override val schema = Schema.of(
    s"${columnPrefix}_weightedMean" -> DoubleType,
    s"${columnPrefix}_weightedStandardDeviation" -> DoubleType,
    s"${columnPrefix}_weightedTStat" -> DoubleType,
    s"${columnPrefix}_observationCount" -> LongType
  )

  override def toT(r: InternalRow): T =
    (
      valueExtractor(r),
      weightExtractor(r)
    )

  override def fromV(v: V): InternalRow = InternalRow.fromSeq(v.productIterator.toSeq)
}
