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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ WeightedMeanTestOutput, WeightedMeanTestState, WeightedMeanTestSummarizer => WMSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, Summarizer, SummarizerFactory, anyToDouble }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class WeightedMeanTestSummarizerFactory(valueColumn: String, weightColumn: String) extends SummarizerFactory {
  override def apply(inputSchema: StructType): WeightedMeanTestSummarizer =
    WeightedMeanTestSummarizer(inputSchema, prefixOpt, valueColumn, weightColumn)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(valueColumn, weightColumn))
}

case class WeightedMeanTestSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  valueColumn: String,
  weightColumn: String
) extends Summarizer {
  private val valueColumnIndex = inputSchema.fieldIndex(valueColumn)
  private val weightColumnIndex = inputSchema.fieldIndex(weightColumn)
  private val valueToDouble = anyToDouble(inputSchema(valueColumnIndex).dataType)
  private val weightToDouble = anyToDouble(inputSchema(weightColumnIndex).dataType)
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
      valueToDouble(r.get(valueColumnIndex, inputSchema(valueColumnIndex).dataType)),
      weightToDouble(r.get(weightColumnIndex, inputSchema(weightColumnIndex).dataType))
    )

  override def fromV(v: V): InternalRow = InternalRow.fromSeq(v.productIterator.toSeq)
}
