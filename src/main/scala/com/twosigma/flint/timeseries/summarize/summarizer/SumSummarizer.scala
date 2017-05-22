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

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class SumSummarizerFactory(sumColumn: String) extends BaseSummarizerFactory(sumColumn) {
  override def apply(inputSchema: StructType): SumSummarizer = SumSummarizer(inputSchema, prefixOpt, requiredColumns)
}

case class SumSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends LeftSubtractableSummarizer with FilterNullInput {
  private val Sequence(Seq(sumColumn)) = requiredColumns
  private val sumColumnIndex = inputSchema.fieldIndex(sumColumn)

  override type T = Double
  override type U = Double
  override type V = Double
  override val summarizer = subtractable.SumSummarizer[Double]()
  override val schema = Schema.of(s"${sumColumn}_sum" -> DoubleType)

  private final val sumExtractor = asDoubleExtractor(inputSchema(sumColumnIndex).dataType, sumColumnIndex)

  override def toT(r: InternalRow): T = sumExtractor(r)

  override def fromV(v: V): InternalRow = InternalRow(v)
}
