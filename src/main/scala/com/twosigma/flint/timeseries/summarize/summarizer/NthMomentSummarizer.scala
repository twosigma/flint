/*
 *  Copyright 2015-2016 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ NthMomentState, NthMomentSummarizer => NMSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, Summarizer, SummarizerFactory, anyToDouble }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class NthMomentSummarizerFactory(column: String, moment: Int) extends SummarizerFactory {
  override def apply(inputSchema: StructType): NthMomentSummarizer =
    NthMomentSummarizer(inputSchema, prefixOpt, column, moment)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(column))
}

case class NthMomentSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  column: String,
  moment: Int
) extends Summarizer {
  private val columnIndex = inputSchema.fieldIndex(column)

  override type T = Double
  override type U = NthMomentState
  override type V = Double

  private final val toDouble = anyToDouble(inputSchema(columnIndex).dataType)

  override val summarizer = NMSummarizer(moment)
  override val schema = Schema.of(s"${column}_${moment}thMoment" -> DoubleType)

  override def toT(r: InternalRow): T = toDouble(r.get(columnIndex, inputSchema(columnIndex).dataType))

  override def fromV(v: V): InternalRow = InternalRow(v)
}
