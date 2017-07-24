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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ ArrowSummarizerState, ArrowSummarizer => ArrowSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize.{ ColumnList, InputAlwaysValid, Summarizer, SummarizerFactory }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ BinaryType, StructType }

case class ArrowSummarizerFactory(columns: Seq[String]) extends SummarizerFactory {
  override val requiredColumns: ColumnList = Sequence(columns)
  override def apply(inputSchema: StructType): ArrowSummarizer =
    ArrowSummarizer(inputSchema, prefixOpt, requiredColumns)
}

case class ArrowSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends Summarizer with InputAlwaysValid {
  override type T = InternalRow
  override type U = ArrowSummarizerState
  override type V = Array[Byte]
  override val summarizer = ArrowSum(inputSchema)
  override val schema: StructType = Schema.of("arrow_bytes" -> BinaryType)
  override def toT(r: InternalRow): T = r
  override def fromV(v: V): InternalRow = InternalRow(v)
}