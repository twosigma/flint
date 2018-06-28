/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ ArrowSummarizerResult, ArrowSummarizerState, ArrowSummarizer => ArrowSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList
import com.twosigma.flint.timeseries.summarize.{ ColumnList, InputAlwaysValid, Summarizer, SummarizerFactory }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ ArrayType, BinaryType, StructType }

object ArrowSummarizer {
  val baseRowsColumnName = "__baseRows"
  val arrowBatchColumnName = "arrow_bytes"
}

/**
 * Summarize columns into arrow batch.
 *
 * @param columns
 */
case class ArrowSummarizerFactory(columns: Seq[String], includeBaseRows: Boolean) extends SummarizerFactory {
  override val requiredColumns: ColumnList =
    if (includeBaseRows) {
      ColumnList.All
    } else {
      ColumnList.Sequence(columns)
    }

  override def apply(inputSchema: StructType): ArrowSummarizer = {
    val outputBatchSchema = StructType(columns.map(col => inputSchema(inputSchema.fieldIndex(col))))
    ArrowSummarizer(inputSchema, outputBatchSchema, includeBaseRows, prefixOpt, requiredColumns)
  }
}

case class ArrowSummarizer(
  override val inputSchema: StructType,
  outputBatchSchema: StructType,
  includeBaseRows: Boolean,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends Summarizer with InputAlwaysValid {
  override type T = InternalRow
  override type U = ArrowSummarizerState
  override type V = ArrowSummarizerResult
  override val summarizer = ArrowSum(inputSchema, outputBatchSchema, includeBaseRows)
  override val schema: StructType =
    if (includeBaseRows) {
      Schema.of(
        ArrowSummarizer.baseRowsColumnName -> ArrayType(inputSchema),
        ArrowSummarizer.arrowBatchColumnName -> BinaryType
      )
    } else {
      Schema.of(
        ArrowSummarizer.arrowBatchColumnName -> BinaryType
      )
    }

  override def toT(r: InternalRow): T = r
  override def fromV(v: V): InternalRow =
    if (includeBaseRows) {
      InternalRow(new GenericArrayData(v.baseRows), v.arrowBatch)
    } else {
      InternalRow(v.arrowBatch)
    }
}