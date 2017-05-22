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

import java.util.ArrayDeque

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, InputAlwaysValid, LeftSubtractableSummarizer, SummarizerFactory }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

case class RowsSummarizerFactory(column: String) extends SummarizerFactory {
  override val requiredColumns: ColumnList = ColumnList.All
  override def apply(inputSchema: StructType): RowsSummarizer =
    RowsSummarizer(inputSchema, prefixOpt, requiredColumns, column)
}

case class RowsSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList,
  column: String
) extends LeftSubtractableSummarizer with InputAlwaysValid {
  override type T = InternalRow
  override type U = ArrayDeque[InternalRow]
  override type V = Array[InternalRow]
  override val summarizer = subtractable.InternalRowsSummarizer()
  override val schema = Schema.of(column -> ArrayType(inputSchema))

  override def toT(r: InternalRow): T = r

  override def fromV(v: V): InternalRow = {
    val values = new GenericArrayData(v.asInstanceOf[Array[Any]])
    InternalRow(values)
  }
}
