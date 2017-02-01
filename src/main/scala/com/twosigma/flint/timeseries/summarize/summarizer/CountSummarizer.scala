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

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, LeftSubtractableSummarizer, SummarizerFactory }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class CountSummarizerFactory() extends SummarizerFactory {
  override def apply(inputSchema: StructType): CountSummarizer = CountSummarizer(inputSchema, prefixOpt)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq())
}

case class CountSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String]
) extends LeftSubtractableSummarizer {
  override type T = Any
  override type U = Long
  override type V = Long
  override val summarizer = subtractable.CountSummarizer()
  override val schema = Schema.of("count" -> LongType)

  override def toT(r: InternalRow): T = r

  override def fromV(v: V): InternalRow = InternalRow(v)
}
