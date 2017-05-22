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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize.{ BaseSummarizerFactory, ColumnList, SummarizerFactory }
import org.apache.spark.sql.types._

case class MeanSummarizerFactory(column: String)
  extends BaseSummarizerFactory(column) {
  override def apply(inputSchema: StructType): MeanSummarizer =
    new MeanSummarizer(inputSchema, prefixOpt, requiredColumns)
}

class MeanSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList
) extends NthMomentSummarizer(inputSchema, prefixOpt, requiredColumns, 1) {
  private val Sequence(Seq(column)) = requiredColumns
  override val schema = Schema.of(s"${column}_mean" -> DoubleType)
}
