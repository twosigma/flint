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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, SummarizerFactory }
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{ DoubleType, StructType }

case class CovarianceSummarizerFactory(columnX: String, columnY: String) extends SummarizerFactory {
  override def apply(inputSchema: StructType): CovarianceSummarizer =
    new CovarianceSummarizer(inputSchema, prefixOpt, columnX, columnY)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(columnX, columnY))
}

class CovarianceSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columnX: String,
  columnY: String
) extends AbstractCorrelationSummarizer(inputSchema, prefixOpt, columnX, columnY) {
  override val schema = Schema.of(
    s"${columnPrefix}_covariance" -> DoubleType
  )

  override def fromV(v: V): GenericInternalRow = new GenericInternalRow(Array[Any](v.covariance))
}
