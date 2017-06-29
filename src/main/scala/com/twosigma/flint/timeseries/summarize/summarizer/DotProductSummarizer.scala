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

import com.twosigma.flint.math.Kahan
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ DotProductSummarizer => DotProductSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class DotProductSummarizerFactory(columnX: String, columnY: String)
  extends BaseSummarizerFactory(columnX, columnY) {
  override def apply(inputSchema: StructType): DotProductSummarizer = new DotProductSummarizer(
    inputSchema,
    prefixOpt,
    requiredColumns
  )
}

class DotProductSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList
) extends LeftSubtractableSummarizer with FilterNullInput {
  val Sequence(Seq(columnX, columnY)) = requiredColumns
  private val columnXIndex = inputSchema.fieldIndex(columnX)
  private val columnYIndex = inputSchema.fieldIndex(columnY)
  private final val columnXExtractor = asDoubleExtractor(inputSchema(columnXIndex).dataType, columnXIndex)
  private final val columnYExtractor = asDoubleExtractor(inputSchema(columnYIndex).dataType, columnYIndex)
  private val columnPrefix = s"${columnX}_${columnY}"

  override type T = (Double, Double)
  override type U = Kahan
  override type V = Double
  override val summarizer = new DotProductSum()

  override val schema = Schema.of(s"${columnPrefix}_dotProduct" -> DoubleType)
  override def toT(r: InternalRow): T =
    (
      columnXExtractor(r),
      columnYExtractor(r)
    )

  override def fromV(v: V): InternalRow = InternalRow(v)
}
