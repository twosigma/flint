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

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ ProductState, ProductSummarizer => ProductSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class ProductSummarizerFactory(productColumn: String) extends BaseSummarizerFactory(productColumn) {
  override def apply(inputSchema: StructType): ProductSummarizer = ProductSummarizer(
    inputSchema,
    prefixOpt,
    requiredColumns
  )
}

case class ProductSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends LeftSubtractableSummarizer with FilterNullInput {
  private val Sequence(Seq(productColumn)) = requiredColumns
  private val productColumnIndex = inputSchema.fieldIndex(productColumn)

  override type T = Double
  override type U = ProductState
  override type V = Double
  override val summarizer = new ProductSum()
  override val schema = Schema.of(s"${productColumn}_product" -> DoubleType)

  private final val productExtractor = asDoubleExtractor(inputSchema(productColumnIndex).dataType, productColumnIndex)

  override def toT(r: InternalRow): T = productExtractor(r)

  override def fromV(v: V): InternalRow = InternalRow(v)
}
