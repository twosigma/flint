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

import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ ProductState, GeometricMeanSummarizer => GeometricMeanSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class GeometricMeanSummarizerFactory(geometricMeanColumn: String)
  extends BaseSummarizerFactory(geometricMeanColumn) {
  override def apply(inputSchema: StructType): GeometricMeanSummarizer = GeometricMeanSummarizer(
    inputSchema,
    prefixOpt,
    requiredColumns
  )
}

case class GeometricMeanSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends LeftSubtractableSummarizer with FilterNullInput {
  private val Sequence(Seq(geometricMeanColumn)) = requiredColumns
  private val geometricMeanColumnIndex = inputSchema.fieldIndex(geometricMeanColumn)

  override type T = Double
  override type U = ProductState
  override type V = Double
  override val summarizer = new GeometricMeanSum()
  override val schema = Schema.of(s"${geometricMeanColumn}_geometricMean" -> DoubleType)

  private final val geometricMeanExtractor = asDoubleExtractor(
    inputSchema(geometricMeanColumnIndex).dataType,
    geometricMeanColumnIndex
  )

  override def toT(r: InternalRow): T = geometricMeanExtractor(r)

  override def fromV(v: V): InternalRow = InternalRow(v)
}
