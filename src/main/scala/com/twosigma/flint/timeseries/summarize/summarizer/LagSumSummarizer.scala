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

import com.twosigma.flint.timeseries.summarize._
import com.twosigma.flint.timeseries.window.TimeWindow
import com.twosigma.flint.timeseries.Windows
import com.twosigma.flint.timeseries.row.Schema
import org.apache.spark.sql.types._
import com.twosigma.flint.rdd.function.summarize.summarizer.overlappable.{ LagSumSummarizerState, LagSumSummarizer => LSSummarizer }
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import org.apache.spark.sql.catalyst.InternalRow

/**
 * N.B. LagSumSummarizer exists solely to be a trivial example of an OverlappableSummarizerFactory that we can
 * use to test common functionality. It should not be depended on by application classes or exposed
 */
private[flint] object LagSumSummarizer {
  val lagSumColumn: String = "lagSum"
  val sumColumn: String = "sum"

  val outputSchema = Schema.of(
    lagSumColumn -> DoubleType,
    sumColumn -> DoubleType
  )
}

private[flint] case class LagSumSummarizerFactory(column: String, maxLookback: String)
  extends OverlappableSummarizerFactory {
  override val window: TimeWindow = Windows.pastAbsoluteTime(maxLookback)

  override val requiredColumns: ColumnList = {
    ColumnList.Sequence(Seq(column))
  }
  override def apply(inputSchema: StructType): LagSumSummarizer =
    LagSumSummarizer(
      inputSchema,
      prefixOpt,
      requiredColumns
    )

}

private[flint] case class LagSumSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList
) extends OverlappableSummarizer with FilterNullInput {

  private val Sequence(Seq(column)) = requiredColumns
  private val columnId = inputSchema.fieldIndex(column)

  private final val columnExtractor = asDoubleExtractor(inputSchema(columnId).dataType, columnId)

  override type T = Double
  override type U = LagSumSummarizerState
  override type V = LagSumSummarizerState

  override val summarizer = new LSSummarizer

  override def toT(r: InternalRow): Double = columnExtractor(r)

  override val schema = LagSumSummarizer.outputSchema

  override def fromV(o: LagSumSummarizerState): InternalRow = {
    InternalRow.fromSeq(Array[Any](
      o.lagSum,
      o.sum
    ))
  }
}
