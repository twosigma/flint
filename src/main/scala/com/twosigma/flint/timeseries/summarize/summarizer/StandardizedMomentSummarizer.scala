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

import breeze.numerics.pow
import com.twosigma.flint.rdd.function.summarize.summarizer.subtractable.{ NthCentralMomentOutput, NthCentralMomentState, NthCentralMomentSummarizer => NthCentralMomentSum }
import com.twosigma.flint.timeseries.summarize.summarizer.StandardizedMomentSummarizerType.StandardizedMomentType
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.ColumnList.Sequence
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * See https://en.wikipedia.org/wiki/Skewness and https://en.wikipedia.org/wiki/Kurtosis for the formulas to calculate
 * these values.
 */
object StandardizedMomentSummarizerType extends Enumeration {
  type StandardizedMomentType = Value
  val Skewness = Value("skewness")
  val Kurtosis = Value("kurtosis")
}

case class StandardizedMomentSummarizerFactory(column: String, standardizedMomentType: StandardizedMomentType)
  extends BaseSummarizerFactory(column) {
  override def apply(inputSchema: StructType): StandardizedMomentSummarizer = {
    val moment = standardizedMomentType match {
      case StandardizedMomentSummarizerType.Skewness => 3
      case StandardizedMomentSummarizerType.Kurtosis => 4
    }
    StandardizedMomentSummarizer(
      inputSchema,
      prefixOpt,
      requiredColumns,
      moment,
      standardizedMomentType,
      standardizedMomentType.toString
    )
  }
}

case class StandardizedMomentSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  requiredColumns: ColumnList,
  moment: Integer,
  standardizedMomentType: StandardizedMomentType,
  outputColumnName: String
) extends LeftSubtractableSummarizer with FilterNullInput {
  private val Sequence(Seq(column)) = requiredColumns
  private val columnIndex = inputSchema.fieldIndex(column)
  private final val valueExtractor = asDoubleExtractor(inputSchema(columnIndex).dataType, columnIndex)

  override type T = Double
  override type U = NthCentralMomentState
  override type V = NthCentralMomentOutput

  override val summarizer = NthCentralMomentSum(moment)
  override val schema = Schema.of(s"${column}_$outputColumnName" -> DoubleType)

  override def toT(r: InternalRow): T = valueExtractor(r)

  override def fromV(v: V): InternalRow = {
    val variance = v.nthCentralMoment(2)
    val standardizedMoment = v.nthCentralMoment(moment) / pow(variance, moment / 2.0)
    standardizedMomentType match {
      case StandardizedMomentSummarizerType.Kurtosis =>
        InternalRow(standardizedMoment - 3.0)
      case StandardizedMomentSummarizerType.Skewness =>
        InternalRow(standardizedMoment)
    }
  }
}
