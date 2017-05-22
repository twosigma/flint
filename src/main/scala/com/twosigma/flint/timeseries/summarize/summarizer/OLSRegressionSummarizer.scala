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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ OLSRegressionOutput, OLSRegressionState, RegressionRow, OLSRegressionSummarizer => RegressionSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object OLSRegressionSummarizer {
  val samplesColumn: String = "samples"
  val betaColumn: String = "beta"
  val interceptColumn: String = "intercept"
  val hasInterceptColumn: String = "hasIntercept"
  val stdErrOfInterceptColumn: String = "stdErr_intercept"
  val stdErrOfBetaColumn: String = "stdErr_beta"
  val rSquaredColumn: String = "rSquared"
  val rColumn: String = "r"
  val tStatOfInterceptColumn: String = "tStat_intercept"
  val tStatOfBetaColumn: String = "tStat_beta"
  val logLikelihoodColumn: String = "logLikelihood"
  val akaikeICColumn: String = "akaikeIC"
  val bayesICColumn: String = "bayesIC"
  val conditionColumn: String = "cond"
  val constantsColumn: String = "const_columns"

  val outputSchema = Schema.of(
    samplesColumn -> LongType,
    betaColumn -> ArrayType(DoubleType),
    interceptColumn -> DoubleType,
    hasInterceptColumn -> BooleanType,
    stdErrOfInterceptColumn -> DoubleType,
    stdErrOfBetaColumn -> ArrayType(DoubleType),
    rSquaredColumn -> DoubleType,
    rColumn -> DoubleType,
    tStatOfInterceptColumn -> DoubleType,
    tStatOfBetaColumn -> ArrayType(DoubleType),
    logLikelihoodColumn -> DoubleType,
    akaikeICColumn -> DoubleType,
    bayesICColumn -> DoubleType,
    conditionColumn -> DoubleType,
    constantsColumn -> ArrayType(StringType)
  )
}

case class OLSRegressionSummarizerFactory(
  yColumn: String,
  xColumns: Array[String],
  weightColumn: String,
  shouldIntercept: Boolean,
  shouldIgnoreConstants: Boolean
) extends SummarizerFactory {
  type K = Long

  override val requiredColumns: ColumnList = {
    val weightSeq = if (weightColumn == null) Seq() else Seq(weightColumn)
    ColumnList.Sequence(Seq(yColumn) ++ xColumns ++ weightSeq)
  }

  override def apply(inputSchema: StructType): OLSRegressionSummarizer =
    OLSRegressionSummarizer(
      inputSchema,
      prefixOpt,
      requiredColumns,
      yColumn,
      xColumns,
      Option(weightColumn),
      shouldIntercept,
      shouldIgnoreConstants
    )
}

case class OLSRegressionSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  yColumn: String,
  xColumns: Array[String],
  weightColumn: Option[String],
  shouldIntercept: Boolean,
  shouldIgnoreConstants: Boolean
) extends Summarizer with FilterNullInput {

  private val dimensionOfX = xColumns.length
  private val isWeighted = weightColumn != null
  private val yColumnId = inputSchema.fieldIndex(yColumn)
  private val xColumnIds = xColumns.map(inputSchema.fieldIndex)
  private val weightColumnId = weightColumn.map(inputSchema.fieldIndex)

  private final val yExtractor = asDoubleExtractor(inputSchema(yColumnId).dataType, yColumnId)
  private final val weightExtractor = weightColumnId.map { id => asDoubleExtractor(inputSchema(id).dataType, id) }
  private final val xExtractors = xColumnIds.map {
    id => id -> asDoubleExtractor(inputSchema(id).dataType, id)
  }.toMap

  override type T = RegressionRow
  override type U = OLSRegressionState
  override type V = OLSRegressionOutput

  override val summarizer =
    new RegressionSummarizer(dimensionOfX, shouldIntercept, isWeighted, shouldIgnoreConstants)

  override def toT(r: InternalRow): RegressionRow = {
    val x = new Array[Double](xColumnIds.length)
    var i = 0
    while (i < x.length) {
      x(i) = xExtractors(xColumnIds(i))(r)
      i += 1
    }

    RegressionRow(
      time = 0L,
      y = yExtractor(r),
      x = x,
      weight = weightExtractor.fold(1.0)(f => f(r))
    )
  }

  override val schema = OLSRegressionSummarizer.outputSchema

  override def fromV(o: OLSRegressionOutput): InternalRow = InternalRow.fromSeq(
    Array[Any](
      o.count,
      new GenericArrayData(o.beta),
      o.intercept,
      o.hasIntercept,
      o.stdErrOfIntercept,
      new GenericArrayData(o.stdErrOfBeta),
      o.rSquared,
      o.r,
      o.tStatOfIntercept,
      new GenericArrayData(o.tStatOfBeta),
      o.logLikelihood,
      o.akaikeIC,
      o.bayesIC,
      o.cond,
      new GenericArrayData(o.constantsCoordinates.map(i => UTF8String.fromString(xColumns(i))))
    )
  )
}
