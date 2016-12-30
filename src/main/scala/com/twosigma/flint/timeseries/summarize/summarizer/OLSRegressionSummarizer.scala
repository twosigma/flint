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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ OLSRegressionOutput, OLSRegressionState, RegressionRow, OLSRegressionSummarizer => RegressionSummarizer }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, Summarizer, SummarizerFactory, anyToDouble }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

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
    tStatOfBetaColumn -> ArrayType(DoubleType)
  )
}

case class OLSRegressionSummarizerFactory(
  yColumn: String,
  xColumns: Array[String],
  weightColumn: String,
  shouldIntercept: Boolean
) extends SummarizerFactory {
  type K = Long

  override def apply(inputSchema: StructType): OLSRegressionSummarizer =
    OLSRegressionSummarizer(
      inputSchema,
      prefixOpt,
      yColumn,
      xColumns,
      Option(weightColumn),
      shouldIntercept
    )

  override def requiredColumns(): ColumnList = {
    val weightSeq = if (Option(weightColumn).isEmpty) Seq() else Seq(weightColumn)
    ColumnList.Sequence(Seq(yColumn) ++ xColumns ++ weightSeq)
  }
}

case class OLSRegressionSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  yColumn: String,
  xColumns: Array[String],
  weightColumn: Option[String],
  shouldIntercept: Boolean
) extends Summarizer {

  private val dimensionOfX = xColumns.length
  private val isWeighted = weightColumn != null
  private val yColumnId = inputSchema.fieldIndex(yColumn)
  private val xColumnIds = xColumns.map(inputSchema.fieldIndex)
  private val weightColumnId = weightColumn.map(inputSchema.fieldIndex)

  private val yToDouble = anyToDouble(inputSchema(yColumnId).dataType)
  private val weightToDouble = weightColumnId.map { id => anyToDouble(inputSchema(id).dataType) }
  private val xToDouble = xColumnIds.map { id => id -> anyToDouble(inputSchema(id).dataType) }.toMap

  override type T = RegressionRow
  override type U = OLSRegressionState
  override type V = OLSRegressionOutput

  override val summarizer = new RegressionSummarizer(dimensionOfX, shouldIntercept, isWeighted)

  override def toT(r: InternalRow): RegressionRow = RegressionRow(
    time = 0L,
    y = yToDouble(r.get(yColumnId, inputSchema(yColumnId).dataType)),
    x = xColumnIds.map { xi => xToDouble(xi)(r.get(xi, inputSchema(xi).dataType)) },
    weight = weightToDouble.fold(1.0)(f => f(r.get(weightColumnId.get, inputSchema(weightColumnId.get).dataType)))
  )

  override val schema = OLSRegressionSummarizer.outputSchema

  override def fromV(o: OLSRegressionOutput): InternalRow = {
    InternalRow.fromSeq(Array[Any](
      o.count,
      new GenericArrayData(o.beta),
      o.intercept,
      o.hasIntercept,
      o.stdErrOfIntercept,
      new GenericArrayData(o.stdErrOfBeta),
      o.rSquared,
      o.r,
      o.tStatOfIntercept,
      new GenericArrayData(o.tStatOfBeta)
    ))
  }
}
