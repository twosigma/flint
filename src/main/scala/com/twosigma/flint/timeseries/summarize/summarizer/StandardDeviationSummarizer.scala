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

import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, SummarizerFactory }
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{ DoubleType, StructType }

import scala.math.sqrt

case class StandardDeviationSummarizerFactory(column: String, applyBesselCorrection: Boolean = true)
  extends SummarizerFactory {
  override def apply(inputSchema: StructType): StandardDeviationSummarizer =
    new StandardDeviationSummarizer(inputSchema, prefixOpt, column, applyBesselCorrection)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(column))
}

class StandardDeviationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val column: String,
  val applyBesselCorrection: Boolean
) extends NthCentralMomentSummarizer(inputSchema, prefixOpt, column, 2) {
  override val schema = Schema.of(s"${column}_stddev" -> DoubleType)
  override def fromV(v: V): GenericInternalRow = {
    var variance = v.nthCentralMoment(2)
    if (applyBesselCorrection) {
      variance = variance * (v.count / (v.count - 1d))
    }
    new GenericInternalRow(Array[Any](sqrt(variance)))
  }
}
