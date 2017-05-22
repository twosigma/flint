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

import com.twosigma.flint.timeseries.summarize._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ ArrayType, StructField, StructType }

case class StackSummarizerFactory(factories: Seq[SummarizerFactory])
  extends SummarizerFactory {

  factories.foreach {
    case factory => require(
      !factory.isInstanceOf[OverlappableSummarizerFactory],
      "Stacking overlappable summarizers are not supported"
    )
  }

  /**
   * This doesn't affect input validation because [[StackSummarizer]] extends [[InputAlwaysValid]]
   */
  override val requiredColumns: ColumnList = factories.map(_.requiredColumns).reduce(_ ++ _)

  def apply(inputSchema: StructType): Summarizer = {
    val summarizers = factories.map(f => f.apply(inputSchema))

    new StackSummarizer(inputSchema, prefixOpt, requiredColumns, summarizers)
  }
}

class StackSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  summarizers: Seq[Summarizer]
) extends Summarizer with InputAlwaysValid {

  override type T = InternalRow
  override type U = Seq[Any]
  override type V = Seq[InternalRow]

  require(
    summarizers.forall(s => s.outputSchema == summarizers.head.outputSchema),
    s"Summarizers must have identical schemas to be stacked: ${summarizers.map(_.outputSchema).mkString(" vs. ")}"
  )
  override val schema: StructType = StructType(
    StructField(StackSummarizer.stackColumn, ArrayType(summarizers.head.outputSchema))
      :: Nil
  )

  override val summarizer =
    com.twosigma.flint.rdd.function.summarize.summarizer.StackSummarizer(summarizers)

  // Convert the output of `summarizer` to the InternalRow.
  override def fromV(v: V): InternalRow = InternalRow(new GenericArrayData(v))

  // Convert the InternalRow to the type of row expected by the `summarizer`.
  override def toT(r: InternalRow): T = r

}

object StackSummarizer {
  val stackColumn = "stack"
}
