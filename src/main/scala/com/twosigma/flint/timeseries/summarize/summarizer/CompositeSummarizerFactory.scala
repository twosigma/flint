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
import com.twosigma.flint.timeseries.row.{ DuplicateColumnsException, Schema }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

case class CompositeSummarizerFactory(factory1: SummarizerFactory, factory2: SummarizerFactory)
  extends SummarizerFactory {

  Seq(factory1, factory2).foreach {
    case factory => require(
      !factory.isInstanceOf[OverlappableSummarizerFactory],
      "Composition of overlappable summarizers are not supported"
    )
  }

  /**
   * This doesn't affect input validation because [[CompositeSummarizer]] extends [[InputAlwaysValid]]
   */
  override val requiredColumns = factory1.requiredColumns ++ factory2.requiredColumns

  def apply(inputSchema: StructType): Summarizer = {
    val summarizer1 = factory1.apply(inputSchema)
    val summarizer2 = factory2.apply(inputSchema)

    new CompositeSummarizer(inputSchema, prefixOpt, requiredColumns, summarizer1, summarizer2)
  }
}

class CompositeSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  val summarizer1: Summarizer,
  val summarizer2: Summarizer
) extends Summarizer with InputAlwaysValid {

  override type T = (InternalRow, InternalRow)
  override type U = (Any, Any)
  override type V = (InternalRow, InternalRow)

  override val schema: StructType = StructType(summarizer1.outputSchema.fields ++ summarizer2.outputSchema.fields)
  override val summarizer =
    com.twosigma.flint.rdd.function.summarize.summarizer.CompositeSummarizer(summarizer1, summarizer2)

  requireNoDuplicateColumns(outputSchema)

  // Convert the output of `summarizer` to the InternalRow.
  override def fromV(v: V): InternalRow = {
    val (r1, r2) = v
    new GenericInternalRow((r1.toSeq(summarizer1.outputSchema) ++ r2.toSeq(summarizer2.outputSchema)).toArray)
  }

  // Convert the InternalRow to the type of row expected by the `summarizer`.
  override def toT(r: InternalRow): T = (r, r)

  private def requireNoDuplicateColumns(schema: StructType): Unit = {
    try {
      Schema.requireUniqueColumnNames(schema)
    } catch {
      case e: DuplicateColumnsException =>
        throw new DuplicateColumnsException(
          s"Found conflict output columns: ${e.duplicates}. Use prefix() to rename conflict summarizers to be composed",
          e.duplicates
        )
    }
  }
}
