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

import com.twosigma.flint.timeseries.summarize.{ ColumnList, OverlappableSummarizerFactory, Summarizer, SummarizerFactory }
import com.twosigma.flint.timeseries.row.{ DuplicateColumnsException, Schema }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

// TODO: Currently, this doesn't support overlappable summarizers. One idea for dealing with that in the future is to
//       have multiple types of summarizers inside composite summarizer.
case class CompositeSummarizerFactory(factory1: SummarizerFactory, factory2: SummarizerFactory)
  extends SummarizerFactory {

  Seq(factory1, factory2).foreach {
    case factory => require(
      !factory.isInstanceOf[OverlappableSummarizerFactory],
      "Composition of overlappable summarizers are not supported"
    )
  }

  def apply(inputSchema: StructType): Summarizer = {
    val summarizer1 = factory1.apply(inputSchema)
    val summarizer2 = factory2.apply(inputSchema)

    // http://stackoverflow.com/questions/12229005/getting-reference-to-a-parameter-of-the-outer-function-having-a-conflicting-name
    new {
      private val inputSchemaOuterScope = inputSchema
      private val prefixOptOuterScope = prefixOpt
    } with Summarizer {
      override type T = (summarizer1.T, summarizer2.T)
      override type U = (summarizer1.U, summarizer2.U)
      override type V = (summarizer1.V, summarizer2.V)

      override val schema: StructType = StructType(summarizer1.outputSchema.fields ++ summarizer2.outputSchema.fields)
      override val inputSchema: StructType = inputSchemaOuterScope
      override val prefixOpt: Option[String] = prefixOptOuterScope

      override val summarizer =
        com.twosigma.flint.rdd.function.summarize.summarizer.CompositeSummarizer(
          summarizer1.summarizer, summarizer2.summarizer
        )

      requireNoDuplicateColumns(outputSchema)

      // Convert the output of `summarizer` to the InternalRow.
      override def fromV(v: V): InternalRow = {
        val r1 = summarizer1.fromV(v._1)
        val r2 = summarizer2.fromV(v._2)
        new GenericInternalRow((r1.toSeq(summarizer1.outputSchema) ++ r2.toSeq(summarizer2.outputSchema)).toArray)
      }

      // Convert the InternalRow to the type of row expected by the `summarizer`.
      override def toT(r: InternalRow): T = (summarizer1.toT(r), summarizer2.toT(r))
    }
  }

  def requireNoDuplicateColumns(schema: StructType): Unit = {
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

  override def requiredColumns(): ColumnList =
    ColumnList.union(factory1.requiredColumns(), factory2.requiredColumns())
}
