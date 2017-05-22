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

import com.twosigma.flint.timeseries.summarize.{ ColumnList, FilterNullInput, Summarizer, SummarizerFactory }
import org.apache.spark.sql.CatalystTypeConvertersWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{ DataType, StructType }

class PredicateSummarizerFactory(
  factory: SummarizerFactory,
  f: AnyRef,
  inputColumns: Seq[(String, DataType)]
) extends SummarizerFactory {

  override val requiredColumns: ColumnList = factory.requiredColumns ++ ColumnList.Sequence(inputColumns.map(_._1))

  def apply(inputSchema: StructType): Summarizer = {
    inputColumns.foreach {
      case (column, dataType) =>
        require(inputSchema.fieldNames.contains(column), s"Input schema $inputSchema doesn't contain $column column.")
        require(
          inputSchema(column).dataType == dataType,
          s"Input type: ${inputSchema(column).dataType} isn't equal to $dataType"
        )
    }

    val filterFunction = UDFConverter.udfToFilter(f, inputSchema, inputColumns.map(_._1))
    val innerSummarizer = factory(inputSchema)
    new PredicateSummarizer(inputSchema, prefixOpt, requiredColumns, innerSummarizer, filterFunction)
  }

}

class PredicateSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  override val requiredColumns: ColumnList,
  val innnerSummarizer: Summarizer,
  val predicate: InternalRow => Boolean
) extends Summarizer with FilterNullInput {

  override val schema: StructType = innnerSummarizer.schema

  override val summarizer = innnerSummarizer.summarizer

  override type T = innnerSummarizer.T
  override type U = innnerSummarizer.U
  override type V = innnerSummarizer.V

  override def isValid(r: InternalRow): Boolean = super.isValid(r) && predicate(r)

  override def toT(r: InternalRow): T = innnerSummarizer.toT(r)

  override def fromV(v: V): InternalRow = innnerSummarizer.fromV(v)
}

private object UDFConverter {

  def udfToFilter(function: AnyRef, inputSchema: StructType, columns: Seq[String]): InternalRow => Boolean = {
    val fieldIndices = columns.map(inputSchema.fieldIndex)
    val columnPairs = fieldIndices.map(index => index -> inputSchema.fields(index).dataType)
    buildFilterFunction(function, columnPairs)
  }

  private def buildFilterFunction(function: AnyRef, columns: Seq[(Int, DataType)]): InternalRow => Boolean = {
    val extractors = columns.map {
      case (index, dataType) =>
        val converter = CatalystTypeConvertersWrapper.toScalaConverter(dataType)
        row: InternalRow => converter(row.get(index, dataType))
    }

    columns.size match {
      case 1 =>
        val func = function.asInstanceOf[(Any) => Boolean]
        (input: InternalRow) => {
          func(extractors(0)(input))
        }

      case 2 =>
        val func = function.asInstanceOf[(Any, Any) => Boolean]
        (input: InternalRow) => {
          func(extractors(0)(input), extractors(1)(input))
        }

      case 3 =>
        val func = function.asInstanceOf[(Any, Any, Any) => Boolean]
        (input: InternalRow) => {
          func(extractors(0)(input), extractors(1)(input), extractors(2)(input))
        }

      case 4 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any) => Boolean]
        (input: InternalRow) => {
          func(extractors(0)(input), extractors(1)(input), extractors(2)(input), extractors(3)(input))
        }

      case _ =>
        throw new UnsupportedOperationException("Cannot build function with more than four arguments")
    }
  }
}
