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

import com.twosigma.flint.rdd.function.summarize.summarizer.{ CorrelationOutput, CorrelationState, MultiCorrelationOutput, MultiCorrelationState, CorrelationSummarizer => CorrelationSum, MultiCorrelationSummarizer => MultiCorrelationSum }
import com.twosigma.flint.timeseries.row.Schema
import com.twosigma.flint.timeseries.summarize.{ ColumnList, Summarizer, SummarizerFactory, anyToDouble }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

case class CorrelationSummarizerFactory(columnX: String, columnY: String) extends SummarizerFactory {
  override def apply(inputSchema: StructType): CorrelationSummarizer =
    new CorrelationSummarizer(inputSchema, prefixOpt, columnX, columnY)

  override def requiredColumns(): ColumnList = ColumnList.Sequence(Seq(columnX, columnY))
}

abstract class AbstractCorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columnX: String,
  columnY: String
) extends Summarizer {
  private final val columnXIndex = inputSchema.fieldIndex(columnX)
  private final val columnYIndex = inputSchema.fieldIndex(columnY)
  private final val xToDouble = anyToDouble(inputSchema(columnXIndex).dataType)
  private final val yToDouble = anyToDouble(inputSchema(columnYIndex).dataType)
  protected val columnPrefix = s"${columnX}_${columnY}"
  override final type T = (Double, Double)
  override final type U = CorrelationState
  override final type V = CorrelationOutput
  override final val summarizer = CorrelationSum()
  override final def toT(r: InternalRow): (Double, Double) =
    (
      xToDouble(r.get(columnXIndex, inputSchema(columnXIndex).dataType)),
      yToDouble(r.get(columnYIndex, inputSchema(columnYIndex).dataType))
    )
}

class CorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columnX: String,
  columnY: String
) extends AbstractCorrelationSummarizer(inputSchema, prefixOpt, columnX, columnY) {
  override val schema = Schema.of(
    s"${columnPrefix}_correlation" -> DoubleType,
    s"${columnPrefix}_correlationTStat" -> DoubleType
  )

  override def fromV(v: V): InternalRow = InternalRow(v.correlation, v.tStat)
}

/**
 * The factory for [[MultiCorrelationSummarizer]].
 *
 * If `others` is empty, it gives a summarizer to compute all correlations for all possible pairs in `columns`.
 * Otherwise, it gives a summarizer to compute correlations between the pairs of columns where the left is one
 * of `columns` and the right is one of `others`.
 *
 * @param columns Array of column names.
 * @param others  Option of an array of column names.
 */
case class MultiCorrelationSummarizerFactory(
  columns: Array[String],
  others: Option[Array[String]]
) extends SummarizerFactory {
  override def apply(inputSchema: StructType): MultiCorrelationSummarizer = others.fold {
    val cols = columns.length
    require(columns.nonEmpty, "columns must be non-empty.")
    val pairIndexes = for (i <- 0 until cols; j <- (i + 1) until cols) yield (i, j)
    MultiCorrelationSummarizer(inputSchema, prefixOpt, columns, pairIndexes)
  } {
    case otherColumns =>
      val duplicatedColumns = columns.toSet intersect otherColumns.toSet
      require(columns.nonEmpty && otherColumns.nonEmpty, "columns and others must be non-empty.")
      require(duplicatedColumns.isEmpty, s"otherColumns has some duplicated columns ${duplicatedColumns.mkString(",")}")
      val cols = columns.length + otherColumns.length
      val pairIndexes = for (i <- columns.indices; j <- columns.length until cols) yield (i, j)
      MultiCorrelationSummarizer(inputSchema, prefixOpt, columns ++ otherColumns, pairIndexes)
  }

  override def requiredColumns(): ColumnList = ColumnList.Sequence(columns ++ others.getOrElse(Array[String]()))
}

/**
 * A summarizer to compute correlations for pairs of columns in `columns` where the pairs are specified by `pairIndexes`.
 */
case class MultiCorrelationSummarizer(
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  columns: Array[String],
  pairIndexes: IndexedSeq[(Int, Int)]
) extends Summarizer {
  private val k = columns.length
  private val columnIndexes = columns.map(inputSchema.fieldIndex)
  private val toDoubleFns = columnIndexes.map { case id => id -> anyToDouble(inputSchema(id).dataType) }.toMap
  override type T = Array[Double]
  override type U = MultiCorrelationState
  override type V = MultiCorrelationOutput
  override val summarizer = MultiCorrelationSum(k, pairIndexes)

  override val schema = Schema.of(pairIndexes.flatMap {
    case (i, j) =>
      Seq(
        s"${columns(i)}_${columns(j)}_correlation" -> DoubleType,
        s"${columns(i)}_${columns(j)}_correlationTStat" -> DoubleType
      )
  }: _*)

  override def toT(r: InternalRow): Array[Double] = columnIndexes.map {
    case id => toDoubleFns(id)(r.get(id, inputSchema(id).dataType))
  }

  override def fromV(v: V): InternalRow = InternalRow.fromSeq(
    v.outputs.flatMap { case (i, j, o) => Seq(o.correlation, o.tStat) }.toArray[Any]
  )
}

