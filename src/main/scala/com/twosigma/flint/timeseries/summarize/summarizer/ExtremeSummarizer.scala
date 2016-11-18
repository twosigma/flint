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

import com.twosigma.flint.rdd.function.summarize.summarizer.ExtremesSummarizer
import com.twosigma.flint.timeseries.summarize.summarizer.ExtremeSummarizerType.ExtremeType
import com.twosigma.flint.timeseries.summarize.{ Summarizer, SummarizerFactory, toClassTag, toOrdering }
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import scala.collection.mutable.PriorityQueue
import com.twosigma.flint.timeseries.row.Schema
import scala.reflect.{ ClassTag }

object ExtremeSummarizerType extends Enumeration {
  type ExtremeType = Value
  val Max = Value("max")
  val Min = Value("min")
}

case class ExtremeSummarizerFactory(val column: String, val extremeType: ExtremeType) extends SummarizerFactory {
  override def apply(inputSchema: StructType): Summarizer = {
    val dataType = inputSchema(column).dataType
    val ctag = toClassTag(dataType)
    var order = toOrdering(dataType)
    if (extremeType == ExtremeSummarizerType.Min) {
      order = order.reverse
    }
    ExtremeSummarizer(inputSchema, prefixOpt, column, ctag, order, extremeType.toString())
  }
}

case class ExtremeSummarizer[E](
  override val inputSchema: StructType,
  override val prefixOpt: Option[String],
  val column: String,
  val tag: ClassTag[E],
  val order: Ordering[_],
  val outputColumnName: String
) extends Summarizer {
  private val columnIndex = inputSchema.fieldIndex(column)

  override type T = E
  override type U = PriorityQueue[E]
  override type V = Array[E]

  override val summarizer = ExtremesSummarizer[E](1, tag, order.asInstanceOf[Ordering[E]])
  override val schema = Schema.of(s"${column}_${outputColumnName}" -> inputSchema(column).dataType)

  override def toT(r: InternalRow): T = r.get(columnIndex, inputSchema(columnIndex).dataType).asInstanceOf[E]

  override def fromV(v: V): InternalRow = {
    if (v.isEmpty) {
      InternalRow(null)
    } else {
      InternalRow(v(0))
    }
  }
}
