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

package com.twosigma.flint.sql.function

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import scala.collection.mutable.ArrayBuffer

object Rank {
  /**
   * Calculate percentile rank for a list of rows.
   *
   * Implementation based on [[https://en.wikipedia.org/wiki/Percentile_rank]]
   *
   * @param rows
   * @param valueField The field to rank on.
   * @param rankField The field contains the percentile rank of the row. Type of this field is double.
   */
  def percentRank[T](
    rows: Seq[Row], valueField: String, rankField: String
  )(implicit ord: Ordering[T]): Seq[Row] = {
    val schema = StructType(rows.head.schema.fields :+ StructField(rankField, DoubleType))

    val iter = rows.map {
      row => (row, row.getAs[T](valueField))
    }.zipWithIndex.sortWith {
      case (((_, v1), _), ((_, v2), _)) => ord.lt(v1, v2)
    }.iterator

    val totalNum = rows.length

    val output: ArrayBuffer[((Row, T), Int, Double)] = ArrayBuffer()
    val seen: ArrayBuffer[((Row, T), Int)] = ArrayBuffer()
    var lastValue: Option[T] = None

    while (iter.hasNext) {
      val value = iter.next()
      lastValue match {
        case Some(lastV) =>
          if (ord.lt(lastV, value._1._2)) {
            val rank = (output.length + 0.5 * seen.length) / totalNum
            output.appendAll(seen.map { case (v, rowIndex) => (v, rowIndex, rank) })
            seen.clear()
          }
        case None =>
      }
      seen.append(value)
      lastValue = Some(value._1._2)
    }

    val rank = (output.length + 0.5 * seen.length) / totalNum
    output.appendAll(seen.map { case (v, rowIndex) => (v, rowIndex, rank) })

    output.sortWith(_._2 < _._2).map {
      case (((row, _), _, r)) => new GenericRowWithSchema((row.toSeq :+ r).toArray, schema)
    }
  }
}
