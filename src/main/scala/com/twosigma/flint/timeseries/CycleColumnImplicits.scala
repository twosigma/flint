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

package com.twosigma.flint.timeseries

import org.apache.spark.sql.types.DataType

/**
 * Trait containing implicit conversions to [[CycleColumn]]s. This trait is mixed-in to
 * the [[CycleColumn]] companion object so that conversions are automatically resolved
 * by the compiler without an additional import statement by the client.
 */
trait CycleColumnImplicits {

  /**
   * Implicit conversion from a tuple that returns a `Map[Row, U]` to a [[CycleColumn]].
   *
   * Rows that are not defined in the returned map will get a value of `null`.
   *
   * Example:
   * {{{
   *   val tsRdd: TimeSeriesRDD = ...
   *   tsrdd.addColumnsForCycle("newCol" -> DoubleType -> { rows: Seq[Row] =>
   *     rows.map { row => row -> row.getAs[Double]("otherCol") + 1 }.toMap
   *   })
   * }}}
   *
   */
  implicit def tupleMapToCycleColumn[D <: DataType, U](cycleColumn: CycleColumn.MapForm[D, U]): CycleColumn = {
    val ((name, dataType), f) = cycleColumn
    CycleColumn(name, dataType, CycleColumn.mapFormToSeqForm(f))
  }

  /**
   * Implicit conversion from a tuple that returns values as a `Seq[U]` to a [[CycleColumn]].
   *
   * Example usage:
   * {{{
   *   val tsRdd: TimeSeriesRDD = ...
   *   tsRdd.addColumnsForCycle("newCol" -> DoubleType -> { rows: Seq[Row] =>
   *     rows.map(_.getAs[Double]("otherCol") + 1)
   *   })
   * }}}
   */
  implicit def tupleSeqToCycleColumn[D <: DataType, U](cycleColumn: CycleColumn.SeqForm[D, U]): CycleColumn = {
    val ((name, dataType), udf) = cycleColumn
    CycleColumn(name, dataType, udf)
  }

  /**
   * Applies the column name to complete an unnamed [[CycleColumn]].
   *
   * Example usage:
   * {{{
   *   val tsRdd: TimeSeriesRDD = ...
   *   val unnamedColumn = CycleColumn.unnamed(DoubleType, { rows: Seq[Row] => rows.map(_ => 1.0) })
   *   tsRdd.addColumnsForCycle("newCol" -> unnamedColumn)
   *   })
   * }}}
   */
  implicit def applyNameToUnnamedCycleColumn(
    nameAndUnnamedCycleColumn: (String, CycleColumn.UnnamedCycleColumn)
  ): CycleColumn = {
    val (name, unnamedCycleColumn) = nameAndUnnamedCycleColumn
    unnamedCycleColumn(name)
  }

  /**
   * Implicit conversion of any Seq of elements that can be implicitly converted to [[CycleColumn]].
   *
   * Example usage:
   * {{{
   *   val tsRdd: TimeSeriesRDD = ...
   *   val udf1: CycleColumn.UnnamedCycleColumn = ...
   *   val udf2: CycleColumn.UnnamedCycleColumn = ...
   *   tsRdd.addColumnsForCycle(Seq("newCol1" -> udf1, "newCol2" -> udf2))
   * }}}
   */
  implicit def seqCycleColumn[T](seq: Seq[T])(implicit toCycleColumn: T => CycleColumn): Seq[CycleColumn] =
    seq.map(toCycleColumn)

}
