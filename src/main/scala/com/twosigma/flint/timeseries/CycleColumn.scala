/*
 *  Copyright 2018 TWO SIGMA OPEN SOURCE, LLC
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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

/**
 * Implementations of this trait are used as parameters to [[TimeSeriesRDD.addColumnsForCycle]].
 *
 * Generally, client code shouldn't create implementations of [[CycleColumn]] directly.
 * Instead, use the [[CycleColumn.MapForm]] or [[CycleColumn.SeqForm]] as
 *
 * All implementing classes of this trait *must* be serializable because they are
 * used in closures that are passed to Spark executors.
 */
private[flint] trait CycleColumn extends Serializable {

  /** The column name where the result stored */
  def name: String

  /** The SparkSQL `DataType` that this UDF returns */
  def dataType: DataType

  /**
   * This method is called by [[TimeSeriesRDD.addColumnsForCycle]] to apply the
   * UDF to the current cycle.
   *
   * The returned values is joined by index with the original row.
   *
   * @return a seq of values. If the length of the sequence is less than the input
   *         in `rows`, [[TimeSeriesRDD.addColumnsForCycle]] will pad the sequence
   *         with `null`.
   */
  def applyCycle(rows: Seq[Row]): Seq[Any]

}

object CycleColumn extends CycleColumnImplicits {

  /**
   * Type alias for [[CycleColumn]] tuple definitions with a cycle function that returns a `Map[Row, U]`.
   *
   * @tparam D The SparkSQL [[DataType]] that the Udf returns
   * @tparam U The Scala data type that the Udf returns
   */
  type MapForm[D <: DataType, U] = ((String, D), Seq[Row] => Map[Row, U])

  /**
   * Type alias for [[CycleColumn]] tuple definitions with a cycle function that returns a `Seq[U]`.
   *
   * @tparam D The SparkSQL [[DataType]] that the Udf returns
   * @tparam U The Scala data type that the Udf returns
   */
  type SeqForm[D <: DataType, U] = ((String, D), Seq[Row] => Seq[U])

  /**
   * Type alias for a [[CycleColumn]] without a `name`.
   *
   * Typical usage uses the implicit conversion for `(String, UnnamedCycleColumn)`
   * defined in [[CycleColumnImplicits]] to apply the column name to the column.
   *
   * Example usage:
   * {{{
   *   val unnamedCycleColumn: UnnamedCycleColumn = ...
   *   tsRdd.addColumnsForCycle("output" -> unnamedCycleColumn)
   * }}}
   */
  type UnnamedCycleColumn = String => CycleColumn

  /**
   * Factory method for creating a predefined [[CycleColumn]].
   *
   * For all other usage, use [[MapForm]] or [[SeqForm]] as parameters to [[TimeSeriesRDD.addColumnsForCycle]]
   * instead of this method.
   *
   * @param f any values referenced in the closure `f` must be serializable.
   */
  def apply(columnName: String, dt: DataType, f: Seq[Row] => Seq[Any]): CycleColumn = new CycleColumn {
    val name: String = columnName
    val dataType: DataType = dt
    def applyCycle(input: Seq[Row]): Seq[Any] = f(input)
  }

  /**
   * Factory method for creating a predefined [[UnnamedCycleColumn]].
   *
   * For all other usage, use [[MapForm]] or [[SeqForm]] as parameters to [[TimeSeriesRDD.addColumnsForCycle]]
   * instead of this method.
   *
   * @param f any values referenced in the closure `f` must be serializable.
   */
  def unnamed(dt: DataType, f: Seq[Row] => Seq[Any]): UnnamedCycleColumn = { name: String =>
    apply(name, dt, f)
  }

  /**
   * Converts a cycle function that returns a `Map[Row, _]` to one that returns `Seq[Any]`.
   */
  def mapFormToSeqForm(f: Seq[Row] => Map[Row, _]): Seq[Row] => Seq[Any] = { input: Seq[Row] =>
    val rowValueMap: Map[Row, _] = f(input)
    input.map(row => rowValueMap.getOrElse(row, null))
  }
}

