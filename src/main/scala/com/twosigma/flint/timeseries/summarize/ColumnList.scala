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

package com.twosigma.flint.timeseries.summarize

/**
 * A ColumnList trait represents operation requirements on the input columns.
 */
sealed trait ColumnList {

  /**
   * Combines this [[ColumnList]] with another one.
   *
   * @param other the first list.
   * @return a composed [[ColumnList]].
   */
  def ++(other: ColumnList): ColumnList
}

object ColumnList {

  /**
   * [[All]] object should be used when an operation needs all columns.
   */
  case object All extends ColumnList {
    def ++(other: ColumnList): ColumnList = All
  }

  /**
   * [[Sequence]] should be used to specify a list of columns.
   */
  case class Sequence(columns: Seq[String]) extends ColumnList {
    def ++(other: ColumnList): ColumnList = other match {
      case All => All
      case Sequence(seq) => Sequence(columns ++ seq)
    }
  }
}
