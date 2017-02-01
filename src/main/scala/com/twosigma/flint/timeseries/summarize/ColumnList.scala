/*
 *  Copyright 2015-2017 TWO SIGMA OPEN SOURCE, LLC
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
sealed trait ColumnList

object ColumnList {

  /**
   * [[All]] object should be used when an operation needs all columns.
   */
  case object All extends ColumnList

  /**
   * [[Sequence]] should be used to specify a list of columns.
   */
  case class Sequence(columns: Seq[String]) extends ColumnList

  /**
   * Combines two column lists into one.
   *
   * @param list1 the first list.
   * @param list2 the second list.
   * @return a composed [[ColumnList]].
   */
  def union(list1: ColumnList, list2: ColumnList): ColumnList = (list1, list2) match {
    case (All, _) => All
    case (_, All) => All
    case (Sequence(seq1), Sequence(seq2)) => Sequence((seq1 ++ seq2).distinct)
  }
}
