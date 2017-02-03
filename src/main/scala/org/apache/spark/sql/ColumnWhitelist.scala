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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.execution.python.PythonUDF

object ColumnWhitelist {

  def preservesOrdering(column: Column): Boolean = {
    preservesOrdering(new ColumnWrapper(column).expression)
  }

  def preservesOrdering(expr: Expression): Boolean = {
    val isOrdered: Boolean = expr match {
      // Basic expressions in Expression.scala
      case _: LeafExpression => true
      case _: UnaryExpression => true
      case _: BinaryExpression => true
      case _: TernaryExpression => true

      case _: AtLeastNNonNulls => true
      case _: CaseWhenBase => true
      case _: Concat => true
      case _: CreateArray => true
      case _: CreateNamedStruct => true
      case _: CreateNamedStructUnsafe => true
      case _: FormatString => true
      case _: Generator => true
      case _: Greatest => true
      case _: If => true
      case _: In => true
      case _: JsonTuple => true
      case _: Least => true
      case _: StaticInvoke => true
      case _: StringPredicate => true

      case _: ScalaUDF => true
      case _: PythonUDF => true

      case _ => false
    }

    isOrdered && expr.children.forall(preservesOrdering)
  }
}
