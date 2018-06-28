/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
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

import com.twosigma.flint.annotation.PythonApi

import org.apache.spark.sql.catalyst.plans.logical.{ Filter, LogicalPlan, Project, Generate }

/**
 * A class to used to check whether a DataFrame operation is partition preserving.
 *
 * See doc/partition-preserving-operation.md
 */
object OrderPreservingOperation {
  // Accessing analyzedPlan will force the cause it to be evaluated and change the original df
  // Create a new df to ensure the original df is not changed
  def analyzedPlan(df: DataFrame): LogicalPlan =
    DFConverter.newDataFrame(df).queryExecution.analyzed

  private def isOrderPreservingLogicalNode(node: LogicalPlan): Boolean = node match {
    case _: Project => true
    case _: Filter => true
    case _: Generate => true
    case _ => false
  }

  /**
   * Complexity O(n).
   */
  private[sql] def isSubtree(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    if (treeEquals(plan1, plan2)) {
      true
    } else {
      plan2.children.exists(p => isSubtree(plan1, p))
    }
  }

  private[sql] def treeEquals(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    if (plan1.fastEquals(plan2)) {
      assert(
        plan1.children.length == plan2.children.length,
        "Node equals but number of children node are different"
      )
      val childrenEqual = (plan1.children zip plan2.children).forall { case (p1, p2) => treeEquals(p1, p2) }
      // Because of the way logical plan works, if the root of two tree equals,
      // the two tree should equal. If this assumption is violated, we throw an exception.
      assert(
        childrenEqual,
        s"Root node equals but tree don't equal. plan1: ${plan1}, plan2: ${plan2}"
      )
      childrenEqual
    } else {
      false
    }
  }

  private def isOrderPreserving(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    if (treeEquals(plan1, plan2)) {
      true
    } else {
      if (isOrderPreservingLogicalNode(plan2)) {
        // OrderPreservingLogicalNode is always an UnaryNode
        isOrderPreserving(plan1, plan2.children.head)
      } else {
        false
      }
    }
  }

  /**
   * Check if df2 is derived from df1, i.e., if df2 is the result of applying one or more operations on df1
   */
  def isDerivedFrom(df1: DataFrame, df2: DataFrame): Boolean = isSubtree(analyzedPlan(df1), analyzedPlan(df2))

  /**
   * Check if df1 -> df2 is order preserving.
   *
   * @throws IllegalArgumentException if df2 is not derived from df1
   */
  @PythonApi
  def isOrderPreserving(df1: DataFrame, df2: DataFrame): Boolean =
    isOrderPreserving(analyzedPlan(df1), analyzedPlan(df2))
}
