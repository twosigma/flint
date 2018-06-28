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
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.python.BatchEvalPythonExec

/**
 * A class to used to check whether a DataFrame operation is partition preserving.
 *
 * See doc/partition.md
 */
object PartitionPreservingOperation {

  /**
   * Return the root [[SparkPlan]] in the `df`'s executedPlan.
   *
   * @param df [[DataFrame]] to exam
   * @note Accessing executedPlan will force the cause it to be evaluated and change the original [[DataFrame]].
   *       Create a new [[DataFrame]] to ensure the original df is not changed
   * @return the root [[SparkPlan]] in the `df`'s executedPlan.
   */
  def executedPlan(df: DataFrame): SparkPlan =
    DFConverter.newDataFrame(df).queryExecution.executedPlan

  /**
   * Return the leaf [[SparkPlan]] in the `df`'s executedPlan.
   *
   * @param df [[DataFrame]] to exam
   * @note Accessing executedPlan will force the cause it to be evaluated and change the original [[DataFrame]].
   *       Create a new df to ensure the original [[DataFrame]] is not changed
   * @return the leaf [[SparkPlan]] in the `df`'s executedPlan.
   */
  def leafExecutedPlan(df: DataFrame): SparkPlan = {
    var plan = DFConverter.newDataFrame(df).queryExecution.executedPlan
    while (plan.children.nonEmpty) {
      require(plan.children.length == 1)
      plan = plan.children.head
    }
    plan
  }

  private def isPartitionPreservingUnaryNode(node: SparkPlan): Boolean = {
    node match {
      case _: ProjectExec => true
      case _: FilterExec => true
      case _: BatchEvalPythonExec => true
      case _: WholeStageCodegenExec => true
      case _: InputAdapter => true
      case _: GenerateExec => true
      case _: SerializeFromObjectExec => true
      case _ => false
    }
  }

  private def isPartitionPreservingLeafNode(node: SparkPlan): Boolean = {
    node match {
      case _: RDDScanExec => true
      case _: InMemoryTableScanExec => true
      case _: ExternalRDDScanExec[_] => true
      case _ => false
    }
  }

  private def isPartitionPreservingPlan(node: SparkPlan): Boolean =
    if (node.children.isEmpty) {
      isPartitionPreservingLeafNode(node)
    } else {
      isPartitionPreservingUnaryNode(node) && isPartitionPreservingPlan(
        node.children.head
      )
    }

  def isPartitionPreservingDataFrame(df: DataFrame): Boolean =
    isPartitionPreservingPlan(executedPlan(df))

  /**
   * Checks if df1 -> df2 is partition preserving.
   * @throws IllegalArgumentException if df2 is not derived from df1
   */
  @PythonApi
  def isPartitionPreserving(df1: DataFrame, df2: DataFrame): Boolean = {
    require(
      OrderPreservingOperation.isDerivedFrom(df1, df2),
      s"df2 is not derived from df1. analyzed1: ${df1.queryExecution.analyzed} " +
        s"analyzed2: ${df2.queryExecution.analyzed}"
    )

    isPartitionPreservingDataFrame(df2)
  }
}
