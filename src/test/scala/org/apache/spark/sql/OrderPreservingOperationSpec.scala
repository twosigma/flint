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

package org.apache.spark.sql

import com.twosigma.flint.FlintSuite
import org.apache.spark.sql.PartitionPreservingOperation.isPartitionPreserving

class OrderPreservingOperationSpec extends FlintSuite with FlintTestData {

  val inputTransformations = Set(withTime2Column, withTime3ColumnUdf, filterV, orderByV, repartition)
  val allInputTransformations = inputTransformations.subsets().filter(_.nonEmpty).flatMap{ transformations =>
    transformations.toList.permutations
  }.map { transformations =>
    transformations.reduce((x, y) => x.andThen(y))
  }.toList

  def withInputTransform(
    transformations: Seq[(DataFrame) => DataFrame]
  )(df: DataFrame)(test: (DataFrame) => Unit): Unit = {
    for (ts <- transformations) {
      test(ts(df))
    }
  }

  def assertOrderPreserving(op: (DataFrame) => DataFrame, isOrderPreserving: Boolean): Unit = {
    def test(df: DataFrame): Unit = {
      assert(OrderPreservingOperation.isOrderPreserving(df, op(df)) == isOrderPreserving)
    }
    withInputTransform(allInputTransformations)(testData)(test)
  }

  it should "test select('col')" in {
    assertOrderPreserving(selectV, true)
  }

  it should "test selectExpr('col + 1 as col')" in {
    assertOrderPreserving(selectExprVPlusOne, true)
  }

  it should "test selectExpr('sum(col)')" in {
    assertOrderPreserving(selectExprSumV, false)
  }

  it should "test filter" in {
    assertOrderPreserving(filterV, true)
  }

  it should "test withColumn simple column expression" in {
    assertOrderPreserving(withTime2Column, true)
  }

  it should "test withColumn udf" in {
    assertOrderPreserving(withTime3ColumnUdf, true)
  }

  it should "test orderBy" in {
    assertOrderPreserving(orderByTime, false)
  }

  it should "test withColumn window expression" ignore {
    // TODO: This test needs HiveContext
    assertOrderPreserving(addRankColumn, false)
  }

  it should "test select aggregation" in {
    assertOrderPreserving(selectSumV, false)
  }

  it should "test groupBy aggregation" in {
    assertOrderPreserving(groupByTimeSumV, false)
  }

  it should "test repartition" in {
    assertOrderPreserving(repartition, false)
  }

  it should "test coalesce" in {
    assertOrderPreserving(coalesce, false)
  }

  it should "test cache" in {
    val data = DFConverter.newDataFrame(testData)
    val op = cache
    assert(OrderPreservingOperation.isOrderPreserving(data, op(data)))
    data.unpersist
  }

  it should "test cache and select" in {
    val data = DFConverter.newDataFrame(testData)
    val op = cache.andThen(selectV)
    assert(OrderPreservingOperation.isOrderPreserving(data, op(data)))
    data.unpersist
  }

  it should "throw exception when not derived" in {
    intercept[IllegalArgumentException] {
      OrderPreservingOperation.isOrderPreserving(testData.select("time", "v"), testData2.select("time", "v"))
    }
  }
}
