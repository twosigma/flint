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
import PartitionPreservingOperation.{ executedPlan, isPartitionPreserving }
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

import org.apache.spark.sql.{ functions => F }

class PartitionPreservingOperationSpec extends FlintSuite with FlintTestData {

  def assertPartitionPreserving(op: (DataFrame) => DataFrame, expected: Boolean): Unit = {
    assert(isPartitionPreserving(testData, op(testData)) == expected)
    assert(isPartitionPreserving(testDataCached, op(testDataCached)) == expected)
  }

  it should "test RDDScanDataFrame correctly" in {
    assert(PartitionPreservingOperation.isPartitionPreservingDataFrame(testData), true)
  }

  it should "test select('col')" in {
    assertPartitionPreserving(selectV, true)
  }

  it should "test selectExpr('col + 1 as col')" in {
    assertPartitionPreserving(selectExprVPlusOne, true)
  }

  it should "test selectExpr('sum(col)')" in {
    assertPartitionPreserving(selectExprSumV, false)
  }

  it should "test filter" in {
    assertPartitionPreserving(filterV, true)
  }

  it should "test withColumn simple expression" in {
    assertPartitionPreserving(withTime2Column, true)
  }

  it should "test withColumn udf" in {
    assertPartitionPreserving(withTime3ColumnUdf, true)
  }

  it should "test orderBy" in {
    assertPartitionPreserving(orderByTime, false)
  }

  it should "test select aggregation" in {
    assertPartitionPreserving(selectSumV, false)
  }

  it should "test groupBy aggregation" in {
    assertPartitionPreserving(groupByTimeSumV, false)
  }

  it should "test repartition" in {
    assertPartitionPreserving(repartition, false)
  }

  it should "test coalesce" in {
    assertPartitionPreserving(coalesce, false)
  }

  it should "test cache" in {
    val data = DFConverter.newDataFrame(testData)
    val op = cache
    val expected = true
    assert(isPartitionPreserving(data, op(data)) == expected)
    data.unpersist
  }

  it should "test cache and select" in {
    val data = DFConverter.newDataFrame(testData)
    val op = cache.andThen(selectV)
    val expected = true
    assert(isPartitionPreserving(data, op(data)) == expected)
    data.unpersist
  }

  it should "get executedPlan of cached DataFrame" in {
    val data = DFConverter.newDataFrame(testData)
    data.cache()
    data.count()
    assert(executedPlan(data).isInstanceOf[InMemoryTableScanExec])
    data.unpersist()

    val orderedData = data.orderBy("time")
    orderedData.cache()
    orderedData.count()
    assert(executedPlan(orderedData).isInstanceOf[InMemoryTableScanExec])
    orderedData.unpersist()
  }

  it should "test explode" in {
    val data = new DataFrame(sqlContext, testData.logicalPlan)
    var result = data.withColumn("values", F.array(F.lit(1), F.lit(2)))
    result = result.withColumn("value", F.explode(F.col("values")))
    assert(isPartitionPreserving(data, result))
  }

  it should "throw exception when not derived" in {
    intercept[IllegalArgumentException] {
      isPartitionPreserving(testData.select("time", "v"), testData2.select("time", "v"))
    }
  }
}
