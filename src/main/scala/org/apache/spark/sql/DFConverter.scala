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

import com.twosigma.flint.rdd.OrderedRDD

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType

/**
 * Functions to convert an RDD with internal rows to DataFrame.
 */
object DFConverter {
  def toDataFrame(sqlContext: SQLContext, schema: StructType, rdd: OrderedRDD[Long, InternalRow]): DataFrame = {
    val internalRows = rdd.values

    val logicalPlan = LogicalRDD(schema.toAttributes, internalRows)(sqlContext)
    DataFrame(sqlContext, logicalPlan)
  }
}
