/*
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

import java.util.concurrent.TimeUnit

import com.twosigma.flint.annotation.SparklyrApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

@SparklyrApi
class TimeSeriesRDDBuilder(
  isSorted: Boolean,
  timeUnit: TimeUnit,
  timeColumn: String
) {
  def fromRDD(
    rdd: RDD[Row],
    schema: StructType
  ): TimeSeriesRDD = {
    TimeSeriesRDD.fromRDD(rdd, schema)(isSorted, timeUnit, timeColumn)
  }

  def fromDF(
    dataFrame: DataFrame
  ): TimeSeriesRDD = {
    TimeSeriesRDD.fromDF(dataFrame)(isSorted, timeUnit, timeColumn)
  }
}
