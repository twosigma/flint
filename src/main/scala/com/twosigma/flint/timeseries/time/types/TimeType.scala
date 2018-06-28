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

package com.twosigma.flint.timeseries.time.types

import com.twosigma.flint.FlintConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{ SQLContext, SparkSession, types }

trait TimeType {
  /**
   * return the time of a row in nanoseconds since epoch.
   * @return
   */
  def internalToNanos(value: Long): Long

  /**
   * return the internal numeric value of the time type that is
   * same as the input nanoseconds since epoch.
   */
  def nanosToInternal(nanosSinceEpoch: Long): Long

  /**
   * round nanoseconds to the closest value in the past that is
   * supported by this time type's precision.
   */
  def roundDownPrecision(nanosSinceEpoch: Long): Long
}

object TimeType {
  case object LongType extends TimeType {
    override def internalToNanos(value: Long): Long = value
    override def nanosToInternal(nanos: Long): Long = nanos
    override def roundDownPrecision(nanos: Long): Long = nanos
  }

  // Spark sql represents timestamp as microseconds internally
  case object TimestampType extends TimeType {
    override def internalToNanos(value: Long): Long = value * 1000
    override def nanosToInternal(nanos: Long): Long = nanos / 1000
    override def roundDownPrecision(nanos: Long): Long = nanos - nanos % 1000
  }

  def apply(timeType: String): TimeType = {
    timeType match {
      case "long" => LongType
      case "timestamp" => TimestampType
      case _ => throw new IllegalAccessException(s"Unsupported time type: ${timeType}. " +
        s"Only `long` and `timestamp` are supported.")
    }
  }

  def apply(sqlType: types.DataType): TimeType = {
    sqlType match {
      case types.LongType => LongType
      case types.TimestampType => TimestampType
      case _ => throw new IllegalArgumentException(s"Unsupported time type: ${sqlType}")
    }
  }

  def get(sparkSession: SparkSession): TimeType = {
    TimeType(sparkSession.conf.get(
      FlintConf.TIME_TYPE_CONF, FlintConf.TIME_TYPE_DEFAULT
    ))
  }
}
