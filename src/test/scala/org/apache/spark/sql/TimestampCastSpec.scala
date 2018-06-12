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

import java.sql.Timestamp
import java.time.Instant

import scala.reflect.runtime.universe.TypeTag

import com.twosigma.flint.timeseries.TimeSeriesSuite
import org.apache.spark.sql.functions._

class TimestampCastSpec extends TimeSeriesSuite {
  import TimestampCastSpec._
  import testImplicits._

  behavior of "TimestampToNanos"

  testEvalAndCodegen("retain up to microsecond precision", nanosToTimestamp(expectedNanos)){ df =>
    val actual = df.select(TimestampToNanos(col("time")).as("long"))
      .collect()
      .map(_.getAs[Long]("long"))

    assert(actual === expectedNanos)
  }

  behavior of "LongToTimestamp"

  testEvalAndCodegen("retain up to microsecond precision", expectedNanos) { df =>
    val actual = df.select(NanosToTimestamp(col("time")).as("timestamp"))
      .collect()
      .map(_.getAs[Timestamp]("timestamp"))

    val expectedTimestamps = expectedNanos.map { nanos =>
      Timestamp.from(Instant.ofEpochSecond(0, nanos))
    }
    assert(actual === expectedTimestamps)
  }

  /**
   * @param text the description of the test case.
   * @param data A sequence to use as the data for a local relation and an external RDD.
   * @param f The test case function given a DataFrame.
   */
  private def testEvalAndCodegen[T: TypeTag](text: String, data: Seq[T])(f: DataFrame => Unit): Unit = {
    it should s"$text (eval)" in {
      f(asLocalRelation(data))
    }
    it should s"$text (codegen)" in {
      f(asExternalRDD(data))
    }
  }

  /**
   * Spark optimizes a local collection and projection into a local relation
   * which uses eval instead of running code gen.
   */
  private def asLocalRelation[T: TypeTag](input: Seq[T]): DataFrame = {
    input.map(Tuple1(_)).toDF("time")
  }

  /**
   * Avoid Spark's local relation optimization by adding a map before
   * creating a dataframe with the test data. Used to invoke code gen in the
   * test cases.
   */
  private def asExternalRDD[T: TypeTag](input: Seq[T]): DataFrame = {
    sc.range(0, input.size.toLong).map { i =>
      Tuple1(input(i.toInt))
    }.toDF("time")
  }

}

object TimestampCastSpec {

  val expectedNanos = Seq[Long](
    0L,
    Long.MaxValue - (Long.MaxValue % 1000), // clip to microsecond precision
    946684800000000000L, // 2001-01-01
    1262304000000000000L, // 2010-01-01
    1893456000000000000L // 2030-01-01
  )

  def nanosToTimestamp(input: Seq[Long]): Seq[Timestamp] = input.map { v =>
    Timestamp.from(Instant.ofEpochSecond(0, v))
  }

}
