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

import com.twosigma.flint.timeseries.TimeSeriesSuite
import org.apache.spark.sql.functions._

class TimestampCastSpec extends TimeSeriesSuite {
  import TimestampCastSpec._
  import testImplicits._

  behavior of "TimestampToNanos"

  it should "retain up to microsecond precision with eval" in {
    testTimestampToNanos {
      // Spark optimizes a local collection and projection into a local relation
      // which uses eval instead of running code gen.
      expectedNanos.map { nanos =>
        Tuple1(Timestamp.from(Instant.ofEpochSecond(0, nanos)))
      }.toDF("time")
    }
  }

  it should "retain up to microsecond precision with codegen" in {
    testTimestampToNanos {
      // Avoid Spark's local relation optimization by adding a map before
      // creating a dataframe with the test data.
      // This will invoke code gen in the test case.
      sc.range(0, expectedNanos.size).map { i =>
        Tuple1(Timestamp.from(Instant.ofEpochSecond(0, expectedNanos(i.toInt))))
      }.toDF("time")
    }
  }

  def testTimestampToNanos(df: DataFrame): Unit = {
    val actual = df.select(TimestampToNanos(col("time")).as("long"))
      .collect()
      .map(_.getAs[Long]("long"))

    assert(actual === expectedNanos)
  }

  behavior of "NanosToTimestamp"

  it should "retain up to microsecond precision with eval" in
    testNanosToTimestamp {
      expectedNanos.map(Tuple1(_)).toDF("time")
    }

  it should "retain up to microsecond precision with codegen" in
    testNanosToTimestamp {
      sc.range(0, expectedNanos.size).map { i =>
        Tuple1(expectedNanos(i.toInt))
      }.toDF("time")
    }

  def testNanosToTimestamp(df: DataFrame): Unit = {
    val actual = df.select(NanosToTimestamp(col("time")).as("timestamp"))
      .collect()
      .map(_.getAs[Timestamp]("timestamp"))

    val expected = expectedNanos.map { nanos =>
      Timestamp.from(Instant.ofEpochSecond(0, nanos))
    }
    assert(actual === expected)
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

}
