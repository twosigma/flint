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

package com.twosigma.flint.timeseries

import com.twosigma.flint.timeseries.row.Schema
import org.scalatest.FlatSpec
import org.apache.spark.sql.types._

class SchemaSpec extends FlatSpec {
  "Schema" should "create a schema correctly" in {
    val schema = StructType(Array(
      StructField("time", LongType),
      StructField("price", DoubleType)
    ))
    assert(Schema("time" -> LongType, "price" -> DoubleType) == schema)
    assert(Schema("price" -> DoubleType) == schema)
  }

  it should "`of` correctly" in {
    val schema = StructType(Array(
      StructField("foo", LongType),
      StructField("price", DoubleType)
    ))
    assert(Schema.of("foo" -> LongType, "price" -> DoubleType) == schema)
  }

  it should "create a time schema correctly without specifying any column" in {
    val schema = StructType(Array(
      StructField("time", LongType)
    ))
    assert(Schema() == schema)
  }

  it should "throw exception if `time` is not a LongType" in {
    intercept[IllegalArgumentException] {
      Schema("time" -> DoubleType)
    }
  }

  it should "throw an exception if you try to cast the `time` column" in {
    val schema = Schema()

    intercept[IllegalArgumentException] {
      Schema.cast(schema, "time" -> DoubleType)
    }
  }

  it should "throw an exception if you try to cast non-existing columns" in {
    val schema = Schema()

    intercept[IllegalArgumentException] {
      Schema.cast(schema, "foo" -> DoubleType)
    }
  }

  it should "throw an exception if you try to cast non-numeric columns" in {
    val schema = Schema("foo" -> StringType)

    intercept[IllegalArgumentException] {
      Schema.cast(schema, "foo" -> DoubleType)
    }
  }

  it should "cast numeric columns" in {
    val currentSchema = Schema("foo" -> IntegerType, "bar" -> ShortType)
    val newSchema = Schema.cast(currentSchema, "foo" -> DoubleType, "bar" -> IntegerType)

    assert(Schema.of("time" -> LongType, "foo" -> DoubleType, "bar" -> IntegerType) == newSchema)
  }
}
